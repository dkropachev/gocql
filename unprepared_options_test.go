// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/exec"
)

// TestUnpreparedValidatesOptionsPerCall checks that every Unprepared call,
// cql.Command included, validates its exec.Options on each call: an invalid
// option fails the call with a cql.ValidationError before anything is sent,
// and does not stick to the session.
func TestUnpreparedValidatesOptionsPerCall(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	srv := newGridServer(t, peers[0], gridPages(t))
	defer srv.stop(t, peers[0])
	defer func() { _ = s.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bad := exec.Options{Timeout: cql.Some(-time.Second)}
	var dst []byte
	for name, call := range map[string]func(exec.Options) error{
		"UnpreparedExec": func(o exec.Options) error {
			_, err := s.UnpreparedExec(ctx, gridUpdate(cql.OrdinaryStatement), gridKey, o)
			return err
		},
		"Command": func(o exec.Options) error {
			_, err := s.UnpreparedExec(ctx, cql.Command("TRUNCATE ks.tbl"), cql.EmptyParams{}, o)
			return err
		},
		"UnpreparedOne": func(o exec.Options) error {
			_, _, err := s.UnpreparedOne(ctx, gridSelect(), gridKey, &dst, o)
			return err
		},
		"UnpreparedEach": func(o exec.Options) error {
			_, err := s.UnpreparedEach(ctx, gridSelect(), gridKey, &dst, func(*[]byte) error { return nil }, o)
			return err
		},
		"UnpreparedAll": func(o exec.Options) error {
			for _, err := range s.UnpreparedAll(ctx, gridSelect(), gridKey, &dst, o) {
				if err != nil {
					return err
				}
			}
			return nil
		},
	} {
		if _, ok := errors.AsType[*cql.ValidationError](call(bad)); !ok {
			t.Errorf("%s with a negative timeout did not fail validation", name)
		}
	}
	if sent := srv.sent(); len(sent) != 0 {
		t.Fatalf("invalid options sent %d requests", len(sent))
	}
	if _, _, err := s.UnpreparedOne(ctx, gridSelect(), gridKey, &dst, gridOptions); err != nil {
		t.Fatalf("a valid call after invalid ones failed: %v", err)
	}
	if sent := srv.sent(); len(sent) != 1 || sent[0].consistency != cql.Three {
		t.Fatalf("valid call sent %+v", sent)
	}
}

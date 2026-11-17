// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// leasedView returns a view of data that borrows a live lease, and the lease.
func leasedView(t *testing.T, data string) (Bytes, *wire.Lease) {
	t.Helper()
	c, err := wire.NewChain(wire.Borrow([]byte(data)))
	if err != nil {
		t.Fatal(err)
	}
	l := wire.NewLease(c, nil)
	return bytesView(c, wire.RefOf(l)), l
}

func TestViewCopyLive(t *testing.T) {
	b, l := leasedView(t, "hello")
	defer l.Release()
	got, err := b.Copy()
	if err != nil || string(got) != "hello" {
		t.Fatalf("Bytes.Copy = %q, %v", got, err)
	}
	s, err := textView(b).Copy()
	if err != nil || s != "hello" {
		t.Fatalf("Text.Copy = %q, %v", s, err)
	}
	if s := textView(b).String(); s != "hello" {
		t.Fatalf("Text.String = %q", s)
	}
}

func TestViewCopyUnleased(t *testing.T) {
	got, err := BorrowBytes([]byte("x")).Copy()
	if err != nil || string(got) != "x" {
		t.Fatalf("Bytes.Copy = %q, %v", got, err)
	}
}

func TestViewCopyExpired(t *testing.T) {
	b, l := leasedView(t, "hello")
	l.Release()
	if got, err := b.Copy(); !errors.Is(err, ErrReleased) || got != nil {
		t.Fatalf("Bytes.Copy = %q, %v; want ErrReleased", got, err)
	}
	if s, err := textView(b).Copy(); !errors.Is(err, ErrReleased) || s != "" {
		t.Fatalf("Text.Copy = %q, %v; want ErrReleased", s, err)
	}
	for name, f := range map[string]func(){
		"Text.String":      func() { _ = textView(b).String() },
		"Bytes.OwnedBytes": func() { _ = b.OwnedBytes() },
		"Bytes.Clone":      func() { _ = b.Clone() },
	} {
		func() {
			defer func() {
				err, _ := recover().(error)
				if !errors.Is(err, ErrReleased) {
					t.Errorf("%s panicked with %v; want ErrReleased", name, err)
				}
			}()
			f()
		}()
	}
}

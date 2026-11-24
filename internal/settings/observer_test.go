// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import (
	"context"
	"testing"

	"github.com/scylladb/gocql/v2/observe"
	"github.com/scylladb/gocql/v2/routing"
)

type requestOnly struct{}

func (requestOnly) StartRequest(ctx context.Context, _ observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
	return ctx, nil
}

type attemptOnly struct{}

func (attemptOnly) StartAttempt(context.Context, observe.AttemptStart) func(routing.AttemptEvent) {
	return nil
}

type bothHooks struct {
	requestOnly
	attemptOnly
}

func TestNewObserverHooks(t *testing.T) {
	if h, err := NewObserverHooks(nil); h != nil || err != nil {
		t.Fatalf("nil observer: %v, %v", h, err)
	}
	if _, err := NewObserverHooks(struct{}{}); err == nil {
		t.Fatal("an observer without hooks was accepted")
	}
	for _, tc := range []struct {
		name             string
		observer         observe.Observer
		request, attempt bool
	}{
		{"RequestOnly", requestOnly{}, true, false},
		{"AttemptOnly", attemptOnly{}, false, true},
		{"Both", bothHooks{}, true, true},
	} {
		h, err := NewObserverHooks(tc.observer)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if (h.Request != nil) != tc.request || (h.Attempt != nil) != tc.attempt {
			t.Errorf("%s: request hook %t, attempt hook %t", tc.name, h.Request != nil, h.Attempt != nil)
		}
	}
	cfg := DefaultConfig("127.0.0.1")
	cfg.Observer = struct{}{}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate accepted an observer without hooks")
	}
}

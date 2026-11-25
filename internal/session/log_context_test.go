// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"encoding/binary"
	"log/slog"
	"sync"
	"testing"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/observe"
	"github.com/scylladb/gocql/v2/routing"
)

type logCtxKey struct{}

// ctxHandler records the logCtxKey value of each record's context.
type ctxHandler struct {
	mu     sync.Mutex
	values []any
}

func (h *ctxHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *ctxHandler) Handle(ctx context.Context, _ slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.values = append(h.values, ctx.Value(logCtxKey{}))
	return nil
}
func (h *ctxHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *ctxHandler) WithGroup(string) slog.Handler      { return h }

// TestRequestLogContext checks that a hook's panic is logged with the
// request's context, on the direct and the general path, and with the
// context the Request hook returned once it has run.
func TestRequestLogContext(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	handler := new(ctxHandler)
	implCluster.SessionData(s).InternalLogger = slog.New(handler)
	void := binary.BigEndian.AppendUint32(nil, 1)
	go serveLoopback(peers[0], func([]byte) []byte { return void }, nil)
	b := runtimeQuery(t)
	defer b.Close()
	ctx := context.WithValue(context.Background(), logCtxKey{}, "caller")
	panics := observe.Hooks{Attempt: func(context.Context, observe.AttemptStart) func(routing.AttemptEvent) {
		return func(routing.AttemptEvent) { panic("attempt end") }
	}}
	withRequest := observe.Hooks{
		Request: func(ctx context.Context, _ observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
			return context.WithValue(ctx, logCtxKey{}, "request"), func(observe.RequestEvent) { panic("request end") }
		},
		Attempt: panics.Attempt,
	}
	for _, tc := range []struct {
		name    string
		options Options
		want    []any
	}{
		{"Direct", Options{Observer: panics}, []any{"caller"}},
		{"General", Options{Observer: panics, Profile: generalPathProfile()}, []any{"caller"}},
		{"RequestHook", Options{Observer: withRequest}, []any{"request", "request"}},
	} {
		handler.mu.Lock()
		handler.values = nil
		handler.mu.Unlock()
		if _, err := Exec(ctx, s, b, testOptions(tc.options)); err != nil {
			t.Fatal(err)
		}
		handler.mu.Lock()
		got := handler.values
		handler.mu.Unlock()
		if len(got) != len(tc.want) {
			t.Fatalf("%s: logged with %v, want %v", tc.name, got, tc.want)
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Fatalf("%s: logged with %v, want %v", tc.name, got, tc.want)
			}
		}
	}
}

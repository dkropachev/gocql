// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
)

// TestRunPageSize checks that RunPage's page size argument reaches the wire:
// zero keeps the resolved page size, cqltype.Unpaged sends none, any other
// positive value is sent, and a negative one is rejected before a request.
func TestRunPageSize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, _, peers := runtimeTestSession(t, 1)
	defer s.Close()
	defer peers[0].Close()
	requests := make(chan oneShotRequest, 4)
	p := oneShotPrepare(t, s, peers[0], requests)
	q := codecTestMust(p.Query(Options{PageSize: cqltype.Some[int32](25)}))
	defer q.Close()
	for _, tc := range []struct {
		name string
		size int32
		want int32 // -1: no page size on the wire
	}{
		{"Resolved", 0, 25},
		{"Explicit", 7, 7},
		{"Unpaged", cqltype.Unpaged, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := make(chan int32, 1)
			go func() {
				r := <-requests
				got <- r.pageSize
				_, _ = peers[0].Write(typedTestResponse(t, 4, r.stream, runtimeRowsBody(nil, "a")))
			}()
			page, err := q.RunPage(ctx, 1, model.StartPage(), tc.size)
			if err != nil {
				t.Fatal(err)
			}
			if err := page.Close(); err != nil {
				t.Fatal(err)
			}
			if size := <-got; size != tc.want {
				t.Fatalf("page size on the wire %d, want %d", size, tc.want)
			}
		})
	}
	t.Run("Negative", func(t *testing.T) {
		if _, err := q.RunPage(ctx, 1, model.StartPage(), -1); err == nil {
			t.Fatal("negative page size accepted")
		}
		select {
		case r := <-requests:
			t.Fatalf("negative page size sent a request: %+v", r)
		default:
		}
		// The rejected call released the query.
		go func() {
			r := <-requests
			_, _ = peers[0].Write(typedTestResponse(t, 4, r.stream, runtimeRowsBody(nil, "a")))
		}()
		page, err := q.RunPage(ctx, 1, model.StartPage(), 0)
		if err != nil {
			t.Fatal(err)
		}
		_ = page.Close()
	})
}

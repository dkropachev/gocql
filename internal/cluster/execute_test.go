// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/cqlerr"
)

// TestNoConnection checks the error of a request that found no connection:
// its context's error once that has ended, ErrClosed once Close has begun,
// and ErrNoConnections otherwise. Close cancels the executions it finds
// before closing the pools, so one that picks a host after that must not
// report an empty pool.
func TestNoConnection(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	closing := &Core{}
	closing.privateStorage.closedOrClosing.Store(true)
	for name, tc := range map[string]struct {
		s    *Core
		ctx  context.Context
		want error
	}{
		"Open":                  {&Core{}, context.Background(), cqlerr.ErrNoConnections},
		"Closing":               {closing, context.Background(), cqlerr.ErrClosed},
		"Cancelled":             {&Core{}, cancelled, context.Canceled},
		"CancelledWhileClosing": {closing, cancelled, context.Canceled},
	} {
		t.Run(name, func(t *testing.T) {
			if err := tc.s.noConnection(tc.ctx); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

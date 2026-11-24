// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package observe_test

import (
	"context"
	"log"
	"sync/atomic"

	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/observe"
	"github.com/scylladb/gocql/v2/routing"
)

// Two observers built from functions, one counting failed requests and one
// logging retried attempts, installed together with Join.
func ExampleJoin() {
	var failed atomic.Uint64
	failures := observe.RequestObserverFunc(func(ctx context.Context, _ observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
		return ctx, func(e observe.RequestEvent) {
			if e.Err != nil {
				failed.Add(1)
			}
		}
	})
	retries := observe.AttemptObserverFunc(func(_ context.Context, start observe.AttemptStart) func(routing.AttemptEvent) {
		if start.Attempt == 1 {
			return nil
		}
		return func(e routing.AttemptEvent) {
			log.Printf("request %d attempt %d on %v: %v", e.RequestID, e.Attempt, e.Target, e.Err)
		}
	})
	cfg, err := config.New(config.ScyllaV4, "localhost:9042").
		WithObserver(observe.Join(failures, retries)).
		Build()
	if err != nil {
		log.Fatal(err)
	}
	_ = cfg
}

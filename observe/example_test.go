// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package observe_test

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/observe"
	"github.com/scylladb/gocql/v2/routing"
)

// Two sets of hooks, one counting failed requests and one logging retried
// attempts, installed together with Join.
func ExampleJoin() {
	var failed atomic.Uint64
	failures := observe.Hooks{Request: func(ctx context.Context, _ observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
		return ctx, func(e observe.RequestEvent) {
			if e.Err != nil {
				failed.Add(1)
			}
		}
	}}
	retries := observe.Hooks{Attempt: func(_ context.Context, start observe.AttemptStart) func(routing.AttemptEvent) {
		if start.Attempt == 1 {
			return nil
		}
		return func(e routing.AttemptEvent) {
			log.Printf("request %d attempt %d on %s: %v", e.RequestID, e.Attempt, e.Address, e.Err)
		}
	}}
	cfg, err := config.New(config.ScyllaV4, "localhost:9042").
		WithObserver(observe.Join(failures, retries)).
		Build()
	if err != nil {
		log.Fatal(err)
	}
	_ = cfg
}

// observer is the method of a prometheus.Observer the hooks below use. With
// the Prometheus client, histogramFor is a HistogramVec's WithLabelValues:
//
//	latency := prometheus.NewHistogramVec(prometheus.HistogramOpts{
//		Name: "cql_request_duration_seconds",
//	}, []string{"outcome"})
//	histogramFor := func(outcome string) observer { return latency.WithLabelValues(outcome) }
type observer interface{ Observe(float64) }

// Hooks that record each request's latency in a histogram labeled with how
// it ended, and count attempts by host, for an exporter such as Prometheus.
// The driver imports no metrics library; the hooks only call one.
func Example_prometheus() {
	var histogramFor func(outcome string) observer      // latency.WithLabelValues
	var attemptsOn func(host string) interface{ Inc() } // attempts.WithLabelValues
	hooks := observe.Hooks{
		Request: func(ctx context.Context, start observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
			return ctx, func(e observe.RequestEvent) {
				outcome := "ok"
				if e.Err != nil {
					outcome = cqlerr.KindOf(e.Err).String()
				}
				histogramFor(outcome).Observe(time.Since(start.Started).Seconds())
			}
		},
		Attempt: func(_ context.Context, start observe.AttemptStart) func(routing.AttemptEvent) {
			attemptsOn(start.Address).Inc()
			return nil
		},
	}
	cfg, err := config.New(config.ScyllaV4, "localhost:9042").WithObserver(hooks).Build()
	if err != nil {
		log.Fatal(err)
	}
	_ = cfg
}

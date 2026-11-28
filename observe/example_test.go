// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package observe_test

import (
	"context"
	"log"
	"strings"
	"sync/atomic"

	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/observe"
	"github.com/scylladb/gocql/v2/routing"
)

// Two sets of hooks, one counting failed requests and one logging retried
// and speculative attempts, installed together with Join. Attempts are
// numbered within their page, so the first attempt of every page is 1.
func ExampleJoin() {
	var failed atomic.Uint64
	failures := observe.Hooks{Request: func(ctx context.Context, _ observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
		return ctx, func(e observe.RequestEvent) {
			if e.Err != nil {
				failed.Add(1)
			}
		}
	}}
	retries := observe.Hooks{Attempt: func(_ context.Context, start routing.AttemptStart) func(routing.AttemptEvent) {
		if start.Attempt == 1 {
			return nil
		}
		return func(e routing.AttemptEvent) {
			log.Printf("request %d page %d attempt %d on %s: %v", e.RequestID, e.Page, e.Attempt, e.Address, e.Err)
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
// The driver imports no metrics library; the hooks only call one. A request
// that returns rows ends when its rows are closed, so its latency includes
// the time the application spent reading them; the attempts' latencies are
// the cluster's alone.
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
				histogramFor(outcome).Observe(e.Finished.Sub(start.Started).Seconds())
			}
		},
		Attempt: func(_ context.Context, start routing.AttemptStart) func(routing.AttemptEvent) {
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

// A Request hook that logs each statement with its bound values, the column
// named password redacted. The values are borrowed from the request and
// valid until the request's end function returns, so a hook may format them
// in the start hook, as here, or in the end function, but must copy what it
// keeps longer, such as for a goroutine of its own.
func ExampleBoundValues_Format() {
	redact := func(column cql.ColumnSpec) bool { return column.Name == "password" }
	hooks := observe.Hooks{Request: func(ctx context.Context, start observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
		var values strings.Builder
		if err := start.Values.Format(&values, redact); err == nil {
			log.Printf("request %d: %s [%s]", start.ID, start.Statement, values.String())
		}
		return ctx, nil
	}}
	cfg, err := config.New(config.ScyllaV4, "localhost:9042").WithObserver(hooks).Build()
	if err != nil {
		log.Fatal(err)
	}
	_ = cfg
}

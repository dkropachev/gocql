// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"time"

	"log/slog"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/logging"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/retry"
	"github.com/scylladb/gocql/v2/routing"
)

type ConvictionPolicy = implSettings.ConvictionPolicy

type LifecycleEvent = implSettings.LifecycleEvent
type LifecycleObserver = implSettings.LifecycleObserver

// Observer receives completed requests and individual attempts. Implementations
// are shared between executions and must be safe for concurrent use.
type Observer interface {
	ObserveRequest(RequestEvent)
	ObserveAttempt(AttemptEvent)
}

type AttemptEvent = implRouting.AttemptEvent

// Profile supplies reusable execution defaults. Build one with NewProfile.
// Config and query construction snapshot its fields; policy objects stay shared.
type Profile struct {
	Retry                retry.Policy
	Routing              routing.Policy
	Speculation          SpeculationPolicy
	Timeout, PageTimeout time.Duration
	Prefetch             int
	// PageSize is the rows per page; cql.Unpaged requests the whole result
	// in one response.
	PageSize          int32
	SerialConsistency cql.Option[cql.Consistency]
	Consistency       cql.Consistency
}
type ReconnectPolicy = implSettings.ReconnectPolicy
type RequestEvent = implSettings.RequestEvent

type SimpleConvictionPolicy = implSettings.SimpleConvictionPolicy

// SpeculationPolicy schedules additional attempts of eligible executions.
type SpeculationPolicy interface {
	Delay(attempt int, metrics routing.MetricsSnapshot) (time.Duration, bool)
}

// PrintfLogger is satisfied by *log.Logger and most printf-style loggers.
type PrintfLogger = logging.Printfer

// NewPrintfLogger adapts a printf-style logger for WithLogger. Each record at or above level
// becomes one logfmt line passed to l.Printf.
func NewPrintfLogger(l PrintfLogger, level slog.Leveler) *slog.Logger {
	return slog.New(logging.NewPrintfHandler(l, level))
}

func internalProfile(p Profile) implSettings.Profile {
	return implSettings.Profile{Retry: p.Retry, Routing: p.Routing, Speculation: p.Speculation,
		Timeout: p.Timeout, PageTimeout: p.PageTimeout, Prefetch: p.Prefetch,
		PageSize: p.PageSize, SerialConsistency: p.SerialConsistency, Consistency: p.Consistency}
}

func publicProfile(p implSettings.Profile) Profile {
	return Profile{Retry: p.Retry, Routing: p.Routing, Speculation: p.Speculation,
		Timeout: p.Timeout, PageTimeout: p.PageTimeout, Prefetch: p.Prefetch,
		PageSize: p.PageSize, SerialConsistency: p.SerialConsistency, Consistency: p.Consistency}
}

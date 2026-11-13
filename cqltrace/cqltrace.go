// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package cqltrace reads CQL trace sessions: the server-side query traces a
// cluster records in system_traces for executions run with
// exec.Options.CQLTrace. It is unrelated to client-side tracing such as
// OpenTelemetry, which instruments the driver through config.Observer.
package cqltrace

import implCluster "github.com/scylladb/gocql/v2/internal/cluster"

// Info is the trace session of one traced execution.
type Info = implCluster.TraceInfo

// Event is one activity a node recorded in a trace session.
type Event = implCluster.TraceEvent

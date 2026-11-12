// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package trace holds the query traces gocql.Session.Trace reads back.
package trace

import implCluster "github.com/scylladb/gocql/v2/internal/cluster"

// Info is the trace session of one traced execution.
type Info = implCluster.TraceInfo

// Event is one activity a node recorded in a trace session.
type Event = implCluster.TraceEvent

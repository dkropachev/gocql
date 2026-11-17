// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/metadata"
)

// Info describes a request for routing: what it reads or writes, and any
// host or shard it must go to.
type Info struct {
	// Keyspace and Table name the statement's table, when known.
	Keyspace, Table string
	// Token is the token of the request's partition, when known.
	Token cql.Option[int64]
	// Kind and Conditional classify the statement.
	Kind        cql.StatementKind
	Conditional bool
	// Consistency is the request's consistency level.
	Consistency cql.Consistency
	// HostID and ShardID pin the request to a host and shard when set.
	HostID  cql.Option[cql.UUID]
	ShardID cql.Option[uint32]
}

// AttemptStart describes an attempt as it is sent to its target.
type AttemptStart struct {
	Started     time.Time
	RequestID   uint64
	Attempt     int
	Target      metadata.Target
	Speculative bool
}

// AttemptEvent describes how an attempt ended.
type AttemptEvent struct {
	Started   time.Time
	Err       error
	RequestID uint64
	Attempt   int
	Latency   time.Duration
	Target    metadata.Target
	// CQLTraceID is the CQL trace session the attempt's response named, when
	// the execution ran with CQL tracing and a response arrived.
	CQLTraceID             cql.Option[cql.UUID]
	Cancelled, Speculative bool
}

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
//
// The driver fills it; its zero value describes a request with no keyspace,
// token or target.
type Info struct {
	// Keyspace and Table name the statement's table, when known.
	Keyspace, Table string
	// Token is the token of the request's partition, when known.
	Token cql.Option[int64]
	// Kind and Conditional classify the statement.
	Kind cql.StatementKind
	// Conditional reports that the request is routed and retried as a
	// conditional statement: Kind is not OrdinaryStatement, or its
	// consistency is serial.
	Conditional bool
	// Consistency is the request's consistency level.
	Consistency cql.Consistency
	// HostID and ShardID pin the request to a host and shard when set.
	HostID cql.Option[cql.UUID]
	// ShardID is the shard the request is pinned to, when set; it requires
	// HostID.
	ShardID cql.Option[uint32]
}

// AttemptStart describes an attempt as it is sent to its target.
//
// The driver fills it; its zero value is empty.
type AttemptStart struct {
	// Started is when the attempt was sent.
	Started time.Time
	// RequestID is the ID of the attempt's request.
	RequestID uint64
	// Attempt is the 1-based number of the attempt within its request.
	Attempt int
	// Target is the host, and shard, the attempt was sent to.
	Target metadata.Target
	// Speculative reports a speculative attempt.
	Speculative bool
}

// AttemptEvent describes how an attempt ended.
//
// The driver fills it; its zero value is empty.
type AttemptEvent struct {
	// Started is when the attempt was sent.
	Started time.Time
	// Err is the attempt's error, or nil when it succeeded.
	Err error
	// RequestID is the ID of the attempt's request.
	RequestID uint64
	// Attempt is the 1-based number of the attempt within its request.
	Attempt int
	// Latency is how long the attempt took.
	Latency time.Duration
	// Target is the host, and shard, the attempt was sent to.
	Target metadata.Target
	// CQLTraceID is the CQL trace session the attempt's response named, when
	// the execution ran with CQL tracing and a response arrived.
	CQLTraceID cql.Option[cql.UUID]
	// Cancelled reports an attempt that ended with its context, and
	// Speculative a speculative attempt.
	Cancelled, Speculative bool
}

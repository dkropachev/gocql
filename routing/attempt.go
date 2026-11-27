// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
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
	// ShardID is the shard the request is pinned to, when set; it requires
	// HostID.
	ShardID cql.Option[uint32]
	// Consistency is the request's consistency level.
	Consistency cql.Consistency
	// Kind classifies the statement, or a batch's statements together. Any
	// Kind but OrdinaryStatement is routed as possibly conditional, and so is
	// a request with SERIAL or LOCAL_SERIAL consistency.
	Kind cql.StatementKind
	// HostID pins the request to a host, when set.
	HostID cql.Option[cql.UUID]
	// AnyShardFallback lets a request pinned to ShardID use another shard of
	// HostID when ShardID has no connection or the host has no such shard,
	// as exec.Options.AnyShardFallback sets it; without it the request stays
	// on ShardID.
	AnyShardFallback bool
}

// AttemptEvent describes how an attempt ended.
//
// The driver fills it; its zero value is empty.
type AttemptEvent struct {
	// Started is when the attempt was sent.
	Started time.Time
	// Err is the attempt's error, or nil when it succeeded.
	Err error
	// Address is the host:port of the connection the attempt was sent on,
	// or the socket path of a Unix socket connection, as in
	// cqlerr.AttemptFailure.
	Address string
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
	// Consistency is the consistency level the attempt used.
	Consistency cql.Consistency
	// ExecutionStatus reports what the attempt may have done on the
	// cluster: cqlerr.Executed when it succeeded, otherwise what
	// cqlerr.AttemptFailure.ExecutionStatus reports for it.
	ExecutionStatus cqlerr.ExecutionStatus
	// Canceled reports an attempt whose error is cqlerr.KindCanceled: the
	// caller's context was canceled or its deadline passed, or the driver
	// canceled the attempt, as when another attempt of the request won.
	// TimedOut reports one whose error is cqlerr.KindTimeout: the server or
	// the driver gave up waiting. Speculative reports a speculative attempt.
	Canceled, TimedOut, Speculative bool
}

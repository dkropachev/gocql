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
	// Kind classifies the statement, or a batch's statements together. Any
	// Kind but OrdinaryStatement is routed as possibly conditional, and so is
	// a request with SERIAL or LOCAL_SERIAL consistency.
	Kind cql.StatementKind
	// Consistency is the request's consistency level.
	Consistency cql.Consistency
	// HostID pins the request to a host, when set.
	HostID cql.Option[cql.UUID]
	// ShardID is the shard the request is pinned to, when set; it requires
	// HostID.
	ShardID cql.Option[uint32]
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
	// Canceled reports an attempt whose error is cqlerr.KindCanceled: the
	// caller canceled the request, or the driver canceled the attempt, as
	// when another attempt of the request won. TimedOut reports one whose
	// error is cqlerr.KindTimeout: the server or the driver gave up
	// waiting, or the caller's deadline passed. Speculative reports a
	// speculative attempt.
	Canceled, TimedOut, Speculative bool
}

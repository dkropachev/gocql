// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"

	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implSession "github.com/scylladb/gocql/v2/internal/session"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

// Batch collects statements, each with its values, to send as one CQL
// BATCH request. Prepared.AddToBatch, Query.AddToBatch and
// Session.UnpreparedAddToBatch append a statement; Session.ExecBatch sends
// the batch, and Session.RunConditionalBatch sends a conditional one and
// returns its rows. Reset empties it for reuse. Once a batch has grown to its
// working size, appending allocates nothing. A Batch is not safe for
// concurrent use and must not be copied.
//
// Every member must come from the session that sends the batch; members may
// write tables of different keyspaces. The batch routes by its first member
// whose partition key is known, which needs a prepared member; a batch of
// unprepared members only has no replica to route to. A batch holds at most
// MaxBatchStatements statements.
//
// The exported field Kind cql.BatchKind selects the batch's kind and is sent
// with every execution: cql.LoggedBatch (the zero value),
// cql.UnloggedBatch or cql.CounterBatch. It belongs to the batch rather than
// to each send because the contents decide it: a counter batch holds only
// counter updates, and an unlogged batch is chosen because its members share
// a partition. Set it once when building:
//
//	batch := gocql.Batch{Kind: cql.UnloggedBatch}
//
// The zero Batch is a logged batch, ready to use.
type Batch implSession.Batch

// StopIteration may be returned by an Each callback to stop successfully.
// A single-error wrapper has the same effect; joined errors remain failures.
// The execution is recorded as successful and all response storage is released.
// Other callback errors are returned unchanged. Borrowed callback fields still
// expire when the callback returns.
var StopIteration = implSession.StopIteration

// Len returns the number of statements in the batch.
func (b *Batch) Len() int { return (*implSession.Batch)(b).Len() }

// Size returns the bytes of the encoded values of the batch's statements,
// each value's length prefix included. Statement texts and prepared IDs are
// not counted. The server's batch_size_warn_threshold and
// batch_size_fail_threshold measure the mutations the batch makes, which
// follow from these values but also count per-cell overhead and values
// written as CQL literals, so Size is an estimate a caller can use to split a
// batch well below the threshold.
func (b *Batch) Size() int { return (*implSession.Batch)(b).Size() }

// Reset clears statements while preserving the batch's reusable capacity and
// its Kind.
func (b *Batch) Reset() { (*implSession.Batch)(b).Reset() }

// MaxBatchStatements is the most statements one Batch can hold: the native
// protocol counts a BATCH request's statements in an unsigned 16-bit field.
// AddToBatch rejects the statement after it.
const MaxBatchStatements = implCluster.MaxBatchStatements

// Connect opens a session with cfg, which must come from
// config.ConfigBuilder.Build. The config's dialect fixes the protocol version
// and the database: Connect fails when the cluster does not speak that
// version, or is not that database (cqlerr.ServerMismatchError). The zero
// Config is rejected with a *cql.ValidationError.
func Connect(ctx context.Context, cfg config.Config) (*Session, error) {
	settings, err := implSettings.InternalResolvePublic(cfg)
	if err != nil {
		return nil, err
	}
	session, err := implSession.Connect(ctx, settings)
	return (*Session)(session), err
}

// PreparedInfo describes a prepared statement.
//
// The driver fills it; its zero value is empty.
type PreparedInfo struct {
	// Keyspace and Table name the statement's table, when the server
	// reported it.
	Keyspace, Table string
	// PartitionKeyIndexes are the positions of the bind markers that hold the
	// partition key, in key order; the session routes with them.
	PartitionKeyIndexes []int
	// Kind classifies the statement: the statement's own Kind, an
	// UnknownStatement resolved as cql.UnknownStatement describes, except
	// that it is ConditionalStatement when the server flagged a lightweight
	// transaction while preparing it, as ScyllaDB does.
	Kind cql.StatementKind
	// Generation counts the statement's re-preparations after schema changes.
	Generation uint64
}

// StreamInfo reports delivered rows, pages and execution progress.
//
// The driver fills it; its zero value is empty.
type StreamInfo struct {
	// Result describes the execution so far.
	Result cql.Result
	// DeliveredRows and Pages count the rows NextInto delivered and the pages
	// they came from.
	DeliveredRows, Pages int64
	// OutputStarted reports that a row has been written to the caller's
	// storage.
	OutputStarted bool
}

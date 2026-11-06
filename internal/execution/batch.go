// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"bytes"
	"context"
	"fmt"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// maxBatchEntries is the protocol's limit on the statements of one BATCH.
const maxBatchEntries = 65535

// Batch collects statements, each with its values, to send as one CQL
// BATCH request. Query.AddTo appends a statement; Query.ExecBatch and
// Query.RunBatch send the batch. Statements of several queries may share a
// batch when the queries belong to one session.
//
// Appending copies the encoded values into storage the batch owns and reuses:
// once a batch has grown to its working size, appending and Reset allocate
// nothing. The zero Batch is empty and ready to use. A Batch is not safe for
// concurrent use and must not be copied. It may be sent any number of times;
// Reset empties it for the next batch.
type Batch struct {
	entries []batchEntry
	// values holds the encoded values of every entry back to back.
	values  []byte
	session *implCluster.Session
	routing RoutingInfo
	// state is the execution state lent to executions with owner.
	state InternalBatchState
	// owner is the reusable owner of the latest execution that used it. An
	// execution starting before it drained gets an owner of its own.
	owner     InternalInputOwner
	ownerUsed bool
	// shared reports that an execution not tracked by owner may still read
	// entries or values: Reset must abandon them instead of reusing them.
	shared bool
}

// batchEntry is one statement of a batch. Its encoded values are
// values[start:end] of the batch.
type batchEntry struct {
	source     statementSource
	statement  string
	columns    Columns
	start, end int
	count      int
	unset      bool // a value is unset, which requires protocol v4
}

// InternalBatchState is the batch an execution sends: a snapshot of the
// Batch's entries and values when the execution started.
type InternalBatchState struct {
	entries []batchEntry
	values  []byte
	kind    BatchKind
}

// Len returns the number of statements in the batch.
func (b *Batch) Len() int {
	if b == nil {
		return 0
	}
	return len(b.entries)
}

// Reset empties the batch, keeping its storage for the next statements. If
// an execution of the batch may still be writing it, which happens after its
// context was cancelled, Reset leaves the storage to that execution instead.
func (b *Batch) Reset() {
	if b.shared || (b.ownerUsed && !b.owner.drained.Load()) {
		b.entries, b.values = nil, nil
	} else {
		clear(b.entries)
		b.entries, b.values = b.entries[:0], b.values[:0]
	}
	b.session, b.routing, b.shared = nil, RoutingInfo{}, false
}

// add appends one statement whose encoded values are values. request carries
// the statement, its source and its routing.
func (b *Batch) add(s *implCluster.Session, request *InternalExecutionRequest) error {
	if b == nil {
		return typeValidation("batch", "nil batch")
	}
	if len(b.entries) == maxBatchEntries {
		return typeValidation("batch", "requires 1 to 65535 entries")
	}
	if b.session != nil && b.session != s {
		return typeValidation("batch session", "statements belong to different sessions")
	}
	r := &request.InternalRouting
	if r.Keyspace != "" && b.routing.Keyspace != "" && r.Keyspace != b.routing.Keyspace {
		return typeValidation("batch keyspace", "members use different keyspaces")
	}
	values := request.InternalValues
	start := len(b.values)
	n := int(values.Framed.Len())
	b.values = append(b.values, make([]byte, n)...)
	if _, err := values.Framed.CopyTo(b.values[start:]); err != nil {
		b.values = b.values[:start]
		return err
	}
	unset := false
	for i := range values.Cells {
		if values.Cells[i].State == Unset {
			unset = true
			break
		}
	}
	b.entries = append(b.entries, batchEntry{
		source:    request.InternalSource,
		statement: request.InternalStatement,
		columns:   values.Columns,
		start:     start,
		end:       start + n,
		count:     len(values.Cells),
		unset:     unset,
	})
	if len(b.entries) == 1 {
		b.routing.Kind = OrdinaryStatement
	}
	b.session = s
	if r.Keyspace != "" {
		b.routing.Keyspace = r.Keyspace
	}
	if !b.routing.Token.Set && r.Token.Set {
		b.routing.Token, b.routing.Table = r.Token, r.Table
	}
	switch {
	case r.Kind == ConditionalStatement:
		b.routing.Kind = ConditionalStatement
	case r.Kind == UnknownStatement && b.routing.Kind != ConditionalStatement:
		b.routing.Kind = UnknownStatement
	}
	return nil
}

// InternalAddBound appends bound's statement and values to batch.
func InternalAddBound[R any](batch *Batch, s *implCluster.Session, bound *Bound[R]) error {
	if bound == nil || bound.privateStorage.InternalRequest.InternalOwner == nil {
		return typeValidation("batch item", "nil binding")
	}
	request := &bound.privateStorage.InternalRequest
	if state := request.InternalPrepared(); state != nil && state.InternalSession != s {
		return typeValidation("batch session", "prepared statement belongs to another session")
	}
	if err := request.InternalOwner.InternalAdmit(); err != nil {
		return err
	}
	defer request.InternalOwner.InternalRelease()
	return batch.add(s, request)
}

// runBatch sends batch on s and returns its first and only page.
func runBatch[R any](ctx context.Context, s *implCluster.Session, kind BatchKind, batch *Batch, decoder RowDecoder[R], options Options) (*Page[R], error) {
	if batch == nil {
		return nil, typeValidation("batch", "nil batch")
	}
	if options.values().PageSize.Set {
		return nil, &UnsupportedSettingError{Setting: "batch page size", Reason: "native BATCH responses do not support paging"}
	}
	if v := options.values(); v.Prefetch.Set && v.Prefetch.Value != 0 {
		return nil, &UnsupportedSettingError{Setting: "batch prefetch", Reason: "native BATCH responses do not support paging"}
	}
	if len(batch.entries) == 0 {
		return nil, typeValidation("batch", "requires 1 to 65535 entries")
	}
	if kind != LoggedBatch && kind != UnloggedBatch && kind != CounterBatch {
		return nil, typeValidation("batch kind", "unknown kind")
	}
	if decoder == nil {
		return nil, typeValidation("batch rows", "nil decoder")
	}
	if batch.session != s {
		return nil, typeValidation("batch session", "statements belong to another session")
	}
	// The happy path reuses the batch's owner and state: the previous
	// execution drained before returning. One that was cancelled may still
	// be writing, so this execution gets its own and Reset must not reuse
	// the storage that one reads.
	owner, state := &batch.owner, &batch.state
	if batch.ownerUsed && !owner.drained.Load() {
		owner, state = new(InternalInputOwner), new(InternalBatchState)
		batch.shared = true
	} else {
		batch.ownerUsed = true
	}
	owner.resetOpen()
	*state = InternalBatchState{kind: kind, entries: batch.entries, values: batch.values}
	routing := batch.routing
	routing.Conditional = routing.Kind != OrdinaryStatement
	bound := new(Bound[R])
	bound.privateStorage.InternalRequest = InternalExecutionRequest{InternalOwner: owner, InternalRouting: routing, batch: state}
	bound.privateStorage.InternalRows = decoder
	page, err := RunPage(ctx, s, bound, StartPage(), options)
	owner.InternalClose()
	return page, err
}

// execBatch sends batch on s and drains its result, which must have no
// columns.
func execBatch(ctx context.Context, s *implCluster.Session, kind BatchKind, batch *Batch, options Options) (Result, error) {
	page, err := runBatch(ctx, s, kind, batch, InternalExecRowsDecoder{}, options)
	if err != nil {
		return InternalExecutionErrorResult(err), err
	}
	defer page.Close()
	var row EmptyRecord
	for page.NextInto(&row) {
	}
	if page.Err() == nil && page.Columns().Len() > 0 {
		page.privateStorage.err = typeValidation("ExecBatch", "result columns require RunBatch")
	}
	return page.Info(), page.Err()
}

// InternalRunBatch sends batch on s; tests use it to run batches built from
// bindings.
func InternalRunBatch[R any](ctx context.Context, s *implCluster.Session, kind BatchKind, batch *Batch, decoder RowDecoder[R], options Options) (*Page[R], error) {
	return runBatch(ctx, s, kind, batch, decoder, options)
}

// InternalExecBatch sends batch on s and drains its result.
func InternalExecBatch(ctx context.Context, s *implCluster.Session, kind BatchKind, batch *Batch, options Options) (Result, error) {
	return execBatch(ctx, s, kind, batch, options)
}

func (e *batchEntry) sourceOrText() statementSource {
	if e.source == nil {
		return cqlText{}
	}
	return e.source
}

func (e *batchEntry) encoded() encodedValues {
	return encodedValues{Columns: e.columns}
}

func (s *InternalSessionRuntime) prepareBatchAttempt(ctx context.Context, conn *Conn, batch *InternalBatchState) (*typedBatchRequest, error) {
	request := &typedBatchRequest{Kind: batch.kind, Items: make([]typedBatchItem, len(batch.entries))}
	for i := range batch.entries {
		entry := &batch.entries[i]
		if entry.unset && conn.InternalVersion < 4 {
			return nil, typeValidation("unset", "requires protocol v4")
		}
		item := typedBatchItem{Statement: entry.statement, Values: wire.ChainOf(batch.values[entry.start:entry.end]), ValueCount: entry.count}
		prepared, err := entry.sourceOrText().onConn(ctx, s, conn, entry.encoded(), false, nil)
		if err != nil {
			return nil, err
		}
		if prepared != nil {
			item.PreparedID = prepared.InternalId
		}
		request.Items[i] = item
	}
	return request, nil
}

func (s *InternalSessionRuntime) repairBatch(ctx context.Context, conn *Conn, batch *InternalBatchState, id []byte) error {
	for i := range batch.entries {
		entry := &batch.entries[i]
		state := entry.sourceOrText().prepared()
		if state == nil {
			continue
		}
		state.InternalMu.RLock()
		cached := state.InternalHosts[conn.InternalHostID()]
		state.InternalMu.RUnlock()
		if cached != nil && bytes.Equal(cached.InternalId, id) {
			_, err := entry.sourceOrText().onConn(ctx, s, conn, entry.encoded(), true, nil)
			return err
		}
	}
	return &ProtocolError{Stage: "batch unprepared", Err: fmt.Errorf("unknown statement ID")}
}

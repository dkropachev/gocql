// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// maxBatchEntries is the protocol's limit on the statements of one BATCH.
const maxBatchEntries = 65535

// Batch collects statements, each with its values, to send as one CQL
// BATCH request. Query.AddToBatch appends a statement; Query.ExecBatch and
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
	// The latest execution: one sent through a query is tracked by the
	// query's generation, one sent without by its owner.
	lastQuery *queryState
	lastOwner *InternalInputOwner
	// state is the execution state lent to an execution while no earlier
	// one may still read it.
	state   InternalBatchState
	routing RoutingInfo
	// exec is the request a query's ExecBatch admits; admission copies it.
	exec    Bound[EmptyRecord]
	lastGen uint64
	// shared reports that an execution no longer tracked may still read
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
	// attempt is the wire batch an attempt lends while attemptBusy is set;
	// a concurrent speculative attempt builds its own.
	attempt     typedBatchRequest
	attemptBusy atomic.Bool
	kind        BatchKind
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
	if b.shared || !b.drained() {
		b.entries, b.values = nil, nil
	} else {
		clear(b.entries)
		b.entries, b.values = b.entries[:0], b.values[:0]
	}
	b.session, b.routing, b.shared = nil, RoutingInfo{}, false
}

// drained reports whether the latest execution of the batch, if any, has
// stopped reading it.
func (b *Batch) drained() bool {
	if b.lastOwner != nil && !b.lastOwner.drained.Load() {
		return false
	}
	if q := b.lastQuery; q != nil && q.generation.Load() == b.lastGen {
		q.mu.Lock()
		busy := q.busy
		q.mu.Unlock()
		return !busy
	}
	return true
}

// begin returns the state for an execution of kind. While an earlier
// execution may still read the batch's state, this one gets its own, and
// the batch's storage is left to the earlier one at the next Reset.
func (b *Batch) begin(kind BatchKind) *InternalBatchState {
	state := &b.state
	if !b.drained() {
		state = new(InternalBatchState)
		b.shared = true
	}
	// The wire batch's item storage is kept for the next execution.
	state.kind, state.entries, state.values = kind, b.entries, b.values
	return state
}

// request returns the batch's request routing.
func (b *Batch) requestRouting() RoutingInfo {
	routing := b.routing
	routing.Conditional = routing.Kind != OrdinaryStatement
	return routing
}

// validate checks that batch can be sent as kind with options on s.
func validateBatch(s *implCluster.Session, kind BatchKind, batch *Batch, options Options) error {
	if batch == nil {
		return typeValidation("batch", "nil batch")
	}
	if options.values().PageSize.Set {
		return &UnsupportedSettingError{Setting: "batch page size", Reason: "native BATCH responses do not support paging"}
	}
	if v := options.values(); v.Prefetch.Set && v.Prefetch.Value != 0 {
		return &UnsupportedSettingError{Setting: "batch prefetch", Reason: "native BATCH responses do not support paging"}
	}
	if len(batch.entries) == 0 {
		return typeValidation("batch", "requires 1 to 65535 entries")
	}
	if kind != LoggedBatch && kind != UnloggedBatch && kind != CounterBatch {
		return typeValidation("batch kind", "unknown kind")
	}
	if batch.session != s {
		return typeValidation("batch session", "statements belong to another session")
	}
	return nil
}

// add appends one statement whose encoded values are values. request carries
// the statement, its source and its routing.
func (b *Batch) add(s *implCluster.Session, source statementSource, statement string, r *RoutingInfo, values encodedValues) error {
	if b == nil {
		return typeValidation("batch", "nil batch")
	}
	if len(b.entries) == maxBatchEntries {
		return typeValidation("batch", "requires 1 to 65535 entries")
	}
	if b.session != nil && b.session != s {
		return typeValidation("batch session", "statements belong to different sessions")
	}
	if r.Keyspace != "" && b.routing.Keyspace != "" && r.Keyspace != b.routing.Keyspace {
		return typeValidation("batch keyspace", "members use different keyspaces")
	}
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
		source:    source,
		statement: statement,
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
	return batch.add(s, request.InternalSource, request.InternalStatement, &request.InternalRouting, request.InternalValues)
}

// runBatch sends batch on s without a query and returns its first and
// only page.
func runBatch[R any](ctx context.Context, s *implCluster.Session, kind BatchKind, batch *Batch, decoder RowDecoder[R], options Options) (*Page[R], error) {
	if err := validateBatch(s, kind, batch, options); err != nil {
		return nil, err
	}
	if decoder == nil {
		return nil, typeValidation("batch rows", "nil decoder")
	}
	state := batch.begin(kind)
	owner := newInputOwner()
	batch.lastOwner, batch.lastQuery = owner, nil
	bound := new(Bound[R])
	bound.privateStorage.InternalRequest = InternalExecutionRequest{InternalOwner: owner, InternalRouting: batch.requestRouting(), batch: state}
	bound.privateStorage.InternalRows = decoder
	page, err := RunPage(ctx, s, bound, StartPage(), options)
	owner.InternalClose()
	return page, err
}

// queryExec prepares the batch's request for an execution on query q,
// which began its own execution generation for it. Admission copies the
// request into the query's inline request.
func (b *Batch) queryExec(dst *InternalExecutionRequest, q *queryState, kind BatchKind) {
	state := b.begin(kind)
	b.lastQuery, b.lastGen, b.lastOwner = q, q.generation.Load(), nil
	*dst = InternalExecutionRequest{InternalOwner: q.owner, query: q, InternalRouting: b.requestRouting(), batch: state}
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

// prepareBatchAttempt fills the wire batch of one attempt on conn. It
// refills request when given one, and otherwise takes the state's own wire
// batch unless another attempt holds it; the caller releases that with
// releaseAttempt.
func (s *InternalSessionRuntime) prepareBatchAttempt(ctx context.Context, conn *Conn, batch *InternalBatchState, request *typedBatchRequest) (*typedBatchRequest, error) {
	if request == nil {
		if batch.attemptBusy.CompareAndSwap(false, true) {
			request = &batch.attempt
		} else {
			request = new(typedBatchRequest)
		}
	}
	request.Kind = batch.kind
	items := request.Items[:0]
	if cap(items) < len(batch.entries) {
		items = make([]typedBatchItem, 0, len(batch.entries))
	}
	for i := range batch.entries {
		entry := &batch.entries[i]
		if entry.unset && conn.InternalVersion < 4 {
			return request, typeValidation("unset", "requires protocol v4")
		}
		item := typedBatchItem{Statement: entry.statement, Values: wire.ChainOf(batch.values[entry.start:entry.end]), ValueCount: entry.count}
		prepared, err := entry.sourceOrText().onConn(ctx, s, conn, entry.encoded(), false, nil)
		if err != nil {
			return request, err
		}
		if prepared != nil {
			item.PreparedID = prepared.InternalId
		}
		items = append(items, item)
	}
	request.Items = items
	return request, nil
}

// releaseAttempt returns the state's wire batch if request is it.
func (batch *InternalBatchState) releaseAttempt(request *typedBatchRequest) {
	if request == &batch.attempt {
		batch.attemptBusy.Store(false)
	}
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

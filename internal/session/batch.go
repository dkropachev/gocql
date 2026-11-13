// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/binding"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// Batch collects statements, each with its values, to send as one CQL
// BATCH request. Prepared.AddToBatch and Query.AddToBatch append a
// statement; Session.ExecBatch and Session.RunBatch send the batch.
// Statements of several prepared statements and queries may share a batch
// when they belong to one session.
//
// Appending copies the encoded values into storage the batch owns and reuses:
// once a batch has grown to its working size, appending and Reset allocate
// nothing. The zero Batch is empty, logged and ready to use. A Batch is not
// safe for concurrent use and must not be copied. It may be sent any number of
// times; Reset empties it for the next batch and keeps its Kind.
type Batch struct {
	entries []batchEntry
	// values holds the encoded values of every entry back to back.
	values  []byte
	session *implCluster.Core
	// params encodes AddToBatch values straight into values.
	params binding.ParameterBuffer
	// The latest execution: one sent through a query is tracked by the
	// query's generation, one sent without by its owner.
	lastQuery *queryState
	lastOwner *inputOwner
	// rows is the *batchRows[R] of the latest RunBatch, reused by the next
	// RunBatch with the same R once its rows are closed.
	rows any
	// state is the execution state lent to an execution while no earlier
	// one may still read it.
	state   batchState
	routing model.RoutingInfo
	// exec is the request Session.ExecBatch admits; admission copies it.
	exec    Bound[codec.EmptyRecord]
	lastGen uint64
	// shared reports that an execution no longer tracked may still read
	// entries or values: Reset must abandon them instead of reusing them.
	shared bool
	// Kind is the batch's kind, sent with every execution of the batch. The
	// zero value is LoggedBatch.
	Kind cqltype.BatchKind
}

// batchEntry is one statement of a batch. Its encoded values are
// values[start:end] of the batch.
type batchEntry struct {
	source     statementSource
	statement  string
	columns    cqltype.Columns
	start, end int
	count      int
	// kind is the statement's classification when it joined the batch.
	kind cqltype.StatementKind
}

// batchState is the batch an execution sends: a snapshot of the
// Batch's entries and values when the execution started.
type batchState struct {
	entries []batchEntry
	values  []byte
	// attempt is the wire batch an attempt lends while attemptBusy is set;
	// a concurrent speculative attempt builds its own.
	attempt     model.TypedBatchRequest
	attemptBusy atomic.Bool
	kind        cqltype.BatchKind
}

// Len returns the number of statements in the batch.
func (b *Batch) Len() int {
	if b == nil {
		return 0
	}
	return len(b.entries)
}

// Reset empties the batch, keeping its storage and Kind for the next
// statements. If
// an execution of the batch may still be writing it, which happens after its
// context was cancelled, Reset leaves the storage to that execution instead.
func (b *Batch) Reset() {
	if b.shared || !b.drained() {
		b.entries, b.values = nil, nil
	} else {
		clear(b.entries)
		b.entries, b.values = b.entries[:0], b.values[:0]
	}
	b.session, b.routing, b.shared = nil, model.RoutingInfo{}, false
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

// begin returns the state for an execution of the batch. While an earlier
// execution may still read the batch's state, this one gets its own, and
// the batch's storage is left to the earlier one at the next Reset.
func (b *Batch) begin() *batchState {
	state := &b.state
	if !b.drained() {
		state = new(batchState)
		b.shared = true
	}
	// The wire batch's item storage is kept for the next execution.
	state.kind, state.entries, state.values = b.Kind, b.entries, b.values
	return state
}

// request returns the batch's request routing.
func (b *Batch) requestRouting() model.RoutingInfo {
	routing := b.routing
	routing.Conditional = routing.Kind != cqltype.OrdinaryStatement
	return routing
}

// validate checks that batch can be sent on s. A BATCH response is one page
// with no continuation, so paging options do not apply to it and are
// ignored: the options compiled for statements serve batches too.
func validateBatch(s *implCluster.Core, batch *Batch) error {
	if batch == nil {
		return cqltype.TypeValidation("batch", "nil batch")
	}
	if len(batch.entries) == 0 {
		return cqltype.TypeValidation("batch", "requires 1 to 65535 entries")
	}
	if kind := batch.Kind; kind != cqltype.LoggedBatch && kind != cqltype.UnloggedBatch && kind != cqltype.CounterBatch {
		return cqltype.TypeValidation("batch kind", "unknown kind")
	}
	if batch.session != s {
		return cqltype.TypeValidation("batch session", "statements belong to another session")
	}
	return nil
}

// add appends one statement whose encoded values are values, copying them
// into the batch. r is the statement's routing.
func (b *Batch) add(s *implCluster.Core, source statementSource, statement string, r *model.RoutingInfo, values model.EncodedValues) error {
	if err := b.admit(s, r); err != nil {
		return err
	}
	start := len(b.values)
	n := int(values.Framed.Len())
	b.values = append(b.values, make([]byte, n)...)
	if _, err := values.Framed.CopyTo(b.values[start:]); err != nil {
		b.values = b.values[:start]
		return err
	}
	b.appendEntry(s, source, statement, r, values, start)
	return nil
}

// addValue encodes value with plan straight into the batch's storage and
// appends the statement. template is the statement's routing; the token is
// computed from the encoded values. b.params keeps viewing the entry's bytes
// until the next add; nothing reads them through it.
func (b *Batch) addValue[P any](s *implCluster.Core, source statementSource, statement string, template *model.RoutingInfo, plan codec.ParamPlan[P], columns cqltype.Columns, value P, allowUnset bool) error {
	if err := b.admit(s, template); err != nil {
		return err
	}
	start := len(b.values)
	storage, values, err := b.params.EncodeAppend(plan, columns, value, allowUnset, b.values)
	if err != nil {
		return err
	}
	r := *template
	r.Token, err = source.token(values.Cells)
	if err == nil {
		b.values = storage
		b.appendEntry(s, source, statement, &r, values, start)
	}
	return err
}

// admit checks that a statement of session s with routing r may join b.
func (b *Batch) admit(s *implCluster.Core, r *model.RoutingInfo) error {
	if b == nil {
		return cqltype.TypeValidation("batch", "nil batch")
	}
	if len(b.entries) == implCluster.MaxBatchStatements {
		return cqltype.TypeValidation("batch", "requires 1 to 65535 entries")
	}
	if b.session != nil && b.session != s {
		return cqltype.TypeValidation("batch session", "statements belong to different sessions")
	}
	if r.Keyspace != "" && b.routing.Keyspace != "" && r.Keyspace != b.routing.Keyspace {
		return cqltype.TypeValidation("batch keyspace", "members use different keyspaces")
	}
	return nil
}

// appendEntry records a statement whose values are b.values[start:].
func (b *Batch) appendEntry(s *implCluster.Core, source statementSource, statement string, r *model.RoutingInfo, values model.EncodedValues, start int) {
	b.entries = append(b.entries, batchEntry{
		source:    source,
		statement: statement,
		columns:   values.Columns,
		start:     start,
		end:       len(b.values),
		count:     len(values.Cells),
		kind:      r.Kind,
	})
	if len(b.entries) == 1 {
		b.routing.Kind = cqltype.OrdinaryStatement
	}
	b.session = s
	if r.Keyspace != "" {
		b.routing.Keyspace = r.Keyspace
	}
	if !b.routing.Token.Set && r.Token.Set {
		b.routing.Token, b.routing.Table = r.Token, r.Table
	}
	switch {
	case r.Kind == cqltype.ConditionalStatement:
		b.routing.Kind = cqltype.ConditionalStatement
	case r.Kind == cqltype.UnknownStatement && b.routing.Kind != cqltype.ConditionalStatement:
		b.routing.Kind = cqltype.UnknownStatement
	}
}

// InternalConditionalStatement returns the index of the statement whose
// result is row i of the rows of a conditional batch on ScyllaDB, which
// returns one row per conditional statement, in statement order. It reports
// false when the batch has no i-th conditional statement, or when the kind of
// a statement before it is unknown.
func (b *Batch) InternalConditionalStatement(i int) (int, bool) {
	if b == nil || i < 0 {
		return 0, false
	}
	for index := range b.entries {
		switch b.entries[index].kind {
		case cqltype.UnknownStatement:
			return 0, false
		case cqltype.ConditionalStatement:
			if i == 0 {
				return index, true
			}
			i--
		}
	}
	return 0, false
}

// InternalAddBound appends bound's statement and values to batch.
func InternalAddBound[R any](batch *Batch, s *implCluster.Core, bound *Bound[R]) error {
	if bound == nil || bound.privateStorage.InternalRequest.InternalOwner == nil {
		return cqltype.TypeValidation("batch item", "nil binding")
	}
	request := &bound.privateStorage.InternalRequest
	if state := request.InternalPrepared(); state != nil && state.InternalSession != s {
		return cqltype.TypeValidation("batch session", "prepared statement belongs to another session")
	}
	if err := request.InternalOwner.InternalAdmit(); err != nil {
		return err
	}
	defer request.InternalOwner.InternalRelease()
	return batch.add(s, request.InternalSource, request.statement, &request.routing, request.InternalValues)
}

// runBatch sends batch on s without a query and returns its first and
// only page.
func runBatch[R any](ctx context.Context, s *implCluster.Core, batch *Batch, decoder codec.RowDecoder[R], options CompiledOptions) (*Page[R], error) {
	if err := validateBatch(s, batch); err != nil {
		return nil, err
	}
	if decoder == nil {
		return nil, cqltype.TypeValidation("batch rows", "nil decoder")
	}
	state := batch.begin()
	owner := InternalNewInputOwner()
	batch.lastOwner, batch.lastQuery = owner, nil
	bound := new(Bound[R])
	bound.privateStorage.InternalRequest = InternalExecutionRequest{InternalOwner: owner, routing: batch.requestRouting(), batch: state}
	bound.privateStorage.InternalRows = decoder
	page, err := runtimeOf(s).runSinglePage(ctx, bound, model.StartPage(), 0, options)
	owner.InternalClose()
	return page, err
}

// queryExec prepares the batch's request for an execution on query q,
// which began its own execution generation for it. Admission copies the
// request into the query's inline request.
func (b *Batch) queryExec(dst *InternalExecutionRequest, q *queryState) {
	state := b.begin()
	b.lastQuery, b.lastGen, b.lastOwner = q, q.generation.Load(), nil
	*dst = InternalExecutionRequest{InternalOwner: q.owner, query: q, routing: b.requestRouting(), batch: state}
}

// execBatch sends batch on s and drains its result, which must have no
// columns.
func execBatch(ctx context.Context, s *implCluster.Core, batch *Batch, options CompiledOptions) (model.Result, error) {
	if batch != nil && batch.routing.Kind == cqltype.ConditionalStatement {
		return conditionalExec()
	}
	page, err := runBatch(ctx, s, batch, execRowsDecoder{}, options)
	if err != nil {
		return InternalExecutionErrorResult(err), err
	}
	defer page.Close()
	var row codec.EmptyRecord
	for page.NextInto(&row) {
	}
	if page.Err() == nil && page.Columns().Len() > 0 {
		page.privateStorage.err = cqltype.TypeValidation("ExecBatch", "result columns require RunBatch")
	}
	return page.Info(), page.Err()
}

// batchQueries returns the pool of queries that run rt's batches, created on
// first use. A batch runs on a pooled query, as a prepared statement's
// one-shot call does, so that sending a reused batch allocates nothing.
func (rt *InternalSessionRuntime) batchQueries() (*queryPool[codec.EmptyRecord, codec.EmptyRecord], error) {
	rt.batchOnce.Do(func() {
		s := rt.Core
		rt.batchPool, rt.batchErr = newQueryPool(s, func(base *queryPoolBase) (*Query[codec.EmptyRecord, codec.EmptyRecord], error) {
			q := &Query[codec.EmptyRecord, codec.EmptyRecord]{}
			q.bound.privateStorage.rows = &q.rows
			if err := newQueryState(s, &q.state, q.retire, base); err != nil {
				return nil, err
			}
			q.state.resolved = CompiledOptions{}.resolveFixed(q.state.runtime)
			q.state.execCtx = newExecutionContext()
			return q, nil
		})
	})
	return rt.batchPool, rt.batchErr
}

// beginBatch validates a batch call on s and takes a pooled query that has
// begun an execution for it. The caller releases the query's owner when the
// execution no longer needs it.
func beginBatch(ctx context.Context, s *implCluster.Core, batch *Batch) (*Query[codec.EmptyRecord, codec.EmptyRecord], error) {
	if err := validateBatch(s, batch); err != nil {
		return nil, err
	}
	pool, err := InternalRuntimeFor(s).batchQueries()
	if err != nil {
		return nil, err
	}
	return pool.begin(ctx)
}

// InternalRunBatch sends batch on s; tests use it to run batches built from
// bindings.
func InternalRunBatch[R any](ctx context.Context, s *implCluster.Core, batch *Batch, decoder codec.RowDecoder[R], options CompiledOptions) (*Page[R], error) {
	return runBatch(ctx, s, batch, decoder, options)
}

// InternalExecBatch sends batch on s and drains its result.
func InternalExecBatch(ctx context.Context, s *implCluster.Core, batch *Batch, options CompiledOptions) (model.Result, error) {
	return execBatch(ctx, s, batch, options)
}

// batchRows is the storage of one RunBatch execution with row type R: its
// request, its result plans and its iterator. A Batch keeps it, so that a
// reused batch sent again with RunBatch allocates nothing.
type batchRows[R any] struct {
	holder rowsHolder[R]
	plans  InternalResultPlanCache[R]
	bound  Bound[R]
	// gen numbers the executions that used holder, so that a handle kept
	// from an earlier one is stale. The pooled query each execution runs
	// on has a generation of its own, which another query can repeat.
	gen  uint64
	used bool
}

// rowsFor returns the storage for a RunBatch of b decoded with decoder. It
// reuses the storage of b's latest RunBatch with the same R once that
// execution's rows are closed, keeping its compiled plans while decoder is
// the same value.
func (b *Batch) rowsFor[R any](decoder codec.RowDecoder[R]) *batchRows[R] {
	r, ok := b.rows.(*batchRows[R])
	if !ok || !r.idle() {
		r = new(batchRows[R])
		r.bound.privateStorage.rows = &r.holder
		r.bound.privateStorage.plans = &r.plans
		b.rows = r
	} else if !sameRowDecoder(r.bound.privateStorage.InternalRows, decoder) {
		r.plans = InternalResultPlanCache[R]{}
	}
	r.bound.privateStorage.InternalRows = decoder
	return r
}

// idle reports whether no rows of r's latest execution are open.
func (r *batchRows[R]) idle() bool {
	if !r.used {
		return true
	}
	rows := &r.holder.rows
	rows.privateStorage.mu.Lock()
	closed := rows.privateStorage.closed
	rows.privateStorage.mu.Unlock()
	return closed
}

// retag gives rows, just created in r's storage, a generation of r's own.
func (r *batchRows[R]) retag(rows Rows[R]) Rows[R] {
	r.used = true
	r.gen++
	rows.impl.gen.Store(r.gen)
	rows.gen = r.gen
	return rows
}

// sameRowDecoder reports whether a and b are the same interface value: the
// same dynamic type and the same data word. A decoder kept in a variable and
// passed again matches; an equal decoder built anew may not, which only
// costs a plan compile. Plain == would panic on decoders that are not
// comparable, such as cql.Row's.
func sameRowDecoder[R any](a, b codec.RowDecoder[R]) bool {
	type iface struct{ tab, data unsafe.Pointer }
	return *(*iface)(unsafe.Pointer(&a)) == *(*iface)(unsafe.Pointer(&b))
}

func (e *batchEntry) sourceOrText() statementSource {
	if e.source == nil {
		return cqlText{}
	}
	return e.source
}

func (e *batchEntry) encoded() model.EncodedValues {
	return model.EncodedValues{Columns: e.columns}
}

// prepareBatchAttempt fills the wire batch of one attempt on conn. It
// refills request when given one, and otherwise takes the state's own wire
// batch unless another attempt holds it; the caller releases that with
// releaseAttempt.
func (s *InternalSessionRuntime) prepareBatchAttempt(ctx context.Context, conn *transport.Conn, batch *batchState, request *model.TypedBatchRequest) (*model.TypedBatchRequest, error) {
	if request == nil {
		if batch.attemptBusy.CompareAndSwap(false, true) {
			request = &batch.attempt
		} else {
			request = new(model.TypedBatchRequest)
		}
	}
	request.Kind = batch.kind
	items := request.Items[:0]
	if cap(items) < len(batch.entries) {
		items = make([]model.TypedBatchItem, 0, len(batch.entries))
	}
	for i := range batch.entries {
		entry := &batch.entries[i]
		item := model.TypedBatchItem{Statement: entry.statement, Values: wire.ChainOf(batch.values[entry.start:entry.end]), ValueCount: entry.count}
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
func (batch *batchState) releaseAttempt(request *model.TypedBatchRequest) {
	if request == &batch.attempt {
		batch.attemptBusy.Store(false)
	}
}

func (s *InternalSessionRuntime) repairBatch(ctx context.Context, conn *transport.Conn, batch *batchState, id []byte) error {
	for i := range batch.entries {
		entry := &batch.entries[i]
		state := entry.sourceOrText().prepared()
		if state == nil {
			continue
		}
		cached := state.host(conn.InternalHostID())
		if cached != nil && bytes.Equal(cached.InternalId, id) {
			_, err := entry.sourceOrText().onConn(ctx, s, conn, entry.encoded(), true, nil)
			return err
		}
	}
	return &cqltype.ProtocolError{Stage: "batch unprepared", Err: fmt.Errorf("unknown statement ID")}
}

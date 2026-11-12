// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/scylladb/gocql/v2/internal/binding"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/paging"
	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/transport"
)

var ErrBusy = runtimeerrors.ErrBusy

type queryState struct {
	owner        *inputOwner
	execCtx      *executionContext // reusable direct-path context; nil keeps context.WithTimeout
	pageCtx      *executionContext // reusable direct-path page timeout, created on first use
	releaseOwner func()            // bound owner.InternalRelease, the execution drain
	runtime      *InternalSessionRuntime
	logical      *logicalExecution
	done         chan struct{} // created by the first waiter of a busy generation
	retire       func()
	resolved     *InternalResolvedOptions // the query's options, resolved at creation
	// optionsScratch holds options an execution resolves or overrides for
	// itself, allocated on first use (see InternalExecutionRequest.ownOptions).
	// It also holds the place of a removed field: moving the fields after it
	// slows admission by about 3%.
	optionsScratch *InternalResolvedOptions
	// historyInline is the inline paging history storage the query's
	// iterator lends its history.
	historyInline [paging.InternalInlineHistory]model.PageState
	inlineLogical logicalExecution
	request       InternalExecutionRequest
	execOwner     inputOwner // per-execution owner, reset each generation
	// response holds an execution's first response page. The next execution
	// can begin only after this one released it.
	response     transport.InternalResponsePage
	routing      routing.InternalLazyRoutingPlan
	generation   atomic.Uint64 // advanced in begin under mu; read without it by cursors
	mu           sync.Mutex
	pageCtxBusy  atomic.Bool // a page holds pageCtx
	busy, closed bool
	// perCall reports that each execution brings its own options instead of
	// using resolved.
	perCall bool
	// held reports that a pooled query's execution is held open by a handle
	// the pool's caller owns, so the session registry tracks the query until
	// it becomes idle. It is set before that execution begins and cleared by
	// the pool's idle hook, which runs after the execution drained.
	held bool
}

// noopCancel is shared so that retiring an execution allocates no closure.
var noopCancel context.CancelFunc = func() {}

type queryCursor struct {
	query      *queryState
	generation uint64
}

func executionCursor(request *InternalExecutionRequest) queryCursor {
	if request.query == nil {
		return queryCursor{}
	}
	return queryCursor{query: request.query, generation: request.query.generation.Load()}
}

// current reports whether the cursor's execution is still the query's
// current one. Handles are used from one goroutine, which also starts the
// next generation, so a plain atomic read suffices.
func (c queryCursor) current() bool {
	return c.query == nil || c.query.generation.Load() == c.generation
}

// Query owns reusable parameter storage and allows one active execution.
// Prepared statements remain shareable; a Query must not be copied.
type Query[P, R any] struct {
	params  codec.ParamPlan[P]
	options CompiledOptions
	columns cqltype.Columns
	scratch eachScratch   // Each and One decoding state
	rows    rowsHolder[R] // Run's iterator storage, reused each execution
	buffer  binding.ParameterBuffer
	bound   Bound[R]
	state   queryState
	// allowUnset reports whether parameters may be unset: always for CQL
	// text, from Protocol v4 for a prepared statement.
	allowUnset bool
}

// routingScratch holds Routing's encoding buffers. Routing is rare, so the
// buffers live in a pool rather than in every Query next to its hot fields.
var routingScratch = sync.Pool{New: func() any { return new(binding.ParameterBuffer) }}

// newQueryState sets up state for session. A query of a pool is not tracked
// by the session runtime: the runtime counts busy pooled queries instead,
// and the pool hands its idle ones to Close.
func newQueryState(s *implCluster.Core, state *queryState, retire func(), pool *queryPoolBase) error {
	if s == nil {
		return cqltype.TypeValidation("query", "nil session")
	}
	data := implCluster.SessionData(s)
	data.InternalSessionStateMu.RLock()
	defer data.InternalSessionStateMu.RUnlock()
	if data.InternalIsClosing || data.InternalIsClosed {
		return runtimeerrors.ErrClosed
	}
	state.runtime = InternalRuntimeFor(s)
	state.owner = InternalNewInputOwner()
	state.releaseOwner = state.owner.InternalRelease
	state.retire = retire
	state.owner.onIdle = state.idle
	if pool != nil {
		return nil
	}
	state.runtime.registerQuery(state)
	return nil
}

// Query creates a reusable query from p. Its options are validated and
// resolved for p's session once, here.
func (p *Prepared[P, R]) Query(values Options) (*Query[P, R], error) {
	if p == nil || p.privateStorage.InternalState == nil {
		return nil, cqltype.TypeValidation("prepared", "nil handle")
	}
	options, err := queryOptions(p.privateStorage.InternalState.InternalSession, values)
	if err != nil {
		return nil, err
	}
	return newPreparedQuery(p, options, nil)
}

// InternalCompileOptions freezes values for session s. Empty values become the
// zero CompiledOptions, which use the session's shared default resolution.
func InternalCompileOptions(s *implCluster.Core, values Options) (CompiledOptions, error) {
	return queryOptions(s, values)
}

// queryOptions freezes a query's values for its session. Empty values use
// the session's shared default resolution.
func queryOptions(s *implCluster.Core, values Options) (CompiledOptions, error) {
	if reflect.ValueOf(&values).Elem().IsZero() {
		return CompiledOptions{}, nil
	}
	if s == nil {
		return CompiledOptions{}, cqltype.TypeValidation("query", "nil session")
	}
	return NewCompiledOptions(s, values)
}

// preparedRouting returns the routing template of a prepared statement of
// kind, as newPreparedQuery builds it; executions add the token.
func preparedRouting(state *InternalPreparedState, kind cqltype.StatementKind) model.RoutingInfo {
	info := &state.InternalInfo
	return model.RoutingInfo{Keyspace: info.Keyspace, Table: info.Table, Kind: kind, Conditional: kind != cqltype.OrdinaryStatement}
}

func newPreparedQuery[P, R any](p *Prepared[P, R], options CompiledOptions, pool *queryPoolBase) (*Query[P, R], error) {
	if p == nil || p.privateStorage.InternalState == nil {
		return nil, cqltype.TypeValidation("prepared", "nil handle")
	}
	state := p.privateStorage.InternalState
	// Statement, keyspace, parameters and info are fixed when the prepared
	// state is built, so the query's request takes them once.
	q := &Query[P, R]{options: options, params: p.privateStorage.params, columns: state.InternalParameters, allowUnset: true}
	q.bound.privateStorage.InternalRows = p.privateStorage.rows
	q.bound.privateStorage.plans = p.privateStorage.plans
	q.bound.privateStorage.rows = &q.rows
	info := &state.InternalInfo
	kind := p.privateStorage.kind
	q.bound.privateStorage.InternalRequest = InternalExecutionRequest{statement: state.InternalStatement, keyspace: state.InternalKeyspace, InternalSource: preparedSource{state: state}, routing: model.RoutingInfo{Keyspace: info.Keyspace, Table: info.Table, Kind: kind, Conditional: kind != cqltype.OrdinaryStatement}, hasCounter: state.hasCounter}
	if err := newQueryState(p.privateStorage.InternalState.InternalSession, &q.state, q.retire, pool); err != nil {
		return nil, err
	}
	q.state.resolved = options.resolveFixed(q.state.runtime)
	q.state.execCtx = newExecutionContext()
	return q, nil
}

func newUnpreparedQuery[P, R any](s *implCluster.Core, statement binding.Statement[P, R], options CompiledOptions) (*Query[P, R], error) {
	if err := binding.ValidateStatement(&statement); err != nil {
		return nil, err
	}
	columns := statement.BindMarkers
	plan, err := codec.CompileParamPlan(statement.Params, columns)
	if err != nil {
		return nil, err
	}
	q := &Query[P, R]{params: plan, columns: columns, options: options, allowUnset: true}
	q.bound.privateStorage.InternalRows = statement.Rows
	q.bound.privateStorage.plans = &InternalResultPlanCache[R]{}
	q.bound.privateStorage.rows = &q.rows
	q.bound.privateStorage.InternalRequest = InternalExecutionRequest{statement: statement.CQL, InternalSource: cqlText{}, routing: model.RoutingInfo{Kind: statement.Kind, Conditional: statement.Kind != cqltype.OrdinaryStatement}, hasCounter: hasCounterColumn(columns)}
	if err := newQueryState(s, &q.state, q.retire, nil); err != nil {
		return nil, err
	}
	q.state.resolved = options.resolveFixed(q.state.runtime)
	q.state.execCtx = newExecutionContext()
	return q, nil
}

func (s *queryState) idle() {
	s.mu.Lock()
	if !s.busy {
		s.mu.Unlock()
		return
	}
	s.retire()
	s.logical = nil
	s.busy = false
	if s.done != nil {
		close(s.done)
		s.done = nil
	}
	closed := s.closed
	s.mu.Unlock()
	if s.perCall {
		s.runtime.pooledIdle()
		return
	}
	if closed {
		s.runtime.unregisterQuery(s)
	}
}

// begin starts an execution generation. A previous execution that already
// finished and only drains is waited for within ctx; one still running, or
// held open by a handle, makes begin return ErrBusy.
func (s *queryState) begin(ctx context.Context) error {
	if s.owner == nil || s.runtime == nil {
		return cqltype.TypeValidation("query", "uninitialized query")
	}
	if ctx == nil {
		return cqltype.TypeValidation("context", "nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	for s.busy && !s.closed {
		// The previous execution is busy. If it has released its input, it
		// has finished and only drains (accepted writes and attempts), so
		// wait for it; otherwise it is still running or a handle the caller
		// holds keeps it open, and overlapping use is an error.
		l := s.logical
		if l == nil || !l.lifetime.inputReleased.Load() {
			s.mu.Unlock()
			return ErrBusy
		}
		if s.done == nil {
			s.done = make(chan struct{})
		}
		done := s.done
		s.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
		s.mu.Lock()
	}
	if s.closed {
		s.mu.Unlock()
		return runtimeerrors.ErrClosed
	}
	s.busy = true
	s.generation.Add(1)
	s.mu.Unlock()
	if s.perCall {
		// Close waits until no pooled query is busy. Counting before the
		// closed check means Close either sees this query or this sees Close.
		s.runtime.pooledBusy.Add(1)
		if s.runtime.Core.Closed() {
			s.idle()
			return runtimeerrors.ErrClosed
		}
	}
	// Never acquire owner.mu while holding state.mu: the final release calls idle.
	if err := s.owner.InternalAdmit(); err != nil {
		s.idle()
		return err
	}
	return nil
}

// capture captures value into the query's storage in a new execution
// generation.
func (q *Query[P, R]) capture(ctx context.Context, value P) error {
	if q == nil {
		return cqltype.TypeValidation("query", "nil query")
	}
	if err := q.state.begin(ctx); err != nil {
		return err
	}
	request := &q.bound.privateStorage.InternalRequest
	values, err := q.buffer.Encode(q.params, q.columns, value, q.allowUnset)
	if err != nil {
		q.state.owner.InternalRelease()
		return err
	}
	request.InternalValues = values
	token, err := request.InternalSource.token(values.Cells)
	if err != nil {
		q.state.owner.InternalRelease()
		return err
	}
	request.routing.Token = token
	request.InternalOwner, request.query = q.state.owner, &q.state
	return nil
}

func (q *Query[P, R]) retire() {
	q.bound.privateStorage.InternalRequest.InternalValues = model.EncodedValues{}
	if q.state.request.InternalLogical == &q.state.inlineLogical {
		lifetime := &q.state.inlineLogical.lifetime
		lifetime.mu.Lock()
		lifetime.stop = nil
		lifetime.cancel = noopCancel
		lifetime.mu.Unlock()
	}
	q.state.request = InternalExecutionRequest{}
	q.state.routing = routing.InternalLazyRoutingPlan{}
	q.buffer.Retire()
}

// Exec executes the query with value. The statement must not return rows.
// The query is ready for its next execution when Exec returns.
func (q *Query[P, R]) Exec(ctx context.Context, value P) (model.Result, error) {
	if err := q.capture(ctx, value); err != nil {
		return InternalExecutionErrorResult(err), err
	}
	defer q.state.owner.InternalRelease()
	return q.state.runtime.exec(ctx, &q.bound, q.options)
}

// Run executes the query and returns an iterator over its rows. The query
// stays busy until the rows are closed; see Rows for stale handles.
func (q *Query[P, R]) Run(ctx context.Context, value P) (Rows[R], error) {
	if err := q.capture(ctx, value); err != nil {
		return Rows[R]{}, err
	}
	defer q.state.owner.InternalRelease()
	return q.state.runtime.run(ctx, &q.bound, q.options)
}

// RunPage executes one page of the query with value, resuming from state, as
// the package-level RunPage does with pageSize.
func (q *Query[P, R]) RunPage(ctx context.Context, value P, state model.PageState, pageSize int32) (*Page[R], error) {
	if err := q.capture(ctx, value); err != nil {
		return nil, err
	}
	defer q.state.owner.InternalRelease()
	return q.state.runtime.runSinglePage(ctx, &q.bound, state, pageSize, q.options)
}

// Routing encodes value and returns its routing information without sending a
// request. It encodes into scratch of its own, not the execution's storage, so
// it works while the query runs, drains or has an open handle; only a closed
// query rejects it.
func (q *Query[P, R]) Routing(value P) (model.RoutingInfo, error) {
	if q == nil || q.state.owner == nil || q.state.runtime == nil {
		return model.RoutingInfo{}, cqltype.TypeValidation("query", "uninitialized query")
	}
	q.state.mu.Lock()
	closed := q.state.closed
	q.state.mu.Unlock()
	if closed {
		return model.RoutingInfo{}, runtimeerrors.ErrClosed
	}
	scratch := routingScratch.Get().(*binding.ParameterBuffer)
	values, err := scratch.Encode(q.params, q.columns, value, q.allowUnset)
	request := &q.bound.privateStorage.InternalRequest
	routing := request.routing
	if err == nil {
		routing.Token, err = request.InternalSource.token(values.Cells)
	}
	scratch.Retire()
	routingScratch.Put(scratch)
	if err != nil {
		return model.RoutingInfo{}, err
	}
	return routing, nil
}

// AddToBatch encodes value with the query's parameters straight into batch's
// storage and appends the statement. It uses none of the query's execution
// state, so it works while the query runs, drains or has an open handle; only
// a closed query or session rejects it. Closing the query later does not affect
// the batch.
func (q *Query[P, R]) AddToBatch(batch *Batch, value P) error {
	if batch == nil {
		return cqltype.TypeValidation("batch", "nil batch")
	}
	if q == nil || q.state.owner == nil || q.state.runtime == nil {
		return cqltype.TypeValidation("query", "uninitialized query")
	}
	q.state.mu.Lock()
	closed := q.state.closed
	q.state.mu.Unlock()
	if closed {
		return runtimeerrors.ErrClosed
	}
	s := q.state.runtime.Core
	if s.Closed() {
		return runtimeerrors.ErrClosed
	}
	request := &q.bound.privateStorage.InternalRequest
	return batch.addValue(s, request.InternalSource, request.statement, &request.routing, q.params, q.columns, value, q.allowUnset)
}

func (s *queryState) close() {
	l := s.stop()
	if l != nil {
		l.shutdown()
	}
}
func (s *queryState) stop() *logicalExecution {
	if s.owner == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	l := s.logical
	busy := s.busy
	s.mu.Unlock()
	s.owner.InternalClose()
	if !busy && !s.perCall {
		s.runtime.unregisterQuery(s)
	}
	if l != nil {
		l.cancelExecution()
	}
	return l
}
func (s *queryState) wait(ctx context.Context) error {
	if ctx == nil {
		return cqltype.TypeValidation("context", "nil context")
	}
	if s.owner == nil {
		return cqltype.TypeValidation("query", "uninitialized query")
	}
	s.mu.Lock()
	if !s.busy {
		s.mu.Unlock()
		return nil
	}
	// Only a waiter needs the completion channel; idle closes it.
	if s.done == nil {
		s.done = make(chan struct{})
	}
	done := s.done
	s.mu.Unlock()
	select {
	case <-done:
		return nil
	default:
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops the query permanently and returns at once. A running call is
// cancelled and returns an error matching context.Canceled; Rows, a Page or a batch page of the query
// that is still open is stopped and reports ErrClosed; later calls return
// ErrClosed. The last execution's accepted writes, attempts and observer
// callbacks drain in the background: Wait joins them, and Session.Close joins
// any that remain. Close may be called from any goroutine, and again.
func (q *Query[P, R]) Close() {
	if q == nil {
		return
	}
	q.state.close()
}

// Wait waits until the query's last execution has drained: accepted
// transport writes, attempts in flight and their observer callbacks. It
// returns ctx.Err() if ctx ends first. After Close, a nil result means
// nothing of the query remains in flight.
func (q *Query[P, R]) Wait(ctx context.Context) error {
	if q == nil {
		return cqltype.TypeValidation("query", "nil query")
	}
	return q.state.wait(ctx)
}

func cloneQueryOptions(options Options) (Options, error) {
	options.CustomPayload = append([]model.PayloadEntry(nil), options.CustomPayload...)
	for i := range options.CustomPayload {
		entry := &options.CustomPayload[i]
		if entry.Value.Valid {
			if err := cqltype.CheckBytesAlive(entry.Value.Value); err != nil {
				return Options{}, err
			}
			entry.Value.Value = entry.Value.Value.Clone()
		}
	}
	return options, nil
}

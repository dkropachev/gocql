// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/scylladb/gocql/v2/internal/binding"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/paging"
	"github.com/scylladb/gocql/v2/internal/routing"
)

// ErrBusy indicates that a query still owns an active execution, transport
// write, or callback. Wait joins that execution before the query is reused.
var ErrBusy = errors.New("gocql: query is busy")

type queryState struct {
	owner        *inputOwner
	execCtx      *executionContext // reusable direct-path context; nil keeps context.WithTimeout
	pageCtx      *executionContext // reusable direct-path page timeout, created on first use
	releaseOwner func()            // bound owner.InternalRelease, the execution drain
	runtime      *InternalSessionRuntime
	logical      *InternalLogicalExecution
	done         chan struct{} // created by the first waiter of a busy generation
	retire       func()
	resolved     *InternalResolvedOptions // the query's options, resolved at creation
	// _ holds the place of a removed field: moving the fields after it
	// slows admission by about 3%.
	_ [8]byte
	// historyInline is the inline paging history storage the query's
	// iterator lends its history.
	historyInline [paging.InternalInlineHistory]PageState
	inlineLogical InternalLogicalExecution
	request       InternalExecutionRequest
	execOwner     InternalInputOwner // per-execution owner, reset each generation
	// response holds an execution's first response page. The next execution
	// can begin only after this one released it.
	response     responsePage
	routing      routing.InternalLazyRoutingPlan
	generation   atomic.Uint64 // advanced in begin under mu; read without it by cursors
	mu           sync.Mutex
	pageCtxBusy  atomic.Bool // a page holds pageCtx
	busy, closed bool
	// perCall reports that each execution brings its own options instead of
	// using resolved.
	perCall bool
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
	params  ParamPlan[P]
	options Options
	columns Columns
	last    Result        // the latest Exec, Each or One outcome
	scratch eachScratch   // Each and One decoding state
	rows    rowsHolder[R] // Run's iterator storage, reused each execution
	buffer  binding.ParameterBuffer
	bound   Bound[R]
	state   queryState
	// allowUnset reports whether parameters may be unset: always for CQL
	// text, from Protocol v4 for a prepared statement.
	allowUnset bool
}

// LastResult returns the result information of the query's latest Exec,
// Each or One: the coordinator, attempts, latency, row count and any
// warnings, trace IDs or custom payload. Run reports it through Rows.Info.
// Like the query's other methods, it is not safe for concurrent use.
func (q *Query[P, R]) LastResult() Result {
	if q == nil {
		return Result{}
	}
	return cloneResult(q.last)
}

// newQueryState sets up state for session. A query of a pool is tracked by
// the pool, not by the session runtime.
func newQueryState(s *implCluster.Session, state *queryState, retire func(), pool *queryPoolBase) error {
	if s == nil {
		return typeValidation("query", "nil session")
	}
	data := implCluster.SessionData(s)
	data.InternalSessionStateMu.RLock()
	defer data.InternalSessionStateMu.RUnlock()
	if data.InternalIsClosing || data.InternalIsClosed {
		return ErrClosed
	}
	state.runtime = InternalRuntimeFor(s)
	state.owner = newInputOwner()
	state.releaseOwner = state.owner.InternalRelease
	state.retire = retire
	state.owner.onIdle = state.idle
	if pool != nil {
		return pool.adopt(state)
	}
	state.runtime.InternalMu.Lock()
	if state.runtime.queries == nil {
		state.runtime.queries = make(map[*queryState]struct{})
	}
	state.runtime.queries[state] = struct{}{}
	state.runtime.InternalMu.Unlock()
	return nil
}

// NewQuery creates a reusable query from a prepared statement. Its options
// are validated and resolved for the statement's session once, here.
func NewQuery[P, R any](p *Prepared[P, R], values OptionValues) (*Query[P, R], error) {
	if p == nil || p.privateStorage.InternalState == nil {
		return nil, typeValidation("prepared", "nil handle")
	}
	options, err := queryOptions(p.privateStorage.InternalState.InternalSession, values)
	if err != nil {
		return nil, err
	}
	return newPreparedQuery(p, options, nil)
}

// InternalBindOptions freezes values for session s. Empty values become the
// zero Options, which use the session's shared default resolution.
func InternalBindOptions(s *implCluster.Session, values OptionValues) (Options, error) {
	return queryOptions(s, values)
}

// queryOptions freezes a query's values for its session. Empty values use
// the session's shared default resolution.
func queryOptions(s *implCluster.Session, values OptionValues) (Options, error) {
	if reflect.ValueOf(&values).Elem().IsZero() {
		return Options{}, nil
	}
	if s == nil {
		return Options{}, typeValidation("query", "nil session")
	}
	return NewOptions(s, values)
}

func newPreparedQuery[P, R any](p *Prepared[P, R], options Options, pool *queryPoolBase) (*Query[P, R], error) {
	if p == nil || p.privateStorage.InternalState == nil {
		return nil, typeValidation("prepared", "nil handle")
	}
	state := p.privateStorage.InternalState
	// Statement, keyspace, parameters and info are fixed when the prepared
	// state is built, so the query's request takes them once.
	q := &Query[P, R]{options: options, params: p.privateStorage.InternalParams, columns: state.InternalParameters, allowUnset: state.InternalProtocol >= 4}
	q.bound.privateStorage.InternalRows = p.privateStorage.InternalRows
	q.bound.privateStorage.plans = p.privateStorage.InternalPlans
	q.bound.privateStorage.rows = &q.rows
	info := &state.InternalInfo
	kind := p.privateStorage.InternalKind
	q.bound.privateStorage.InternalRequest = InternalExecutionRequest{InternalStatement: state.InternalStatement, keyspace: state.InternalKeyspace, InternalSource: preparedSource{state: state}, InternalRouting: RoutingInfo{Keyspace: info.Keyspace, Table: info.Table, Kind: kind, Conditional: kind != OrdinaryStatement}}
	if err := newQueryState(p.privateStorage.InternalState.InternalSession, &q.state, q.retire, pool); err != nil {
		return nil, err
	}
	q.state.resolved = options.resolveFixed(q.state.runtime)
	q.state.execCtx = newExecutionContext()
	return q, nil
}

// NewUnpreparedQuery compiles parameters once against statement.BindMarkers
// and captures each execution's input in query-owned storage, including
// values passed to custom codecs.
func NewUnpreparedQuery[P, R any](s *implCluster.Session, statement Statement[P, R], values OptionValues) (*Query[P, R], error) {
	options, err := queryOptions(s, values)
	if err != nil {
		return nil, err
	}
	return newUnpreparedQuery(s, statement, options)
}

func newUnpreparedQuery[P, R any](s *implCluster.Session, statement Statement[P, R], options Options) (*Query[P, R], error) {
	if err := validateStatement(&statement); err != nil {
		return nil, err
	}
	columns := statement.BindMarkers
	plan, err := compileParamPlan(statement.Params, columns)
	if err != nil {
		return nil, err
	}
	q := &Query[P, R]{params: plan, columns: columns, options: options, allowUnset: true}
	q.bound.privateStorage.InternalRows = statement.Rows
	q.bound.privateStorage.plans = &InternalResultPlanCache[R]{}
	q.bound.privateStorage.rows = &q.rows
	q.bound.privateStorage.InternalRequest = InternalExecutionRequest{InternalStatement: statement.CQL, InternalSource: cqlText{}, InternalRouting: RoutingInfo{Kind: statement.Kind, Conditional: statement.Kind != OrdinaryStatement}}
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
	if closed {
		s.runtime.InternalMu.Lock()
		delete(s.runtime.queries, s)
		s.runtime.InternalMu.Unlock()
	}
}

func (s *queryState) begin(ctx context.Context) error {
	if s.owner == nil || s.runtime == nil {
		return typeValidation("query", "uninitialized query")
	}
	if ctx == nil {
		return typeValidation("context", "nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if s.busy {
		s.mu.Unlock()
		return ErrBusy
	}
	s.busy = true
	s.generation.Add(1)
	s.mu.Unlock()
	// Never acquire owner.mu while holding state.mu: the final release calls idle.
	if err := s.owner.InternalAdmit(); err != nil {
		s.idle()
		return err
	}
	return nil
}

func (q *Query[P, R]) capture(ctx context.Context, value P) error {
	if q == nil {
		return typeValidation("query", "nil query")
	}
	if err := q.state.begin(ctx); err != nil {
		return err
	}
	request := &q.bound.privateStorage.InternalRequest
	values, err := binding.EncodeInto(&q.buffer, q.params, q.columns, value, q.allowUnset)
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
	request.InternalRouting.Token = token
	request.InternalOwner, request.query = q.state.owner, &q.state
	return nil
}

func (q *Query[P, R]) retire() {
	q.bound.privateStorage.InternalRequest.InternalValues = encodedValues{}
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

// Exec executes the query, which must not return rows. LastResult reports
// the outcome's result information.
func (q *Query[P, R]) Exec(ctx context.Context, value P) error {
	if err := q.capture(ctx, value); err != nil {
		q.last = InternalExecutionErrorResult(err)
		return err
	}
	defer q.state.owner.InternalRelease()
	var err error
	q.last, err = Exec(ctx, q.state.runtime.Session, &q.bound, q.options)
	return err
}

// Run executes the query and returns an iterator over its rows. The query
// stays busy until the rows are closed; see Rows for stale handles.
func (q *Query[P, R]) Run(ctx context.Context, value P) (Rows[R], error) {
	if err := q.capture(ctx, value); err != nil {
		return Rows[R]{}, err
	}
	defer q.state.owner.InternalRelease()
	return Run(ctx, q.state.runtime.Session, &q.bound, q.options)
}
func (q *Query[P, R]) RunPage(ctx context.Context, value P, state PageState) (*Page[R], error) {
	if err := q.capture(ctx, value); err != nil {
		return nil, err
	}
	defer q.state.owner.InternalRelease()
	return RunPage(ctx, q.state.runtime.Session, &q.bound, state, q.options)
}
func (q *Query[P, R]) RunUnpaged(ctx context.Context, value P) (*Page[R], error) {
	if err := q.capture(ctx, value); err != nil {
		return nil, err
	}
	defer q.state.owner.InternalRelease()
	return RunUnpaged(ctx, q.state.runtime.Session, &q.bound, q.options)
}

// Routing captures parameters and returns their routing information without
// sending a request. It observes the same single-execution admission rules.
func (q *Query[P, R]) Routing(value P) (RoutingInfo, error) {
	if err := q.capture(context.Background(), value); err != nil {
		return RoutingInfo{}, err
	}
	defer q.state.owner.InternalRelease()
	return q.bound.privateStorage.InternalRequest.InternalRouting, nil
}

// AddTo encodes value with the query's parameters and appends the statement
// to batch. The values are copied into the batch, so the query is free for
// its next use as soon as AddTo returns, and closing it does not affect the
// batch.
func (q *Query[P, R]) AddTo(batch *Batch, value P) error {
	if batch == nil {
		return typeValidation("batch", "nil batch")
	}
	if err := q.capture(context.Background(), value); err != nil {
		return err
	}
	defer q.state.owner.InternalRelease()
	return batch.add(q.state.runtime.Session, &q.bound.privateStorage.InternalRequest)
}

// ExecBatch sends batch as one CQL BATCH of kind on the query's session.
// The batch's statements must produce no result columns; RunBatch reads
// the rows of a conditional batch. The query need not be one of the
// batch's statements; it is busy while the batch runs, as for its own
// executions.
func (q *Query[P, R]) ExecBatch(ctx context.Context, kind BatchKind, batch *Batch, options Options) (Result, error) {
	if q == nil || q.state.runtime == nil {
		err := typeValidation("query", "nil query")
		return InternalExecutionErrorResult(err), err
	}
	s := q.state.runtime.Session
	if err := validateBatch(s, kind, batch, options); err != nil {
		return InternalExecutionErrorResult(err), err
	}
	if err := q.state.begin(ctx); err != nil {
		return InternalExecutionErrorResult(err), err
	}
	defer q.state.owner.InternalRelease()
	batch.queryExec(&batch.exec.privateStorage.InternalRequest, &q.state, kind)
	return Exec(ctx, s, &batch.exec, options)
}

// RunBatch sends batch as one CQL BATCH of kind on the query's session and
// returns its rows, decoded with the query's row decoder. A conditional
// batch returns one row per conditional statement, or one row reporting
// that it was not applied. The query is busy until the page is closed.
func (q *Query[P, R]) RunBatch(ctx context.Context, kind BatchKind, batch *Batch, options Options) (*Page[R], error) {
	if q == nil || q.state.runtime == nil {
		return nil, typeValidation("query", "nil query")
	}
	s := q.state.runtime.Session
	if err := validateBatch(s, kind, batch, options); err != nil {
		return nil, err
	}
	if err := q.state.begin(ctx); err != nil {
		return nil, err
	}
	defer q.state.owner.InternalRelease()
	bound := &Bound[R]{privateStorage: BoundStorage[R]{InternalRows: q.bound.privateStorage.InternalRows}}
	batch.queryExec(&bound.privateStorage.InternalRequest, &q.state, kind)
	return RunPage(ctx, s, bound, StartPage(), options)
}

func (s *queryState) close() {
	l := s.stop()
	if l != nil {
		l.shutdown()
	}
}
func (s *queryState) stop() *InternalLogicalExecution {
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
	if !busy {
		s.runtime.InternalMu.Lock()
		delete(s.runtime.queries, s)
		s.runtime.InternalMu.Unlock()
	}
	if l != nil {
		l.cancelExecution()
	}
	return l
}
func (s *queryState) wait(ctx context.Context) error {
	if ctx == nil {
		return typeValidation("context", "nil context")
	}
	if s.owner == nil {
		return typeValidation("query", "uninitialized query")
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

// Close permanently stops this query. Wait joins all accepted transport writes
// and callbacks; cancellation alone does not make a busy query reusable.
func (q *Query[P, R]) Close() {
	if q != nil {
		q.state.close()
	}
}
func (q *Query[P, R]) Wait(ctx context.Context) error {
	if q == nil {
		return typeValidation("query", "nil query")
	}
	return q.state.wait(ctx)
}

// CloseAndWait closes the query and waits until its accepted transport writes
// and callbacks finish or ctx is done.
func (q *Query[P, R]) CloseAndWait(ctx context.Context) error {
	q.Close()
	return q.Wait(ctx)
}

func cloneQueryOptions(options OptionValues) (OptionValues, error) {
	options.CustomPayload = append([]PayloadEntry(nil), options.CustomPayload...)
	for i := range options.CustomPayload {
		entry := &options.CustomPayload[i]
		if entry.Value.Valid {
			if err := cqltype.CheckBytesAlive(entry.Value.Value); err != nil {
				return OptionValues{}, err
			}
			entry.Value.Value = entry.Value.Value.Clone()
		}
	}
	return options, nil
}

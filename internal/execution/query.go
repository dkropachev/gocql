// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"errors"
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
	request       InternalExecutionRequest
	inlineLogical InternalLogicalExecution
	routing       routing.InternalLazyRoutingPlan
	mu            sync.Mutex
	owner         *inputOwner
	execOwner     InternalInputOwner // per-execution owner, reset each generation
	execCtx       *executionContext  // reusable direct-path context; nil keeps context.WithTimeout
	pageCtx       *executionContext  // reusable direct-path page timeout, created on first use
	pageCtxBusy   atomic.Bool        // a page holds pageCtx
	releaseOwner  func()             // bound owner.InternalRelease, the execution drain
	runtime       *InternalSessionRuntime
	logical       *InternalLogicalExecution
	busy, closed  bool
	generation    atomic.Uint64 // advanced in begin under mu; read without it by cursors
	done          chan struct{} // created by the first waiter of a busy generation
	retire        func()
	resolved      resolvedOptionsCache
	// response holds an execution's first response page. The next execution
	// can begin only after this one released it.
	response responsePage
	// responseAlt is the second slot a prefetching Each alternates with
	// response: one page is decoded while the next is parsed.
	responseAlt responsePage
	prefetched  chan eachFetch // a prefetching Each's page hand-off, created on first use
	// historyInline is the inline paging history storage the query's
	// iterator lends its history.
	historyInline [paging.InternalInlineHistory]PageState
}

// resolvedOptionsCache keeps a query's resolved options. A query's options
// never change, so the resolution depends only on the session and explicit
// profile pointers, which ProfileHandle.Replace swaps.
type resolvedOptionsCache struct {
	session, explicit *Profile
	valid             bool
	value             InternalResolvedOptions
}

// resolveOptions resolves the query's options, reusing the previous result
// while neither profile was replaced. It runs during admission, which a busy
// query never overlaps. Options with a custom payload are resolved each time:
// their borrowed values must be checked for liveness on every execution.
func (s *queryState) resolveOptions(session *implCluster.Session, options Options) (*InternalResolvedOptions, error) {
	if len(options.CustomPayload) != 0 {
		resolved, err := InternalResolveOptions(session, options)
		return &resolved, err
	}
	var sessionProfile *Profile
	if session != nil {
		sessionProfile = InternalRuntimeFor(session).InternalProfile.InternalCurrent()
	}
	explicit := options.Profile.InternalCurrent()
	c := &s.resolved
	if c.valid && c.session == sessionProfile && c.explicit == explicit {
		return &c.value, nil
	}
	resolved, err := InternalResolveOptions(session, options)
	if err != nil {
		c.valid = false
		return nil, err
	}
	*c = resolvedOptionsCache{session: sessionProfile, explicit: explicit, valid: true, value: resolved}
	return &c.value, nil
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
	state    queryState
	bound    Bound[R]
	prepared *Prepared[P, R]
	params   ParamPlan[P]
	columns  Columns
	buffer   binding.ParameterBuffer
	options  Options
	scratch  eachScratch   // Each and One decoding state
	rows     rowsHolder[R] // Run's iterator storage, reused each execution
	last     Result        // the latest Exec, Each or One outcome
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

func newQueryState(s *implCluster.Session, state *queryState, retire func()) error {
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
	state.runtime.InternalMu.Lock()
	if state.runtime.queries == nil {
		state.runtime.queries = make(map[*queryState]struct{})
	}
	state.runtime.queries[state] = struct{}{}
	state.runtime.InternalMu.Unlock()
	return nil
}

// NewQuery creates a reusable query from a prepared statement. Defaults are
// loaded from the selected profile on each execution; options are overrides.
func NewQuery[P, R any](p *Prepared[P, R], options Options) (*Query[P, R], error) {
	if p == nil || p.privateStorage.InternalState == nil {
		return nil, typeValidation("prepared", "nil handle")
	}
	options, err := cloneQueryOptions(options)
	if err != nil {
		return nil, err
	}
	q := &Query[P, R]{prepared: p, options: options}
	q.bound.privateStorage.InternalRows = p.privateStorage.InternalRows
	q.bound.privateStorage.plans = p.privateStorage.InternalPlans
	q.bound.privateStorage.rows = &q.rows
	if err := newQueryState(p.privateStorage.InternalState.InternalSession, &q.state, q.retire); err != nil {
		return nil, err
	}
	q.state.execCtx = newExecutionContext()
	return q, nil
}

// NewUnpreparedQuery compiles parameters once and captures each execution's
// input in query-owned storage, including values passed to custom codecs.
func NewUnpreparedQuery[P, R any](s *implCluster.Session, statement Statement[P, R], columns Columns, options Options) (*Query[P, R], error) {
	if err := validateStatement(statement); err != nil {
		return nil, err
	}
	plan, err := compileParamPlan(statement.Params, columns)
	if err != nil {
		return nil, err
	}
	options, err = cloneQueryOptions(options)
	if err != nil {
		return nil, err
	}
	q := &Query[P, R]{params: plan, columns: columns, options: options}
	q.bound.privateStorage.InternalRows = statement.Rows
	q.bound.privateStorage.plans = &InternalResultPlanCache[R]{}
	q.bound.privateStorage.rows = &q.rows
	q.bound.privateStorage.InternalRequest = InternalExecutionRequest{InternalStatement: statement.CQL, InternalRouting: RoutingInfo{Kind: statement.Kind, Conditional: statement.Kind != OrdinaryStatement}}
	if err := newQueryState(s, &q.state, q.retire); err != nil {
		return nil, err
	}
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
	plan, columns, allowUnset := q.params, q.columns, true
	if q.prepared != nil {
		p := &q.prepared.privateStorage
		state := p.InternalState
		// Statement, keyspace, parameters and info are fixed when the
		// prepared state is built, so they are read without its lock.
		columns = state.InternalParameters
		info := &state.InternalInfo
		request.InternalStatement, request.keyspace = state.InternalStatement, state.InternalKeyspace
		plan, allowUnset = p.InternalParams, state.InternalProtocol >= 4
		request.InternalPrepared = state
		request.InternalRouting = RoutingInfo{Keyspace: info.Keyspace, Table: info.Table, Kind: p.InternalKind, Conditional: p.InternalKind != OrdinaryStatement}
		values, err := binding.EncodeInto(&q.buffer, plan, columns, value, allowUnset)
		if err != nil {
			q.state.owner.InternalRelease()
			return err
		}
		request.InternalValues = values
		token, err := computeRoutingToken(values.Cells, info.PartitionKeyIndexes, implCluster.InternalSessionCapturedRoutingPartitioner(state.InternalSession, state.InternalPartitioner))
		if err != nil {
			q.state.owner.InternalRelease()
			return err
		}
		request.InternalRouting.Token = token
	} else {
		values, err := binding.EncodeInto(&q.buffer, plan, columns, value, allowUnset)
		if err != nil {
			q.state.owner.InternalRelease()
			return err
		}
		request.InternalValues = values
	}
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

// BatchEntry captures an owned batch member immediately. The item has no Close
// or Wait obligation and does not keep this query busy. Each append transfers
// that encoding's backing storage to the immutable item.
func (q *Query[P, R]) BatchEntry(value P) (BatchItem, error) {
	if err := q.capture(context.Background(), value); err != nil {
		return BatchItem{}, err
	}
	defer q.state.owner.InternalRelease()
	request := q.bound.privateStorage.InternalRequest
	q.buffer = binding.ParameterBuffer{}
	request.query, request.InternalLogical = nil, nil
	request.InternalOwner = newInputOwner()
	return BatchItem{privateStorage: BatchItemStorage{InternalRequest: &request}}, nil
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

func cloneQueryOptions(options Options) (Options, error) {
	options.CustomPayload = append([]PayloadEntry(nil), options.CustomPayload...)
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

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/paging"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/schemafence"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/metrics"
	"github.com/scylladb/gocql/v2/retry"

	routing "github.com/scylladb/gocql/v2/internal/routing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"

	"bytes"
	"context"
	"errors"
	"fmt"

	model "github.com/scylladb/gocql/v2/internal/model"

	"slices"
	"sync"
	"sync/atomic"
	"time"
	"weak"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
)

type prepareCacheKey struct {
	InternalCql, InternalKeyspace string
}
type prepareFlight struct {
	done  chan struct{}
	state *InternalPreparedState
	err   error
}
type InternalSessionRuntime struct {
	*implCluster.Core
	// registry holds the in-flight executions no reusable query owns. It is
	// allocated once, so the runtime keeps the field layout it had with the
	// executions map in this slot.
	registry             *executionRegistry
	queries              map[weak.Pointer[queryState]]*queryEntry
	schema               *schemafence.InternalPreparedSchemaTracker
	preparing            map[prepareCacheKey]*prepareFlight
	InternalProfile      *settings.Profile
	InternalPrepared     map[prepareCacheKey]*InternalPreparedState
	pools                map[*queryPoolEntry]struct{}
	InternalPrepareOrder []prepareCacheKey
	defaultOptions       InternalResolvedOptions // the zero CompiledOptions resolved for this session
	config               settings.Config
	prepareWorkers       sync.WaitGroup
	// pooledWake, guarded by InternalMu, is the channel Close waits on for
	// pooledBusy to reach zero.
	pooledWake chan struct{}
	// batchPool keeps the queries that run the session's batches, created
	// by the first batch.
	batchPool *queryPool[codec.EmptyRecord, codec.EmptyRecord]
	batchErr  error
	// closing is closed when Close starts stopping executions.
	closing          chan struct{}
	batchOnce        sync.Once
	closingOnce      sync.Once
	prepareWorkersMu sync.Mutex
	InternalMu       sync.Mutex
	// refusedRegistrations counts admissions that took a request id but
	// that registerExecution refused: the session was closing or closed, or
	// the reusable query was closed. Requests is requestID minus this, so
	// admission writes no separate request counter.
	refusedRegistrations atomic.Uint64
	prepareWorkersClosed bool

	// The fields below are written on the request path. Each group has its
	// own cache line, so that the writes do not evict the read-mostly fields
	// above (config, defaultOptions) from other cores' caches, nor one
	// another's lines.
	_         cacheLinePad
	requestID atomic.Uint64
	// timestamps is written with requestID, by the same admission.
	timestamps startTimestamps
	_          cacheLinePad
	// attempts..inflight feed Metrics only.
	attempts, errors, retries, speculations, cancelled atomic.Uint64
	inflight                                           atomic.Int64
	// pooledBusy counts busy queries of prepared statements' one-shot pools.
	// Close waits for it to reach zero on pooledWake.
	pooledBusy atomic.Int64
	_          cacheLinePad
}

// startTimestamps is the session's default client timestamp source. It takes
// each timestamp from the start time admission already read, instead of
// reading the clock again.
type startTimestamps struct{ last atomic.Int64 }

// next returns started in microseconds, raised above every timestamp returned
// before, so the timestamps are strictly increasing and never earlier than
// their start. Uncontended, it takes one CAS. Once another request has raised
// the last timestamp to started or beyond, one increment past it is used; the
// CAS is retried only while the last timestamp stays below started.
func (t *startTimestamps) next(started time.Time) int64 {
	now := started.UnixMicro()
	for {
		prev := t.last.Load()
		if now <= prev {
			return t.last.Add(1)
		}
		if t.last.CompareAndSwap(prev, now) {
			return now
		}
	}
}

// cacheLineSize is the coherence unit hot counters are padded to.
const cacheLineSize = 64

// cacheLinePad keeps the fields on either side of it off one cache line,
// whatever the alignment of the struct that holds it.
type cacheLinePad struct{ _ [cacheLineSize]byte }

func InternalRuntimeFor(s *implCluster.Core) *InternalSessionRuntime {
	// The runtime is created once per session and never replaced, so every
	// call after initialization takes this lock-free path.
	if rt, ok := implCluster.SessionData(s).InternalTypedReady.Load().(*InternalSessionRuntime); ok {
		return rt
	}
	implCluster.SessionData(s).InternalTypedMu.Lock()
	defer implCluster.SessionData(s).InternalTypedMu.Unlock()
	if implCluster.SessionData(s).InternalTypedState == nil {
		rt := &InternalSessionRuntime{Core: s, InternalPrepared: make(map[prepareCacheKey]*InternalPreparedState), closing: make(chan struct{}), registry: new(executionRegistry)}
		rt.InternalSetProfile(nil)
		implCluster.SessionData(s).InternalTypedState = rt
	}
	rt := implCluster.SessionData(s).InternalTypedState.(*InternalSessionRuntime)
	if rt.Core == nil {
		rt.Core = s
	}
	if rt.schema == nil {
		rt.schema = s.SchemaTracker()
	}
	implCluster.SessionData(s).InternalTypedReady.Store(rt)
	return rt
}

// sessionDone is closed once Close starts stopping executions. Waits that
// involve no connection, such as retry delays, select on it: closing the
// connections ends the others, so Close need not cancel each execution.
func (rt *InternalSessionRuntime) sessionDone() <-chan struct{} { return rt.closing }

func (rt *InternalSessionRuntime) metricsSnapshot() metrics.Snapshot {
	cache := implCluster.SessionData(rt.Core).InternalConnCfg
	result := metrics.Snapshot{Requests: rt.requestCount(), Attempts: rt.attempts.Load(), Errors: rt.errors.Load(), Retries: rt.retries.Load(), Speculations: rt.speculations.Load(), Cancelled: rt.cancelled.Load(), InFlight: rt.inflight.Load()}
	if cache != nil {
		stats := cache.InternalReceiveCache.Stats()
		result.ReceiveCacheHits, result.ReceiveCacheMisses = stats.Hits, stats.Misses
		result.ReceiveCacheBytes, result.ReceiveCacheObjects = stats.RetainedBytes, stats.RetainedObjects
	}
	return result
}

// requestCount reports the admitted requests: every request id taken, minus
// the admissions registerExecution refused. The refusals are read first, so a
// refusal racing with this read cannot outnumber the ids it read. The count
// may briefly include an admission that has its id but is about to be refused.
func (rt *InternalSessionRuntime) requestCount() uint64 {
	refused := rt.refusedRegistrations.Load()
	return rt.requestID.Load() - refused
}

func (s *InternalSessionRuntime) prepareTyped(ctx context.Context, statement, keyspace string, options PrepareOptions) (state *InternalPreparedState, err error) {
	rt := s
	generation := s.Core.TypedGeneration()
	tracker := rt.InternalSchemaTracker()
	started := tracker.InternalStart()
	key := prepareCacheKey{InternalCql: statement, InternalKeyspace: keyspace}
	rt.InternalMu.Lock()
	cached := rt.InternalPrepared[key]
	if cached != nil && cached.cacheCurrent() {
		rt.touchPrepare(key)
		rt.InternalMu.Unlock()
		return cached, nil
	}
	if cached != nil {
		delete(rt.InternalPrepared, key)
		rt.forgetPrepareOrder(key)
	}
	if flight := rt.preparing[key]; flight != nil {
		rt.InternalMu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-flight.done:
			return flight.state, flight.err
		}
	}
	flight := &prepareFlight{done: make(chan struct{})}
	if rt.preparing == nil {
		rt.preparing = make(map[prepareCacheKey]*prepareFlight)
	}
	rt.preparing[key] = flight
	rt.InternalMu.Unlock()
	defer func() {
		rt.InternalMu.Lock()
		flight.state = state
		flight.err = err
		delete(rt.preparing, key)
		close(flight.done)
		rt.InternalMu.Unlock()
	}()
	conn := options.privateStorage.InternalConnection
	if conn == nil {
		conn, err = s.Core.SelectTypedConn(ctx, model.RoutingInfo{HostID: options.HostID}, nil)
	}
	if err != nil {
		return nil, err
	}
	wireKeyspace := keyspace
	if !conn.InternalWire().QueryKeyspace {
		if keyspace != implCluster.SessionData(s.Core).InternalCfg.Keyspace && !qualifiedForKeyspace(statement, keyspace) {
			return nil, cqltype.TypeValidation("prepare keyspace", "protocol v4 requires the session keyspace")
		}
		wireKeyspace = ""
	}
	page, err := conn.InternalExecuteTyped(ctx, &model.AttemptRequest{Statement: statement, Keyspace: wireKeyspace, Prepare: true, Trace: options.CQLTrace})
	if page != nil {
		defer page.Owner.Release()
	}
	if err != nil {
		return nil, err
	}
	if page == nil {
		return nil, &cqltype.ProtocolError{Stage: "prepare", Err: fmt.Errorf("missing response")}
	}
	info, err := s.hostPreparedInfo(conn, page, &InternalPreparedState{InternalStatement: statement, InternalKeyspace: keyspace})
	if err != nil {
		return nil, err
	}
	info.Generation = generation
	dependencies := tracker.InternalDependencies(info.Keyspace, page.BindColumns(), page.Columns)
	epoch := dependencies.InternalSince(started)
	result := &model.ResultSchemaIdentity{InternalColumns: page.Columns, InternalEpoch: epoch, InternalTracker: dependencies.InternalTracker}
	state = &InternalPreparedState{InternalSession: s.Core, InternalProtocol: conn.InternalVersion, InternalStatement: statement, InternalKeyspace: keyspace, InternalParameters: page.BindColumns(), hasCounter: hasCounterColumn(page.BindColumns()), InternalColumns: page.Columns, InternalInfo: info, InternalDependencies: dependencies, InternalInitialEpoch: epoch, partitioner: s.Core.PreparedPartitionerName(info.Keyspace, info.Table)}
	state.view.Store(&preparedView{hosts: map[cqltype.UUID]*InternalHostPrepared{conn.InternalHostID(): {InternalId: page.PreparedID(), InternalMetadataID: page.MetadataID, InternalColumns: page.Columns, InternalGeneration: generation, InternalRevision: 1, InternalEpoch: epoch, InternalResult: result}}, result: result, resultHost: conn.InternalHostID()})

	tracker.InternalMu.RLock()
	defer tracker.InternalMu.RUnlock()
	rt.InternalMu.Lock()
	defer rt.InternalMu.Unlock()
	if cached = rt.InternalPrepared[key]; cached != nil && cached.cacheCurrent() {
		rt.touchPrepare(key)
		return cached, nil
	}
	limit := rt.config.PreparedCacheSize
	if rt.InternalProfile == nil && limit == 0 {
		limit = 1000
	}
	if limit == 0 || !epoch.InternalCurrent() {
		return state, nil
	}
	for len(rt.InternalPrepared) >= limit && len(rt.InternalPrepareOrder) > 0 {
		old := rt.InternalPrepareOrder[0]
		rt.InternalPrepareOrder[0] = prepareCacheKey{}
		rt.InternalPrepareOrder = rt.InternalPrepareOrder[1:]
		delete(rt.InternalPrepared, old)
	}
	rt.InternalPrepared[key] = state
	rt.touchPrepare(key)
	return state, nil
}
func (s *InternalSessionRuntime) prepareOnConn(ctx context.Context, conn *transport.Conn, state *InternalPreparedState, values model.EncodedValues, force bool) (*InternalHostPrepared, error) {
	return s.prepareOnConnExpected(ctx, conn, state, values, force, nil)
}
func (s *InternalSessionRuntime) prepareOnConnExpected(ctx context.Context, conn *transport.Conn, state *InternalPreparedState, values model.EncodedValues, force bool, failed *InternalHostPrepared) (*InternalHostPrepared, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.Core.Closed() {
		return nil, runtimeerrors.ErrClosed
	}
	if err := state.reconcilePendingConflict(); err != nil {
		return nil, err
	}
	id := conn.InternalHostID()
	cached := state.host(id)
	if cached != nil && cached.InternalEpoch.InternalCurrent() && (!force || failed != nil && cached != failed) {
		return cached, nil
	}
	epoch := state.InternalDependencies.InternalSnapshot()
	state.InternalMu.Lock()
	cached = state.host(id)
	if cached != nil && cached.InternalEpoch.InternalCurrent() && (!force || failed != nil && cached != failed) {
		state.InternalMu.Unlock()
		return cached, nil
	}
	flight := state.preparing[id]
	if flight == nil || flight.previous != cached || !flight.epoch.InternalEqual(epoch) {
		flight = &hostPrepareFlight{done: make(chan struct{}), previous: cached, epoch: epoch, started: state.InternalDependencies.InternalTracker.InternalStart()}
		if state.preparing == nil {
			state.preparing = make(map[cqltype.UUID]*hostPrepareFlight)
		}
		rt := s
		if !rt.beginPreparedWorker(s.Core) {
			state.InternalMu.Unlock()
			return nil, runtimeerrors.ErrClosed
		}
		state.preparing[id] = flight
		parameters := values.Columns
		go func() {
			defer rt.prepareWorkers.Done()
			timeout := s.config.Metadata.RequestTimeout
			sharedCtx, cancel := InternalExecutionContext(conn.InternalCtx, timeout)
			defer cancel()
			result, err := s.prepareHost(sharedCtx, conn, state, parameters, flight)
			state.InternalMu.Lock()
			flight.result = result
			flight.err = err
			if state.preparing[id] == flight {
				delete(state.preparing, id)
			}
			close(flight.done)
			state.InternalMu.Unlock()
		}()
	}
	flight.waiters.Add(1)
	state.InternalMu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-flight.done:
		return flight.result, flight.err
	}
}
func (s *InternalSessionRuntime) prepareHost(ctx context.Context, conn *transport.Conn, state *InternalPreparedState, parameters cqltype.Columns, flight *hostPrepareFlight) (entry *InternalHostPrepared, err error) {
	conflict := false
	defer func() {
		if conflict {
			if refreshErr := state.noteMetadataConflict(); err == nil {
				err = refreshErr
			}
		}
	}()
	wireKeyspace := state.InternalKeyspace
	if !conn.InternalWire().QueryKeyspace {
		wireKeyspace = ""
	}
	page, err := conn.InternalExecuteTyped(ctx, &model.AttemptRequest{Statement: state.InternalStatement, Keyspace: wireKeyspace, Prepare: true})
	if page != nil {
		defer page.Owner.Release()
	}
	if err != nil {
		return nil, err
	}
	if page == nil {
		return nil, &cqltype.ProtocolError{Stage: "prepare", Err: fmt.Errorf("missing response")}
	}
	if !state.InternalParameters.Equal(page.BindColumns()) || !parameters.Equal(page.BindColumns()) {
		return nil, &runtimeerrors.SchemaChangedError{Statement: state.InternalStatement, Previous: parameters, Current: page.BindColumns()}
	}
	info, err := s.hostPreparedInfo(conn, page, state)
	if err != nil {
		return nil, err
	}
	if !slices.Equal(info.PartitionKeyIndexes, state.InternalInfo.PartitionKeyIndexes) || info.Keyspace != state.InternalInfo.Keyspace || info.Table != state.InternalInfo.Table || info.Kind != state.InternalInfo.Kind || info.Conditional != state.InternalInfo.Conditional || len(state.InternalInfo.PartitionKeyIndexes) > 0 && s.Core.PreparedPartitionerName(info.Keyspace, info.Table) != state.partitioner {
		return nil, &runtimeerrors.SchemaChangedError{Statement: state.InternalStatement, Previous: state.InternalParameters, Current: page.BindColumns()}
	}
	revision := uint64(1)
	if flight.previous != nil {
		revision = flight.previous.InternalRevision + 1
	}
	state.InternalDependencies.InternalExtend(state.InternalInfo.Keyspace, state.InternalParameters, page.Columns)
	epoch := state.InternalDependencies.InternalSince(flight.started)
	result := &model.ResultSchemaIdentity{InternalColumns: page.Columns, InternalEpoch: epoch, InternalTracker: state.InternalDependencies.InternalTracker}
	if current := state.loadView().result; current != nil && current.InternalColumns.Equal(page.Columns) {
		result.InternalColumns = current.InternalColumns
		if current.InternalEpoch.InternalEqual(epoch) {
			result = current
		}
	}
	candidate := &InternalHostPrepared{InternalId: page.PreparedID(), InternalMetadataID: page.MetadataID, InternalColumns: result.InternalColumns, InternalGeneration: s.Core.TypedGeneration(), InternalRevision: revision, InternalEpoch: epoch, InternalResult: result}

	// Exact-entry/epoch CAS. A losing response keeps its own immutable candidate;
	// it is never republished against a newly loaded host revision.
	state.InternalDependencies.InternalTracker.InternalMu.RLock()
	defer state.InternalDependencies.InternalTracker.InternalMu.RUnlock()
	state.InternalMu.Lock()
	defer state.InternalMu.Unlock()
	if v := state.loadView(); v.hosts[conn.InternalHostID()] == flight.previous && flight.epoch.InternalCurrent() && candidate.InternalEpoch.InternalCurrent() {
		conflict = v.resultHost != conn.InternalHostID() && v.result != nil && !v.result.InternalColumns.Equal(result.InternalColumns)
		state.publishHost(conn.InternalHostID(), candidate, !conflict, result)
		if !conflict {
			state.InternalInitialEpoch = candidate.InternalEpoch
			state.InternalColumns = result.InternalColumns
		}
	}
	return candidate, nil
}
func (s *InternalSessionRuntime) hostPreparedInfo(conn *transport.Conn, page *transport.InternalResponsePage, state *InternalPreparedState) (PreparedInfo, error) {
	info := PreparedInfo{Keyspace: state.InternalKeyspace, PartitionKeyIndexes: page.PkIndexes(), Conditional: page.Lwt}
	if conn.InternalFramers.InternalDefaults.InternalFlagLWT != 0 {
		info.Kind = cqltype.OrdinaryStatement
	}
	if page.Lwt {
		info.Kind = cqltype.ConditionalStatement
	}
	for _, columns := range []cqltype.Columns{page.BindColumns(), page.Columns} {
		if col, ok := columns.At(0); ok && info.Table == "" {
			if col.Keyspace != "" {
				info.Keyspace = col.Keyspace
			}
			info.Table = col.Table
		}
	}
	for _, index := range info.PartitionKeyIndexes {
		if index < 0 || index >= page.BindColumns().Len() {
			return PreparedInfo{}, &cqltype.ProtocolError{Stage: "prepare", Err: fmt.Errorf("invalid partition key index %d", index)}
		}
	}
	return info, nil
}

// selectPreparedResponse resolves a response using only the attempt's retained
// immutable entry. Later schema events do not invalidate bytes already received.
type resultMetadataObservation struct {
	metadata                    *model.ResultSchemaIdentity
	columns                     cqltype.Columns
	metadataID                  []byte
	columnCount                 int
	kind                        int
	noMetadata, metadataChanged bool
}

func selectPreparedResponse(state *InternalPreparedState, host cqltype.UUID, prepared *InternalHostPrepared, sentID []byte, tracksID bool, response *transport.InternalResponsePage, observed ...uint64) error {
	v := resultMetadataObservation{columns: response.Columns, columnCount: response.ColumnCount, kind: response.Kind, noMetadata: response.NoMetadata, metadataChanged: response.MetadataChanged, metadataID: response.MetadataID, metadata: response.InternalMetadata}
	err := selectPreparedMetadata(state, host, prepared, sentID, tracksID, &v, observed...)
	response.Columns = v.columns
	response.InternalMetadata = v.metadata
	return err
}
func selectPreparedMetadata(state *InternalPreparedState, host cqltype.UUID, prepared *InternalHostPrepared, sentID []byte, tracksID bool, response *resultMetadataObservation, observed ...uint64) (err error) {
	if response.noMetadata {
		if prepared == nil || prepared.InternalResult == nil || prepared.InternalColumns.Len() != response.columnCount || !bytes.Equal(prepared.InternalMetadataID, sentID) || len(response.metadataID) > 0 && !bytes.Equal(response.metadataID, prepared.InternalMetadataID) || tracksID && len(prepared.InternalMetadataID) == 0 {
			return &cqltype.ProtocolError{Stage: "result metadata", Err: fmt.Errorf("missing matching metadata")}
		}
		response.columns = prepared.InternalColumns
		response.metadata = prepared.InternalResult
		return nil
	}
	if state == nil || prepared == nil || response.kind != 2 {
		return nil
	}
	conflict := false
	defer func() {
		if conflict {
			if refreshErr := state.noteMetadataConflict(); err == nil {
				err = refreshErr
			}
		}
	}()
	v := state.loadView()
	conflict = v.hosts[host] == prepared && v.resultHost != host && v.result != nil && !v.result.InternalColumns.Equal(response.columns)
	changed := !prepared.InternalColumns.Equal(response.columns) || len(response.metadataID) > 0 && !bytes.Equal(prepared.InternalMetadataID, response.metadataID)
	if !changed {
		response.columns = prepared.InternalColumns
		response.metadata = prepared.InternalResult
		return nil
	}
	result := prepared.InternalResult
	if !prepared.InternalColumns.Equal(response.columns) {
		result = &model.ResultSchemaIdentity{InternalColumns: response.columns, InternalTracker: state.InternalDependencies.InternalTracker}
	}
	response.metadata = result
	started := state.InternalDependencies.InternalTracker.InternalStart()
	if len(observed) > 0 {
		started = observed[0]
	}
	state.InternalDependencies.InternalExtend(state.InternalInfo.Keyspace, state.InternalParameters, response.columns)
	candidate := *prepared
	candidate.InternalEpoch = state.InternalDependencies.InternalSince(started)
	if result != prepared.InternalResult {
		result.InternalEpoch = candidate.InternalEpoch
	}
	candidate.InternalRevision++
	candidate.InternalColumns = result.InternalColumns
	candidate.InternalResult = result
	candidate.InternalMetadataChanged = response.metadataChanged
	if len(response.metadataID) > 0 {
		candidate.InternalMetadataID = response.metadataID
	}
	state.InternalDependencies.InternalTracker.InternalMu.RLock()
	defer state.InternalDependencies.InternalTracker.InternalMu.RUnlock()
	state.InternalMu.Lock()
	defer state.InternalMu.Unlock()
	if v := state.loadView(); v.hosts[host] == prepared && prepared.InternalEpoch.InternalCurrent() && candidate.InternalEpoch.InternalCurrent() {
		conflict = conflict || v.resultHost != host && v.result != nil && !v.result.InternalColumns.Equal(response.columns)
		adopt := v.result == prepared.InternalResult && !conflict
		state.publishHost(host, &candidate, adopt, result)
		if adopt {
			state.InternalInitialEpoch = candidate.InternalEpoch
			state.InternalColumns = result.InternalColumns
		}
	}
	return nil
}

type logicalExecution struct {
	started  time.Time
	observer settings.Observer
	// observerCtx and requestEnd are what observer.StartRequest returned.
	observerCtx    context.Context
	requestEnd     func(settings.RequestEvent)
	ddlCoordinator atomic.Pointer[cqltype.UUID]
	rt             *InternalSessionRuntime
	request        *InternalExecutionRequest
	lifetime       executionLifetime
	id             uint64
	finished       atomic.Bool
	// attemptTimed is set once an attempt took started as its own start:
	// the first attempt starts with the execution, later ones read the clock.
	attemptTimed bool
}

func (l *logicalExecution) InternalFinish(result model.Result, err error) {
	if l == nil {
		return
	}
	// A reused execution starts unfinished; only the first call records it.
	if !l.finished.CompareAndSwap(false, true) {
		return
	}
	rt := l.rt
	rt.inflight.Add(-1)
	// Failed executions stay out of the percentiles: a burst of timeouts
	// would otherwise pin them at the timeout, and a speculation policy
	// reading them would wait that long before speculating.
	if err != nil {
		rt.errors.Add(1)
	} else {
		rt.RecordV2Latency(l.id, time.Since(l.started))
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		rt.cancelled.Add(1)
	}
	if end := l.requestEnd; end != nil {
		perfAdd(0, 1)
		event := settings.RequestEvent{ID: l.id, Statement: l.request.statement, Parameters: l.requestParameters(), Routing: l.request.routing, BatchStatements: l.batchStatements(), Result: model.CloneResult(result), Err: err}
		rt.ObserveSafely(func() { end(event) })
	}
}

// startRequest calls the observer's StartRequest with the caller's context.
func (l *logicalExecution) startRequest(ctx context.Context) {
	l.observerCtx = ctx
	start := settings.RequestStart{ID: l.id, Started: l.started, Statement: l.request.statement, Parameters: l.requestParameters(), Routing: l.request.routing, BatchStatements: l.batchStatements(), CQLTrace: l.request.options.InternalOptions.CQLTrace}
	l.rt.ObserveSafely(func() {
		observerCtx, end := l.observer.StartRequest(ctx, start)
		if observerCtx != nil {
			l.observerCtx = observerCtx
		}
		l.requestEnd = end
	})
}

// startAttempt calls the observer's StartAttempt, returning the function to
// call with the completed attempt, or nil.
func (l *logicalExecution) startAttempt(start routing.AttemptStart) func(routing.AttemptEvent) {
	if l == nil || l.observer == nil {
		return nil
	}
	var end func(routing.AttemptEvent)
	l.rt.ObserveSafely(func() { end = l.observer.StartAttempt(l.observerCtx, start) })
	return end
}

func (l *logicalExecution) requestParameters() cqltype.Columns {
	if state := l.request.InternalPrepared(); state != nil {
		return state.InternalParameters
	}
	return l.request.InternalValues.Columns
}

func (l *logicalExecution) batchStatements() []string {
	if l.request.batch == nil {
		return nil
	}
	statements := make([]string, 0, len(l.request.batch.entries))
	for _, entry := range l.request.batch.entries {
		statements = append(statements, entry.statement)
	}
	return statements
}

// runtimeOf returns s's runtime, or nil for a nil session so that the
// runtime's methods report the nil session as their callers did.
func runtimeOf(s *implCluster.Core) *InternalSessionRuntime {
	if s == nil {
		return nil
	}
	return InternalRuntimeFor(s)
}

// InternalAdmitExecution admits b for execution on s; tests use it to drive
// admission without executing.
func InternalAdmitExecution[R any](ctx context.Context, s *implCluster.Core, b *Bound[R], options CompiledOptions) (context.Context, context.CancelFunc, *InternalExecutionRequest, error) {
	return runtimeOf(s).admit(ctx, b, options)
}

func (rt *InternalSessionRuntime) admit[R any](ctx context.Context, b *Bound[R], options CompiledOptions) (context.Context, context.CancelFunc, *InternalExecutionRequest, error) {
	if ctx == nil || rt == nil || b == nil || b.privateStorage.InternalRequest.InternalOwner == nil {
		return nil, nil, nil, cqltype.TypeValidation("execution", "nil context, session or binding")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	s := rt.Core
	if s.Closed() {
		return nil, nil, nil, runtimeerrors.ErrClosed
	}
	var request *InternalExecutionRequest
	query := b.privateStorage.InternalRequest.query
	if query != nil {
		request = &query.request
	} else {
		request = new(InternalExecutionRequest)
	}
	*request = b.privateStorage.InternalRequest
	// A query resolved its own options when it was created; a pooled
	// query's come with each call. The request points at shared options;
	// CompiledOptions that need resolving for this session resolve into
	// scratch of the request's own.
	var resolved *InternalResolvedOptions
	if query != nil && !query.perCall && request.batch == nil {
		resolved = query.resolved
	} else if resolved = options.shared(rt); resolved == nil {
		resolved = options.resolve(rt, request.optionsScratch())
	}
	request.options = resolved
	values := &resolved.InternalOptions
	if query := request.query; query != nil {
		request.responseDest = &query.response
	}
	if values.privateStorage.InternalConnection != nil {
		request.connection = values.privateStorage.InternalConnection
	}
	if state := request.InternalPrepared(); state != nil && state.InternalSession != s {
		return nil, nil, nil, cqltype.TypeValidation("session", "prepared statement belongs to another session")
	}
	if request.connection != nil && !implCluster.InternalConnectionOwnedBy(request.connection, s) {
		return nil, nil, nil, cqltype.TypeValidation("connection", "connection belongs to another session")
	}
	request.keyspace = values.Keyspace
	if request.keyspace == "" {
		request.keyspace = b.privateStorage.InternalRequest.keyspace
		if request.keyspace == "" {
			request.keyspace = implCluster.SessionData(s).InternalCfg.Keyspace
		}
	}
	if values.Keyspace != "" && request.routing.Keyspace != "" && values.Keyspace != request.routing.Keyspace {
		return nil, nil, nil, cqltype.TypeValidation("keyspace", "override differs from bound statement")
	}
	if request.routing.Keyspace == "" {
		request.routing.Keyspace = values.Keyspace
		if request.routing.Keyspace == "" {
			request.routing.Keyspace = implCluster.SessionData(s).InternalCfg.Keyspace
		}
	}
	request.routing.Consistency = resolved.InternalProfile.Consistency
	request.routing.HostID = values.HostID
	request.routing.ShardID = values.ShardID
	// SERIAL/LOCAL_SERIAL main consistency is a Paxos read. It is routed and
	// retried like a conditional statement (strict replica order, no
	// speculation, no client timestamp), but its result is ordinary rows, so
	// the none column's up-front conditional rejection does not apply.
	if resolved.InternalProfile.Consistency == cqltype.Serial || resolved.InternalProfile.Consistency == cqltype.LocalSerial {
		request.routing.Kind = cqltype.ConditionalStatement
	}
	request.routing.Conditional = request.routing.Kind != cqltype.OrdinaryStatement
	if err := request.InternalOwner.InternalAdmit(); err != nil {
		return nil, nil, nil, err
	}
	observer := values.Observer
	if observer == nil {
		observer = rt.config.Observer
	}
	inline := query != nil && canUseDirectProfile(resolved.InternalProfile)
	var runctx context.Context
	var cancel context.CancelFunc
	var stop context.CancelFunc // cancels from another goroutine (query or session stop)
	// One clock reading starts both the execution and its deadline.
	started := time.Now()
	if inline && query.execCtx != nil {
		runctx, cancel = query.execCtx.beginAt(ctx, resolved.InternalProfile.Timeout, started)
		stop = query.execCtx.cancelFunc
	} else {
		runctx, cancel = InternalExecutionContext(ctx, resolved.InternalProfile.Timeout)
		stop = cancel
	}
	var logical *logicalExecution
	if inline {
		// A previous generation becomes idle only after all accepted writes
		// and callbacks drain. The direct path has no speculative finishers.
		logical = &query.inlineLogical
	} else {
		logical = new(logicalExecution)
	}
	// Only session-owned executions are joined through their lifetime channel.
	// Reusable queries are joined by queryState.wait.
	var done chan struct{}
	if request.query == nil {
		done = make(chan struct{})
	}
	*logical = logicalExecution{lifetime: executionLifetime{cancel: stop, done: done}, id: rt.requestID.Add(1), started: started, observer: observer, rt: rt, request: request}
	request.InternalLogical = logical
	if err := rt.registerExecution(request.InternalLogical, request.InternalOwner); err != nil {
		// registerExecution refuses a closing or closed session or a closed
		// query; either way this id is not a request.
		rt.refusedRegistrations.Add(1)
		cancel()
		request.InternalLogical.releaseInput()
		return nil, nil, nil, err
	}
	rt.inflight.Add(1)
	if observer != nil {
		logical.startRequest(ctx)
	}
	if values.Timestamp.Set {
		request.timestamp, request.hasTimestamp = values.Timestamp.Value, true
	} else if mode := rt.config.Timestamps.Mode(); mode != settings.ServerTimestampMode && !request.routing.Conditional && !request.hasCounter {
		if mode == settings.DriverTimestampMode {
			request.timestamp = rt.timestamps.next(started)
		} else {
			request.timestamp = rt.config.Timestamps.Generator()()
		}
		request.hasTimestamp = true
	}
	return runctx, cancel, request, nil
}

// Run executes b on s and returns an iterator over its rows.
func Run[R any](ctx context.Context, s *implCluster.Core, b *Bound[R], options CompiledOptions) (Rows[R], error) {
	return runtimeOf(s).run(ctx, b, options)
}

func (rt *InternalSessionRuntime) run[R any](ctx context.Context, b *Bound[R], options CompiledOptions) (Rows[R], error) {
	runctx, cancel, request, err := rt.admit(ctx, b, options)
	if err != nil {
		return Rows[R]{}, err
	}
	response, err := rt.executePage(runctx, request, model.StartPage())
	if err != nil {
		cancel()
		request.InternalLogical.InternalFinish(InternalExecutionErrorResult(err), err)
		request.InternalLogical.releaseInput()
		return Rows[R]{}, err
	}
	return rt.newRowsFromResponse(runctx, cancel, b, request, response)
}

func (rt *InternalSessionRuntime) newRowsFromResponse[R any](runctx context.Context, cancel context.CancelFunc, b *Bound[R], request *InternalExecutionRequest, response *transport.InternalResponsePage) (Rows[R], error) {
	plans := b.privateStorage.plans
	if plans == nil {
		plans = &InternalResultPlanCache[R]{}
	}
	// A reusable query's iterator storage is free again: this execution was
	// admitted only after the previous one drained, which needs its rows
	// closed or stopped. Stale handles carry the previous generation.
	// Elsewhere the first page shares the iterator's allocation.
	holder, gen := b.privateStorage.rows, uint64(0)
	// A pooled query serves other callers once these rows close, so its rows
	// live in storage of their own: a closed handle another goroutine still
	// holds then never reads storage a later execution reuses. A batch's rows
	// live in storage the batch owns, which RunBatch reuses only once they
	// are closed and which numbers its own generations.
	queryOwned := holder != nil && request.query != nil && !request.query.perCall
	if queryOwned {
		gen = request.query.generation.Load()
		*holder = rowsHolder[R]{}
	} else if holder != nil && request.batch != nil {
		*holder = rowsHolder[R]{}
	} else {
		holder = new(rowsHolder[R])
	}
	if err := newPageInto(&holder.page, response, b.privateStorage.InternalRows, plans); err != nil {
		cancel()
		request.InternalLogical.InternalFinish(response.Result, err)
		request.InternalLogical.releaseInput()
		return Rows[R]{}, err
	}
	rows := &holder.rows
	rows.gen.Store(gen)
	rows.privateStorage.cursor = executionCursor(request)
	rows.privateStorage.ctx = runctx
	rows.privateStorage.cancel = cancel
	rows.privateStorage.session = rt.Core
	rows.privateStorage.request = request
	rows.privateStorage.decoder = b.privateStorage.InternalRows
	rows.privateStorage.page = &holder.page
	rows.privateStorage.info = response.Result
	rows.privateStorage.plans = plans
	rows.privateStorage.history = paging.InternalPagingHistory{InternalMaxCount: rt.config.Limits.MaxPagingStates, InternalMaxBytes: rt.config.Limits.MaxPagingStateBytes}
	if queryOwned {
		// A query's iterator lends the query's inline history storage; the
		// history counts from zero, so earlier tokens there are never read.
		rows.privateStorage.history.InternalInline = &request.query.historyInline
	}
	rows.startPrefetch()
	rows.privateStorage.execution = request.InternalLogical
	request.InternalLogical.installStop(rows)
	return newRowsHandle(rows), nil
}

// RunPage executes one page of b, resuming from state. A pageSize of zero uses
// the page size options resolve to; cqltype.Unpaged requests the whole result
// in one response; any other positive value is this page's size.
func RunPage[R any](ctx context.Context, s *implCluster.Core, b *Bound[R], state model.PageState, pageSize int32, options CompiledOptions) (*Page[R], error) {
	return runtimeOf(s).runSinglePage(ctx, b, state, pageSize, options)
}

func (rt *InternalSessionRuntime) runSinglePage[R any](ctx context.Context, b *Bound[R], state model.PageState, pageSize int32, options CompiledOptions) (*Page[R], error) {
	if state.Complete() {
		return nil, runtimeerrors.ErrPagingComplete
	}
	if pageSize < 0 {
		return nil, cqltype.TypeValidation("page size", "must not be negative")
	}
	if v := options.values(); v.Prefetch.Set && v.Prefetch.Value != 0 {
		return nil, &settings.UnsupportedSettingError{Setting: "prefetch", Reason: "single-page execution has no automatic pager"}
	}
	runctx, cancel, request, err := rt.admit(ctx, b, options)
	if err != nil {
		return nil, err
	}
	if pageSize == cqltype.Unpaged {
		pageSize = 0 // no page size on the wire
	} else if pageSize == 0 {
		pageSize = request.options.InternalProfile.PageSize
	}
	if pageSize != request.options.InternalProfile.PageSize {
		// The page size applies to this request only, so it goes in a copy
		// of the shared options. Their parameters template, encoded for the
		// resolved page size, then no longer matches, so the request
		// encodes its parameters field by field.
		request.ownOptions().InternalProfile.PageSize = pageSize
	}
	// A Page outlives its query's generation as a stale handle whose
	// accessors still read its response, so it never uses query storage.
	request.responseDest = nil
	response, err := rt.executePage(runctx, request, state)
	if err != nil {
		cancel()
		request.InternalLogical.InternalFinish(InternalExecutionErrorResult(err), err)
		request.InternalLogical.releaseInput()
		return nil, err
	}
	plans := b.privateStorage.plans
	if plans == nil {
		plans = &InternalResultPlanCache[R]{}
	}
	page, err := newPageWithPlans(response, b.privateStorage.InternalRows, plans, nil)
	if err != nil {
		cancel()
		request.InternalLogical.InternalFinish(response.Result, err)
		request.InternalLogical.releaseInput()
		return nil, err
	}
	page.privateStorage.ctx = runctx
	page.privateStorage.cursor = executionCursor(request)
	page.privateStorage.cancel = cancel
	page.privateStorage.session = rt.Core
	page.privateStorage.release = func() {
		cancel()
		request.InternalLogical.InternalFinish(response.Result, page.privateStorage.err)
		request.InternalLogical.releaseInput()
	}
	request.InternalLogical.installStop(stopFunc(func() {
		request.InternalLogical.InternalFinish(response.Result, runtimeerrors.ErrClosed)
		request.InternalLogical.releaseInput()
	}))
	return page, nil
}

// Exec executes b on s; the statement must not return rows.
func Exec[R any](ctx context.Context, s *implCluster.Core, b *Bound[R], options CompiledOptions) (model.Result, error) {
	return runtimeOf(s).exec(ctx, b, options)
}

func (rt *InternalSessionRuntime) exec[R any](ctx context.Context, b *Bound[R], options CompiledOptions) (model.Result, error) {
	if b == nil {
		return model.Result{}, cqltype.TypeValidation("binding", "nil binding")
	}
	if b.privateStorage.InternalRequest.routing.Kind == cqltype.ConditionalStatement {
		return conditionalExec()
	}
	runctx, cancel, request, err := rt.admit(ctx, b, options)
	if err != nil {
		return InternalExecutionErrorResult(err), err
	}
	response, err := rt.executePage(runctx, request, model.StartPage())
	if err != nil {
		cancel()
		result := InternalExecutionErrorResult(err)
		request.InternalLogical.InternalFinish(result, err)
		request.InternalLogical.releaseInput()
		return result, err
	}
	// Acknowledgements have no row framing or continuation. Avoid constructing
	// an iterator, page, result-plan cache, and no-row decoder on this path.
	if response.Columns.Len() == 0 && response.RowCount == 0 && response.Rows.Len() == 0 && !response.State.More() {
		result := response.Result
		response.Owner.Release()
		cancel()
		request.InternalLogical.InternalFinish(result, nil)
		request.InternalLogical.releaseInput()
		return result, nil
	}
	// Unexpected row results retain full draining, framing validation and the
	// established result-columns error, including all continuation pages.
	execution := &Bound[codec.EmptyRecord]{privateStorage: BoundStorage[codec.EmptyRecord]{InternalRows: execRowsDecoder{}}}
	rows, err := rt.newRowsFromResponse(runctx, cancel, execution, request, response)
	if err != nil {
		return InternalExecutionErrorResult(err), err
	}
	defer rows.Close()
	rows.impl.privateStorage.rejectColumns = true
	var row codec.EmptyRecord
	for rows.NextInto(&row) {
	}
	return rows.Info(), rows.Err()
}

// conditionalExec rejects an Exec of a statement known to be conditional
// before it is sent. It stays out of line so that the check costs exec
// nothing but a compare.
//
//go:noinline
func conditionalExec() (model.Result, error) {
	return model.Result{}, runtimeerrors.ErrConditionalExec
}

type typedAttemptOutcome struct {
	response    *transport.InternalResponsePage
	err         error
	conn        *transport.Conn
	event       routing.AttemptEvent
	consistency cqltype.Consistency
}

// beginAttempt tells the request's observer that an attempt on conn starts
// and returns its target, start time and the observer's end callback.
func (s *InternalSessionRuntime) beginAttempt(request *InternalExecutionRequest, conn *transport.Conn, index int, speculative bool) (model.Target, time.Time, func(routing.AttemptEvent)) {
	target := attemptTarget(request, conn)
	var requestID uint64
	if request.InternalLogical != nil {
		requestID = request.InternalLogical.id
	}
	started := time.Now()
	end := request.InternalLogical.startAttempt(routing.AttemptStart{RequestID: requestID, Attempt: index, Target: target, Started: started, Speculative: speculative})
	return target, started, end
}

// attemptOutcome reports one finished attempt to the observer's end callback
// and the routing policy and returns it for the execution loop.
func (s *InternalSessionRuntime) attemptOutcome(request *InternalExecutionRequest, conn *transport.Conn, target model.Target, index int, started time.Time, response *transport.InternalResponsePage, err error, consistency cqltype.Consistency, speculative bool, end func(routing.AttemptEvent)) typedAttemptOutcome {
	var requestID uint64
	if request.InternalLogical != nil {
		requestID = request.InternalLogical.id
	}
	event := routing.AttemptEvent{RequestID: requestID, Attempt: index, Target: target, Started: started, Latency: time.Since(started), Err: err, CQLTraceID: responseTraceID(response), Cancelled: errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded), Speculative: speculative}
	if end != nil {
		perfAdd(1, 1)
		s.Core.ObserveSafely(func() { end(event) })
	}
	if policy := request.options.InternalProfile.Routing; policy != nil {
		s.Core.ObserveSafely(func() { policy.Observe(event) })
	}
	return typedAttemptOutcome{response: response, err: err, conn: conn, event: event, consistency: consistency}
}

// inlineSpeculation is the state a first attempt running on its execution's
// goroutine shares with the speculative attempts its timer starts.
type inlineSpeculation struct {
	timer   *time.Timer
	due     time.Time // when timer next starts an attempt, or zero
	winner  typedAttemptOutcome
	mu      sync.Mutex
	running bool // the first attempt is still waiting
	won     bool
}

// claimWin records a successful speculative attempt that finished while the
// first attempt still waits, and cancels the page so that the wait ends.
func (sp *inlineSpeculation) claimWin(outcome typedAttemptOutcome, cancelPage context.CancelFunc) bool {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if !sp.running || sp.won {
		return false
	}
	sp.won, sp.winner = true, outcome
	cancelPage()
	return true
}

// speculationOnly reports whether speculation is all that keeps request off
// the direct path.
func speculationOnly(request *InternalExecutionRequest) bool {
	profile := request.options.InternalProfile
	if profile.Speculation == nil || request.InternalLogical == nil || request.connection != nil {
		return false
	}
	profile.Speculation = nil
	return canUseDirectProfile(profile)
}

// speculationDelay returns the delay before the speculative attempt that
// follows started attempts, if request speculates.
func speculationDelay(request *InternalExecutionRequest, started int) (time.Duration, bool) {
	if !request.options.InternalOptions.Idempotent || request.routing.Kind != cqltype.OrdinaryStatement || request.options.InternalProfile.Speculation == nil {
		return 0, false
	}
	perfAdd(2, 1)
	delay, ok := request.options.InternalProfile.Speculation.Delay(started)
	if !ok || delay < 0 {
		return 0, false
	}
	perfAdd(3, 1)
	return delay, true
}

// pagePlan builds the routing plan for one page's attempts.
// tablet is the request's tablet, looked up in metadata.
func (s *InternalSessionRuntime) pagePlan(request *InternalExecutionRequest, metadata *routing.MetadataSnapshot, tablet *routing.InternalTabletLookup, direct bool) (routing.QueryPlan, error) {
	policy := request.options.InternalProfile.Routing
	if policy == nil {
		return nil, nil
	}
	if err := policy.Validate(metadata); err != nil {
		return nil, err
	}
	if builtin, ok := policy.(*routing.InternalRoutingPolicy); ok {
		storage := new(routing.InternalLazyRoutingPlan)
		if direct && request.query != nil { // the plan lives in the query's storage
			storage = &request.query.routing
		}
		return builtin.InternalPlanWith(request.routing, metadata, storage, tablet), nil
	}
	return routing.NewPlan(policy, request.routing, metadata), nil
}

// pinnedUsable reports whether a continuation page may reuse conn.
func pinnedUsable(conn *transport.Conn) bool {
	return conn != nil && !conn.Closed() && (conn.InternalHost == nil || conn.InternalHost.IsUp())
}

func (s *InternalSessionRuntime) executePage(ctx context.Context, request *InternalExecutionRequest, state model.PageState) (*transport.InternalResponsePage, error) {
	direct := canExecuteDirect(request)
	if !direct {
		pageRequest := *request
		pageRequest.responseDest = nil // concurrent attempts cannot share it
		request = &pageRequest         // speculative branches retain this page's routing capture.
	}
	// A deadline context already supplies cancellation for this page's attempts.
	// With no page deadline, retain an independent cancellation scope so a winner
	// still stops speculative siblings without cancelling the logical execution.
	pageTimeout := request.options.InternalProfile.PageTimeout
	var cancel context.CancelFunc = func() {}
	deadline, bounded := ctx.Deadline()
	if !direct {
		ctx, cancel = InternalExecutionContext(ctx, pageTimeout)
	} else if pageTimeout > 0 && (!bounded || time.Until(deadline) > pageTimeout) {
		// Pages of one execution normally run one at a time, so the query's
		// page context is reused; it follows the execution context directly.
		// A page that overlaps another (a prefetch racing a close), or one of
		// an execution without a query, takes a context of its own.
		if query := request.query; query != nil && query.pageCtxBusy.CompareAndSwap(false, true) {
			if query.pageCtx == nil {
				query.pageCtx = newExecutionContext()
			}
			var finish context.CancelFunc
			ctx, finish = query.pageCtx.begin(ctx, pageTimeout)
			cancel = func() { finish(); query.pageCtxBusy.Store(false) }
		} else {
			ctx, cancel = InternalExecutionContext(ctx, pageTimeout)
		}
	}
	defer cancel()
	metadata := s.Core.Metadata()
	request.tabletFeedback = s.Core.CaptureTabletFeedback(request.routing, metadata)
	tablet := lookupRequestTablet(metadata, request)
	request.tabletVersionBlock = tablet.VersionBlock(request.routing)
	if direct && pinnedUsable(request.pinned) {
		// A continuation page goes to the connection that served the previous
		// page; a plan is built only if that attempt has to be retried.
		return s.executePageDirect(ctx, request, state, nil)
	}
	plan, err := s.pagePlan(request, metadata, &tablet, direct)
	if err != nil {
		return nil, err
	}
	if direct {
		return s.executePageDirect(ctx, request, state, plan)
	}
	outcomes := make(chan typedAttemptOutcome)
	type delayedAttempt struct {
		conn        *transport.Conn
		due         time.Time
		consistency cqltype.Consistency
	}
	// Retries waiting for their delay, and a timer for the earliest of them.
	// The nil channels disable their receive cases until a retry actually
	// needs a delay. Most pages complete without scheduling any.
	var delayed []delayedAttempt
	var delayTimer *time.Timer
	var delayFired <-chan time.Time
	var closing <-chan struct{}
	armDelay := func() {
		if len(delayed) == 0 {
			delayFired, closing = nil, nil
			return
		}
		due := delayed[0].due
		for _, d := range delayed[1:] {
			if d.due.Before(due) {
				due = d.due
			}
		}
		if delayTimer == nil {
			delayTimer = time.NewTimer(time.Until(due))
		} else {
			delayTimer.Reset(time.Until(due))
		}
		delayFired, closing = delayTimer.C, s.sessionDone()
	}
	aggregate := model.Result{AchievedConsistency: request.options.InternalProfile.Consistency}
	startedCount, active := 0, 0
	// spec coordinates a first attempt running on this goroutine with the
	// speculative attempts that its timer starts.
	var spec inlineSpeculation
	launch := func(conn *transport.Conn, consistency cqltype.Consistency, speculative bool) error {
		var err error
		if conn == nil && request.connection != nil {
			conn = request.connection
		}
		if conn == nil {
			conn, err = s.Core.SelectTypedConn(ctx, request.routing, plan)
			if err != nil {
				return err
			}
		}
		startedCount++
		active++
		index := startedCount
		request.InternalOwner.retain()
		s.attempts.Add(1)
		if speculative {
			s.speculations.Add(1)
		}
		go func() {
			defer request.InternalOwner.InternalRelease()
			target, started, end := s.beginAttempt(request, conn, index, speculative)
			response, err := s.executeTypedAttempt(ctx, conn, request, state, consistency)
			outcome := s.attemptOutcome(request, conn, target, index, started, response, err, consistency, speculative, end)
			if err == nil && spec.claimWin(outcome, cancel) {
				return
			}
			select {
			case outcomes <- outcome:
			case <-ctx.Done():
				if response != nil {
					response.Owner.Release()
				}
			}
		}()
		return nil
	}
	var firstOutcome *typedAttemptOutcome
	var inline typedAttemptOutcome
	// specDue is when the inline first attempt's speculation timer would
	// next start an attempt, or zero.
	var specDue time.Time
	var won bool
	// ended is set when the page context ended the inline first attempt.
	var ended bool
	if speculationOnly(request) {
		// The first attempt runs on this goroutine. A speculative attempt
		// gets a goroutine of its own only when its timer fires, and stops
		// this wait if it wins.
		consistency := request.options.InternalProfile.Consistency
		conn, err := s.Core.SelectTypedConn(ctx, request.routing, plan)
		if err != nil {
			return nil, newExecutionError(err, aggregate, cqltype.NotExecuted, nil)
		}
		if conn.InternalStreamObserver != nil {
			// A stream-observed connection keeps its attempt on a goroutine.
			if err := launch(conn, consistency, false); err != nil {
				return nil, newExecutionError(err, aggregate, cqltype.NotExecuted, nil)
			}
		} else {
			startedCount, active = 1, 1
			s.attempts.Add(1)
			target, started, end := s.beginAttempt(request, conn, 1, false)
			// The callback takes spec.mu, so even after a zero delay it runs
			// only once the timer is stored.
			spec.mu.Lock()
			spec.running = true
			if delay, ok := speculationDelay(request, startedCount); ok {
				spec.due = started.Add(delay)
				spec.timer = time.AfterFunc(delay, func() {
					spec.mu.Lock()
					defer spec.mu.Unlock()
					if !spec.running || request.InternalLogical != nil && request.InternalLogical.ddlCoordinator.Load() != nil {
						return
					}
					spec.due = time.Time{}
					if launch(nil, consistency, true) == nil {
						if delay, ok := speculationDelay(request, startedCount); ok {
							spec.due = time.Now().Add(delay)
							spec.timer.Reset(delay)
						}
					}
				})
			}
			spec.mu.Unlock()
			response, err := s.executeTypedAttempt(ctx, conn, request, state, consistency)
			spec.mu.Lock()
			spec.running = false
			if spec.timer != nil {
				spec.timer.Stop()
			}
			won, specDue = spec.won, spec.due
			spec.mu.Unlock()
			inline = s.attemptOutcome(request, conn, target, 1, started, response, err, consistency, false, end)
			switch {
			case won && inline.err == nil:
				// Both succeeded before this attempt saw the cancel: keep its
				// response and drop the speculative one.
				if spec.winner.response != nil {
					spec.winner.response.Owner.Release()
				}
				active--
				firstOutcome = &inline
			case won:
				// A speculative attempt won and cancelled the page context,
				// which ended this attempt: the winner is the page's outcome.
				if inline.response != nil {
					inline.response.Owner.Release()
				}
				active--
				inline = spec.winner
				firstOutcome = &inline
			case inline.err != nil && ctx.Err() != nil:
				// The page context (the caller's, or the page timeout) ended
				// the attempt: the loop ends the execution as it does for
				// attempts on goroutines.
				if inline.response != nil {
					inline.response.Owner.Release()
				}
				ended = true
			default:
				firstOutcome = &inline
			}
		}
	} else if err := launch(nil, request.options.InternalProfile.Consistency, false); err != nil {
		return nil, newExecutionError(err, aggregate, cqltype.NotExecuted, nil)
	}
	var timer *time.Timer
	var speculate <-chan time.Time
	arm := func() {
		if timer != nil {
			timer.Stop()
			timer = nil
		}
		speculate = nil
		delay, ok := speculationDelay(request, startedCount)
		if !ok {
			return
		}
		timer = time.NewTimer(delay)
		speculate = timer.C
	}
	if firstOutcome == nil && !ended {
		arm()
	} else if firstOutcome != nil && !won && !specDue.IsZero() && (startedCount > 1 || firstOutcome.err != nil) {
		// Attempts remain: keep the schedule the inline attempt started.
		timer = time.NewTimer(max(time.Until(specDue), 0))
		speculate = timer.C
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
		if delayTimer != nil {
			delayTimer.Stop()
		}
	}()
	var lastErr error
	var log attemptLog
	for {
		var outcome typedAttemptOutcome
		if firstOutcome != nil {
			outcome, firstOutcome = *firstOutcome, nil
		} else {
			received := false
			select {
			case <-ctx.Done():
				aggregate.Attempts = startedCount
				failure := ctx.Err()
				if active > 0 {
					// An abandoned in-flight attempt may have been applied.
					log.raise(cqltype.MaybeExecuted)
				}
				if request.InternalLogical != nil {
					if coordinator := request.InternalLogical.ddlCoordinator.Load(); coordinator != nil {
						aggregate.HostID = *coordinator
						failure = fmt.Errorf("gocql: DDL succeeded; automatic agreement/refresh failed: %w", failure)
						log.raise(cqltype.PartiallyExecuted)
					}
				}
				return nil, log.error(failure, aggregate)
			case <-closing:
				// A closing session ends the delayed retries instead of
				// launching them.
				delayed = delayed[:0]
				armDelay()
				lastErr = runtimeerrors.ErrClosed
				if active == 0 {
					return nil, log.error(lastErr, aggregate)
				}
			case now := <-delayFired:
				due := delayed[:0]
				var launchErr error
				for _, pending := range delayed {
					if pending.due.After(now) {
						due = append(due, pending)
						continue
					}
					if err := launch(pending.conn, pending.consistency, false); err != nil {
						launchErr = err
					} else {
						s.retries.Add(1)
						arm()
					}
				}
				clear(delayed[len(due):])
				delayed = due
				armDelay()
				if launchErr != nil {
					lastErr = launchErr
					if active == 0 && len(delayed) == 0 {
						return nil, log.error(launchErr, aggregate)
					}
				}
			case <-speculate:
				if request.InternalLogical != nil && request.InternalLogical.ddlCoordinator.Load() != nil {
					speculate = nil
					continue
				}
				if err := launch(nil, request.options.InternalProfile.Consistency, true); err == nil {
					arm()
				} else {
					speculate = nil
				}
			case outcome = <-outcomes:
				received = true
			}
			if !received {
				continue
			}
		}
		{
			active--
			response, attemptErr := outcome.response, outcome.err
			aggregate.Attempts = startedCount
			aggregate.Latency += outcome.event.Latency
			aggregate.HostID = outcome.event.Target.HostID
			aggregate.AchievedConsistency = outcome.consistency
			if response != nil {
				mergeResponseResult(&aggregate, response, model.CQLTraceRef{HostID: outcome.event.Target.HostID, Attempt: outcome.event.Attempt, Failed: attemptErr != nil, Speculative: outcome.event.Speculative})
			}
			if attemptErr == nil {
				response.Result = aggregate
				response.Result.Rows = int64(response.RowCount)
				if response.PagingState != nil {
					response.State = model.PageStateOwned(response.PagingState, 1)
				} else {
					response.State = model.CompletePageState()
				}
				return response, nil
			}
			// Automatic post-DDL work cannot cause a successful statement to replay.
			if response != nil && response.SchemaChange() {
				response.Owner.Release()
				log.fail(attemptErr, outcome.conn, outcome.event.Attempt, outcome.consistency, outcome.event.CQLTraceID)
				log.raise(cqltype.PartiallyExecuted)
				return nil, log.error(fmt.Errorf("gocql: DDL succeeded; automatic agreement/refresh failed: %w", attemptErr), aggregate)
			}
			lastErr = attemptErr
			attemptStatus := log.fail(attemptErr, outcome.conn, outcome.event.Attempt, outcome.consistency, outcome.event.CQLTraceID)
			if response != nil {
				response.Owner.Release()
			}
			decision := retry.Decision{}
			if policy := request.options.InternalProfile.Retry; policy != nil {
				decision = policy.Decide(retry.Attempt{Err: attemptErr, Number: outcome.event.Attempt, Idempotent: request.options.InternalOptions.Idempotent, Consistency: outcome.consistency, Conditional: request.routing.Conditional, BytesWritten: attemptStatus != cqltype.NotExecuted, ExecutionStatus: attemptStatus})
			}
			if decision.Action == retry.IgnoreWrite {
				return &transport.InternalResponsePage{ResponseData: model.ResponseData{State: model.CompletePageState(), Result: aggregate}}, nil
			}
			if decision.Action == retry.Stop {
				if active == 0 && len(delayed) == 0 {
					return nil, log.error(lastErr, aggregate)
				}
				continue
			}
			if decision.Action != retry.Same && decision.Action != retry.Next {
				return nil, log.error(cqltype.TypeValidation("retry", "unknown retry action"), aggregate)
			}
			consistency := outcome.consistency
			if decision.Consistency.Set {
				if !settings.InternalValidConsistency(decision.Consistency.Value) {
					return nil, log.error(cqltype.TypeValidation("retry consistency", "invalid consistency"), aggregate)
				}
				consistency = decision.Consistency.Value
			}
			if decision.Delay < 0 {
				return nil, log.error(cqltype.TypeValidation("retry delay", "negative delay"), aggregate)
			}
			conn := outcome.conn
			if decision.Action == retry.Next {
				conn = nil
			}
			if decision.Delay > 0 {
				perfAdd(5, 1)
				delayed = append(delayed, delayedAttempt{conn: conn, consistency: consistency, due: time.Now().Add(decision.Delay)})
				armDelay()
				continue
			}

			if err := launch(conn, consistency, false); err != nil {
				lastErr = err
				if active == 0 {
					return nil, log.error(lastErr, aggregate)
				}
			} else {
				s.retries.Add(1)
				arm()
			}
		}
	}
}

// newAttempt validates request for conn and builds its attempt at state in
// w, without the prepared statement, the batch or the parameters template.
// The caller owns w, so an attempt that does not outlive its caller stays
// off the heap.
func (s *InternalSessionRuntime) newAttempt(w *model.AttemptRequest, conn *transport.Conn, request *InternalExecutionRequest, state model.PageState, consistency cqltype.Consistency) error {
	opts := &request.options.InternalOptions
	version := conn.InternalWire()
	if !version.QueryKeyspace && request.keyspace != "" && request.keyspace != implCluster.SessionData(s.Core).InternalCfg.Keyspace && !requestQualifiedForKeyspace(request, request.keyspace) {
		return cqltype.TypeValidation("keyspace", "protocol v4 requires the session keyspace")
	}
	if opts.NowInSeconds.Set && !version.NowInSeconds {
		return cqltype.TypeValidation("now in seconds", "requires protocol v5")
	}
	*w = model.AttemptRequest{TabletVersionBlock: request.tabletVersionBlock, Statement: request.statement, Values: request.InternalValues.Framed, ValueCount: len(request.InternalValues.Cells), Consistency: consistency, PageSize: int(request.options.InternalProfile.PageSize), Keyspace: request.keyspace, Trace: opts.CQLTrace, Payload: opts.CustomPayload}
	if !version.QueryKeyspace {
		w.Keyspace = ""
	}
	if state.More() {
		w.PagingState = model.PageToken(state)
	}
	if request.options.InternalProfile.SerialConsistency.Set {
		w.SerialConsistency = request.options.InternalProfile.SerialConsistency.Value
	}
	if request.hasTimestamp {
		w.Timestamp = request.timestamp
		w.DefaultTimestamp = true
	}
	if opts.NowInSeconds.Set {
		w.HasNowInSeconds, w.NowInSeconds = true, opts.NowInSeconds.Value
	}
	return nil
}
func (s *InternalSessionRuntime) executeTypedAttempt(ctx context.Context, conn *transport.Conn, request *InternalExecutionRequest, state model.PageState, consistency cqltype.Consistency) (*transport.InternalResponsePage, error) {
	opts := &request.options.InternalOptions
	wireRequest := new(model.AttemptRequest)
	if err := s.newAttempt(wireRequest, conn, request, state, consistency); err != nil {
		return nil, err
	}
	source := request.source()
	prepared, err := source.onConn(ctx, s, conn, request.InternalValues, false, nil)
	if err != nil {
		return nil, err
	}
	setPrepared(wireRequest, prepared, conn, opts.IncludeMetadata)
	if request.batch != nil {
		wireRequest.Batch, err = s.prepareBatchAttempt(ctx, conn, request.batch, nil)
		defer request.batch.releaseAttempt(wireRequest.Batch)
		if err != nil {
			return nil, err
		}
	}
	// After the batch is set, so that a batch takes no template.
	wireRequest.Params = paramsTemplate(request.options, wireRequest, conn.InternalVersion)
	lease := request.InternalOwner.InternalWriterLease(request.InternalValues.Framed)
	defer lease.Release()
	wireRequest.Lease = lease
	// One UNPREPARED repair, or for a batch one per entry: the server names
	// one missing statement per response, and its cache can have evicted
	// several of a batch's statements.
	repairs := 1
	if request.batch != nil {
		repairs = max(1, len(request.batch.entries))
	}
	var repaired []byte
	for repair := 0; repair <= repairs; repair++ {
		observed := source.observe()
		if dest := request.responseDest; dest != nil && prepared != nil {
			// The parser returns these columns for equal result metadata, so
			// that checking the response against them compares identities.
			dest.Columns = prepared.InternalColumns
		}
		response, executeErr := conn.InternalExecuteTypedInto(ctx, wireRequest, request.responseDest)
		if len(wireRequest.PreparedID) > 0 && wireRequest.Batch == nil {
			s.Core.ApplyTypedFeedbackStamped(response, request.routing, request.tabletFeedback, conn.InternalFramers.InternalDefaults)
		}
		if unprepared, ok := errors.AsType[*protocol.UnpreparedError](executeErr); ok && repair < repairs && !bytes.Equal(unprepared.StatementID, repaired) {
			repaired = unprepared.StatementID
			if response != nil {
				response.Owner.Release()
			}
			if source.rejected(unprepared.StatementID, wireRequest.PreparedID) {
				prepared, err = source.onConn(ctx, s, conn, request.InternalValues, true, prepared)
				if err != nil {
					return nil, err
				}
				setPrepared(wireRequest, prepared, conn, opts.IncludeMetadata)
				continue
			}
			if request.batch != nil {
				if err = s.repairBatch(ctx, conn, request.batch, unprepared.StatementID); err != nil {
					return nil, err
				}
				wireRequest.Batch, err = s.prepareBatchAttempt(ctx, conn, request.batch, wireRequest.Batch)
				if err != nil {
					return nil, err
				}
				continue
			}
			return nil, executeErr
		}
		if executeErr != nil {
			return response, executeErr
		}
		if response == nil {
			return nil, &cqltype.ProtocolError{Stage: "execute", Err: fmt.Errorf("missing response")}
		}
		if err := selectPreparedResponse(source.prepared(), conn.InternalHostID(), prepared, wireRequest.ResultMetadataID, conn.InternalTracksResultMetadataID(), response, observed); err != nil {
			// Hand the rejected response back with its error, as for a
			// server error: callers record its CQL trace as a failed
			// attempt's and then release it.
			return response, err
		}

		if response.SchemaChange() {
			if request.InternalLogical != nil {
				coordinator := conn.InternalHostID()
				request.InternalLogical.ddlCoordinator.Store(&coordinator)
			}
			config := s.config.Metadata
			if config.AutomaticAgreement {
				if err := s.Core.AwaitV2SchemaAgreementOn(ctx, conn); err != nil {
					return response, err
				}
			}
			if config.AutomaticAgreement && config.RefreshAfterAgreement {
				var refreshErr error
				if response.Keyspace() != "" {
					refreshErr = s.Core.RefreshV2Keyspace(ctx, response.Keyspace())
				} else {
					refreshErr = s.Core.RefreshMetadata(ctx)
				}
				if refreshErr != nil && !errors.Is(refreshErr, runtimeerrors.ErrKeyspaceDoesNotExist) {
					return response, refreshErr
				}
			}
		}
		return response, nil
	}
	return nil, fmt.Errorf("gocql: failed unprepared repair")
}

func (rt *InternalSessionRuntime) forgetPrepareOrder(key prepareCacheKey) {
	order := rt.InternalPrepareOrder[:0]
	for _, old := range rt.InternalPrepareOrder {
		if old != key {
			order = append(order, old)
		}
	}
	clear(rt.InternalPrepareOrder[len(order):])
	rt.InternalPrepareOrder = order
}
func (rt *InternalSessionRuntime) touchPrepare(key prepareCacheKey) {
	rt.forgetPrepareOrder(key)
	rt.InternalPrepareOrder = append(rt.InternalPrepareOrder, key)
}

func InternalExecutionErrorResult(err error) model.Result {
	if failure, ok := errors.AsType[*ExecutionError](err); ok {
		return failure.Result
	}
	return model.Result{}
}
func attemptBytesWritten(err error) bool {
	return runtimeerrors.ExecutionStatusOf(err) != cqltype.NotExecuted
}

// hasCounterColumn reports whether columns, a statement's parameters, include
// a counter: executing the statement is then a counter write.
func hasCounterColumn(columns cqltype.Columns) bool {
	for _, column := range cqltype.ColumnSlice(columns) {
		if column.Type.Kind() == cqltype.TypeCounter {
			return true
		}
	}
	return false
}

// lookupRequestTablet finds the tablet of request's token in m, through its
// prepared statement's placement cache when it has one.
func lookupRequestTablet(m *routing.MetadataSnapshot, request *InternalExecutionRequest) routing.InternalTabletLookup {
	if source, ok := request.InternalSource.(preparedSource); ok && source.state != nil {
		return routing.InternalLookupTabletCached(m, request.routing, &source.state.tabletPlacement)
	}
	return routing.InternalLookupTablet(m, request.routing)
}

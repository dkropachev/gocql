// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/retry"

	routing "github.com/scylladb/gocql/v2/internal/routing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"

	"bytes"
	"context"
	"errors"
	"fmt"

	model "github.com/scylladb/gocql/v2/internal/model"

	"slices"
	"sync"
	"sync/atomic"
	"time"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
)

type InternalPrepareCacheKey struct {
	InternalCql, InternalKeyspace string
}
type InternalPrepareFlight struct {
	done  chan struct{}
	state *InternalPreparedState
	err   error
}
type InternalSessionRuntime struct {
	*implCluster.Session
	InternalExecutions                                           map[*InternalLogicalExecution]struct{}
	queries                                                      map[*queryState]struct{}
	schema                                                       *preparedSchemaTracker
	preparing                                                    map[InternalPrepareCacheKey]*InternalPrepareFlight
	InternalProfile                                              *Profile
	InternalPrepared                                             map[InternalPrepareCacheKey]*InternalPreparedState
	pools                                                        map[*queryPoolEntry]struct{}
	InternalPrepareOrder                                         []InternalPrepareCacheKey
	defaultOptions                                               InternalResolvedOptions // the zero Options resolved for this session
	InternalConfig                                               Config
	prepareWorkers                                               sync.WaitGroup
	requestID                                                    atomic.Uint64
	requests, attempts, errors, retries, speculations, cancelled atomic.Uint64
	inflight                                                     atomic.Int64
	prepareWorkersMu                                             sync.Mutex
	InternalMu                                                   sync.Mutex
	prepareWorkersClosed                                         bool
}

func InternalRuntimeFor(s *implCluster.Session) *InternalSessionRuntime {
	// The runtime is created once per session and never replaced, so every
	// call after initialization takes this lock-free path.
	if rt, ok := implCluster.SessionData(s).InternalTypedReady.Load().(*InternalSessionRuntime); ok {
		return rt
	}
	implCluster.SessionData(s).InternalTypedMu.Lock()
	defer implCluster.SessionData(s).InternalTypedMu.Unlock()
	if implCluster.SessionData(s).InternalTypedState == nil {
		rt := &InternalSessionRuntime{Session: s, InternalPrepared: make(map[InternalPrepareCacheKey]*InternalPreparedState)}
		rt.InternalSetProfile(nil)
		implCluster.SessionData(s).InternalTypedState = rt
	}
	rt := implCluster.SessionData(s).InternalTypedState.(*InternalSessionRuntime)
	if rt.Session == nil {
		rt.Session = s
	}
	if rt.schema == nil {
		rt.schema = implCluster.InternalSessionSchemaTracker(s)
	}
	implCluster.SessionData(s).InternalTypedReady.Store(rt)
	return rt
}

func (rt *InternalSessionRuntime) metricsSnapshot() MetricsSnapshot {
	cache := implCluster.SessionData(rt.Session).InternalConnCfg
	result := MetricsSnapshot{Requests: rt.requests.Load(), Attempts: rt.attempts.Load(), Errors: rt.errors.Load(), Retries: rt.retries.Load(), Speculations: rt.speculations.Load(), Cancelled: rt.cancelled.Load(), InFlight: rt.inflight.Load()}
	if cache != nil {
		stats := cache.InternalReceiveCache.Stats()
		result.ReceiveCacheHits, result.ReceiveCacheMisses = stats.Hits, stats.Misses
		result.ReceiveCacheBytes, result.ReceiveCacheObjects = stats.RetainedBytes, stats.RetainedObjects
	}
	return result
}

func (s *InternalSessionRuntime) InternalPrepareTyped(ctx context.Context, statement, keyspace string, options PrepareOptions) (state *InternalPreparedState, err error) {
	rt := s
	generation := implCluster.InternalSessionTypedGeneration(s.Session)
	tracker := rt.InternalSchemaTracker()
	started := tracker.InternalStart()
	key := InternalPrepareCacheKey{InternalCql: statement, InternalKeyspace: keyspace}
	rt.InternalMu.Lock()
	cached := rt.InternalPrepared[key]
	if cached != nil && cached.InternalCacheCurrent() {
		rt.InternalTouchPrepare(key)
		rt.InternalMu.Unlock()
		return cached, nil
	}
	if cached != nil {
		delete(rt.InternalPrepared, key)
		rt.InternalForgetPrepareOrder(key)
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
	flight := &InternalPrepareFlight{done: make(chan struct{})}
	if rt.preparing == nil {
		rt.preparing = make(map[InternalPrepareCacheKey]*InternalPrepareFlight)
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
		conn, err = implCluster.InternalSessionSelectTypedConn(s.Session, ctx, RoutingInfo{HostID: options.HostID}, nil)
	}
	if err != nil {
		return nil, err
	}
	wireKeyspace := keyspace
	if conn.InternalVersion < 5 {
		if keyspace != implCluster.SessionData(s.Session).InternalCfg.Keyspace && !InternalQualifiedForKeyspace(statement, keyspace) {
			return nil, typeValidation("prepare keyspace", "protocol v3/v4 requires the session keyspace")
		}
		wireKeyspace = ""
	}
	page, err := conn.InternalExecuteTyped(ctx, &attemptRequest{Statement: statement, Keyspace: wireKeyspace, Prepare: true, Trace: options.Trace})
	if page != nil {
		defer page.Owner.Release()
	}
	if err != nil {
		return nil, err
	}
	if page == nil {
		return nil, &ProtocolError{Stage: "prepare", Err: fmt.Errorf("missing response")}
	}
	info, err := s.InternalHostPreparedInfo(conn, page, &InternalPreparedState{InternalStatement: statement, InternalKeyspace: keyspace})
	if err != nil {
		return nil, err
	}
	info.Generation = generation
	dependencies := tracker.InternalDependencies(info.Keyspace, page.BindColumns(), page.Columns)
	epoch := dependencies.InternalSince(started)
	result := &typedResultMetadata{InternalColumns: page.Columns, InternalEpoch: epoch, InternalTracker: dependencies.InternalTracker}
	state = &InternalPreparedState{InternalSession: s.Session, InternalProtocol: conn.InternalVersion, InternalStatement: statement, InternalKeyspace: keyspace, InternalParameters: page.BindColumns(), InternalColumns: page.Columns, InternalInfo: info, InternalDependencies: dependencies, InternalInitialEpoch: epoch, InternalResult: result, InternalResultHost: conn.InternalHostID(), InternalPartitioner: implCluster.InternalSessionPreparedPartitionerName(s.Session, info.Keyspace, info.Table), InternalHosts: map[UUID]*InternalHostPrepared{conn.InternalHostID(): {InternalId: page.PreparedID(), InternalMetadataID: page.MetadataID, InternalColumns: page.Columns, InternalGeneration: generation, InternalRevision: 1, InternalEpoch: epoch, InternalResult: result}}}

	tracker.InternalMu.RLock()
	defer tracker.InternalMu.RUnlock()
	rt.InternalMu.Lock()
	defer rt.InternalMu.Unlock()
	if cached = rt.InternalPrepared[key]; cached != nil && cached.InternalCacheCurrent() {
		rt.InternalTouchPrepare(key)
		return cached, nil
	}
	limit := rt.InternalConfig.PreparedCacheSize
	if rt.InternalProfile == nil && limit == 0 {
		limit = 1000
	}
	if limit == 0 || !epoch.InternalCurrent() {
		return state, nil
	}
	for len(rt.InternalPrepared) >= limit && len(rt.InternalPrepareOrder) > 0 {
		old := rt.InternalPrepareOrder[0]
		rt.InternalPrepareOrder[0] = InternalPrepareCacheKey{}
		rt.InternalPrepareOrder = rt.InternalPrepareOrder[1:]
		delete(rt.InternalPrepared, old)
	}
	rt.InternalPrepared[key] = state
	rt.InternalTouchPrepare(key)
	return state, nil
}
func (s *InternalSessionRuntime) InternalPrepareOnConn(ctx context.Context, conn *Conn, state *InternalPreparedState, values encodedValues, force bool) (*InternalHostPrepared, error) {
	return s.InternalPrepareOnConnExpected(ctx, conn, state, values, force, nil)
}
func (s *InternalSessionRuntime) InternalPrepareOnConnExpected(ctx context.Context, conn *Conn, state *InternalPreparedState, values encodedValues, force bool, failed *InternalHostPrepared) (*InternalHostPrepared, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.Session.Closed() {
		return nil, ErrClosed
	}
	if err := state.InternalReconcilePendingConflict(); err != nil {
		return nil, err
	}
	id := conn.InternalHostID()
	state.InternalMu.RLock()
	cached := state.InternalHosts[id]
	state.InternalMu.RUnlock()
	if cached != nil && cached.InternalEpoch.InternalCurrent() && (!force || failed != nil && cached != failed) {
		return cached, nil
	}
	epoch := state.InternalDependencies.InternalSnapshot()
	state.InternalMu.Lock()
	cached = state.InternalHosts[id]
	if cached != nil && cached.InternalEpoch.InternalCurrent() && (!force || failed != nil && cached != failed) {
		state.InternalMu.Unlock()
		return cached, nil
	}
	flight := state.InternalPreparing[id]
	if flight == nil || flight.previous != cached || !flight.epoch.InternalEqual(epoch) {
		flight = &InternalHostPrepareFlight{done: make(chan struct{}), previous: cached, epoch: epoch, started: state.InternalDependencies.InternalTracker.InternalStart()}
		if state.InternalPreparing == nil {
			state.InternalPreparing = make(map[UUID]*InternalHostPrepareFlight)
		}
		rt := s
		if !rt.beginPreparedWorker(s.Session) {
			state.InternalMu.Unlock()
			return nil, ErrClosed
		}
		state.InternalPreparing[id] = flight
		parameters := values.Columns
		go func() {
			defer rt.prepareWorkers.Done()
			timeout := s.InternalConfig.Metadata.RequestTimeout
			sharedCtx, cancel := InternalExecutionContext(conn.InternalCtx, timeout)
			defer cancel()
			result, err := s.prepareHost(sharedCtx, conn, state, parameters, flight)
			state.InternalMu.Lock()
			flight.result = result
			flight.err = err
			if state.InternalPreparing[id] == flight {
				delete(state.InternalPreparing, id)
			}
			close(flight.done)
			state.InternalMu.Unlock()
		}()
	}
	flight.InternalWaiters.Add(1)
	state.InternalMu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-flight.done:
		return flight.result, flight.err
	}
}
func (s *InternalSessionRuntime) prepareHost(ctx context.Context, conn *Conn, state *InternalPreparedState, parameters Columns, flight *InternalHostPrepareFlight) (entry *InternalHostPrepared, err error) {
	conflict := false
	defer func() {
		if conflict {
			if refreshErr := state.InternalNoteMetadataConflict(); err == nil {
				err = refreshErr
			}
		}
	}()
	wireKeyspace := state.InternalKeyspace
	if conn.InternalVersion < 5 {
		wireKeyspace = ""
	}
	page, err := conn.InternalExecuteTyped(ctx, &attemptRequest{Statement: state.InternalStatement, Keyspace: wireKeyspace, Prepare: true})
	if page != nil {
		defer page.Owner.Release()
	}
	if err != nil {
		return nil, err
	}
	if page == nil {
		return nil, &ProtocolError{Stage: "prepare", Err: fmt.Errorf("missing response")}
	}
	if !state.InternalParameters.Equal(page.BindColumns()) || !parameters.Equal(page.BindColumns()) {
		return nil, &SchemaChangedError{Statement: state.InternalStatement, Previous: parameters, Current: page.BindColumns()}
	}
	info, err := s.InternalHostPreparedInfo(conn, page, state)
	if err != nil {
		return nil, err
	}
	if !slices.Equal(info.PartitionKeyIndexes, state.InternalInfo.PartitionKeyIndexes) || info.Keyspace != state.InternalInfo.Keyspace || info.Table != state.InternalInfo.Table || info.Kind != state.InternalInfo.Kind || info.Conditional != state.InternalInfo.Conditional || len(state.InternalInfo.PartitionKeyIndexes) > 0 && implCluster.InternalSessionPreparedPartitionerName(s.Session, info.Keyspace, info.Table) != state.InternalPartitioner {
		return nil, &SchemaChangedError{Statement: state.InternalStatement, Previous: state.InternalParameters, Current: page.BindColumns()}
	}
	revision := uint64(1)
	if flight.previous != nil {
		revision = flight.previous.InternalRevision + 1
	}
	state.InternalDependencies.InternalExtend(state.InternalInfo.Keyspace, state.InternalParameters, page.Columns)
	epoch := state.InternalDependencies.InternalSince(flight.started)
	result := &typedResultMetadata{InternalColumns: page.Columns, InternalEpoch: epoch, InternalTracker: state.InternalDependencies.InternalTracker}
	state.InternalMu.RLock()
	if state.InternalResult != nil && state.InternalResult.InternalColumns.Equal(page.Columns) {
		result.InternalColumns = state.InternalResult.InternalColumns
		if state.InternalResult.InternalEpoch.InternalEqual(epoch) {
			result = state.InternalResult
		}
	}
	state.InternalMu.RUnlock()
	candidate := &InternalHostPrepared{InternalId: page.PreparedID(), InternalMetadataID: page.MetadataID, InternalColumns: result.InternalColumns, InternalGeneration: implCluster.InternalSessionTypedGeneration(s.Session), InternalRevision: revision, InternalEpoch: epoch, InternalResult: result}

	// Exact-entry/epoch CAS. A losing response keeps its own immutable candidate;
	// it is never republished against a newly loaded host revision.
	state.InternalDependencies.InternalTracker.InternalMu.RLock()
	defer state.InternalDependencies.InternalTracker.InternalMu.RUnlock()
	state.InternalMu.Lock()
	defer state.InternalMu.Unlock()
	if state.InternalHosts[conn.InternalHostID()] == flight.previous && flight.epoch.InternalCurrent() && candidate.InternalEpoch.InternalCurrent() {
		state.InternalHosts[conn.InternalHostID()] = candidate
		conflict = state.InternalResultHost != conn.InternalHostID() && state.InternalResult != nil && !state.InternalResult.InternalColumns.Equal(result.InternalColumns)
		if !conflict {
			state.InternalResultHost = conn.InternalHostID()
			state.InternalResult = result
			state.InternalInitialEpoch = candidate.InternalEpoch
			state.InternalColumns = result.InternalColumns
		}
	}
	return candidate, nil
}
func (s *InternalSessionRuntime) InternalHostPreparedInfo(conn *Conn, page *responsePage, state *InternalPreparedState) (PreparedInfo, error) {
	info := PreparedInfo{Keyspace: state.InternalKeyspace, PartitionKeyIndexes: page.PkIndexes(), Conditional: page.Lwt}
	if conn.InternalFramers.InternalDefaults.InternalFlagLWT != 0 {
		info.Kind = OrdinaryStatement
	}
	if page.Lwt {
		info.Kind = ConditionalStatement
	}
	for _, columns := range []Columns{page.BindColumns(), page.Columns} {
		if col, ok := columns.At(0); ok && info.Table == "" {
			if col.Keyspace != "" {
				info.Keyspace = col.Keyspace
			}
			info.Table = col.Table
		}
	}
	if conn.InternalVersion < 4 && len(info.PartitionKeyIndexes) == 0 && info.Table != "" {
		if ks := routing.SnapshotData(s.Session.Metadata()).InternalKeyspaces[info.Keyspace]; ks != nil {
			if table := ks.Tables[info.Table]; table != nil {
				for _, key := range table.PartitionKey {
					index := -1
					for i, c := range cqltype.ColumnSlice(page.BindColumns()) {
						if c.Name == key.Name && c.Table == info.Table && c.Keyspace == info.Keyspace {
							index = i
							break
						}
					}
					if index < 0 {
						info.PartitionKeyIndexes = nil
						break
					}
					info.PartitionKeyIndexes = append(info.PartitionKeyIndexes, index)
				}
			}
		}
	}
	for _, index := range info.PartitionKeyIndexes {
		if index < 0 || index >= page.BindColumns().Len() {
			return PreparedInfo{}, &ProtocolError{Stage: "prepare", Err: fmt.Errorf("invalid partition key index %d", index)}
		}
	}
	return info, nil
}

// selectPreparedResponse resolves a response using only the attempt's retained
// immutable entry. Later schema events do not invalidate bytes already received.
type InternalResultMetadataObservation struct {
	metadata                    *typedResultMetadata
	columns                     Columns
	metadataID                  []byte
	columnCount                 int
	kind                        int
	noMetadata, metadataChanged bool
}

func InternalSelectPreparedResponse(state *InternalPreparedState, host UUID, prepared *InternalHostPrepared, sentID []byte, tracksID bool, response *responsePage, observed ...uint64) error {
	v := InternalResultMetadataObservation{columns: response.Columns, columnCount: response.ColumnCount, kind: response.Kind, noMetadata: response.NoMetadata, metadataChanged: response.MetadataChanged, metadataID: response.MetadataID, metadata: response.InternalMetadata}
	err := InternalSelectPreparedMetadata(state, host, prepared, sentID, tracksID, &v, observed...)
	response.Columns = v.columns
	response.InternalMetadata = v.metadata
	return err
}
func InternalSelectPreparedMetadata(state *InternalPreparedState, host UUID, prepared *InternalHostPrepared, sentID []byte, tracksID bool, response *InternalResultMetadataObservation, observed ...uint64) (err error) {
	if response.noMetadata {
		if prepared == nil || prepared.InternalResult == nil || prepared.InternalColumns.Len() != response.columnCount || !bytes.Equal(prepared.InternalMetadataID, sentID) || len(response.metadataID) > 0 && !bytes.Equal(response.metadataID, prepared.InternalMetadataID) || tracksID && len(prepared.InternalMetadataID) == 0 {
			return &ProtocolError{Stage: "result metadata", Err: fmt.Errorf("missing matching metadata")}
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
			if refreshErr := state.InternalNoteMetadataConflict(); err == nil {
				err = refreshErr
			}
		}
	}()
	state.InternalMu.RLock()
	conflict = state.InternalHosts[host] == prepared && state.InternalResultHost != host && state.InternalResult != nil && !state.InternalResult.InternalColumns.Equal(response.columns)
	state.InternalMu.RUnlock()
	changed := !prepared.InternalColumns.Equal(response.columns) || len(response.metadataID) > 0 && !bytes.Equal(prepared.InternalMetadataID, response.metadataID)
	if !changed {
		response.columns = prepared.InternalColumns
		response.metadata = prepared.InternalResult
		return nil
	}
	result := prepared.InternalResult
	if !prepared.InternalColumns.Equal(response.columns) {
		result = &typedResultMetadata{InternalColumns: response.columns, InternalTracker: state.InternalDependencies.InternalTracker}
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
	if state.InternalHosts[host] == prepared && prepared.InternalEpoch.InternalCurrent() && candidate.InternalEpoch.InternalCurrent() {
		conflict = conflict || state.InternalResultHost != host && state.InternalResult != nil && !state.InternalResult.InternalColumns.Equal(response.columns)
		state.InternalHosts[host] = &candidate
		if state.InternalResult == prepared.InternalResult && !conflict {
			state.InternalResultHost = host
			state.InternalResult = result
			state.InternalInitialEpoch = candidate.InternalEpoch
			state.InternalColumns = result.InternalColumns
		}
	}
	return nil
}

type InternalLogicalExecution struct {
	started        time.Time
	observer       Observer
	ddlCoordinator atomic.Pointer[UUID]
	session        *implCluster.Session
	request        *InternalExecutionRequest
	lifetime       InternalExecutionLifetime
	id             uint64
	finished       atomic.Bool
	// attemptTimed is set once an attempt took started as its own start:
	// the first attempt starts with the execution, later ones read the clock.
	attemptTimed bool
}

func (l *InternalLogicalExecution) InternalFinish(result Result, err error) {
	if l == nil {
		return
	}
	// A reused execution starts unfinished; only the first call records it.
	if !l.finished.CompareAndSwap(false, true) {
		return
	}
	rt := InternalRuntimeFor(l.session)
	rt.inflight.Add(-1)
	implCluster.InternalSessionRecordV2Latency(l.session, time.Since(l.started))
	if err != nil {
		rt.errors.Add(1)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		rt.cancelled.Add(1)
	}
	if l.observer != nil {
		perfAdd(0, 1)
		event := RequestEvent{ID: l.id, Statement: l.request.InternalStatement, Parameters: l.request.InternalValues.Columns, Routing: l.request.InternalRouting, Result: cloneResult(result), Err: err}
		if state := l.request.InternalPrepared(); state != nil {
			event.Parameters = state.InternalParameters
		}
		if l.request.batch != nil {
			for _, entry := range l.request.batch.items {
				event.BatchStatements = append(event.BatchStatements, entry.InternalStatement)
			}
		}
		implCluster.InternalSessionObserveSafely(l.session, func() { l.observer.ObserveRequest(event) })
	}
}

func InternalAdmitExecution[R any](ctx context.Context, s *implCluster.Session, b *Bound[R], options Options) (context.Context, context.CancelFunc, *InternalExecutionRequest, error) {
	if ctx == nil || s == nil || b == nil || b.privateStorage.InternalRequest.InternalOwner == nil {
		return nil, nil, nil, typeValidation("execution", "nil context, session or binding")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	if s.Closed() {
		return nil, nil, nil, ErrClosed
	}
	rt := InternalRuntimeFor(s)
	var request *InternalExecutionRequest
	query := b.privateStorage.InternalRequest.query
	if query != nil {
		request = &query.request
	} else {
		request = new(InternalExecutionRequest)
	}
	*request = b.privateStorage.InternalRequest
	// A query resolved its own options when it was created; a pooled
	// query's come with each call. Options that need resolving for this
	// session resolve straight into the request.
	var resolved *InternalResolvedOptions
	if query != nil && !query.perCall {
		resolved = query.resolved
	} else {
		resolved = options.resolve(rt, &request.InternalOptions)
	}
	if resolved != &request.InternalOptions {
		request.InternalOptions = *resolved
	}
	values := &request.InternalOptions.InternalOptions
	if query := request.query; query != nil {
		request.responseDest = &query.response
	}
	if values.privateStorage.InternalConnection != nil {
		request.connection = values.privateStorage.InternalConnection
	}
	if state := request.InternalPrepared(); state != nil && state.InternalSession != s {
		return nil, nil, nil, typeValidation("session", "prepared statement belongs to another session")
	}
	if request.connection != nil && !implCluster.InternalConnectionOwnedBy(request.connection, s) {
		return nil, nil, nil, typeValidation("connection", "connection belongs to another session")
	}
	request.keyspace = values.Keyspace
	if request.keyspace == "" {
		request.keyspace = b.privateStorage.InternalRequest.keyspace
		if request.keyspace == "" {
			request.keyspace = implCluster.SessionData(s).InternalCfg.Keyspace
		}
	}
	if values.Keyspace != "" && request.InternalRouting.Keyspace != "" && values.Keyspace != request.InternalRouting.Keyspace {
		return nil, nil, nil, typeValidation("keyspace", "override differs from bound statement")
	}
	if request.InternalRouting.Keyspace == "" {
		request.InternalRouting.Keyspace = values.Keyspace
		if request.InternalRouting.Keyspace == "" {
			request.InternalRouting.Keyspace = implCluster.SessionData(s).InternalCfg.Keyspace
		}
	}
	request.InternalRouting.Consistency = resolved.InternalProfile.Consistency
	request.InternalRouting.HostID = values.HostID
	request.InternalRouting.ShardID = values.ShardID
	if resolved.InternalProfile.Consistency == Serial || resolved.InternalProfile.Consistency == LocalSerial {
		request.InternalRouting.Kind = ConditionalStatement
	}
	request.InternalRouting.Conditional = request.InternalRouting.Kind != OrdinaryStatement
	if err := request.InternalOwner.InternalAdmit(); err != nil {
		return nil, nil, nil, err
	}
	observer := values.Observer
	if observer == nil {
		observer = rt.InternalConfig.Observer
	}
	inline := query != nil && observer == nil && rt.InternalConfig.CoalesceWait == 0 && canUseDirectProfile(resolved.InternalProfile)
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
	var logical *InternalLogicalExecution
	if inline {
		// A previous generation becomes idle only after all accepted writes
		// and callbacks drain. The direct path has no speculative finishers.
		logical = &query.inlineLogical
	} else {
		logical = new(InternalLogicalExecution)
	}
	// Only session-owned executions are joined through their lifetime channel.
	// Reusable queries are joined by queryState.wait.
	var done chan struct{}
	if request.query == nil {
		done = make(chan struct{})
	}
	*logical = InternalLogicalExecution{lifetime: InternalExecutionLifetime{cancel: stop, done: done}, id: rt.requestID.Add(1), started: started, observer: observer, session: s, request: request}
	request.InternalLogical = logical
	if err := InternalRuntimeFor(s).registerExecution(request.InternalLogical, request.InternalOwner); err != nil {
		cancel()
		request.InternalLogical.InternalReleaseInput()
		return nil, nil, nil, err
	}
	rt.requests.Add(1)
	rt.inflight.Add(1)
	if !request.InternalRouting.Conditional && !InternalRequestHasCounter(request) && !request.InternalOptions.InternalOptions.Timestamp.Set && rt.InternalConfig.TimestampGenerator != nil {
		request.InternalOptions.InternalOptions.Timestamp = Option[int64]{Value: rt.InternalConfig.TimestampGenerator(), Set: true}
	}
	return runctx, cancel, request, nil
}
func Run[R any](ctx context.Context, s *implCluster.Session, b *Bound[R], options Options) (Rows[R], error) {
	runctx, cancel, request, err := InternalAdmitExecution(ctx, s, b, options)
	if err != nil {
		return Rows[R]{}, err
	}
	response, err := InternalRuntimeFor(s).executePage(runctx, request, StartPage())
	if err != nil {
		cancel()
		request.InternalLogical.InternalFinish(InternalExecutionErrorResult(err), err)
		request.InternalLogical.InternalReleaseInput()
		return Rows[R]{}, err
	}
	return newRowsFromResponse(runctx, cancel, s, b, request, response)
}
func newRowsFromResponse[R any](runctx context.Context, cancel context.CancelFunc, s *implCluster.Session, b *Bound[R], request *InternalExecutionRequest, response *responsePage) (Rows[R], error) {
	plans := b.privateStorage.plans
	if plans == nil {
		plans = &InternalResultPlanCache[R]{}
	}
	// A reusable query's iterator storage is free again: this execution was
	// admitted only after the previous one drained, which needs its rows
	// closed or stopped. Stale handles carry the previous generation.
	// Elsewhere the first page shares the iterator's allocation.
	holder, gen := b.privateStorage.rows, uint64(0)
	queryOwned := holder != nil && request.query != nil
	if queryOwned {
		gen = request.query.generation.Load()
		*holder = rowsHolder[R]{}
	} else {
		holder = new(rowsHolder[R])
	}
	if err := newPageInto(&holder.page, response, b.privateStorage.InternalRows, plans); err != nil {
		cancel()
		request.InternalLogical.InternalFinish(response.Result, err)
		request.InternalLogical.InternalReleaseInput()
		return Rows[R]{}, err
	}
	rows := &holder.rows
	rows.gen.Store(gen)
	rt := InternalRuntimeFor(s)
	rows.privateStorage.cursor = executionCursor(request)
	rows.privateStorage.InternalCtx = runctx
	rows.privateStorage.cancel = cancel
	rows.privateStorage.session = s
	rows.privateStorage.InternalRequest = request
	rows.privateStorage.decoder = b.privateStorage.InternalRows
	rows.privateStorage.InternalPage = &holder.page
	rows.privateStorage.info = response.Result
	rows.privateStorage.plans = plans
	rows.privateStorage.InternalHistory = pagingHistory{InternalMaxCount: rt.InternalConfig.Limits.MaxPagingStates, InternalMaxBytes: rt.InternalConfig.Limits.MaxPagingStateBytes}
	if queryOwned {
		// A query's iterator lends the query's inline history storage; the
		// history counts from zero, so earlier tokens there are never read.
		rows.privateStorage.InternalHistory.InternalInline = &request.query.historyInline
	}
	rows.startPrefetch()
	rows.privateStorage.execution = request.InternalLogical
	request.InternalLogical.installStop(rows)
	return newRowsHandle(rows), nil
}
func RunPage[R any](ctx context.Context, s *implCluster.Session, b *Bound[R], state PageState, options Options) (*Page[R], error) {
	return InternalRunSinglePage(ctx, s, b, state, options, false)
}
func RunUnpaged[R any](ctx context.Context, s *implCluster.Session, b *Bound[R], options Options) (*Page[R], error) {
	return InternalRunSinglePage(ctx, s, b, StartPage(), options, true)
}
func InternalRunSinglePage[R any](ctx context.Context, s *implCluster.Session, b *Bound[R], state PageState, options Options, unpaged bool) (*Page[R], error) {
	if state.Complete() {
		return nil, ErrPagingComplete
	}
	if v := options.values(); v.Prefetch.Set && v.Prefetch.Value != 0 {
		return nil, &UnsupportedSettingError{Setting: "prefetch", Reason: "single-page execution has no automatic pager"}
	}
	if unpaged && options.values().PageSize.Set {
		return nil, &UnsupportedSettingError{Setting: "page size", Reason: "unpaged execution has no page size"}
	}
	runctx, cancel, request, err := InternalAdmitExecution(ctx, s, b, options)
	if err != nil {
		return nil, err
	}
	// A Page outlives its query's generation as a stale handle whose
	// accessors still read its response, so it never uses query storage.
	request.responseDest = nil
	if unpaged {
		request.InternalOptions.InternalProfile.PageSize = 0
	}
	response, err := InternalRuntimeFor(s).executePage(runctx, request, state)
	if err != nil {
		cancel()
		request.InternalLogical.InternalFinish(InternalExecutionErrorResult(err), err)
		request.InternalLogical.InternalReleaseInput()
		return nil, err
	}
	if unpaged && response.State.More() {
		response.Owner.Release()
		err = typeValidation("unpaged response", "server returned continuation")
		cancel()
		request.InternalLogical.InternalFinish(response.Result, err)
		request.InternalLogical.InternalReleaseInput()
		return nil, err
	}
	plans := b.privateStorage.plans
	if plans == nil {
		plans = &InternalResultPlanCache[R]{}
	}
	page, err := InternalNewPageWithPlans(response, b.privateStorage.InternalRows, plans, nil)
	if err != nil {
		cancel()
		request.InternalLogical.InternalFinish(response.Result, err)
		request.InternalLogical.InternalReleaseInput()
		return nil, err
	}
	page.privateStorage.InternalCtx = runctx
	page.privateStorage.cursor = executionCursor(request)
	page.privateStorage.cancel = cancel
	page.privateStorage.session = s
	page.privateStorage.InternalRelease = func() {
		cancel()
		request.InternalLogical.InternalFinish(response.Result, page.privateStorage.err)
		request.InternalLogical.InternalReleaseInput()
	}
	request.InternalLogical.installStop(stopFunc(func() {
		request.InternalLogical.InternalFinish(response.Result, ErrClosed)
		request.InternalLogical.InternalReleaseInput()
	}))
	return page, nil
}
func Exec[R any](ctx context.Context, s *implCluster.Session, b *Bound[R], options Options) (Result, error) {
	if b == nil {
		return Result{}, typeValidation("binding", "nil binding")
	}
	runctx, cancel, request, err := InternalAdmitExecution(ctx, s, b, options)
	if err != nil {
		return InternalExecutionErrorResult(err), err
	}
	response, err := InternalRuntimeFor(s).executePage(runctx, request, StartPage())
	if err != nil {
		cancel()
		result := InternalExecutionErrorResult(err)
		request.InternalLogical.InternalFinish(result, err)
		request.InternalLogical.InternalReleaseInput()
		return result, err
	}
	// Acknowledgements have no row framing or continuation. Avoid constructing
	// an iterator, page, result-plan cache, and no-row decoder on this path.
	if response.Columns.Len() == 0 && response.RowCount == 0 && response.Rows.Len() == 0 && !response.State.More() {
		result := response.Result
		response.Owner.Release()
		cancel()
		request.InternalLogical.InternalFinish(result, nil)
		request.InternalLogical.InternalReleaseInput()
		return result, nil
	}
	// Unexpected row results retain full draining, framing validation and the
	// established result-columns error, including all continuation pages.
	execution := &Bound[EmptyRecord]{privateStorage: BoundStorage[EmptyRecord]{InternalRows: InternalExecRowsDecoder{}}}
	rows, err := newRowsFromResponse(runctx, cancel, s, execution, request, response)
	if err != nil {
		return InternalExecutionErrorResult(err), err
	}
	defer rows.Close()
	rows.impl.privateStorage.rejectColumns = true
	for rows.Next() {
	}
	return rows.Info(), rows.Err()
}

type InternalTypedAttemptOutcome struct {
	response    *responsePage
	err         error
	conn        *Conn
	event       AttemptEvent
	consistency Consistency
}

// pagePlan builds the routing plan for one page's attempts.
// tablet is the request's tablet, looked up in metadata.
func (s *InternalSessionRuntime) pagePlan(request *InternalExecutionRequest, metadata *routing.MetadataSnapshot, tablet *routing.InternalTabletLookup, direct bool) (QueryPlan, error) {
	policy := request.InternalOptions.InternalProfile.Routing
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
		return builtin.InternalPlanWith(request.InternalRouting, metadata, storage, tablet), nil
	}
	return routing.NewPlan(policy, request.InternalRouting, metadata), nil
}

// pinnedUsable reports whether a continuation page may reuse conn.
func pinnedUsable(conn *Conn) bool {
	return conn != nil && !conn.Closed() && (conn.InternalHost == nil || conn.InternalHost.IsUp())
}

func (s *InternalSessionRuntime) executePage(ctx context.Context, request *InternalExecutionRequest, state PageState) (*responsePage, error) {
	direct := canExecuteDirect(s, request)
	if !direct {
		pageRequest := *request
		pageRequest.responseDest = nil // concurrent attempts cannot share it
		request = &pageRequest         // speculative branches retain this page's routing capture.
	}
	// A deadline context already supplies cancellation for this page's attempts.
	// With no page deadline, retain an independent cancellation scope so a winner
	// still stops speculative siblings without cancelling the logical execution.
	pageTimeout := request.InternalOptions.InternalProfile.PageTimeout
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
	metadata := s.Session.Metadata()
	request.tabletFeedback = implCluster.InternalSessionCaptureTabletFeedback(s.Session, request.InternalRouting, metadata)
	tablet := lookupRequestTablet(metadata, request)
	request.tabletVersionBlock = tablet.VersionBlock(request.InternalRouting)
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
	outcomes := make(chan InternalTypedAttemptOutcome)
	type delayedAttempt struct {
		conn        *Conn
		consistency Consistency
	}
	// A nil channel disables the receive case until a retry actually needs a
	// delay. Most pages complete without scheduling any delayed attempt.
	var delayed chan delayedAttempt
	scheduled := 0
	aggregate := Result{AchievedConsistency: request.InternalOptions.InternalProfile.Consistency}
	startedCount, active := 0, 0
	launch := func(conn *Conn, consistency Consistency, speculative bool) error {
		var err error
		if conn == nil && request.connection != nil {
			conn = request.connection
		}
		if conn == nil {
			conn, err = implCluster.InternalSessionSelectTypedConn(s.Session, ctx, request.InternalRouting, plan)
			if err != nil {
				return err
			}
		}
		startedCount++
		active++
		index := startedCount
		request.InternalOwner.InternalRetain()
		s.attempts.Add(1)
		if speculative {
			s.speculations.Add(1)
		}
		go func() {
			defer request.InternalOwner.InternalRelease()
			started := time.Now()
			response, err := s.executeTypedAttempt(ctx, conn, request, state, consistency)
			event := AttemptEvent{Attempt: index, Target: Target{HostID: conn.InternalHostID(), ShardID: request.InternalRouting.ShardID}, Started: started, Latency: time.Since(started), Err: err, Cancelled: errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded), Speculative: speculative}
			if implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0 {
				event.Target.ShardID = Option[uint32]{Value: uint32(implEndpoint.ScyllaConnectionFeaturesData(&conn.InternalScyllaSupported).InternalShard), Set: true}
			}
			if request.InternalLogical != nil {
				event.RequestID = request.InternalLogical.id
				if request.InternalLogical.observer != nil {
					perfAdd(1, 1)
					implCluster.InternalSessionObserveSafely(s.Session, func() { request.InternalLogical.observer.ObserveAttempt(event) })
				}
			}
			if policy := request.InternalOptions.InternalProfile.Routing; policy != nil {
				implCluster.InternalSessionObserveSafely(s.Session, func() { policy.Observe(event) })
			}
			select {
			case outcomes <- InternalTypedAttemptOutcome{response: response, err: err, conn: conn, event: event, consistency: consistency}:
			case <-ctx.Done():
				if response != nil {
					response.Owner.Release()
				}
			}
		}()
		return nil
	}
	if err := launch(nil, request.InternalOptions.InternalProfile.Consistency, false); err != nil {
		return nil, &ExecutionError{Err: err, Result: aggregate}
	}
	var timer *time.Timer
	var speculate <-chan time.Time
	arm := func() {
		if timer != nil {
			timer.Stop()
			timer = nil
		}
		speculate = nil
		if !request.InternalOptions.InternalOptions.Idempotent || request.InternalRouting.Kind != OrdinaryStatement || request.InternalOptions.InternalProfile.Speculation == nil {
			return
		}
		perfAdd(2, 1)
		delay, ok := request.InternalOptions.InternalProfile.Speculation.Delay(startedCount, s.Session.Metrics())
		if !ok || delay < 0 {
			return
		}
		perfAdd(3, 1)
		timer = time.NewTimer(delay)
		speculate = timer.C
	}
	arm()
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	var lastErr error
	for {
		select {
		case <-ctx.Done():
			aggregate.Attempts = startedCount
			failure := ctx.Err()
			if request.InternalLogical != nil {
				if coordinator := request.InternalLogical.ddlCoordinator.Load(); coordinator != nil {
					aggregate.HostID = *coordinator
					failure = fmt.Errorf("gocql: DDL succeeded; automatic agreement/refresh failed: %w", failure)
				}
			}
			return nil, &ExecutionError{Err: failure, Result: aggregate}
		case pending := <-delayed:
			scheduled--
			if err := launch(pending.conn, pending.consistency, false); err != nil {
				lastErr = err
				if active == 0 && scheduled == 0 {
					return nil, &ExecutionError{Err: err, Result: aggregate}
				}
			} else {
				s.retries.Add(1)
				arm()
			}
		case <-speculate:
			if request.InternalLogical != nil && request.InternalLogical.ddlCoordinator.Load() != nil {
				speculate = nil
				continue
			}
			if err := launch(nil, request.InternalOptions.InternalProfile.Consistency, true); err == nil {
				arm()
			} else {
				speculate = nil
			}
		case outcome := <-outcomes:
			active--
			response, attemptErr := outcome.response, outcome.err
			aggregate.Attempts = startedCount
			aggregate.Latency += outcome.event.Latency
			aggregate.HostID = outcome.event.Target.HostID
			aggregate.AchievedConsistency = outcome.consistency
			if response != nil {
				mergeResponseResult(&aggregate, response)
			}
			if attemptErr == nil {
				response.Result = aggregate
				response.Result.Rows = int64(response.RowCount)
				if response.PagingState != nil {
					response.State = model.PageStateOwned(response.PagingState, 1)
				} else {
					response.State = completePageState()
				}
				return response, nil
			}
			// Automatic post-DDL work cannot cause a successful statement to replay.
			if response != nil && response.SchemaChange() {
				response.Owner.Release()
				return nil, &ExecutionError{Err: fmt.Errorf("gocql: DDL succeeded; automatic agreement/refresh failed: %w", attemptErr), Result: aggregate}
			}
			lastErr = attemptErr
			if response != nil {
				response.Owner.Release()
			}
			decision := retry.Decision{}
			if policy := request.InternalOptions.InternalProfile.Retry; policy != nil {
				decision = policy.Decide(retry.Attempt{Err: attemptErr, Number: outcome.event.Attempt, Idempotent: request.InternalOptions.InternalOptions.Idempotent, Consistency: outcome.consistency, Conditional: request.InternalRouting.Conditional, BytesWritten: InternalAttemptBytesWritten(attemptErr)})
			}
			if decision.Action == retry.IgnoreWrite {
				return &responsePage{InternalResponseData: responseData{State: completePageState(), Result: aggregate}}, nil
			}
			if decision.Action == retry.Stop {
				if active == 0 && scheduled == 0 {
					return nil, &ExecutionError{Err: lastErr, Result: aggregate}
				}
				continue
			}
			if decision.Action != retry.Same && decision.Action != retry.Next {
				return nil, &ExecutionError{Err: typeValidation("retry", "unknown retry action"), Result: aggregate}
			}
			consistency := outcome.consistency
			if decision.Consistency.Set {
				if !validConsistency(decision.Consistency.Value) {
					return nil, typeValidation("retry consistency", "invalid consistency")
				}
				consistency = decision.Consistency.Value
			}
			if decision.Delay < 0 {
				return nil, typeValidation("retry delay", "negative delay")
			}
			conn := outcome.conn
			if decision.Action == retry.Next {
				conn = nil
			}
			if decision.Delay > 0 {
				if delayed == nil {
					delayed = make(chan delayedAttempt)
				}
				scheduled++
				request.InternalOwner.InternalRetain()
				go func(wait time.Duration, next delayedAttempt, ready chan<- delayedAttempt) {
					defer request.InternalOwner.InternalRelease()
					perfAdd(5, 1)
					timer := time.NewTimer(wait)
					defer timer.Stop()
					select {
					case <-ctx.Done():
						return
					case <-timer.C:
					}
					select {
					case ready <- next:
					case <-ctx.Done():
					}
				}(decision.Delay, delayedAttempt{conn: conn, consistency: consistency}, delayed)
				continue
			}

			if err := launch(conn, consistency, false); err != nil {
				lastErr = err
				if active == 0 {
					return nil, &ExecutionError{Err: lastErr, Result: aggregate}
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
func (s *InternalSessionRuntime) newAttempt(w *attemptRequest, conn *Conn, request *InternalExecutionRequest, state PageState, consistency Consistency) error {
	opts := &request.InternalOptions.InternalOptions
	if conn.InternalVersion < 5 && request.keyspace != "" && request.keyspace != implCluster.SessionData(s.Session).InternalCfg.Keyspace && !InternalRequestQualifiedForKeyspace(request, request.keyspace) {
		return typeValidation("keyspace", "protocol v3/v4 requires the session keyspace")
	}
	if conn.InternalVersion < 4 {
		if len(opts.CustomPayload) > 0 {
			return typeValidation("payload", "requires protocol v4")
		}
		for _, cell := range request.InternalValues.Cells {
			if cell.State == Unset {
				return typeValidation("unset", "requires protocol v4")
			}
		}
	}
	if opts.NowInSeconds.Set && conn.InternalVersion < 5 {
		return typeValidation("now in seconds", "requires protocol v5")
	}
	*w = attemptRequest{TabletVersionBlock: request.tabletVersionBlock, Statement: request.InternalStatement, Values: request.InternalValues.Framed, ValueCount: len(request.InternalValues.Cells), Consistency: consistency, PageSize: int(request.InternalOptions.InternalProfile.PageSize), Keyspace: request.keyspace, Trace: opts.Trace, Payload: opts.CustomPayload}
	if conn.InternalVersion < 5 {
		w.Keyspace = ""
	}
	if state.More() {
		w.PagingState = model.PageToken(state)
	}
	if request.InternalOptions.InternalProfile.SerialConsistency.Set {
		w.SerialConsistency = request.InternalOptions.InternalProfile.SerialConsistency.Value
	}
	if opts.Timestamp.Set {
		w.Timestamp = opts.Timestamp.Value
		w.DefaultTimestamp = true
	}
	if opts.NowInSeconds.Set {
		w.HasNowInSeconds, w.NowInSeconds = true, opts.NowInSeconds.Value
	}
	return nil
}
func (s *InternalSessionRuntime) executeTypedAttempt(ctx context.Context, conn *Conn, request *InternalExecutionRequest, state PageState, consistency Consistency) (*responsePage, error) {
	opts := &request.InternalOptions.InternalOptions
	wireRequest := new(attemptRequest)
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
		wireRequest.Batch, err = s.prepareBatchAttempt(ctx, conn, request.batch)
		if err != nil {
			return nil, err
		}
	}
	// After the batch is set, so that a batch takes no template.
	wireRequest.Params = paramsTemplate(&request.InternalOptions, wireRequest, conn.InternalVersion)
	lease := request.InternalOwner.InternalWriterLease(request.InternalValues.Framed)
	defer lease.Release()
	wireRequest.Lease = lease
	for repair := 0; repair < 2; repair++ {
		observed := source.observe()
		if dest := request.responseDest; dest != nil && prepared != nil {
			// The parser returns these columns for equal result metadata, so
			// that checking the response against them compares identities.
			dest.Columns = prepared.InternalColumns
		}
		response, executeErr := conn.InternalExecuteTypedInto(ctx, wireRequest, request.responseDest)
		if len(wireRequest.PreparedID) > 0 && wireRequest.Batch == nil {
			implCluster.InternalSessionApplyTypedFeedbackStamped(s.Session, response, request.InternalRouting, request.tabletFeedback, conn.InternalFramers.InternalDefaults)
		}
		if unprepared := unpreparedError(executeErr); unprepared != nil && repair == 0 {
			if response != nil {
				response.Owner.Release()
			}
			if source.rejected(unprepared.StatementId, wireRequest.PreparedID) {
				prepared, err = source.onConn(ctx, s, conn, request.InternalValues, true, prepared)
				if err != nil {
					return nil, err
				}
				setPrepared(wireRequest, prepared, conn, opts.IncludeMetadata)
				continue
			}
			if request.batch != nil {
				if err = s.repairBatch(ctx, conn, request.batch, unprepared.StatementId); err != nil {
					return nil, err
				}
				wireRequest.Batch, err = s.prepareBatchAttempt(ctx, conn, request.batch)
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
			return nil, &ProtocolError{Stage: "execute", Err: fmt.Errorf("missing response")}
		}
		if err := InternalSelectPreparedResponse(source.prepared(), conn.InternalHostID(), prepared, wireRequest.ResultMetadataID, conn.InternalTracksResultMetadataID(), response, observed); err != nil {
			response.Owner.Release()
			return nil, err
		}

		if response.SchemaChange() {
			if request.InternalLogical != nil {
				coordinator := conn.InternalHostID()
				request.InternalLogical.ddlCoordinator.Store(&coordinator)
			}
			config := s.InternalConfig.Metadata
			if config.AutomaticAgreement {
				if err := implCluster.InternalSessionAwaitV2SchemaAgreementOn(s.Session, ctx, conn); err != nil {
					return response, err
				}
			}
			if config.AutomaticAgreement && config.RefreshAfterAgreement {
				var refreshErr error
				if response.Keyspace() != "" {
					refreshErr = implCluster.InternalSessionRefreshV2Keyspace(s.Session, ctx, response.Keyspace())
				} else {
					refreshErr = s.Session.RefreshMetadata(ctx)
				}
				if refreshErr != nil && !errors.Is(refreshErr, ErrKeyspaceDoesNotExist) {
					return response, refreshErr
				}
			}
		}
		return response, nil
	}
	return nil, fmt.Errorf("gocql: failed unprepared repair")
}

// unpreparedError extracts an UNPREPARED response. Its errors.As target lives
// here, so successful attempts do not allocate it.
func unpreparedError(err error) *RequestErrUnprepared {
	if err == nil {
		return nil
	}
	var unprepared *RequestErrUnprepared
	if errors.As(err, &unprepared) {
		return unprepared
	}
	return nil
}

func (rt *InternalSessionRuntime) InternalForgetPrepareOrder(key InternalPrepareCacheKey) {
	order := rt.InternalPrepareOrder[:0]
	for _, old := range rt.InternalPrepareOrder {
		if old != key {
			order = append(order, old)
		}
	}
	clear(rt.InternalPrepareOrder[len(order):])
	rt.InternalPrepareOrder = order
}
func (rt *InternalSessionRuntime) InternalTouchPrepare(key InternalPrepareCacheKey) {
	rt.InternalForgetPrepareOrder(key)
	rt.InternalPrepareOrder = append(rt.InternalPrepareOrder, key)
}

func InternalExecutionErrorResult(err error) Result {
	var failure *ExecutionError
	if errors.As(err, &failure) {
		return failure.Result
	}
	return Result{}
}
func InternalAttemptBytesWritten(err error) bool {
	var query *QueryError
	if errors.As(err, &query) {
		return query.PotentiallyExecuted()
	}
	return true
}

func InternalRequestHasCounter(request *InternalExecutionRequest) bool {
	if request.batch != nil && request.batch.kind == CounterBatch {
		return true
	}
	for _, cell := range request.InternalValues.Cells {
		if cell.Typ.Kind() == TypeCounter {
			return true
		}
	}
	return false
}

// lookupRequestTablet finds the tablet of request's token in m, through its
// prepared statement's placement cache when it has one.
func lookupRequestTablet(m *routing.MetadataSnapshot, request *InternalExecutionRequest) routing.InternalTabletLookup {
	if source, ok := request.InternalSource.(preparedSource); ok && source.state != nil {
		return routing.InternalLookupTabletCached(m, request.InternalRouting, &source.state.tabletPlacement)
	}
	return routing.InternalLookupTablet(m, request.InternalRouting)
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/binding"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	routing "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/schemafence"
	"github.com/scylladb/gocql/v2/internal/transport"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

type PrepareOptions struct {
	privateStorage PrepareOptionsStorage
	Keyspace       string
	HostID         cql.Option[cql.UUID]
	CQLTrace       bool
}
type PreparedInfo struct {
	Keyspace, Table     string
	PartitionKeyIndexes []int
	Kind                cql.StatementKind
	Generation          uint64
}
type InternalHostPrepared struct {
	InternalResult                 *model.ResultSchemaIdentity
	InternalEpoch                  schemafence.InternalPreparedSchemaStamp
	InternalId, InternalMetadataID []byte
	InternalColumns                cql.Columns
	InternalRevision               uint64
	InternalGeneration             uint64
	InternalMetadataChanged        bool
}
type InternalPreparedState struct {
	InternalSession      *implCluster.Core
	view                 atomic.Pointer[preparedView]
	preparing            map[cql.UUID]*hostPrepareFlight
	InternalDependencies *schemafence.InternalPreparedDependencies
	// tabletPlacement caches where the statement's table is placed in the
	// snapshot it last routed by: its tablet placement, or else its token
	// ranges.
	tabletPlacement                     routing.InternalPlacementCache
	InternalStatement, InternalKeyspace string
	partitioner                         string
	InternalParameters, InternalColumns cql.Columns
	InternalInitialEpoch                schemafence.InternalPreparedSchemaStamp
	InternalInfo                        PreparedInfo
	conflictEpoch                       atomic.Uint64
	InternalMu                          sync.RWMutex
	conflictMu                          sync.Mutex
	conflictDirty                       atomic.Bool
	conflictRefresh                     atomic.Bool
	InternalProtocol                    uint8
	// hasCounter reports a counter parameter, computed once from
	// InternalParameters.
	hasCounter bool
}
type hostPrepareFlight struct {
	err      error
	done     chan struct{}
	previous *InternalHostPrepared
	result   *InternalHostPrepared
	epoch    schemafence.InternalPreparedSchemaStamp
	started  uint64
	waiters  atomic.Int32
}
type Prepared[P, R any] struct {
	privateStorage PreparedStorage[P, R]
}
type Bound[R any] struct {
	privateStorage BoundStorage[R]
}

// preparedStatement contains the transport preparation and parameter plan shared
// by buffered and streaming preparation. Result decoding is selected separately.
type preparedStatement[P any] struct {
	state      *InternalPreparedState
	params     cql.CompiledParams[P]
	columns    cql.Columns
	kind       cql.StatementKind
	idempotent bool
}

func prepareStatement[P, R any](ctx context.Context, s *implCluster.Core, statement *binding.Statement[P, R], options PrepareOptions) (preparedStatement[P], error) {
	var empty preparedStatement[P]
	if ctx == nil || s == nil {
		return empty, cqltype.TypeValidation("prepare", "nil context or session")
	}
	if err := transport.ContextError(ctx); err != nil {
		return empty, err
	}
	if s.Closed() {
		return empty, cqlerr.ErrClosed
	}
	if options.HostID.Set && options.HostID.Value.IsZero() {
		return empty, cqltype.TypeValidation("prepare host", "empty UUID")
	}
	if options.privateStorage.InternalConnection != nil && !implCluster.InternalConnectionOwnedBy(options.privateStorage.InternalConnection, s) {
		return empty, cqltype.TypeValidation("prepare connection", "connection belongs to another session")
	}
	if err := binding.ValidateStatement(statement); err != nil {
		return empty, err
	}
	rt := InternalRuntimeFor(s)
	if err := checkKeyspaceOption(&rt.config, options.Keyspace); err != nil {
		return empty, err
	}
	keyspace := options.Keyspace
	if keyspace == "" {
		keyspace = implCluster.SessionData(s).InternalCfg.Keyspace
	}
	state, err := rt.prepareTyped(ctx, statement.CQL, keyspace, options)
	if err != nil {
		return empty, err
	}
	state.InternalMu.RLock()
	parameters, columns := state.InternalParameters, state.InternalColumns
	confirmedKind := state.InternalInfo.Kind
	state.InternalMu.RUnlock()
	plan, err := codec.CompileParams(statement.Params, parameters)
	if err != nil {
		return empty, err
	}
	// ValidateStatement resolved statement.Kind; a server that flags a
	// lightweight transaction, as ScyllaDB does, overrides an ordinary one.
	kind := statement.Kind
	if confirmedKind == cql.ConditionalStatement {
		kind = confirmedKind
	}
	return preparedStatement[P]{state: state, params: plan, columns: columns, kind: kind, idempotent: statement.Idempotent}, nil
}

func Prepare[P, R any](ctx context.Context, s *implCluster.Core, statement binding.Statement[P, R], options PrepareOptions) (*Prepared[P, R], error) {
	prepared, err := prepareStatement(ctx, s, &statement, options)
	if err != nil {
		return nil, err
	}
	state, plan, columns := prepared.state, prepared.params, prepared.columns
	kind := prepared.kind
	var rows cql.CompiledRow[R]
	// Cassandra omits conditional result metadata from PREPARE. Conditional
	// statements therefore defer absent result shapes until EXECUTE;
	// actual metadata still selects and validates the plan before any row output.
	// A confirmed ordinary statement keeps eager zero-column validation.
	// ScyllaDB does return conditional result metadata; a conditional
	// statement's plan still compiles at EXECUTE when it does not fit here,
	// as on Cassandra, so Exec rejects it with ErrConditionalExec instead of
	// Prepare failing on the [applied] column.
	if columns.Len() != 0 || kind == cql.OrdinaryStatement {
		rows, err = codec.CompileRow(statement.Rows, columns)
		if err != nil && kind != cql.ConditionalStatement {
			return nil, err
		}
	}
	return func() *Prepared[P, R] {
		opaqueStorageValue := new(Prepared[P, R])
		opaqueStorageValue.privateStorage.kind = kind
		opaqueStorageValue.privateStorage.idempotent = prepared.idempotent
		opaqueStorageValue.privateStorage.InternalState = state
		opaqueStorageValue.privateStorage.params = plan
		opaqueStorageValue.privateStorage.rows = statement.Rows
		opaqueStorageValue.privateStorage.plans = &InternalResultPlanCache[R]{columns: columns, plan: rows}
		return opaqueStorageValue
	}(), nil
}

func (p *Prepared[P, R]) BindMarkers() cql.Columns {
	if p == nil || p.privateStorage.InternalState == nil {
		return cql.Columns{}
	}
	p.privateStorage.InternalState.InternalMu.RLock()
	defer p.privateStorage.InternalState.InternalMu.RUnlock()
	return p.privateStorage.InternalState.InternalParameters
}
func (p *Prepared[P, R]) Columns() cql.Columns {
	if p == nil || p.privateStorage.InternalState == nil {
		return cql.Columns{}
	}
	p.privateStorage.InternalState.InternalMu.RLock()
	defer p.privateStorage.InternalState.InternalMu.RUnlock()
	return p.privateStorage.InternalState.InternalColumns
}
func (p *Prepared[P, R]) Info() PreparedInfo {
	if p == nil || p.privateStorage.InternalState == nil {
		return PreparedInfo{}
	}
	p.privateStorage.InternalState.InternalMu.RLock()
	defer p.privateStorage.InternalState.InternalMu.RUnlock()
	v := p.privateStorage.InternalState.InternalInfo
	v.Kind = p.privateStorage.kind
	v.PartitionKeyIndexes = append([]int(nil), v.PartitionKeyIndexes...)
	return v
}
func BindBorrowed[P, R any](p *Prepared[P, R], value P) (*Bound[R], error) {
	return bindPrepared(p, value, false)
}
func BindOwned[P, R any](p *Prepared[P, R], value P) (*Bound[R], error) {
	return bindPrepared(p, value, true)
}
func bindPrepared[P, R any](p *Prepared[P, R], value P, owned bool) (*Bound[R], error) {
	if p == nil || p.privateStorage.InternalState == nil {
		return nil, cqltype.TypeValidation("prepared", "nil handle")
	}
	p.privateStorage.InternalState.InternalMu.RLock()
	columns := p.privateStorage.InternalState.InternalParameters
	info := p.privateStorage.InternalState.InternalInfo
	statement, keyspace := p.privateStorage.InternalState.InternalStatement, p.privateStorage.InternalState.InternalKeyspace
	p.privateStorage.InternalState.InternalMu.RUnlock()
	allowUnset := true
	values, err := binding.InternalEncodeParameters(p.privateStorage.params, columns, value, owned, allowUnset)
	if err != nil {
		return nil, err
	}
	token, err := routing.InternalComputeRoutingToken(values.Cells, info.PartitionKeyIndexes, p.privateStorage.InternalState.InternalSession.CapturedRoutingPartitioner(p.privateStorage.InternalState.partitioner))
	if err != nil {
		return nil, err
	}
	return func() *Bound[R] {
		opaqueStorageValuex := new(Bound[R])
		opaqueStorageValuex.privateStorage.InternalRequest = InternalExecutionRequest{statement: statement, keyspace: keyspace, InternalSource: preparedSource{state: p.privateStorage.InternalState}, InternalValues: values, InternalOwner: InternalNewInputOwner(), routing: routing2.Info{Keyspace: info.Keyspace, Table: info.Table, Token: token, Kind: p.privateStorage.kind}, hasCounter: p.privateStorage.InternalState.hasCounter, idempotent: p.privateStorage.idempotent}
		opaqueStorageValuex.privateStorage.InternalRows = p.privateStorage.rows
		opaqueStorageValuex.privateStorage.plans = p.privateStorage.plans
		return opaqueStorageValuex
	}(), nil
}
func BindUnpreparedBorrowed[P, R any](s binding.Statement[P, R], columns cql.Columns, value P) (*Bound[R], error) {
	return bindUnprepared(s, columns, value, false)
}
func BindUnpreparedOwned[P, R any](s binding.Statement[P, R], columns cql.Columns, value P) (*Bound[R], error) {
	return bindUnprepared(s, columns, value, true)
}
func bindUnprepared[P, R any](s binding.Statement[P, R], columns cql.Columns, value P, owned bool) (*Bound[R], error) {
	if err := binding.ValidateStatement(&s); err != nil {
		return nil, err
	}
	plan, err := codec.CompileParams(s.Params, columns)
	if err != nil {
		return nil, err
	}
	values, err := binding.InternalEncodeParameters(plan, columns, value, owned, true)
	if err != nil {
		return nil, err
	}
	kind := s.Kind
	return func() *Bound[R] {
		opaqueStorageValuexx := new(Bound[R])
		opaqueStorageValuexx.privateStorage.InternalRequest = InternalExecutionRequest{statement: s.CQL, InternalSource: cqlText{}, InternalValues: values, InternalOwner: InternalNewInputOwner(), routing: routing2.Info{Kind: kind}, hasCounter: hasCounterColumn(columns), idempotent: s.Idempotent}
		opaqueStorageValuexx.privateStorage.InternalRows = s.Rows
		opaqueStorageValuexx.privateStorage.plans = &InternalResultPlanCache[R]{}
		return opaqueStorageValuexx
	}(), nil
}

func (b *Bound[R]) Close() {
	if b != nil && b.privateStorage.InternalRequest.InternalOwner != nil {
		b.privateStorage.InternalRequest.InternalOwner.InternalClose()
	}
}
func (b *Bound[R]) Wait(ctx context.Context) error {
	if b == nil || b.privateStorage.InternalRequest.InternalOwner == nil {
		return cqltype.TypeValidation("binding", "nil binding")
	}
	if ctx == nil {
		return cqltype.TypeValidation("context", "nil context")
	}
	return b.privateStorage.InternalRequest.InternalOwner.InternalWait(ctx)
}
func (b *Bound[R]) Routing() routing2.Info {
	if b == nil {
		return routing2.Info{}
	}
	return b.privateStorage.InternalRequest.routing
}

type PrepareOptionsStorage struct {
	InternalConnection *transport.Conn
}

type PreparedStorage[P, R any] struct {
	params        cql.CompiledParams[P]
	rows          cql.RowDecoder[R]
	poolErr       error
	InternalState *InternalPreparedState
	plans         *InternalResultPlanCache[R]
	pool          *queryPool[P, R]
	poolOnce      sync.Once
	// kind is the kind executions run as, resolved at Prepare.
	kind cql.StatementKind
	// idempotent is the statement's cql.Statement.Idempotent.
	idempotent bool
}

type BoundStorage[R any] struct {
	InternalRows    cql.RowDecoder[R]
	plans           *InternalResultPlanCache[R]
	rows            *rowsHolder[R] // a reusable query's iterator storage
	InternalRequest InternalExecutionRequest
}

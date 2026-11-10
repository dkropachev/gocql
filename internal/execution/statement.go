// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"sync"
	"sync/atomic"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	routing "github.com/scylladb/gocql/v2/internal/routing"
)

type PrepareOptions struct {
	privateStorage PrepareOptionsStorage
	Keyspace       string
	HostID         Option[UUID]
	Trace          bool
}
type PreparedInfo struct {
	Keyspace, Table     string
	PartitionKeyIndexes []int
	Kind                StatementKind
	Conditional         bool
	Generation          uint64
}
type InternalHostPrepared struct {
	InternalResult                 *typedResultMetadata
	InternalEpoch                  preparedSchemaStamp
	InternalId, InternalMetadataID []byte
	InternalColumns                Columns
	InternalRevision               uint64
	InternalGeneration             uint64
	InternalMetadataChanged        bool
}
type InternalPreparedState struct {
	InternalResult       *typedResultMetadata
	InternalSession      *implCluster.Core
	InternalHosts        map[UUID]*InternalHostPrepared
	InternalPreparing    map[UUID]*InternalHostPrepareFlight
	InternalDependencies *preparedDependencies
	// tabletPlacement finds the statement's table among tablet placements.
	tabletPlacement                     routing.InternalPlacementCache
	InternalStatement, InternalKeyspace string
	InternalPartitioner                 string
	InternalParameters, InternalColumns Columns
	InternalInitialEpoch                preparedSchemaStamp
	InternalInfo                        PreparedInfo
	InternalConflictEpoch               atomic.Uint64
	InternalMu                          sync.RWMutex
	conflictMu                          sync.Mutex
	InternalConflictDirty               atomic.Bool
	InternalConflictRefresh             atomic.Bool
	InternalResultHost                  UUID
	InternalProtocol                    uint8
}
type InternalHostPrepareFlight struct {
	err             error
	done            chan struct{}
	previous        *InternalHostPrepared
	result          *InternalHostPrepared
	epoch           preparedSchemaStamp
	started         uint64
	InternalWaiters atomic.Int32
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
	state   *InternalPreparedState
	params  ParamPlan[P]
	columns Columns
	kind    StatementKind
}

func prepareStatement[P, R any](ctx context.Context, s *implCluster.Core, statement *Statement[P, R], options PrepareOptions) (preparedStatement[P], error) {
	var empty preparedStatement[P]
	if ctx == nil || s == nil {
		return empty, typeValidation("prepare", "nil context or session")
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if s.Closed() {
		return empty, ErrClosed
	}
	if options.HostID.Set && options.HostID.Value.IsEmpty() {
		return empty, typeValidation("prepare host", "empty UUID")
	}
	if options.privateStorage.InternalConnection != nil && !implCluster.InternalConnectionOwnedBy(options.privateStorage.InternalConnection, s) {
		return empty, typeValidation("prepare connection", "connection belongs to another session")
	}
	if err := validateStatement(statement); err != nil {
		return empty, err
	}
	keyspace := options.Keyspace
	if keyspace == "" {
		keyspace = implCluster.SessionData(s).InternalCfg.Keyspace
	}
	state, err := InternalRuntimeFor(s).InternalPrepareTyped(ctx, statement.CQL, keyspace, options)
	if err != nil {
		return empty, err
	}
	state.InternalMu.RLock()
	parameters, columns := state.InternalParameters, state.InternalColumns
	confirmedKind := state.InternalInfo.Kind
	state.InternalMu.RUnlock()
	plan, err := compileParamPlan(statement.Params, parameters)
	if err != nil {
		return empty, err
	}
	kind := statement.Kind
	if confirmedKind == ConditionalStatement || kind == UnknownStatement {
		kind = confirmedKind
	}
	return preparedStatement[P]{state: state, params: plan, columns: columns, kind: kind}, nil
}

func Prepare[P, R any](ctx context.Context, s *implCluster.Core, statement Statement[P, R], options PrepareOptions) (*Prepared[P, R], error) {
	prepared, err := prepareStatement(ctx, s, &statement, options)
	if err != nil {
		return nil, err
	}
	state, plan, columns, kind := prepared.state, prepared.params, prepared.columns, prepared.kind
	var rows RowPlan[R]
	// Cassandra omits conditional result metadata from PREPARE. Unknown and
	// conditional statements therefore defer absent result shapes until EXECUTE;
	// actual metadata still selects and validates the plan before any row output.
	// A confirmed ordinary statement keeps eager zero-column validation.
	if columns.Len() != 0 || kind == OrdinaryStatement {
		rows, err = compileRowPlan(statement.Rows, columns)
		if err != nil {
			return nil, err
		}
	}
	return func() *Prepared[P, R] {
		opaqueStorageValue := new(Prepared[P, R])
		opaqueStorageValue.privateStorage.InternalKind = kind
		opaqueStorageValue.privateStorage.InternalState = state
		opaqueStorageValue.privateStorage.InternalParams = plan
		opaqueStorageValue.privateStorage.InternalRows = statement.Rows
		opaqueStorageValue.privateStorage.InternalPlans = &InternalResultPlanCache[R]{columns: columns, InternalPlan: rows}
		return opaqueStorageValue
	}(), nil
}

func (p *Prepared[P, R]) Parameters() Columns {
	if p == nil || p.privateStorage.InternalState == nil {
		return Columns{}
	}
	p.privateStorage.InternalState.InternalMu.RLock()
	defer p.privateStorage.InternalState.InternalMu.RUnlock()
	return p.privateStorage.InternalState.InternalParameters
}
func (p *Prepared[P, R]) Columns() Columns {
	if p == nil || p.privateStorage.InternalState == nil {
		return Columns{}
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
	v.Kind = p.privateStorage.InternalKind
	v.Conditional = p.privateStorage.InternalKind == ConditionalStatement
	v.PartitionKeyIndexes = append([]int(nil), v.PartitionKeyIndexes...)
	return v
}
func BindBorrowed[P, R any](p *Prepared[P, R], value P) (*Bound[R], error) {
	return InternalBindPrepared(p, value, false)
}
func BindOwned[P, R any](p *Prepared[P, R], value P) (*Bound[R], error) {
	return InternalBindPrepared(p, value, true)
}
func InternalBindPrepared[P, R any](p *Prepared[P, R], value P, owned bool) (*Bound[R], error) {
	if p == nil || p.privateStorage.InternalState == nil {
		return nil, typeValidation("prepared", "nil handle")
	}
	p.privateStorage.InternalState.InternalMu.RLock()
	columns := p.privateStorage.InternalState.InternalParameters
	info := p.privateStorage.InternalState.InternalInfo
	statement, keyspace := p.privateStorage.InternalState.InternalStatement, p.privateStorage.InternalState.InternalKeyspace
	p.privateStorage.InternalState.InternalMu.RUnlock()
	allowUnset := p.privateStorage.InternalState.InternalProtocol >= 4
	values, err := encodeParameters(p.privateStorage.InternalParams, columns, value, owned, allowUnset)
	if err != nil {
		return nil, err
	}
	token, err := computeRoutingToken(values.Cells, info.PartitionKeyIndexes, implCluster.InternalSessionCapturedRoutingPartitioner(p.privateStorage.InternalState.InternalSession, p.privateStorage.InternalState.InternalPartitioner))
	if err != nil {
		return nil, err
	}
	return func() *Bound[R] {
		opaqueStorageValuex := new(Bound[R])
		opaqueStorageValuex.privateStorage.InternalRequest = InternalExecutionRequest{InternalStatement: statement, keyspace: keyspace, InternalSource: preparedSource{state: p.privateStorage.InternalState}, InternalValues: values, InternalOwner: newInputOwner(), InternalRouting: RoutingInfo{Keyspace: info.Keyspace, Table: info.Table, Token: token, Kind: p.privateStorage.InternalKind, Conditional: p.privateStorage.InternalKind != OrdinaryStatement}}
		opaqueStorageValuex.privateStorage.InternalRows = p.privateStorage.InternalRows
		opaqueStorageValuex.privateStorage.plans = p.privateStorage.InternalPlans
		return opaqueStorageValuex
	}(), nil
}
func BindUnpreparedBorrowed[P, R any](s Statement[P, R], columns Columns, value P) (*Bound[R], error) {
	return InternalBindUnprepared(s, columns, value, false)
}
func BindUnpreparedOwned[P, R any](s Statement[P, R], columns Columns, value P) (*Bound[R], error) {
	return InternalBindUnprepared(s, columns, value, true)
}
func InternalBindUnprepared[P, R any](s Statement[P, R], columns Columns, value P, owned bool) (*Bound[R], error) {
	if err := validateStatement(&s); err != nil {
		return nil, err
	}
	plan, err := compileParamPlan(s.Params, columns)
	if err != nil {
		return nil, err
	}
	values, err := encodeParameters(plan, columns, value, owned, true)
	if err != nil {
		return nil, err
	}
	return func() *Bound[R] {
		opaqueStorageValuexx := new(Bound[R])
		opaqueStorageValuexx.privateStorage.InternalRequest = InternalExecutionRequest{InternalStatement: s.CQL, InternalSource: cqlText{}, InternalValues: values, InternalOwner: newInputOwner(), InternalRouting: RoutingInfo{Kind: s.Kind, Conditional: s.Kind != OrdinaryStatement}}
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
		return typeValidation("binding", "nil binding")
	}
	if ctx == nil {
		return typeValidation("context", "nil context")
	}
	return b.privateStorage.InternalRequest.InternalOwner.InternalWait(ctx)
}
func (b *Bound[R]) Routing() RoutingInfo {
	if b == nil {
		return RoutingInfo{}
	}
	return b.privateStorage.InternalRequest.InternalRouting
}

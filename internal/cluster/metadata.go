// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/codec"
	connectivity "github.com/scylladb/gocql/v2/internal/connectivity"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	pool1 "github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/protocol"
	routing "github.com/scylladb/gocql/v2/internal/routing"
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"

	model "github.com/scylladb/gocql/v2/internal/model"

	"context"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"sort"

	"sync"
	"sync/atomic"
	"time"

	"github.com/scylladb/gocql/v2/internal/tablets"
)

type TraceInfo struct {
	Coordinator netip.Addr
	Started     time.Time
	Events      []TraceEvent
	Duration    time.Duration
	ID          cqltype.UUID
}
type TraceEvent struct {
	Source   netip.Addr
	Activity string
	Thread   string
	Elapsed  time.Duration
	ID       cqltype.UUID
}

type InternalV2LatencySummary struct{ p50, p95, p99 time.Duration }

type InternalV2PeripheralState struct {
	routeDiagnostic       error
	InternalSnapshot      atomic.Pointer[routing.MetadataSnapshot]
	InternalSubscribers   map[*Subscription]struct{}
	partitioners          atomic.Pointer[map[string]routing.TokenPartitioner] // InternalConfig.Partitioners, for lock-free reads
	InternalSchemaEpoch   map[string]uint64
	schemaRevision        map[string]uint64
	InternalTableRevision map[routing.InternalMetadataTable]uint64
	// schemaEpochs and tableRevisions are copies of InternalSchemaEpoch and
	// InternalTableRevision, replaced whole on every change, that a request
	// reads without the lock when it captures its tablet feedback stamp.
	schemaEpochs                  atomic.Pointer[map[string]uint64]
	tableRevisions                atomic.Pointer[map[routing.InternalMetadataTable]uint64]
	schemaDropped                 map[string]bool
	InternalPreparedRefreshScopes map[string]uint64
	InternalFeedback              map[InternalV2TabletFeedbackKey]InternalV2PendingTabletFeedback
	feedbackWake                  chan struct{}
	InternalDone                  chan struct{}
	latencySummary                atomic.Pointer[InternalV2LatencySummary]
	schemaStale                   map[string]bool
	knownTabletTables             map[routing.InternalMetadataTable]bool
	refreshDiagnostics            []error
	InternalConfig                settings.Config
	latency                       [1024]atomic.Int64
	workers                       sync.WaitGroup
	InternalPublicationRevision   uint64
	preparedRefreshVersion        uint64
	InternalSchemaReconciled      atomic.Uint64
	schemaGeneration              atomic.Uint64
	InternalPlacementEpoch        atomic.Uint64
	latencyCount                  atomic.Uint64
	orphans                       atomic.Uint64
	InternalMu                    sync.RWMutex // per-request feedback captures read under RLock
	metricsOnce                   sync.Once
	periodicOnce                  sync.Once
	InternalFeedbackOnce          sync.Once
	publishMu                     sync.Mutex
	hostMu                        sync.Mutex
	InternalFeedbackMu            sync.Mutex
	InternalRetryScheduled        atomic.Bool
	schemaRetryNeeded             atomic.Bool
	closed                        bool
	InternalFeedbackResync        bool
}

func InternalSessionPeripheral(s *Core) *InternalV2PeripheralState {
	if s == nil {
		return nil
	}
	if p := s.privateStorage.v2Peripheral.Load(); p != nil {
		return p
	}
	p := &InternalV2PeripheralState{InternalSubscribers: make(map[*Subscription]struct{}), InternalSchemaEpoch: make(map[string]uint64), schemaRevision: make(map[string]uint64), InternalTableRevision: make(map[routing.InternalMetadataTable]uint64), schemaDropped: make(map[string]bool), schemaStale: make(map[string]bool), InternalFeedback: make(map[InternalV2TabletFeedbackKey]InternalV2PendingTabletFeedback), feedbackWake: make(chan struct{}, 1), knownTabletTables: make(map[routing.InternalMetadataTable]bool), InternalDone: make(chan struct{})}
	p.schemaEpochs.Store(&map[string]uint64{})
	p.tableRevisions.Store(&map[routing.InternalMetadataTable]uint64{})
	if s.privateStorage.v2Peripheral.CompareAndSwap(nil, p) {
		return p
	}
	return s.privateStorage.v2Peripheral.Load()
}
func (s *Core) initV2Peripheral(config settings.Config) error {
	InternalSessionConfigureV2Peripheral(s, config)
	p := InternalSessionPeripheral(s)
	p.InternalFeedbackOnce.Do(func() { p.InternalStartWorker(func() { InternalSessionRunV2TabletFeedback(s, p) }) })
	p.metricsOnce.Do(func() {
		p.InternalStartWorker(func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-p.InternalDone:
					return
				case <-ticker.C:
					p.InternalPublishLatencySummary()
				}
			}
		})
	})
	if !InternalSessionPublishV2Metadata(s) {
		return s.v2MetadataPublicationError()
	}
	if config.DefaultProfile != nil {
		if policy := config.DefaultProfile.Routing; policy != nil {
			if err := policy.Validate(s.Metadata()); err != nil {
				return err
			}
		}
	}
	InternalSessionObserveV2Lifecycle(s, settings.LifecycleEvent{Kind: "session", Stage: "ready", At: time.Now()})
	if config.Metadata.PeriodicRefresh > 0 {
		InternalSessionPeripheral(s).periodicOnce.Do(func() {
			InternalSessionPeripheral(s).InternalStartWorker(func() { s.runV2PeriodicRefresh(config.Metadata) })
		})
	}
	return nil
}
func InternalSessionConfigureV2Peripheral(s *Core, config settings.Config) {
	p := InternalSessionPeripheral(s)
	p.InternalMu.Lock()
	p.InternalConfig = config
	partitioners := config.Partitioners
	p.partitioners.Store(&partitioners)
	p.InternalMu.Unlock()
}
func (s *Core) Metadata() *routing.MetadataSnapshot {
	p := InternalSessionPeripheral(s)
	if p == nil {
		return routing.NewMetadataSnapshot(routing.SnapshotState{})
	}
	if snapshot := p.InternalSnapshot.Load(); snapshot != nil {
		return snapshot
	}
	for {
		InternalSessionPublishV2Metadata(s)
		if snapshot := p.InternalSnapshot.Load(); snapshot != nil {
			return snapshot
		}
	}
}
func (s *Core) Metrics() routing.MetricsSnapshot {
	if s == nil {
		return routing.MetricsSnapshot{}
	}
	var result routing.MetricsSnapshot
	if owner := InternalCurrentExecution(s); owner != nil {
		result = owner.MetricsSnapshot()
	}
	p := InternalSessionPeripheral(s)
	result.Orphans = p.orphans.Load()
	if summary := p.latencySummary.Load(); summary != nil {
		result.LatencyP50 = summary.p50
		result.LatencyP95 = summary.p95
		result.LatencyP99 = summary.p99
	}
	return result
}

// InternalSessionPublishV2Metadata publishes a snapshot of the current
// metadata sources. A build loses when a source changes while it runs, and
// every schema event in the cluster changes one, so under concurrent DDL a
// few immediate rebuilds can all lose. That is transient: each change
// publishes again itself. After four immediate attempts it backs off, 1 ms
// doubling up to 100 ms, for about a second in all before reporting failure.
func InternalSessionPublishV2Metadata(s *Core) bool {
	delay := time.Millisecond
	for attempt := 0; attempt < 16; attempt++ {
		if s.tryPublishV2Metadata() {
			return true
		}
		if attempt >= 3 {
			time.Sleep(delay)
			delay = min(2*delay, 100*time.Millisecond)
		}
	}
	return false
}
func (s *Core) tryPublishV2Metadata() bool {
	p := InternalSessionPeripheral(s)
	if p == nil {
		return false
	}
	old := p.InternalSnapshot.Load()
	p.InternalMu.Lock()
	revision := p.InternalPublicationRevision
	stale := maps.Clone(p.schemaStale)
	config := p.InternalConfig
	diagnostics := append([]error(nil), p.refreshDiagnostics...)
	if p.routeDiagnostic != nil {
		diagnostics = append(diagnostics, p.routeDiagnostic)
	}
	p.InternalMu.Unlock()
	m := routing.NewMetadataSnapshot(routing.SnapshotState{InternalGeneration: 1, InternalKeyspaces: make(map[string]*implMetadata.KeyspaceMetadata), InternalRanges: make(map[string][]routing.TokenRange), InternalTablets: make(map[routing.InternalMetadataTable][]routing.TokenRange)})
	routing.SnapshotData(m).InternalDiagnostics = diagnostics
	if old != nil {
		routing.SnapshotData(m).InternalGeneration = routing.SnapshotData(old).InternalGeneration + 1
	}
	var hosts []*implEndpoint.HostInfo
	var topologyRevision uint64
	if s.privateStorage.InternalHostSource != nil {
		s.privateStorage.InternalHostSource.mu.RLock()
		topologyRevision = s.privateStorage.InternalHostSource.revision
		routing.SnapshotData(m).InternalDiagnostics = append(routing.SnapshotData(m).InternalDiagnostics, s.privateStorage.InternalHostSource.diagnostics...)
		for _, h := range s.privateStorage.InternalHostSource.InternalHosts {
			hosts = append(hosts, h)
		}
		s.privateStorage.InternalHostSource.mu.RUnlock()
	}
	var schemaSource *map[string]*implMetadata.KeyspaceMetadata
	if s.privateStorage.InternalMetadataDescriber != nil && s.privateStorage.InternalMetadataDescriber.InternalMetadata != nil {
		schemaSource = s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalKeyspaceMap.Load()
	}

	replicaHosts := make([]*implEndpoint.HostInfo, 0, len(hosts))
	validTokens := true
	for _, h := range hosts {
		snapshot, err := InternalSnapshotV2Host(s, h)
		if err != nil {
			routing.SnapshotData(m).InternalDiagnostics = append(routing.SnapshotData(m).InternalDiagnostics, err)
			validTokens = false
		}
		routing.SnapshotData(m).InternalHosts = append(routing.SnapshotData(m).InternalHosts, snapshot)
		if routing.SnapshotData(m).InternalPartitioner == "" {
			routing.SnapshotData(m).InternalPartitioner = h.Partitioner()
		}
		replicaHosts = append(replicaHosts, func() *implEndpoint.HostInfo {
			opaqueStorageValue := new(implEndpoint.HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValue).InternalHostId = snapshot.ID
			implEndpoint.HostInfoData(opaqueStorageValue).InternalDataCenter = snapshot.Datacenter
			implEndpoint.HostInfoData(opaqueStorageValue).InternalRack = snapshot.Rack
			implEndpoint.HostInfoData(opaqueStorageValue).InternalTokens = append([]string(nil), h.Tokens()...)
			return opaqueStorageValue
		}())
	}
	sort.Slice(routing.SnapshotData(m).InternalHosts, func(i, j int) bool {
		return routing.SnapshotData(m).InternalHosts[i].ID.String() < routing.SnapshotData(m).InternalHosts[j].ID.String()
	})
	if len(hosts) > 0 && len(routing.SnapshotData(m).InternalHosts) == 0 && old != nil {
		return false
	}
	if s.privateStorage.InternalMetadataDescriber != nil && s.privateStorage.InternalMetadataDescriber.InternalMetadata != nil {
		md := s.privateStorage.InternalMetadataDescriber.InternalMetadata
		if schemaSource != nil {
			routing.SnapshotData(m).InternalSchemaSources = *schemaSource
			for name, ks := range *schemaSource {
				if ks != nil {
					if old != nil && routing.SnapshotData(old).InternalSchemaSources[name] == ks && len(implMetadata.KeyspaceData(ks).InternalTablesInvalidated) == 0 {
						routing.SnapshotData(m).InternalKeyspaces[name] = routing.SnapshotData(old).InternalKeyspaces[name]
					} else {
						routing.SnapshotData(m).InternalKeyspaces[name] = implMetadata.InternalCloneV2Keyspace(ks)
					}
				}
			}
		}
		if md.privateStorage.InternalTabletsMetadata != nil && (old == nil || routing.SnapshotData(old).InternalPlacements == nil) {
			md.privateStorage.InternalTabletsMetadata.ForEach(func(ks, table string, entries tablets.TabletEntryList) bool {
				r := make([]routing.TokenRange, len(entries))
				for i, e := range entries {
					r[i] = routing.TokenRange{Start: e.FirstToken(), End: e.LastToken()}
					for _, replica := range e.ReplicasUnsafe() {
						r[i].Replicas = append(r[i].Replicas, model.Target{HostID: cqltype.UUID(replica.HostUUIDValue()), ShardID: cqltype.Option[uint32]{Value: uint32(replica.ShardID()), Set: replica.ShardID() >= 0}})
					}
				}
				routing.SnapshotData(m).InternalTablets[routing.InternalMetadataTable{InternalKeyspace: ks, InternalTable: table}] = r
				return true
			})
		}
	}
	if old != nil {
		for name, prior := range routing.SnapshotData(old).InternalKeyspaces {
			if routing.SnapshotData(m).InternalKeyspaces[name] == nil && stale[name] {
				routing.SnapshotData(m).InternalKeyspaces[name] = implMetadata.InternalCloneV2Keyspace(prior)
			}
			if current := routing.SnapshotData(m).InternalKeyspaces[name]; current != nil {
				for table := range implMetadata.KeyspaceData(current).InternalTablesInvalidated {
					if priorTable := prior.Tables[table]; priorTable != nil {
						copyKS := implMetadata.InternalCloneV2Keyspace(prior)
						current.Tables[table] = copyKS.Tables[table]
						for name, view := range copyKS.Views {
							if view.BaseTableName == table {
								current.Views[name] = view
							}
						}
						for name, index := range copyKS.Indexes {
							if index.TableName == table {
								current.Indexes[name] = index
							}
						}
					}
				}
			}
		}
		for table := range routing.SnapshotData(old).InternalTablets {
			if ks := routing.SnapshotData(m).InternalKeyspaces[table.InternalKeyspace]; ks != nil {
				if _, ok := ks.Tables[table.InternalTable]; ok {
					if _, known := routing.SnapshotData(m).InternalTablets[table]; !known {
						routing.SnapshotData(m).InternalTablets[table] = nil
					}
				}
			}
		}
	}
	p.InternalFeedbackMu.Lock()
	for table := range p.knownTabletTables {
		if _, _, err := routing.InternalMetadataSnapshotLookupRanges(m, table.InternalKeyspace, table.InternalTable); err != nil {
			delete(p.knownTabletTables, table)
			continue
		}
		if ks := routing.SnapshotData(m).InternalKeyspaces[table.InternalKeyspace]; ks != nil {
			if _, exists := ks.Tables[table.InternalTable]; exists {
				if _, known := routing.SnapshotData(m).InternalTablets[table]; !known {
					routing.SnapshotData(m).InternalTablets[table] = nil
				}
			}
		}
	}
	p.InternalFeedbackMu.Unlock()
	// Reuse the established replication-strategy placement on private cold-path
	// descriptors. Invalid tokens were rejected before ring construction.
	if routing.SnapshotData(m).InternalPartitioner != "" && validTokens {
		partitioner := routing.SnapshotData(m).InternalPartitioner
		if config.Partitioners[routing.SnapshotData(m).InternalPartitioner] != nil {
			partitioner = "Murmur3Partitioner"
		}
		ring, err := implEndpoint.InternalNewTokenRing(partitioner, replicaHosts)
		if err != nil {
			routing.SnapshotData(m).InternalDiagnostics = append(routing.SnapshotData(m).InternalDiagnostics, err)
		} else if _, ok := ring.InternalPartitioner.(implEndpoint.InternalMurmur3Partitioner); ok {
			for ks, meta := range routing.SnapshotData(m).InternalKeyspaces {
				ranges, err := InternalBuildV2VnodeRanges(ring, meta)
				if err != nil {
					routing.SnapshotData(m).InternalDiagnostics = append(routing.SnapshotData(m).InternalDiagnostics, err)
					continue
				}
				routing.SnapshotData(m).InternalRanges[ks] = ranges
			}
		} else {
			routing.SnapshotData(m).InternalDiagnostics = append(routing.SnapshotData(m).InternalDiagnostics, cqltype.TypeValidation("partitioner", "no configured int64 partitioner for %q", routing.SnapshotData(m).InternalPartitioner))
		}
	}
	routing.SnapshotData(m).InternalPlacementEpoch = p.InternalPlacementEpoch.Load()
	routing.SnapshotData(m).InternalPlacementSignal = &p.InternalPlacementEpoch
	routing.SnapshotData(m).InternalPlacements = routing.InternalReconcileTabletPlacement(old, m)
	routing.SnapshotData(m).InternalHostIndex = InternalSessionRoutingHostIndex(s, routing.SnapshotData(m).InternalHosts)
	if config.Metadata.Mode != settings.MetadataFull {
		routing.SnapshotData(m).InternalDiagnostics = append(routing.SnapshotData(m).InternalDiagnostics, cqltype.TypeValidation("partitioner", "table partitioners outside loaded metadata disable token routing"))
	}
	for keyspace, ks := range routing.SnapshotData(m).InternalKeyspaces {
		for table, t := range ks.Tables {
			if t != nil && t.Options.Partitioner != "" && config.Partitioners[t.Options.Partitioner] == nil {
				routing.SnapshotData(m).InternalDiagnostics = append(routing.SnapshotData(m).InternalDiagnostics, cqltype.TypeValidation("partitioner", "unknown partitioner %q for %s.%s; token routing disabled", t.Options.Partitioner, keyspace, table))
			}
		}
	}

	if config.DefaultProfile != nil {
		if policy := config.DefaultProfile.Routing; policy != nil {
			if err := policy.Validate(m); err != nil {
				routing.SnapshotData(m).InternalDiagnostics = append(routing.SnapshotData(m).InternalDiagnostics, err)
			}
		}
	}
	// All potentially user-provided hooks above run before the publication gate.
	// A concurrent tombstone/source replacement rejects this entire stale build.
	p.publishMu.Lock()
	defer p.publishMu.Unlock()
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()
	if p.InternalPublicationRevision != revision || p.InternalSnapshot.Load() != old {
		return false
	}
	if s.privateStorage.InternalHostSource != nil {
		s.privateStorage.InternalHostSource.mu.RLock()
		defer s.privateStorage.InternalHostSource.mu.RUnlock()
		if s.privateStorage.InternalHostSource.revision != topologyRevision {
			return false
		}
	}
	if s.privateStorage.InternalMetadataDescriber != nil && s.privateStorage.InternalMetadataDescriber.InternalMetadata != nil && s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalKeyspaceMap.Load() != schemaSource {
		return false
	}
	InternalSessionUpdatePreparedSchemaSnapshot(s, old, m)
	p.InternalSnapshot.Store(m)
	return true
}
func InternalSnapshotV2Host(s *Core, h *implEndpoint.HostInfo) (model.HostSnapshot, error) {
	out, tokenErr := implEndpoint.SnapshotHost(h)
	if h == nil {
		return out, tokenErr
	}
	if s != nil {
		if cfg, ok := s.v2PoolConfig(); ok {
			if path, _ := connectivity.UnixContact(*cfg); path != "" {
				out.Address = netip.AddrPort{}
			}
		}
	}
	if s != nil {
		out.Enabled = !InternalClusterSettingsFilterHost(&s.privateStorage.InternalCfg, h)
		if s.privateStorage.InternalPool != nil {
			pool, ok := s.privateStorage.InternalPool.InternalGetPool(h)
			out.Connected = ok && pool.Size() > 0
		}
	}
	return out, tokenErr
}
func InternalSessionObserveV2Lifecycle(s *Core, event settings.LifecycleEvent) {
	p := InternalSessionPeripheral(s)
	if p == nil {
		return
	}
	p.InternalMu.Lock()
	observer := p.InternalConfig.LifecycleObserver
	p.InternalMu.Unlock()
	if observer == nil {
		return
	}
	InternalSessionObserveSafely(s, func() { observer.ObserveLifecycle(event) })
}

// RefreshMetadata waits for the shared topology refresh under the caller's
// context. Metadata queries themselves use the session metadata timeout.
func (s *Core) RefreshMetadata(ctx context.Context) error {
	if ctx == nil {
		return cqltype.TypeValidation("context", "nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.Closed() {
		return runtimeerrors.ErrClosed
	}
	if s.privateStorage.InternalRingRefresher == nil {
		return implMetadata.InternalErrNoControl
	}
	p := InternalSessionPeripheral(s)
	if !p.admitMetadataWork() {
		return runtimeerrors.ErrClosed
	}
	defer p.workers.Done()
	var topologyErr error
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err, ok := <-s.privateStorage.InternalRingRefresher.RefreshNow():
		if !ok {
			return runtimeerrors.ErrClosed
		}
		topologyErr = err
	}
	var routeErr error
	if s.privateStorage.InternalClientRoutesHandler != nil {
		routeErr = s.privateStorage.InternalClientRoutesHandler.refreshContext(ctx)
	}
	schemaErr := InternalSessionRefreshV2AllSchema(s, ctx)
	if topologyErr != nil || routeErr != nil {
		return errors.Join(&implMetadata.PartialRefreshError{Generation: s.Metadata().Generation(), Topology: topologyErr, ClientRoutes: routeErr}, schemaErr)
	}
	return schemaErr
}

// routingPartitioner chooses the configured server/table partitioner. Unknown
// names deliberately leave routing tokenless; hashing with a guessed algorithm
// would send requests to the wrong replicas.
func (s *Core) routingPartitioner(keyspace, table string) routing.TokenPartitioner {
	return InternalSessionCapturedRoutingPartitioner(s, InternalSessionPreparedPartitionerName(s, keyspace, table))
}

// Trace reads tracing tables through the same typed QUERY/codec path as user
// requests. Readiness polling is governed by one context across every round.
func (s *Core) Trace(ctx context.Context, id cqltype.UUID) (TraceInfo, error) {
	result := TraceInfo{ID: id}
	if ctx == nil {
		return result, cqltype.TypeValidation("trace context", "nil context")
	}
	if s == nil || s.Closed() {
		return result, runtimeerrors.ErrClosed
	}
	owner := InternalCurrentExecution(s)
	if owner == nil {
		return result, runtimeerrors.ErrNoConnections
	}
	return owner.Trace(ctx, id)
}

// readV2SystemRows is the context-bound typed control-query seam. It detaches
// metadata values before releasing each page and never invokes legacy Scan.
func InternalReadV2SystemRows[P, R any](ctx context.Context, conn *transport.Conn, statement binding.Statement[P, R], columns cqltype.Columns, value P) ([]R, error) {
	if conn == nil {
		return nil, cqltype.TypeValidation("system query", "nil connection")
	}
	return InternalReadV2SystemRowsVia(ctx, conn.InternalVersion, conn.InternalGetSystemRequestState().InternalTimeout, conn.InternalExecuteV2System, statement, columns, value)
}
func InternalReadV2SystemRowsVia[P, R any](ctx context.Context, version byte, timeout time.Duration, execute func(context.Context, *model.AttemptRequest) (*transport.InternalResponsePage, error), statement binding.Statement[P, R], columns cqltype.Columns, value P) ([]R, error) {
	if ctx == nil || execute == nil {
		return nil, cqltype.TypeValidation("system query", "nil context or connection")
	}
	if err := binding.ValidateStatement(&statement); err != nil {
		return nil, err
	}
	return implMetadata.ReadRows(ctx, version, timeout, func(ctx context.Context, request *model.AttemptRequest) (*model.ResponseData, error) {
		response, err := execute(ctx, request)
		if response == nil {
			return nil, err
		}
		return &response.InternalResponseData, err
	}, implMetadata.Query[P, R]{CQL: statement.CQL, Params: statement.Params, Rows: statement.Rows}, columns, value)
}

type InternalAgreementPeer struct {
	id      cqltype.Nullable[cqltype.UUID]
	version cqltype.Nullable[cqltype.UUID]
}

func (s *Core) awaitV2SchemaAgreement(ctx context.Context) error {
	if ctx == nil {
		return cqltype.TypeValidation("schema agreement context", "nil context")
	}
	if s == nil || s.Closed() {
		return runtimeerrors.ErrClosed
	}
	return s.awaitV2SchemaAgreementUsing(ctx, nil, cqltype.UUID{})
}
func InternalSessionAwaitV2SchemaAgreementOn(s *Core, ctx context.Context, conn *transport.Conn) error {
	if ctx == nil || conn == nil || conn.InternalHost == nil {
		return cqltype.TypeValidation("schema agreement", "nil context or coordinator")
	}
	if s == nil || s.Closed() {
		return runtimeerrors.ErrClosed
	}
	return s.awaitV2SchemaAgreementUsing(ctx, conn, implEndpoint.InternalHostInfoHostUUID(conn.InternalHost))
}
func (s *Core) awaitV2SchemaAgreementUsing(ctx context.Context, coordinator *transport.Conn, required cqltype.UUID) error {
	p := InternalSessionPeripheral(s)
	if !p.admitMetadataWork() {
		return runtimeerrors.ErrClosed
	}
	defer p.workers.Done()
	p.InternalMu.Lock()
	options := p.InternalConfig.Metadata
	p.InternalMu.Unlock()
	timeout := options.AgreementTimeout
	if timeout == 0 {
		timeout = s.privateStorage.InternalCfg.MaxWaitSchemaAgreement
	}
	if timeout <= 0 {
		timeout = time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	interval := options.AgreementPoll
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if s.Closed() {
			return runtimeerrors.ErrClosed
		}
		// Retain one immutable scope for this check. A missing/down enabled host
		// prevents agreement; each retry takes a fresh snapshot.
		scope := make(map[cqltype.UUID][]*transport.Conn)
		for _, h := range routing.SnapshotData(s.Metadata()).InternalHosts {
			if h.Enabled {
				scope[h.ID] = s.v2AgreementConnections(h.ID)
			}
		}
		if coordinator != nil {
			scope[required] = append([]*transport.Conn{coordinator}, scope[required]...)
		}
		if len(scope) == 0 {
			return &implMetadata.NoHostsError{Operation: "schema agreement", Causes: []error{runtimeerrors.ErrNoConnections}}
		}
		type observation struct {
			err     error
			version cqltype.UUID
		}
		results := make(chan observation, len(scope))
		// Fixed worker count bounds goroutines even with large topologies.
		jobs := make(chan struct {
			conns []*transport.Conn
			id    cqltype.UUID
		}, len(scope))
		for id, conns := range scope {
			jobs <- struct {
				conns []*transport.Conn
				id    cqltype.UUID
			}{conns: conns, id: id}
		}
		close(jobs)
		var workers sync.WaitGroup
		for i := 0; i < min(16, len(scope)); i++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for job := range jobs {
					v, err := InternalReadV2AgreementHost(ctx, job.id, job.conns)
					results <- observation{err: err, version: v}
				}
			}()
		}
		workers.Wait()
		close(results)
		complete := true
		versions := make(map[cqltype.UUID]bool)
		var terminal error
		for result := range results {
			if result.err != nil {
				complete = false
				if InternalTerminalV2AgreementError(result.err) {
					terminal = result.err
				}
			} else {
				versions[result.version] = true
			}
		}
		if terminal != nil {
			return terminal
		}
		if complete && len(versions) == 1 {
			return nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
func InternalReadV2AgreementHost(ctx context.Context, id cqltype.UUID, conns []*transport.Conn) (cqltype.UUID, error) {
	stmt := binding.Statement[codec.EmptyRecord, InternalAgreementPeer]{CQL: "SELECT host_id, schema_version FROM system.local WHERE key = 'local'", Params: codec.EmptyParamsEncoder, Rows: codec.Row(codec.Column("host_id", codec.NullableCodec(codec.UUIDCodec), func(r *InternalAgreementPeer) *cqltype.Nullable[cqltype.UUID] { return &r.id }), codec.Column("schema_version", codec.NullableCodec(codec.UUIDCodec), func(r *InternalAgreementPeer) *cqltype.Nullable[cqltype.UUID] { return &r.version }))}
	var last error = runtimeerrors.ErrNoConnections
	for _, conn := range conns {
		if conn == nil || conn.Closed() {
			continue
		}
		timeout := time.Duration(0)
		if conn.InternalCfg != nil && conn.InternalCfg.TypedConfig != nil {
			timeout = conn.InternalCfg.TypedConfig.Metadata.RequestTimeout
		}
		rows, err := InternalReadV2SystemRowsVia(ctx, conn.InternalVersion, timeout, conn.InternalExecuteV2System, stmt, cqltype.NewColumns(), codec.EmptyRecord{})
		if err != nil {
			if InternalTerminalV2AgreementError(err) {
				return cqltype.UUID{}, err
			}
			last = err
			continue
		}
		if len(rows) != 1 || !rows[0].id.Valid || !rows[0].version.Valid || rows[0].id.Value != id || rows[0].version.Value.IsEmpty() {
			return cqltype.UUID{}, cqltype.TypeValidation("schema agreement", "missing/mismatched host identity or version")
		}
		return rows[0].version.Value, nil
	}
	return cqltype.UUID{}, last
}
func InternalTerminalV2AgreementError(err error) bool {
	if _, ok := errors.AsType[*cqltype.ProtocolError](err); ok {
		return true
	}
	if _, ok := errors.AsType[*cqltype.ValidationError](err); ok {
		return true
	}
	if server, ok := errors.AsType[protocol.RequestError](err); ok {
		switch runtimeerrors.KindOf(server) {
		case runtimeerrors.KindOverloaded, runtimeerrors.KindTimeout:
			return false
		}
		return server.GetCode() != protocol.ErrCodeUnprepared
	}
	return false
}
func (s *Core) v2AgreementConnections(id cqltype.UUID) []*transport.Conn {
	if s.privateStorage.InternalPool == nil {
		return nil
	}
	s.privateStorage.InternalPool.InternalMu.RLock()
	pool := s.privateStorage.InternalPool.InternalHostConnPools[id]
	s.privateStorage.InternalPool.InternalMu.RUnlock()
	if pool == nil {
		return nil
	}
	pool.InternalMu.RLock()
	defer pool.InternalMu.RUnlock()
	if pool.InternalClosed {
		return nil
	}
	var out []*transport.Conn
	switch picker := pool.InternalConnPicker.(type) {
	case *pool1.InternalTypedConnPicker:
		picker.InternalMu.RLock()
		for _, cs := range picker.InternalConns {
			out = append(out, cs...)
		}
		out = append(out, picker.InternalExcess...)
		picker.InternalMu.RUnlock()
	case *pool1.InternalDefaultConnPicker:
		picker.InternalMu.RLock()
		out = append(out, picker.InternalConns...)
		picker.InternalMu.RUnlock()
	case *pool1.InternalScyllaConnPicker:
		picker.InternalMu.RLock()
		out = append(out, picker.InternalConns...)
		out = append(out, picker.InternalExcessConns...)
		picker.InternalMu.RUnlock()
	}
	return out
}

type InternalV2SystemQueryer interface {
	InternalQueryV2System(context.Context, *model.AttemptRequest) (*transport.InternalResponsePage, error)
}

func (c *InternalControlConn) InternalQueryV2System(ctx context.Context, request *model.AttemptRequest) (*transport.InternalResponsePage, error) {
	h := c.InternalGetConn()
	if h == nil || h.InternalConn == nil {
		return nil, runtimeerrors.ErrNoConnections
	}
	conn, ok := h.InternalConn.(*transport.Conn)
	if !ok {
		return nil, cqltype.TypeValidation("system query", "connection does not support typed requests")
	}
	return conn.InternalExecuteV2System(ctx, request)
}

func InternalSessionRefreshV2AllSchema(s *Core, ctx context.Context) error {
	p := InternalSessionPeripheral(s)
	if !p.admitMetadataWork() {
		return runtimeerrors.ErrClosed
	}
	defer p.workers.Done()
	if err := ctx.Err(); err != nil {
		return err
	}
	p.InternalMu.Lock()
	config := p.InternalConfig
	epochs := maps.Clone(p.InternalSchemaEpoch)
	revisions := maps.Clone(p.schemaRevision)
	p.InternalMu.Unlock()
	if config.DefaultProfile != nil && config.Metadata.Mode == settings.MetadataDisabled {
		if !InternalSessionPublishV2Metadata(s) {
			return s.v2MetadataPublicationError()
		}
		return nil
	}
	rows, err := implMetadata.InternalQueryV2SchemaRows(ctx, InternalNewSchemaSource(s), `SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces`, nil, implMetadata.InternalV2DecodeKeyspace())
	if err != nil {
		return err
	}
	keyspaces := make(map[string]*implMetadata.KeyspaceMetadata, len(rows))
	allowed := make(map[string]bool, len(config.Metadata.Keyspaces))
	for _, name := range config.Metadata.Keyspaces {
		allowed[name] = true
	}
	for _, row := range rows {
		if row.InternalName == "" {
			return cqltype.TypeValidation("keyspace schema", "missing keyspace identity")
		}
		if _, ok := keyspaces[row.InternalName]; ok {
			return cqltype.TypeValidation("keyspace schema", "duplicate identity %q", row.InternalName)
		}
		if len(allowed) > 0 && !allowed[row.InternalName] {
			continue
		}
		keyspaces[row.InternalName] = implMetadata.InternalV2KeyspaceFromRow(row.InternalName, row)
	}
	if err := implMetadata.InternalReadV2ScyllaKeyspaces(ctx, InternalNewSchemaSource(s), keyspaces, ""); err != nil {
		return err
	}

	failures := make(map[string]error)

	for name, base := range keyspaces {
		for attempt := 0; attempt < 8; attempt++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			p.InternalMu.Lock()
			epoch := p.InternalSchemaEpoch[name]
			revision := p.schemaRevision[name]
			tables := p.captureTableRevisionsLocked(name)
			p.InternalMu.Unlock()
			if attempt > 0 {
				base, err = implMetadata.InternalGetV2KeyspaceMetadata(ctx, InternalNewSchemaSource(s), name)
				if errors.Is(err, runtimeerrors.ErrKeyspaceDoesNotExist) {
					if s.dropV2KeyspaceIfCurrent(name, epoch, revision) {
						delete(failures, name)
						break
					}
					continue
				}
				if err != nil {
					failures[name] = err
					break
				}
			}
			var ks *implMetadata.KeyspaceMetadata
			if config.DefaultProfile != nil && config.Metadata.Mode == settings.MetadataMinimal {
				ks = base
				ks.Tables = make(map[string]*implMetadata.TableMetadata)
			} else {
				ks, err = implMetadata.InternalFetchV2Keyspace(ctx, InternalNewSchemaSource(s), base)
			}
			if err != nil {
				// A DROP can occur after the keyspace enumeration but before its
				// schema reads. Confirm absence before treating it as stale data.
				dropped, probeErr := s.confirmV2DroppedKeyspace(ctx, name)
				if dropped {
					delete(failures, name)
					break
				}
				if probeErr != nil && ctx.Err() != nil {
					return ctx.Err()
				}
			} else {
				// Empty table/type result sets are legal, so they cannot by
				// themselves reveal a keyspace dropped during this fetch.
				current, checkErr := implMetadata.InternalGetV2KeyspaceMetadata(ctx, InternalNewSchemaSource(s), name)
				if errors.Is(checkErr, runtimeerrors.ErrKeyspaceDoesNotExist) {
					if s.dropV2KeyspaceIfCurrent(name, epoch, revision) {
						delete(failures, name)
						break
					}
					continue
				}
				if checkErr != nil {
					failures[name] = checkErr
					break
				}
				if current.StrategyClass != base.StrategyClass || !maps.Equal(current.StrategyOptions, base.StrategyOptions) {
					failures[name] = cqltype.TypeValidation("schema refresh", "replication changed during refresh")
					continue
				}
			}
			p.InternalMu.Lock()
			changed := p.InternalSchemaEpoch[name] != epoch || p.schemaRevision[name] != revision || !p.tableRevisionsCurrentLocked(name, tables)
			if changed {
				p.InternalMu.Unlock()
				failures[name] = fmt.Errorf("schema changed during refresh")
				continue
			}
			if err != nil {
				p.InternalMu.Unlock()
				failures[name] = err
				// A concurrent DDL can make one schema read fail while the
				// keyspace is still listed: DESCRIBE no longer finds a keyspace
				// being dropped, or a table being created has no columns yet.
				// Read the keyspace again once that change has landed.
				if attempt < 7 && waitV2SchemaRetry(ctx, attempt) {
					continue
				}
				break
			}
			if s.privateStorage.InternalMetadataDescriber != nil {
				s.invalidateV2ChangedTablets(name, ks)
				s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalSet(name, ks)
				p.schemaRevision[name]++
				delete(p.schemaStale, name)
				delete(p.schemaDropped, name)
			}
			p.InternalMu.Unlock()
			delete(failures, name)
			break
		}
	}

	if s.privateStorage.InternalMetadataDescriber != nil {
		for name := range s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalGet() {
			if len(allowed) > 0 && !allowed[name] {
				continue
			}
			if _, exists := keyspaces[name]; exists {
				continue
			}
			p.InternalMu.Lock()
			if p.InternalSchemaEpoch[name] == epochs[name] && p.schemaRevision[name] == revisions[name] {
				p.schemaRevision[name]++
				s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalRemoveKeyspace(name)
				delete(p.schemaStale, name)
				s.privateStorage.InternalMetadataDescriber.RemoveTabletsWithKeyspace(name)
			}
			p.InternalMu.Unlock()
		}
	}
	p.InternalMu.Lock()
	p.refreshDiagnostics = nil
	for name, cause := range failures {
		p.refreshDiagnostics = append(p.refreshDiagnostics, fmt.Errorf("keyspace %s: %w", name, cause))
	}
	p.InternalMu.Unlock()
	p.schemaGeneration.Add(1)
	if !InternalSessionPublishV2Metadata(s) {
		return s.v2MetadataPublicationError()
	}
	if len(failures) > 0 {
		s.scheduleV2SchemaRetry()
		return &implMetadata.PartialRefreshError{Generation: s.Metadata().Generation(), Keyspaces: failures}
	}
	return nil
}
func InternalSessionRefreshV2Keyspace(s *Core, ctx context.Context, name string) error {
	p := InternalSessionPeripheral(s)
	if !p.admitMetadataWork() {
		return runtimeerrors.ErrClosed
	}
	defer p.workers.Done()
	return s.refreshV2KeyspaceAttempt(ctx, name, 0)
}
func (s *Core) refreshV2KeyspaceAttempt(ctx context.Context, name string, attempt int) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	p := InternalSessionPeripheral(s)
	p.InternalMu.Lock()
	epoch := p.InternalSchemaEpoch[name]
	revision := p.schemaRevision[name]
	tables := p.captureTableRevisionsLocked(name)
	config := p.InternalConfig
	p.InternalMu.Unlock()
	if config.DefaultProfile != nil && (config.Metadata.Mode == settings.MetadataDisabled || len(config.Metadata.Keyspaces) > 0 && !slices.Contains(config.Metadata.Keyspaces, name)) {
		if !InternalSessionPublishV2Metadata(s) {
			return s.v2MetadataPublicationError()
		}
		return nil
	}
	base, err := implMetadata.InternalGetV2KeyspaceMetadata(ctx, InternalNewSchemaSource(s), name)
	if errors.Is(err, runtimeerrors.ErrKeyspaceDoesNotExist) {
		p.InternalMu.Lock()
		if p.InternalSchemaEpoch[name] == epoch && p.schemaRevision[name] == revision {
			p.schemaRevision[name]++
			s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalRemoveKeyspace(name)
			delete(p.schemaStale, name)
			s.privateStorage.InternalMetadataDescriber.RemoveTabletsWithKeyspace(name)
		}
		p.InternalMu.Unlock()
		if !InternalSessionPublishV2Metadata(s) {
			return s.v2MetadataPublicationError()
		}
		return err
	}
	if err != nil {
		return s.v2SchemaRefreshFailure(name, err)
	}
	var ks *implMetadata.KeyspaceMetadata
	if config.DefaultProfile != nil && config.Metadata.Mode == settings.MetadataMinimal {
		ks = base
		ks.Tables = make(map[string]*implMetadata.TableMetadata)
	} else {
		ks, err = implMetadata.InternalFetchV2Keyspace(ctx, InternalNewSchemaSource(s), base)
	}
	if err != nil {
		// As in a full refresh, a concurrent DDL can fail one schema read;
		// confirm a drop, or read the keyspace again once the change landed.
		if dropped, _ := s.confirmV2DroppedKeyspace(ctx, name); dropped {
			if !InternalSessionPublishV2Metadata(s) {
				return s.v2MetadataPublicationError()
			}
			return runtimeerrors.ErrKeyspaceDoesNotExist
		}
		if attempt < 7 && waitV2SchemaRetry(ctx, attempt) {
			return s.refreshV2KeyspaceAttempt(ctx, name, attempt+1)
		}
		return s.v2SchemaRefreshFailure(name, err)
	}
	p.InternalMu.Lock()
	if p.InternalSchemaEpoch[name] != epoch || p.schemaRevision[name] != revision || !p.tableRevisionsCurrentLocked(name, tables) {
		p.InternalMu.Unlock()
		if attempt < 7 {
			return s.refreshV2KeyspaceAttempt(ctx, name, attempt+1)
		}
		return &implMetadata.PartialRefreshError{Generation: s.Metadata().Generation(), Keyspaces: map[string]error{name: fmt.Errorf("schema changed during refresh")}}
	}
	s.invalidateV2ChangedTablets(name, ks)
	s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalSet(name, ks)
	p.schemaRevision[name]++
	delete(p.schemaStale, name)
	delete(p.schemaDropped, name)
	p.schemaGeneration.Add(1)
	p.InternalMu.Unlock()
	if !InternalSessionPublishV2Metadata(s) {
		return s.v2MetadataPublicationError()
	}
	return nil
}

// waitV2SchemaRetry waits before reading a keyspace's schema again after a
// failed read, 25 ms doubling per attempt up to 400 ms, and reports whether
// ctx still allows the retry.
func waitV2SchemaRetry(ctx context.Context, attempt int) bool {
	timer := time.NewTimer(min(25*time.Millisecond<<attempt, 400*time.Millisecond))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *Core) scheduleV2SchemaRetry() {
	InternalSessionPeripheral(s).schemaRetryNeeded.Store(true)
	InternalSessionStartV2SchemaRetry(s)
}
func InternalSessionStartV2SchemaRetry(s *Core) {
	p := InternalSessionPeripheral(s)
	if !p.InternalRetryScheduled.CompareAndSwap(false, true) {
		return
	}
	if !p.InternalStartWorker(func() {
		ctx := implMetadata.InternalV2SchemaContext(InternalNewSchemaSource(s))
		timer := time.NewTimer(time.Second)
		defer timer.Stop()
		select {
		case <-p.InternalDone:
			p.InternalRetryScheduled.Store(false)
			return
		case <-ctx.Done():
			p.InternalRetryScheduled.Store(false)
			return
		case <-timer.C:
		}
		p.InternalMu.Lock()
		closed := p.closed
		timeout := p.InternalConfig.Metadata.RequestTimeout
		scopes := maps.Clone(p.InternalPreparedRefreshScopes)
		p.InternalMu.Unlock()
		if closed {
			p.InternalRetryScheduled.Store(false)
			return
		}
		ctx, cancel := InternalExecutionContext(ctx, timeout)
		if p.schemaRetryNeeded.Swap(false) {
			if err := InternalSessionRefreshV2AllSchema(s, ctx); err != nil {
				p.schemaRetryNeeded.Store(true)
			}
		}
		for keyspace, version := range scopes {
			if err := s.reconcileV2PreparedScope(ctx, keyspace); err == nil {
				p.InternalMu.Lock()
				if p.InternalPreparedRefreshScopes[keyspace] == version {
					delete(p.InternalPreparedRefreshScopes, keyspace)
				}
				p.InternalMu.Unlock()
				p.InternalSchemaReconciled.Add(1)
			}
		}
		cancel()
		p.InternalRetryScheduled.Store(false)
		p.InternalMu.Lock()
		pending := len(p.InternalPreparedRefreshScopes) > 0
		p.InternalMu.Unlock()
		if (pending || p.schemaRetryNeeded.Load()) && !s.Closed() {
			InternalSessionStartV2SchemaRetry(s)
		}
	}) {
		p.InternalRetryScheduled.Store(false)
	}
}

// typedSchemaGeneration deliberately excludes topology and tablet-cache updates.
// Those changes must not invalidate every prepared statement.
func InternalSessionTypedSchemaGeneration(s *Core) uint64 {
	p := InternalSessionPeripheral(s)
	if p == nil {
		return 0
	}
	return p.schemaGeneration.Load() + 1
}

func InternalSessionRecordV2Latency(s *Core, latency time.Duration) {
	if s == nil || latency < 0 {
		return
	}
	p := InternalSessionPeripheral(s)
	index := p.latencyCount.Add(1) - 1
	p.latency[index%uint64(len(p.latency))].Store(int64(latency))

}
func (p *InternalV2PeripheralState) InternalPublishLatencySummary() {
	count := min(p.latencyCount.Load(), uint64(len(p.latency)))
	if count == 0 {
		return
	}
	samples := make([]time.Duration, int(count))
	for i := range samples {
		samples[i] = time.Duration(p.latency[i].Load())
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	percentile := func(percent int) time.Duration { return samples[(len(samples)*percent+99)/100-1] }
	p.latencySummary.Store(&InternalV2LatencySummary{p50: percentile(50), p95: percentile(95), p99: percentile(99)})
}

func InternalSessionRecordV2Orphan(s *Core) {
	if s != nil {
		InternalSessionPeripheral(s).orphans.Add(1)
	}
}

func InternalSessionMarkV2SchemaStale(s *Core, keyspace string, dropped bool) {
	p := InternalSessionPeripheral(s)
	p.InternalMu.Lock()
	if dropped {
		delete(p.schemaStale, keyspace)
	} else {
		p.schemaStale[keyspace] = true
	}
	p.InternalMu.Unlock()
}
func (s *Core) invalidateV2ChangedTablets(keyspace string, current *implMetadata.KeyspaceMetadata) {
	if s.privateStorage.InternalMetadataDescriber == nil {
		return
	}
	previous, _ := s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalGetKeyspace(keyspace)
	if previous == nil {
		if m := InternalSessionPeripheral(s).InternalSnapshot.Load(); m != nil {
			previous = routing.SnapshotData(m).InternalKeyspaces[keyspace]
		}
	}
	if previous == nil {
		return
	}
	if !maps.Equal(previous.StrategyOptions, current.StrategyOptions) || previous.StrategyClass != current.StrategyClass {
		s.privateStorage.InternalMetadataDescriber.RemoveTabletsWithKeyspace(keyspace)
		return
	}
	for name, table := range previous.Tables {
		next := current.Tables[name]
		if next == nil || !table.Equals(next) {
			s.privateStorage.InternalMetadataDescriber.RemoveTabletsWithTable(keyspace, name)
		}
	}
}

func (s *Core) dropV2KeyspaceIfCurrent(name string, epoch uint64, revision ...uint64) bool {
	p := InternalSessionPeripheral(s)
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()
	if p.InternalSchemaEpoch[name] != epoch || len(revision) > 0 && p.schemaRevision[name] != revision[0] {
		return false
	}
	p.schemaRevision[name]++
	if s.privateStorage.InternalMetadataDescriber != nil {
		s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalRemoveKeyspace(name)
		s.privateStorage.InternalMetadataDescriber.RemoveTabletsWithKeyspace(name)
	}
	delete(p.schemaStale, name)
	p.schemaDropped[name] = true
	return true
}
func (s *Core) confirmV2DroppedKeyspace(ctx context.Context, name string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	p := InternalSessionPeripheral(s)
	p.InternalMu.Lock()
	epoch := p.InternalSchemaEpoch[name]
	revision := p.schemaRevision[name]
	p.InternalMu.Unlock()
	_, err := implMetadata.InternalGetV2KeyspaceMetadata(ctx, InternalNewSchemaSource(s), name)
	if errors.Is(err, runtimeerrors.ErrKeyspaceDoesNotExist) {
		return s.dropV2KeyspaceIfCurrent(name, epoch, revision), nil
	}
	return false, err
}

func (s *Core) runV2PeriodicRefresh(options settings.MetadataOptions) {
	ticker := time.NewTicker(options.PeriodicRefresh)
	defer ticker.Stop()
	p := InternalSessionPeripheral(s)
	for {
		select {
		case <-p.InternalDone:
			return
		case <-ticker.C:
		}
		ctx := implMetadata.InternalV2SchemaContext(InternalNewSchemaSource(s))
		timeout := options.RequestTimeout
		ctx, cancel := InternalExecutionContext(ctx, timeout)
		err := s.RefreshMetadata(ctx)
		cancel()
		if err != nil && s.privateStorage.InternalLogger != nil {
			s.privateStorage.InternalLogger.Printf("gocql: periodic metadata refresh: %v", err)
		}
	}
}

func (s *Core) v2MetadataDepthLimit() int {
	p := InternalSessionPeripheral(s)
	p.InternalMu.Lock()
	limit := p.InternalConfig.Limits.MaxMetadataDepth
	p.InternalMu.Unlock()
	if limit <= 0 {
		return 64
	}
	return limit
}

// startWorker serializes admission with shutdown; Add can never race Wait.
func (p *InternalV2PeripheralState) InternalStartWorker(fn func()) bool {
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()
	if p.closed {
		return false
	}
	p.workers.Add(1)
	go func() { defer p.workers.Done(); fn() }()
	return true
}

func (p *InternalV2PeripheralState) admitMetadataWork() bool {
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()
	if p.closed {
		return false
	}
	p.workers.Add(1)
	return true
}

// InternalBumpSchemaEpochLocked advances keyspace's schema epoch. Caller holds p.InternalMu.
func (p *InternalV2PeripheralState) InternalBumpSchemaEpochLocked(keyspace string) {
	p.InternalSchemaEpoch[keyspace]++
	p.schemaEpochs.Store(new(maps.Clone(p.InternalSchemaEpoch)))
}

// InternalBumpTableRevisionLocked advances k's table revision. Caller holds p.InternalMu.
func (p *InternalV2PeripheralState) InternalBumpTableRevisionLocked(k routing.InternalMetadataTable) {
	p.InternalTableRevision[k]++
	p.tableRevisions.Store(new(maps.Clone(p.InternalTableRevision)))
}

func (p *InternalV2PeripheralState) captureTableRevisionsLocked(keyspace string) map[routing.InternalMetadataTable]uint64 {
	out := make(map[routing.InternalMetadataTable]uint64)
	for key, revision := range p.InternalTableRevision {
		if key.InternalKeyspace == keyspace {
			out[key] = revision
		}
	}
	return out
}
func (p *InternalV2PeripheralState) tableRevisionsCurrentLocked(keyspace string, before map[routing.InternalMetadataTable]uint64) bool {
	for key, revision := range p.InternalTableRevision {
		if key.InternalKeyspace == keyspace && before[key] != revision {
			return false
		}
	}
	return true
}

func (s *Core) v2SchemaRefreshFailure(name string, cause error) error {
	p := InternalSessionPeripheral(s)
	p.InternalMu.Lock()
	if !p.schemaDropped[name] {
		p.schemaStale[name] = true
	}
	p.refreshDiagnostics = []error{fmt.Errorf("keyspace %s: %w", name, cause)}
	p.InternalMu.Unlock()
	InternalSessionPublishV2Metadata(s)
	s.scheduleV2SchemaRetry()
	return &implMetadata.PartialRefreshError{Generation: s.Metadata().Generation(), Keyspaces: map[string]error{name: cause}}
}

func (s *Core) v2MetadataPublicationError() error {
	return &implMetadata.PartialRefreshError{Generation: s.Metadata().Generation(), Keyspaces: map[string]error{"<publication>": fmt.Errorf("metadata sources changed throughout publication; refresh again")}}
}

// InternalSessionRoutingHostIndex indexes hosts with the session's routes to
// them, so that plans route to their pools without looking them up.
func InternalSessionRoutingHostIndex(s *Core, hosts []model.HostSnapshot) *routing.InternalRoutingHostIndex {
	if s.privateStorage.InternalPool == nil {
		return routing.InternalBuildRoutingHostIndex(hosts)
	}
	return routing.InternalBuildRoutingHostIndexWith(hosts, s.privateStorage.InternalPool.InternalRoute)
}

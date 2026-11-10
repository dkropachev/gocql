// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"errors"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/model"
	routing "github.com/scylladb/gocql/v2/internal/routing"

	"github.com/scylladb/gocql/v2/internal/tablets"
)

func InternalSessionCaptureTabletFeedback(s *Core, info model.RoutingInfo, m *routing.MetadataSnapshot) routing.InternalTabletFeedbackStamp {
	p := InternalSessionPeripheral(s)
	if p == nil {
		return routing.InternalTabletFeedbackStamp{}
	}
	// No lock: every input is an atomically published copy. A capture that
	// races a schema change can mix values from before and after it; then at
	// least one of them differs from what the publisher compares it with
	// under the lock, and the feedback is dropped, as it would be if it had
	// been captured before the change.
	// The schema source is live, so its identity is read now; the snapshot
	// and the epoch maps are immutable, so what is read from them waits for
	// a hint in InternalSessionResolveTabletFeedback.
	identity, ok := s.tabletSchemaIdentity(info.Keyspace, info.Table)
	return routing.InternalTabletFeedbackStamp{InternalValid: ok, InternalSnapshot: m, InternalEpoch: p.InternalPlacementEpoch.Load(), InternalSchemaEpochs: p.schemaEpochs.Load(), InternalTableRevisions: p.tableRevisions.Load(), InternalIdentity: identity}
}

// InternalSessionResolveTabletFeedback completes a stamp captured for table k
// with the values it deferred, as they were at capture: the stamp is valid
// only if the schema source identity matched the snapshot's.
func sessionResolveTabletFeedback(stamp *routing.InternalTabletFeedbackStamp, k routing.InternalMetadataTable) {
	if stamp.InternalSchemaEpochs == nil {
		return
	}
	identity := routing.InternalMetadataSnapshotTableIdentity(stamp.InternalSnapshot, k)
	stamp.InternalValid = stamp.InternalValid && stamp.InternalIdentity == identity
	stamp.InternalIdentity = identity
	stamp.InternalSchema = (*stamp.InternalSchemaEpochs)[k.InternalKeyspace]
	stamp.InternalTable = (*stamp.InternalTableRevisions)[k]
	stamp.InternalSchemaEpochs, stamp.InternalTableRevisions = nil, nil
}

type InternalV2TabletFeedbackKey struct {
	InternalKeyspace, InternalTable string
	InternalEnd                     int64
}
type InternalV2PendingTabletFeedback struct {
	InternalStamp routing.InternalTabletFeedbackStamp
	InternalHint  tablets.TabletInfo
	observation   routing.InternalTabletRangeObservation
}

// applyV2TabletHint is retained for internal legacy callers/tests. Actual EXECUTE
// responses supply the stamp captured before sending, never a receipt-time stamp.
func sessionApplyV2TabletHint(s *Core, h tablets.TabletInfo) {
	info := model.RoutingInfo{Keyspace: h.KeyspaceName(), Table: h.TableName()}
	InternalSessionQueueTabletFeedback(s, h, InternalSessionCaptureTabletFeedback(s, info, s.Metadata()))
}
func InternalSessionQueueTabletFeedback(s *Core, h tablets.TabletInfo, stamp routing.InternalTabletFeedbackStamp) {
	p := InternalSessionPeripheral(s)
	if p == nil || stamp.InternalSnapshot == nil || !stamp.InternalValid {
		return
	}
	k := routing.InternalMetadataTable{InternalKeyspace: h.KeyspaceName(), InternalTable: h.TableName()}
	if sessionResolveTabletFeedback(&stamp, k); !stamp.InternalValid {
		return
	}
	if _, _, err := routing.InternalMetadataSnapshotLookupRanges(stamp.InternalSnapshot, k.InternalKeyspace, k.InternalTable); err != nil {
		return
	}
	select {
	case <-p.InternalDone:
		return
	default:
	}
	before, _ := routing.InternalMetadataSnapshotTabletRoot(stamp.InternalSnapshot, k)
	observation := routing.InternalObserveTabletRanges(before, h.FirstToken(), h.LastToken())
	stamp.InternalSnapshot = nil // the queue never retains a cluster/schema snapshot or page.
	p.InternalFeedbackMu.Lock()
	key := InternalV2TabletFeedbackKey{k.InternalKeyspace, k.InternalTable, h.LastToken()}
	if !p.knownTabletTables[k] {
		p.knownTabletTables[k] = true
	}
	prior, ok := p.InternalFeedback[key]
	if !ok && len(p.InternalFeedback) >= 1024 {
		clear(p.InternalFeedback)
		p.InternalFeedbackResync = true
		p.InternalPlacementEpoch.Add(1)
	}
	// Keep the earliest pending observation for an overlapping identity/range.
	// Later requests can relearn after it publishes; receipt order is not freshness.
	if !ok || prior.InternalStamp.InternalEpoch != stamp.InternalEpoch || prior.InternalStamp.InternalSchema != stamp.InternalSchema || prior.InternalStamp.InternalTable != stamp.InternalTable || prior.InternalStamp.InternalIdentity != stamp.InternalIdentity {
		p.InternalFeedback[key] = InternalV2PendingTabletFeedback{InternalHint: h, InternalStamp: stamp, observation: observation}
	}
	p.InternalFeedbackMu.Unlock()
	p.InternalFeedbackOnce.Do(func() { p.InternalStartWorker(func() { sessionRunV2TabletFeedback(s, p) }) })
	select {
	case p.feedbackWake <- struct{}{}:
	default:
	}
}
func InternalSessionPublishTabletFeedback(s *Core, p *InternalV2PeripheralState, updates map[InternalV2TabletFeedbackKey]InternalV2PendingTabletFeedback, resync bool) {
	var known []routing.InternalMetadataTable
	if resync {
		p.InternalFeedbackMu.Lock()
		for k := range p.knownTabletTables {
			known = append(known, k)
		}
		p.InternalFeedbackMu.Unlock()
	}
	p.publishMu.Lock()
	p.InternalMu.Lock()
	old := p.InternalSnapshot.Load()
	if old == nil || p.closed {
		p.InternalMu.Unlock()
		p.publishMu.Unlock()
		return
	}
	next := old
	ensureNext := func() {
		if next == old {
			next = routing.InternalClonePlacementSnapshot(old)
			routing.SnapshotData(next).InternalPlacementSignal = &p.InternalPlacementEpoch
		}
	}
	epoch := p.InternalPlacementEpoch.Load()
	if routing.SnapshotData(old).InternalPlacementEpoch != epoch {
		ensureNext()
		routing.SnapshotData(next).InternalPlacements = nil
		routing.SnapshotData(next).InternalPlacementEpoch = epoch
		// Invalidated mappings remain explicitly tablet-based, including tables
		// learned by feedback before metadata advertised tablets.
		routing.InternalPlacementEach(routing.SnapshotData(old).InternalPlacements, func(k routing.InternalMetadataTable, t routing.InternalTabletPlacement) {
			t.InternalRanges = nil
			routing.SnapshotData(next).InternalPlacements = routing.InternalPlacementPut(routing.SnapshotData(next).InternalPlacements, k, t, routing.InternalComparePlacementTable)
		})
	}
	if resync {
		ensureNext()
		routing.SnapshotData(next).InternalDiagnostics = make([]error, 0, len(routing.SnapshotData(old).InternalDiagnostics)+1)
		for _, diagnostic := range routing.SnapshotData(old).InternalDiagnostics {
			if v, ok := errors.AsType[*cqltype.ValidationError](diagnostic); ok && v.Field == "tablet placement" {
				continue
			}
			routing.SnapshotData(next).InternalDiagnostics = append(routing.SnapshotData(next).InternalDiagnostics, diagnostic)
		}
		routing.SnapshotData(next).InternalDiagnostics = append(routing.SnapshotData(next).InternalDiagnostics, cqltype.TypeValidation("tablet placement", "feedback queue saturated; mappings invalidated"))
		for _, k := range known {
			if _, _, err := routing.InternalMetadataSnapshotLookupRanges(old, k.InternalKeyspace, k.InternalTable); err == nil && routing.InternalPlacementGet(routing.SnapshotData(next).InternalPlacements, k, routing.InternalComparePlacementTable) == nil {
				routing.SnapshotData(next).InternalPlacements = routing.InternalPlacementPut(routing.SnapshotData(next).InternalPlacements, k, routing.InternalTabletPlacement{InternalIdentity: routing.InternalMetadataSnapshotTableIdentity(old, k)}, routing.InternalComparePlacementTable)
			}
		}
	}
	for _, u := range updates {
		h, stamp := u.InternalHint, u.InternalStamp
		k := routing.InternalMetadataTable{InternalKeyspace: h.KeyspaceName(), InternalTable: h.TableName()}
		if sessionResolveTabletFeedback(&stamp, k); !stamp.InternalValid {
			continue
		}
		if !s.tabletSchemaCurrent(k, stamp.InternalIdentity) || stamp.InternalEpoch != epoch || stamp.InternalSchema != p.schemaEpoch[k.InternalKeyspace] || stamp.InternalTable != p.tableRevision[k] || p.schemaDropped[k.InternalKeyspace] || stamp.InternalIdentity != routing.InternalMetadataSnapshotTableIdentity(old, k) {
			continue
		}
		if _, _, err := routing.InternalMetadataSnapshotLookupRanges(old, k.InternalKeyspace, k.InternalTable); err != nil {
			continue
		}
		var current *routing.InternalTabletRangeTree
		t := routing.InternalTabletPlacement{InternalIdentity: stamp.InternalIdentity}
		if n := routing.InternalPlacementGet(routing.SnapshotData(next).InternalPlacements, k, routing.InternalComparePlacementTable); n != nil {
			t = n.InternalValue
			current = t.InternalRanges
		}
		// Exact duplicates do not replace nodes, generations, or events.
		n := routing.InternalTabletRangeAfter(current, h.FirstToken())
		if n != nil && routing.InternalSameTabletObservation(n.InternalValue, h) {
			continue
		}
		if u.observation != routing.InternalObserveTabletRanges(current, h.FirstToken(), h.LastToken()) {
			continue
		}
		var changed bool
		t, changed = routing.InternalUpdateTabletPlacement(t, h)
		if changed {
			ensureNext()
			routing.SnapshotData(next).InternalPlacements = routing.InternalPlacementPut(routing.SnapshotData(next).InternalPlacements, k, t, routing.InternalComparePlacementTable)
		}
	}
	changed := next != old
	if changed {
		p.InternalSnapshot.Store(next)
	}
	p.InternalMu.Unlock()
	p.publishMu.Unlock()
	if resync {
		InternalSessionPublishV2LocalEvent(s, Event{Kind: OverflowEvent, Generation: routing.SnapshotData(next).InternalGeneration, Change: "TABLET_RESYNC"})
	}
}
func sessionRunV2TabletFeedback(s *Core, p *InternalV2PeripheralState) {
	for {
		select {
		case <-p.InternalDone:
			return
		case <-p.feedbackWake:
		}
		p.InternalFeedbackMu.Lock()
		pending := p.InternalFeedback
		p.InternalFeedback = make(map[InternalV2TabletFeedbackKey]InternalV2PendingTabletFeedback)
		resync := p.InternalFeedbackResync
		p.InternalFeedbackResync = false
		p.InternalFeedbackMu.Unlock()
		InternalSessionPublishTabletFeedback(s, p, pending, resync)
	}
}

func InternalSessionCapturedRoutingPartitioner(s *Core, name string) routing.TokenPartitioner {
	p := InternalSessionPeripheral(s)
	if p == nil {
		return nil
	}
	if partitioners := p.partitioners.Load(); partitioners != nil {
		return (*partitioners)[name]
	}
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()
	return p.InternalConfig.Partitioners[name]
}

// Source identity is checked as well as the public
// snapshot: a drop can already be fenced while its snapshot build is pending.
func (s *Core) tabletSchemaCurrent(k routing.InternalMetadataTable, identity string) bool {
	id, ok := s.tabletSchemaIdentity(k.InternalKeyspace, k.InternalTable)
	return ok && id == identity
}

// tabletSchemaIdentity returns the identity of a table or view in the
// schema source, or false when the source lacks it or has invalidated it.
func (s *Core) tabletSchemaIdentity(keyspace, table string) (string, bool) {
	if s.privateStorage.InternalMetadataDescriber == nil || s.privateStorage.InternalMetadataDescriber.InternalMetadata == nil {
		return "", false
	}
	source := s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalKeyspaceMap.Load()
	if source == nil {
		return "", false
	}
	ks := (*source)[keyspace]
	if ks == nil {
		return "", false
	}
	if _, invalid := implMetadata.KeyspaceData(ks).InternalTablesInvalidated[table]; invalid {
		return "", false
	}
	if t := ks.Tables[table]; t != nil {
		return t.ID, true
	}
	if v := ks.Views[table]; v != nil {
		return v.ID, true
	}
	return "", false
}

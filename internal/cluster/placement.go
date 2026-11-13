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

func (s *Core) CaptureTabletFeedback(info model.RoutingInfo, m *routing.MetadataSnapshot) routing.InternalTabletFeedbackStamp {
	p := s.Peripheral()
	if p == nil {
		return routing.InternalTabletFeedbackStamp{}
	}
	// No lock: every input is an atomically published copy. A capture that
	// races a schema change can mix values from before and after it; then at
	// least one of them differs from what the publisher compares it with
	// under the lock, and the feedback is dropped, as it would be if it had
	// been captured before the change.
	// The schema source, the snapshot and the epoch maps are all replaced on
	// change, never changed in place, so what is read from them waits for a
	// hint in sessionResolveTabletFeedback.
	source := s.schemaSource()
	return routing.InternalTabletFeedbackStamp{InternalValid: source != nil, InternalSnapshot: m, InternalEpoch: p.InternalPlacementEpoch.Load(), InternalSchemaEpochs: p.schemaEpochs.Load(), InternalTableRevisions: p.tableRevisions.Load(), InternalSchemaSource: source}
}

// sessionResolveTabletFeedback completes a stamp captured for table k
// with the values it deferred, as they were at capture, and returns the
// table's identity. A stamp with epoch maps is valid only if the schema
// source identity matches the snapshot's, which is returned; one without
// returns the schema source identity.
func sessionResolveTabletFeedback(stamp *routing.InternalTabletFeedbackStamp, k routing.InternalMetadataTable) string {
	identity, ok := schemaSourceIdentity(stamp.InternalSchemaSource, k.InternalKeyspace, k.InternalTable)
	stamp.InternalValid = stamp.InternalValid && ok
	stamp.InternalSchemaSource = nil
	if stamp.InternalSchemaEpochs == nil {
		return identity
	}
	snapshot := routing.InternalMetadataSnapshotTableIdentity(stamp.InternalSnapshot, k)
	stamp.InternalValid = stamp.InternalValid && identity == snapshot
	stamp.InternalSchema = (*stamp.InternalSchemaEpochs)[k.InternalKeyspace]
	stamp.InternalTable = (*stamp.InternalTableRevisions)[k]
	stamp.InternalSchemaEpochs, stamp.InternalTableRevisions = nil, nil
	return snapshot
}

type InternalV2TabletFeedbackKey struct {
	InternalKeyspace, InternalTable string
	InternalEnd                     int64
}
type InternalV2PendingTabletFeedback struct {
	// identity is the table's identity sessionResolveTabletFeedback returned
	// for InternalStamp; a stamp not yet resolved is resolved on publication.
	identity      string
	InternalStamp routing.InternalTabletFeedbackStamp
	InternalHint  tablets.TabletInfo
	observation   routing.InternalTabletRangeObservation
}

// applyV2TabletHint is retained for internal legacy callers/tests. Actual EXECUTE
// responses supply the stamp captured before sending, never a receipt-time stamp.
func sessionApplyV2TabletHint(s *Core, h tablets.TabletInfo) {
	info := model.RoutingInfo{Keyspace: h.KeyspaceName(), Table: h.TableName()}
	s.QueueTabletFeedback(h, s.CaptureTabletFeedback(info, s.Metadata()))
}
func (s *Core) QueueTabletFeedback(h tablets.TabletInfo, stamp routing.InternalTabletFeedbackStamp) {
	p := s.Peripheral()
	if p == nil || stamp.InternalSnapshot == nil || !stamp.InternalValid {
		return
	}
	k := routing.InternalMetadataTable{InternalKeyspace: h.KeyspaceName(), InternalTable: h.TableName()}
	identity := sessionResolveTabletFeedback(&stamp, k)
	if !stamp.InternalValid {
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
	// The queue never retains a cluster/schema snapshot or page; resolution
	// already cleared the schema source.
	stamp.InternalSnapshot = nil
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
	if !ok || prior.InternalStamp.InternalEpoch != stamp.InternalEpoch || prior.InternalStamp.InternalSchema != stamp.InternalSchema || prior.InternalStamp.InternalTable != stamp.InternalTable || prior.identity != identity {
		p.InternalFeedback[key] = InternalV2PendingTabletFeedback{InternalHint: h, InternalStamp: stamp, identity: identity, observation: observation}
	}
	p.InternalFeedbackMu.Unlock()
	p.InternalFeedbackOnce.Do(func() { p.InternalStartWorker(func() { sessionRunV2TabletFeedback(s, p) }) })
	select {
	case p.feedbackWake <- struct{}{}:
	default:
	}
}
func (s *Core) PublishTabletFeedback(p *InternalV2PeripheralState, updates map[InternalV2TabletFeedbackKey]InternalV2PendingTabletFeedback, resync bool) {
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
		for k, t := range routing.SnapshotData(old).InternalPlacements.All() {
			t.InternalRanges = nil
			routing.SnapshotData(next).InternalPlacements = routing.SnapshotData(next).InternalPlacements.Put(k, t)
		}
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
			if _, _, err := routing.InternalMetadataSnapshotLookupRanges(old, k.InternalKeyspace, k.InternalTable); err == nil && routing.SnapshotData(next).InternalPlacements.Get(k) == nil {
				routing.SnapshotData(next).InternalPlacements = routing.SnapshotData(next).InternalPlacements.Put(k, routing.InternalTabletPlacement{InternalIdentity: routing.InternalMetadataSnapshotTableIdentity(old, k)})
			}
		}
	}
	for _, u := range updates {
		h, stamp, identity := u.InternalHint, u.InternalStamp, u.identity
		k := routing.InternalMetadataTable{InternalKeyspace: h.KeyspaceName(), InternalTable: h.TableName()}
		if stamp.InternalSchemaSource != nil {
			identity = sessionResolveTabletFeedback(&stamp, k)
		}
		if !stamp.InternalValid {
			continue
		}
		if !s.tabletSchemaCurrent(k, identity) || stamp.InternalEpoch != epoch || stamp.InternalSchema != p.schemaEpoch[k.InternalKeyspace] || stamp.InternalTable != p.tableRevision[k] || p.schemaDropped[k.InternalKeyspace] || identity != routing.InternalMetadataSnapshotTableIdentity(old, k) {
			continue
		}
		if _, _, err := routing.InternalMetadataSnapshotLookupRanges(old, k.InternalKeyspace, k.InternalTable); err != nil {
			continue
		}
		var current *routing.InternalTabletRangeTree
		t := routing.InternalTabletPlacement{InternalIdentity: identity}
		if n := routing.SnapshotData(next).InternalPlacements.Get(k); n != nil {
			t = n.InternalValue
			current = t.InternalRanges
		}
		// Exact duplicates do not replace nodes, generations, or events.
		n := routing.InternalTabletRangeAfter(current, h.FirstToken())
		if n != nil && n.InternalValue.SameTabletObservation(h) {
			continue
		}
		if u.observation != routing.InternalObserveTabletRanges(current, h.FirstToken(), h.LastToken()) {
			continue
		}
		var changed bool
		t, changed = routing.InternalUpdateTabletPlacement(t, h)
		if changed {
			ensureNext()
			routing.SnapshotData(next).InternalPlacements = routing.SnapshotData(next).InternalPlacements.Put(k, t)
		}
	}
	changed := next != old
	if changed {
		p.InternalSnapshot.Store(next)
	}
	p.InternalMu.Unlock()
	p.publishMu.Unlock()
	if resync {
		s.PublishV2LocalEvent(Event{Kind: OverflowEvent, Generation: routing.SnapshotData(next).InternalGeneration, Change: "TABLET_RESYNC"})
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
		s.PublishTabletFeedback(p, pending, resync)
	}
}

func (s *Core) CapturedRoutingPartitioner(name string) routing.TokenPartitioner {
	p := s.Peripheral()
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
	return schemaSourceIdentity(s.schemaSource(), keyspace, table)
}

// schemaSource is the live schema source map, or nil without one. The map
// is replaced on every change, never changed in place.
func (s *Core) schemaSource() *map[string]*implMetadata.KeyspaceMetadata {
	if s.privateStorage.InternalMetadataDescriber == nil || s.privateStorage.InternalMetadataDescriber.InternalMetadata == nil {
		return nil
	}
	return s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalKeyspaceMap.Load()
}

// testHookSchemaIdentity, when set, is called on each schema source
// identity lookup.
var testHookSchemaIdentity func()

// schemaSourceIdentity returns the identity of a table or view in source, or
// false when source lacks it or has invalidated it.
func schemaSourceIdentity(source *map[string]*implMetadata.KeyspaceMetadata, keyspace, table string) (string, bool) {
	if testHookSchemaIdentity != nil {
		testHookSchemaIdentity()
	}
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

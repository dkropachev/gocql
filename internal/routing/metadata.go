// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"sync/atomic"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/link"
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	model "github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/routing"
)

type MetadataSnapshot struct {
	// public is the handle package metadata exposes for the snapshot.
	public *metadata.Snapshot
	state  SnapshotState
	// id is unique among the snapshots of the process, so a cache can key
	// what it found in a snapshot by id without keeping the snapshot alive.
	id uint64
}

// snapshotIDs numbers snapshots for MetadataSnapshot.id.
var snapshotIDs atomic.Uint64

type InternalMetadataTable = model.TableKey

func (m *MetadataSnapshot) Generation() uint64 {
	if m == nil {
		return 0
	}
	return SnapshotData(m).InternalGeneration
}
func (m *MetadataSnapshot) Hosts() []HostSnapshot {
	if m == nil {
		return nil
	}
	out := append([]HostSnapshot(nil), SnapshotData(m).InternalHosts...)
	for i := range out {
		out[i].Tokens = append([]int64(nil), out[i].Tokens...)
	}
	return out
}
func (m *MetadataSnapshot) Keyspace(name string) (*metadata.Keyspace, bool) {
	if m == nil {
		return nil, false
	}
	ks, ok := SnapshotData(m).InternalKeyspaces[name]
	if !ok {
		return nil, false
	}
	return implMetadata.InternalCloneV2Keyspace(ks), true
}

// Keyspaces returns the names of the keyspaces the snapshot holds, sorted.
func (m *MetadataSnapshot) Keyspaces() []string {
	if m == nil {
		return nil
	}
	return slices.Sorted(maps.Keys(SnapshotData(m).InternalKeyspaces))
}

// Table returns a copy of the schema of keyspace.table.
func (m *MetadataSnapshot) Table(keyspace, table string) (*metadata.Table, bool) {
	if m == nil {
		return nil, false
	}
	ks := SnapshotData(m).InternalKeyspaces[keyspace]
	if ks == nil {
		return nil, false
	}
	t := ks.Tables[table]
	if t == nil {
		return nil, false
	}
	return implMetadata.InternalCloneV2Table(t), true
}

// View returns a copy of the schema of the materialized view keyspace.view.
func (m *MetadataSnapshot) View(keyspace, view string) (*metadata.View, bool) {
	if m == nil {
		return nil, false
	}
	ks := SnapshotData(m).InternalKeyspaces[keyspace]
	if ks == nil || ks.Views[view] == nil {
		return nil, false
	}
	return implMetadata.InternalCloneV2View(ks.Views[view]), true
}

// Type returns a copy of the user-defined type keyspace.name.
func (m *MetadataSnapshot) Type(keyspace, name string) (*metadata.Type, bool) {
	if m == nil {
		return nil, false
	}
	ks := SnapshotData(m).InternalKeyspaces[keyspace]
	if ks == nil || ks.Types[name] == nil {
		return nil, false
	}
	return implMetadata.InternalCloneV2Type(ks.Types[name]), true
}

// Tables returns the names of the tables of keyspace, sorted.
func (m *MetadataSnapshot) Tables(keyspace string) []string {
	if m == nil {
		return nil
	}
	ks := SnapshotData(m).InternalKeyspaces[keyspace]
	if ks == nil {
		return nil
	}
	return slices.Sorted(maps.Keys(ks.Tables))
}

// Host returns a copy of the host with id.
func (m *MetadataSnapshot) Host(id cql.UUID) (HostSnapshot, bool) {
	i := m.hostPosition(id)
	if i < 0 {
		return HostSnapshot{}, false
	}
	h := SnapshotData(m).InternalHosts[i]
	h.Tokens = append([]int64(nil), h.Tokens...)
	return h, true
}

// HostInfo returns a copy of the host with id without its tokens.
func (m *MetadataSnapshot) HostInfo(id cql.UUID) (HostSnapshot, bool) {
	i := m.hostPosition(id)
	if i < 0 {
		return HostSnapshot{}, false
	}
	h := SnapshotData(m).InternalHosts[i]
	h.Tokens = nil
	return h, true
}

// hostPosition returns the position of the host with id, or -1.
func (m *MetadataSnapshot) hostPosition(id cql.UUID) int {
	if m == nil {
		return -1
	}
	hosts := SnapshotData(m).InternalHosts
	i, ok := -1, false
	if idx := SnapshotData(m).InternalHostIndex; idx != nil {
		i, ok = idx.host(id)
	}
	if !ok || i >= len(hosts) || hosts[i].HostID != id {
		i = slices.IndexFunc(hosts, func(h HostSnapshot) bool { return h.HostID == id })
	}
	return i
}

// Replicas returns the replicas of token in keyspace.table from the
// snapshot's own placement, as AppendReplicaRoutes finds them: a vnode
// keyspace's ring places a table the snapshot does not hold, and a vnode
// replica's shard is the one owning token on its host when the host's
// sharding is known. A token of a tablet table that no tablet the snapshot
// holds covers is metadata.ErrPlacementUnknown.
func (m *MetadataSnapshot) Replicas(keyspace, table string, token int64) ([]Target, error) {
	if m == nil {
		return nil, cqlerr.ErrNoKeyspace
	}
	if _, _, err := routingRanges(m, keyspace, table); err != nil {
		return nil, err
	}
	r, tablet := m.placementReplicas(keyspace, table, token)
	if len(r) == 0 && tablet {
		return nil, fmt.Errorf("%s.%s token %d: %w", keyspace, table, token, metadata.ErrPlacementUnknown)
	}
	out := append([]Target(nil), r...)
	x := m.routingIndex()
	hosts := SnapshotData(m).InternalHosts
	for i := range out {
		if out[i].ShardID.Set {
			continue
		}
		if j, ok := x.host(out[i].HostID); ok && j < len(hosts) && hosts[j].Shards > 0 {
			out[i].ShardID = cql.Option[uint32]{Value: uint32(shardOf(token, hosts[j].Shards, hosts[j].ShardingIgnoreMSB)), Set: true}
		}
	}
	return out, nil
}

// Token returns the token of the partition whose partition-key columns hold
// key, in column order, for keyspace.table (a table or a materialized view).
// Each value is encoded as the column's schema type, exactly as a prepared
// statement's routing key is, and hashed with the table's partitioner. It
// needs loaded schema metadata; a key of the wrong arity or type, or a null
// component, is a validation error.
func (m *MetadataSnapshot) Token(keyspace, table string, key ...cql.Value) (int64, error) {
	if m == nil {
		return 0, cqlerr.ErrNoKeyspace
	}
	data := SnapshotData(m)
	ks := data.InternalKeyspaces[keyspace]
	if ks == nil {
		return 0, keyspaceNotLoaded(keyspace)
	}
	var columns []*metadata.Column
	partitioner := data.InternalPartitioner
	if t := ks.Tables[table]; t != nil {
		columns = t.PartitionKey
		if t.Options.Partitioner != "" {
			partitioner = t.Options.Partitioner
		}
	} else if v := ks.Views[table]; v != nil {
		columns = v.PartitionKey
		if v.Options.Partitioner != "" {
			partitioner = v.Options.Partitioner
		}
	} else {
		return 0, tableNotLoaded(keyspace, table)
	}
	if len(columns) == 0 {
		return 0, cqltype.TypeValidation("partition key", "%s.%s has no partition-key metadata", keyspace, table)
	}
	if len(key) != len(columns) {
		return 0, cqltype.TypeValidation("partition key", "%s.%s takes %d components, got %d", keyspace, table, len(columns), len(key))
	}
	p := data.InternalPartitioners[partitioner]
	if p == nil {
		return 0, cqltype.TypeValidation("partitioner", "no configured partitioner for %q", partitioner)
	}
	cells := make([]model.EncodedCell, len(key))
	indexes := make([]int, len(key))
	for i, column := range columns {
		typ, err := implMetadata.InternalSchemaDataType(ks, column.Type)
		if err != nil {
			return 0, fmt.Errorf("partition key column %q: %w", column.Name, err)
		}
		if key[i].State() != cql.Present {
			return 0, cqltype.TypeValidation("partition key", "column %q must have a value", column.Name)
		}
		encoded, err := link.Get[func(cql.DataType, cql.Value) (cql.EncodedValue, error)](link.EncodeDynamicBorrowed)(typ, key[i])
		if err != nil {
			return 0, fmt.Errorf("partition key column %q: %w", column.Name, err)
		}
		cells[i] = model.EncodedCell{Typ: typ, Body: cqltype.BytesChain(encoded.Bytes()), State: encoded.State()}
		indexes[i] = i
	}
	token, err := InternalComputeRoutingToken(cells, indexes, p)
	if err != nil {
		return 0, err
	}
	if !token.Set {
		return 0, cqltype.TypeValidation("partition key", "no token for %s.%s", keyspace, table)
	}
	return token.Value, nil
}

// TokenRanges returns the token ranges of keyspace.table with their
// replicas, from the snapshot's own placement, and whether they cover the
// whole ring: a token ring does once it has a range, a tablet table only once
// the snapshot holds every one of its tablets.
func (m *MetadataSnapshot) TokenRanges(keyspace, table string) ([]metadata.TokenRange, bool, error) {
	r, tablet, e := InternalMetadataSnapshotLookupRanges(m, keyspace, table)
	if e != nil {
		return nil, false, e
	}
	var out []metadata.TokenRange
	if root, ok := snapshotTabletRoot(m, InternalMetadataTable{InternalKeyspace: keyspace, InternalTable: table}); ok {
		tablet = true
		for _, r := range root.All() {
			out = append(out, r.InternalSpan)
		}
	} else {
		out = append([]metadata.TokenRange(nil), r...)
	}
	for i := range out {
		out[i].Replicas = append([]Target(nil), out[i].Replicas...)
	}
	if !tablet {
		return out, len(out) > 0, nil
	}
	return out, tabletsComplete(out), nil
}

// tabletsComplete reports whether tablets, in token order, cover the whole
// ring, (math.MinInt64, math.MaxInt64], without a gap.
func tabletsComplete(tablets []metadata.TokenRange) bool {
	next := int64(math.MinInt64)
	for _, t := range tablets {
		if t.Start != next {
			return false
		}
		next = t.End
	}
	return len(tablets) > 0 && next == math.MaxInt64
}
func InternalMetadataSnapshotLookupRanges(m *MetadataSnapshot, keyspace, table string) ([]metadata.TokenRange, bool, error) {
	if m == nil {
		return nil, false, cqlerr.ErrNoKeyspace
	}
	ks, ok := SnapshotData(m).InternalKeyspaces[keyspace]
	if !ok {
		return nil, false, keyspaceNotLoaded(keyspace)
	}
	if table != "" {
		if _, ok := ks.Tables[table]; !ok {
			if _, ok := ks.Views[table]; !ok {
				return nil, false, tableNotLoaded(keyspace, table)
			}
		}
		if r, ok := SnapshotData(m).InternalTablets[InternalMetadataTable{InternalKeyspace: keyspace, InternalTable: table}]; ok {
			return r, true, nil
		}
	}
	return SnapshotData(m).InternalRanges[keyspace], false, nil
}

// keyspaceNotLoaded is the error for a keyspace the snapshot's metadata does
// not hold: a cql.ValidationError that wraps cqlerr.ErrNoKeyspace. The
// keyspace may exist on the cluster but be outside what the session loads.
func keyspaceNotLoaded(keyspace string) error {
	return cqltype.TypeValidation("keyspace", "%q is not loaded in this metadata snapshot: %w", keyspace, cqlerr.ErrNoKeyspace)
}

// tableNotLoaded is keyspaceNotLoaded for a table or materialized view of a
// loaded keyspace; it wraps cqlerr.ErrNoTable.
func tableNotLoaded(keyspace, table string) error {
	return cqltype.TypeValidation("table", "%s.%s is not loaded in this metadata snapshot: %w", keyspace, table, cqlerr.ErrNoTable)
}

// Diagnostics reports routing/metadata omissions without substituting fabricated tokens.
func (m *MetadataSnapshot) Diagnostics() []error {
	if m == nil {
		return nil
	}
	return append([]error(nil), SnapshotData(m).InternalDiagnostics...)
}

type SnapshotState struct {
	InternalHostIndex       *InternalRoutingHostIndex
	InternalPlacements      *tabletPlacementTree
	InternalPlacementSignal *atomic.Uint64
	InternalKeyspaces       map[string]*metadata.Keyspace
	InternalSchemaSources   map[string]*metadata.Keyspace
	InternalRanges          map[string][]metadata.TokenRange
	InternalTablets         map[InternalMetadataTable][]metadata.TokenRange
	// InternalPartitioners is the session's partitioner registry, by name,
	// for Token. It is shared and never mutated.
	InternalPartitioners   map[string]routing.TokenPartitioner
	InternalPartitioner    string
	InternalHosts          []HostSnapshot
	InternalDiagnostics    []error
	InternalPlacementEpoch uint64
	InternalGeneration     uint64
}

type HostSnapshot = metadata.HostSnapshot

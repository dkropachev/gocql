// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"fmt"
	"maps"
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

// Host returns a copy of the host with id.
func (m *MetadataSnapshot) Host(id cql.UUID) (HostSnapshot, bool) {
	if m == nil {
		return HostSnapshot{}, false
	}
	hosts := SnapshotData(m).InternalHosts
	i, ok := -1, false
	if idx := SnapshotData(m).InternalHostIndex; idx != nil {
		i, ok = idx.host(id)
	}
	if !ok || i >= len(hosts) || hosts[i].HostID != id {
		i = slices.IndexFunc(hosts, func(h HostSnapshot) bool { return h.HostID == id })
		if i < 0 {
			return HostSnapshot{}, false
		}
	}
	h := hosts[i]
	h.Tokens = append([]int64(nil), h.Tokens...)
	return h, true
}

func (m *MetadataSnapshot) Replicas(keyspace, table string, token int64) ([]Target, error) {
	if _, _, err := InternalMetadataSnapshotLookupRanges(m, keyspace, table); err != nil {
		return nil, err
	}
	r, _ := m.placementReplicas(keyspace, table, token)
	return append([]Target(nil), r...), nil
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
func (m *MetadataSnapshot) TokenRanges(keyspace, table string) ([]metadata.TokenRange, error) {
	if root, ok := InternalMetadataSnapshotTabletRoot(m, InternalMetadataTable{InternalKeyspace: keyspace, InternalTable: table}); ok {
		if _, _, err := InternalMetadataSnapshotLookupRanges(m, keyspace, table); err != nil {
			return nil, err
		}
		var out []metadata.TokenRange
		for _, r := range root.All() {
			v := r.InternalSpan
			v.Replicas = append([]Target(nil), v.Replicas...)
			out = append(out, v)
		}
		return out, nil
	}
	r, _, e := InternalMetadataSnapshotLookupRanges(m, keyspace, table)
	if e != nil {
		return nil, e
	}
	out := append([]metadata.TokenRange(nil), r...)
	for i := range out {
		out[i].Replicas = append([]Target(nil), out[i].Replicas...)
	}
	return out, nil
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

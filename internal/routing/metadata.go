// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"fmt"

	codec "github.com/scylladb/gocql/v2/internal/codec"
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	model "github.com/scylladb/gocql/v2/internal/model"

	"sync/atomic"
	"time"

	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

type TokenRange struct {
	Replicas   []Target
	Start, End int64
}
type MetadataSnapshot struct {
	state SnapshotState
}
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
func (m *MetadataSnapshot) Keyspace(name string) (*implMetadata.KeyspaceMetadata, bool) {
	if m == nil {
		return nil, false
	}
	ks, ok := SnapshotData(m).InternalKeyspaces[name]
	if !ok {
		return nil, false
	}
	return implMetadata.InternalCloneV2Keyspace(ks), true
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
func (m *MetadataSnapshot) Token(keyspace, table string, key ...codec.Value) (int64, error) {
	if m == nil {
		return 0, runtimeerrors.ErrNoKeyspace
	}
	data := SnapshotData(m)
	ks := data.InternalKeyspaces[keyspace]
	if ks == nil {
		return 0, fmt.Errorf("%s: %w", keyspace, runtimeerrors.ErrNoKeyspace)
	}
	var columns []*implMetadata.ColumnMetadata
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
		return 0, fmt.Errorf("%s.%s: %w", keyspace, table, runtimeerrors.ErrNoTable)
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
		if key[i].State() != cqltype.Present {
			return 0, cqltype.TypeValidation("partition key", "column %q must have a value", column.Name)
		}
		encoded, err := codec.EncodeValueBorrowed(typ, codec.DynamicCodec, key[i])
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
func (m *MetadataSnapshot) TokenRanges(keyspace, table string) ([]TokenRange, error) {
	if root, ok := InternalMetadataSnapshotTabletRoot(m, InternalMetadataTable{InternalKeyspace: keyspace, InternalTable: table}); ok {
		if _, _, err := InternalMetadataSnapshotLookupRanges(m, keyspace, table); err != nil {
			return nil, err
		}
		var out []TokenRange
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
	out := append([]TokenRange(nil), r...)
	for i := range out {
		out[i].Replicas = append([]Target(nil), out[i].Replicas...)
	}
	return out, nil
}
func InternalMetadataSnapshotLookupRanges(m *MetadataSnapshot, keyspace, table string) ([]TokenRange, bool, error) {
	if m == nil {
		return nil, false, runtimeerrors.ErrNoKeyspace
	}
	ks, ok := SnapshotData(m).InternalKeyspaces[keyspace]
	if !ok {
		return nil, false, fmt.Errorf("%s: %w", keyspace, runtimeerrors.ErrNoKeyspace)
	}
	if table != "" {
		if _, ok := ks.Tables[table]; !ok {
			if _, ok := ks.Views[table]; !ok {
				return nil, false, fmt.Errorf("%s.%s: %w", keyspace, table, runtimeerrors.ErrNoTable)
			}
		}
		if r, ok := SnapshotData(m).InternalTablets[InternalMetadataTable{InternalKeyspace: keyspace, InternalTable: table}]; ok {
			return r, true, nil
		}
	}
	return SnapshotData(m).InternalRanges[keyspace], false, nil
}

// Diagnostics reports routing/metadata omissions without substituting fabricated tokens.
func (m *MetadataSnapshot) Diagnostics() []error {
	if m == nil {
		return nil
	}
	return append([]error(nil), SnapshotData(m).InternalDiagnostics...)
}

type MetricsSnapshot struct {
	ReceiveCacheHits, ReceiveCacheMisses                                  uint64
	ReceiveCacheBytes, ReceiveCacheObjects                                int
	Requests, Attempts, Errors, Retries, Speculations, Cancelled, Orphans uint64
	InFlight                                                              int64
	LatencyP50, LatencyP95, LatencyP99                                    time.Duration
}
type SnapshotState struct {
	InternalHostIndex       *InternalRoutingHostIndex
	InternalPlacements      *tabletPlacementTree
	InternalPlacementSignal *atomic.Uint64
	InternalKeyspaces       map[string]*implMetadata.KeyspaceMetadata
	InternalSchemaSources   map[string]*implMetadata.KeyspaceMetadata
	InternalRanges          map[string][]TokenRange
	InternalTablets         map[InternalMetadataTable][]TokenRange
	// InternalPartitioners is the session's partitioner registry, by name,
	// for Token. It is shared and never mutated.
	InternalPartitioners   map[string]TokenPartitioner
	InternalPartitioner    string
	InternalHosts          []HostSnapshot
	InternalDiagnostics    []error
	InternalPlacementEpoch uint64
	InternalGeneration     uint64
}

type HostSnapshot = model.HostSnapshot

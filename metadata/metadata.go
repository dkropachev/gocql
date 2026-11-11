// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package metadata describes what a session knows about the cluster. A
// Snapshot holds the topology (hosts, token ring and tablets) and the schema:
// keyspaces, tables, columns, indexes, views, user types, functions and
// aggregates. Mode controls how much schema a session loads; topology always
// loads.
package metadata

import (
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

type (
	Keyspace     = implMetadata.KeyspaceMetadata
	Table        = implMetadata.TableMetadata
	TableOptions = implMetadata.TableMetadataOptions
	Column       = implMetadata.ColumnMetadata
	ColumnIndex  = implMetadata.ColumnIndexMetadata
	ColumnKind   = implMetadata.ColumnKind
	ColumnOrder  = implMetadata.ColumnOrder
	Index        = implMetadata.IndexMetadata
	View         = implMetadata.ViewMetadata
	Type         = implMetadata.TypeMetadata
	Function     = implMetadata.FunctionMetadata
	Aggregate    = implMetadata.AggregateMetadata
)

const (
	ASC  = implMetadata.ASC
	DESC = implMetadata.DESC
)

const (
	ColumnKindUnknown   = implMetadata.ColumnKindUnknown
	ColumnPartitionKey  = implMetadata.ColumnPartitionKey
	ColumnClusteringKey = implMetadata.ColumnClusteringKey
	ColumnRegular       = implMetadata.ColumnRegular
	ColumnCompact       = implMetadata.ColumnCompact
	ColumnStatic        = implMetadata.ColumnStatic
)

type IndexKind = implMetadata.IndexKind

const (
	IndexKindKeys       = implMetadata.IndexKindKeys
	IndexKindComposites = implMetadata.IndexKindComposites
	IndexKindCustom     = implMetadata.IndexKindCustom
)

// Snapshot is an immutable view of cluster topology and schema.
type Snapshot = implRouting.MetadataSnapshot

// Mode selects how much schema a session loads. Topology (hosts, tokens and
// tablets) loads in every mode.
type Mode = implSettings.MetadataMode

const (
	// SchemaNone loads no schema.
	SchemaNone = implSettings.MetadataSchemaNone
	// SchemaKeyspaces loads keyspaces (replication, durable writes and
	// ScyllaDB keyspace options) without their tables, views, types,
	// functions or aggregates.
	SchemaKeyspaces = implSettings.MetadataSchemaKeyspaces
	// SchemaFull loads keyspaces with everything in them. It is the default.
	SchemaFull = implSettings.MetadataSchemaFull
)

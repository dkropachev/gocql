// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package metadata describes cluster schema: keyspaces, tables, columns,
// indexes, views, user types, functions and aggregates, and the settings
// that control how much of it a session loads.
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
	ColumnUnkownKind    = implMetadata.ColumnUnkownKind
	ColumnPartitionKey  = implMetadata.ColumnPartitionKey
	ColumnClusteringKey = implMetadata.ColumnClusteringKey
	ColumnRegular       = implMetadata.ColumnRegular
	ColumnCompact       = implMetadata.ColumnCompact
	ColumnStatic        = implMetadata.ColumnStatic
)

const IndexKindCustom = implMetadata.IndexKindCustom

// Snapshot is an immutable view of cluster topology and schema.
type Snapshot = implRouting.MetadataSnapshot

// Mode selects how much schema metadata a session loads.
type Mode = implSettings.MetadataMode

const (
	Disabled = implSettings.MetadataDisabled
	Minimal  = implSettings.MetadataMinimal
	Full     = implSettings.MetadataFull
)

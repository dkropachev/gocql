// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"context"
	"errors"
	"strings"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/metadata"
)

type v2ScyllaKeyspaceRow struct {
	name, consistency string
	initial           int32
}

// SELECT * avoids guessing whether an experimental consistency column exists.
// Only the negotiated V2 connection can authorize its leader meaning.
func InternalReadV2ScyllaKeyspaces(ctx context.Context, s Source, keyspaces map[string]*metadata.Keyspace, name string) error {
	settings := s.InternalSchemaSettings()
	if !settings.Scylla {
		return nil
	}
	for _, ks := range keyspaces {
		KeyspaceData(ks).TabletsOption = settings.Tablets
	}
	decoder := InternalV2SchemaRows[v2ScyllaKeyspaceRow]{initialize: func() *v2ScyllaKeyspaceRow { return &v2ScyllaKeyspaceRow{initial: -1} }, InternalFields: []InternalV2SchemaField[v2ScyllaKeyspaceRow]{
		InternalV2RequiredSchemaColumn("keyspace_name", InternalV2MetadataText{}, func(r *v2ScyllaKeyspaceRow) *string { return &r.name }),
		InternalV2SchemaColumn("initial_tablets", cql.Int32Codec, func(r *v2ScyllaKeyspaceRow) *int32 { return &r.initial }),
		InternalV2SchemaColumn("consistency", InternalV2MetadataText{}, func(r *v2ScyllaKeyspaceRow) *string { return &r.consistency }),
	}}
	stmt := `SELECT * FROM system_schema.scylla_keyspaces`
	var args []string
	if name != "" {
		stmt += ` WHERE keyspace_name = ?`
		args = []string{name}
	}
	rows, err := InternalQueryV2SchemaRows(ctx, s, stmt, args, decoder)
	if err != nil {
		if invalid, ok := errors.AsType[cqlerr.ServerError](err); ok && invalid.Code() == cqlerr.CodeInvalid {
			return nil
		}
		return err
	}
	seen := make(map[string]bool, len(rows))
	for _, r := range rows {
		if seen[r.name] {
			return cqltype.TypeValidation("scylla keyspace schema", "duplicate identity %q", r.name)
		}
		seen[r.name] = true
		if ks := keyspaces[r.name]; ks != nil {
			ks.TabletBased = r.initial >= 0
			ks.InitialTablets = max(int(r.initial), 0)
			// The server stores the option as written in CREATE KEYSPACE.
			if settings.TabletsV2 && strings.EqualFold(r.consistency, "global") {
				ks.ConsistencyMode = "global"
			}
		}
	}
	return nil
}

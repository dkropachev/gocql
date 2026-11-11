// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cluster

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/settings"
)

const InternalMaxPreparedRefreshScopes = 64

// Register only affected keyspaces. A full queue fails the logical operation
// visibly; it neither drops required work nor enables a broader schema refresh.
func (s *Core) ScheduleV2PreparedRefresh(keyspace string) error {
	p := s.Peripheral()
	p.InternalMu.Lock()
	if p.closed || s.Closed() {
		p.InternalMu.Unlock()
		return runtimeerrors.ErrClosed
	}
	if _, exists := p.InternalPreparedRefreshScopes[keyspace]; !exists && len(p.InternalPreparedRefreshScopes) >= InternalMaxPreparedRefreshScopes {
		p.InternalMu.Unlock()
		return &cqltype.ProtocolError{Stage: "prepared schema reconciliation", Err: fmt.Errorf("pending keyspace limit %d exceeded", InternalMaxPreparedRefreshScopes)}
	}
	if p.InternalPreparedRefreshScopes == nil {
		p.InternalPreparedRefreshScopes = make(map[string]uint64)
	}
	p.preparedRefreshVersion++
	p.InternalPreparedRefreshScopes[keyspace] = p.preparedRefreshVersion
	p.InternalMu.Unlock()
	s.StartV2SchemaRetry()
	return nil
}

// Required reconciliation validates actual typed schema independently of
// optional background metadata publication. Known scopes never enumerate peers'
// unrelated keyspaces; an unknown statement scope alone requires enumeration.
func (s *Core) reconcileV2PreparedScope(ctx context.Context, keyspace string) error {
	if keyspace == "" {
		rows, err := metadata.InternalQueryV2SchemaRows(ctx, s.NewSchemaSource(), `SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces`, nil, metadata.InternalV2DecodeKeyspace())
		if err != nil {
			return err
		}
		for _, row := range rows {
			if row.InternalName == "" {
				return cqltype.TypeValidation("keyspace schema", "missing keyspace identity in required reconciliation")
			}
			if err := s.reconcileV2PreparedScope(ctx, row.InternalName); err != nil {
				return err
			}
		}
		return nil
	}
	p := s.Peripheral()
	p.InternalMu.Lock()
	epoch := p.schemaEpoch[keyspace]
	revision := p.schemaRevision[keyspace]
	tables := p.captureTableRevisionsLocked(keyspace)
	config := p.InternalConfig
	p.InternalMu.Unlock()
	base, err := metadata.InternalGetV2KeyspaceMetadata(ctx, s.NewSchemaSource(), keyspace)
	absent := errors.Is(err, runtimeerrors.ErrKeyspaceDoesNotExist)
	if err != nil && !absent {
		return err
	}
	var schema, minimal *metadata.KeyspaceMetadata
	if !absent {
		copyBase := *base
		minimal = &copyBase
		schema, err = metadata.InternalFetchV2Keyspace(ctx, s.NewSchemaSource(), base)
		if err != nil {
			return err
		}
	}
	publish := config.Metadata.Mode != settings.MetadataSchemaNone && (len(config.Metadata.Keyspaces) == 0 || slices.Contains(config.Metadata.Keyspaces, keyspace))
	p.InternalMu.Lock()
	if p.schemaEpoch[keyspace] != epoch || p.schemaRevision[keyspace] != revision || !p.tableRevisionsCurrentLocked(keyspace, tables) {
		p.InternalMu.Unlock()
		return &metadata.PartialRefreshError{Keyspaces: map[string]error{keyspace: errors.New("schema changed during prepared reconciliation")}}
	}
	if publish && s.privateStorage.InternalMetadataDescriber != nil {
		p.schemaRevision[keyspace]++
		if absent {
			s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalRemoveKeyspace(keyspace)
		} else {
			if config.Metadata.Mode == settings.MetadataSchemaKeyspaces {
				schema = minimal
			}
			s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalSet(keyspace, schema)
		}
	}
	delete(p.schemaStale, keyspace)
	p.InternalMu.Unlock()
	if absent {
		s.InvalidatePreparedSchema(Event{Keyspace: keyspace, Target: "KEYSPACE"})
	}
	if publish {
		s.PublishV2Metadata()
	}
	return nil
}

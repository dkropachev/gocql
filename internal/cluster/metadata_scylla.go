// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"

	"fmt"
	"sync"

	"golang.org/x/sync/singleflight"

	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/tablets"
)

type Metadata struct {
	privateStorage MetadataStorage
}

// queries the cluster for schema information for a specific keyspace and for tablets
type InternalMetadataDescriber struct {
	keyspaceGroup    singleflight.Group
	tableGroup       singleflight.Group
	InternalSession  *Core
	InternalMetadata *Metadata

	// mu serialises refreshAllSchema calls so the snapshot-compare-refresh
	// cycle runs as an atomic batch.  Individual keyspace/table refreshes
	// are deduplicated by the singleflight groups above and do NOT need
	// this lock.
	//
	// Lock ordering: s.mu → cowKeyspaceMetadataMap.mu (never reversed).
	mu sync.Mutex
}

// creates a session bound schema describer which will query and cache
// keyspace metadata and tablets metadata
func (session *Core) NewMetadataDescriber() *InternalMetadataDescriber {
	return &InternalMetadataDescriber{
		InternalSession: session,
		InternalMetadata: func() *Metadata {
			opaqueStorageValue := new(Metadata)
			opaqueStorageValue.privateStorage.InternalTabletsMetadata = tablets.NewCowTabletList()
			return opaqueStorageValue
		}(),
	}
}

func (s *InternalMetadataDescriber) getKeyspaceInternal(keyspaceName string) (metadata *implMetadata.KeyspaceMetadata, wasReloaded bool, err error) {
	var found bool
	metadata, found = s.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalGetKeyspace(keyspaceName)
	if !found {
		wasReloaded = true
		err = s.deduplicatedRefreshKeyspace(keyspaceName)
		if err != nil {
			return metadata, wasReloaded, err
		}

		metadata, found = s.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalGetKeyspace(keyspaceName)
		if !found {
			return nil, true, fmt.Errorf("keyspace %s: %w", keyspaceName, runtimeerrors.ErrNotFound)
		}
	}

	return metadata, wasReloaded, nil
}

func (s *InternalMetadataDescriber) GetKeyspace(keyspaceName string) (*implMetadata.KeyspaceMetadata, error) {
	metadata, _, err := s.getKeyspaceInternal(keyspaceName)
	return metadata, err
}

func tableNotFoundError(keyspaceName, tableName string) error {
	return fmt.Errorf("table %s.%s: %w", keyspaceName, tableName, runtimeerrors.ErrNotFound)
}

// getTableFromSnapshot resolves a table lookup against a keyspace snapshot.
// It re-reads the latest published keyspace metadata before deciding that an
// invalidated table still needs a refresh, which avoids duplicate refreshes
// for callers holding a stale snapshot.
func (s *InternalMetadataDescriber) getTableFromSnapshot(
	keyspaceName, tableName string,
	keyspaceMetadata *implMetadata.KeyspaceMetadata,
	wasReloaded bool,
) (tableMetadata *implMetadata.TableMetadata, refreshNeeded bool, err error) {
	if tableMetadata, found := keyspaceMetadata.Tables[tableName]; found {
		return tableMetadata, false, nil
	}

	if latestMetadata, found := s.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalGetKeyspace(keyspaceName); found && latestMetadata != keyspaceMetadata {
		keyspaceMetadata = latestMetadata
		if tableMetadata, found := keyspaceMetadata.Tables[tableName]; found {
			return tableMetadata, false, nil
		}
	}

	if wasReloaded {
		return nil, false, tableNotFoundError(keyspaceName, tableName)
	}

	if _, ok := implMetadata.KeyspaceData(keyspaceMetadata).InternalTablesInvalidated[tableName]; !ok {
		return nil, false, tableNotFoundError(keyspaceName, tableName)
	}

	return nil, true, nil
}

func (s *InternalMetadataDescriber) GetTable(keyspaceName, tableName string) (*implMetadata.TableMetadata, error) {
	keyspaceMetadata, wasReloaded, err := s.getKeyspaceInternal(keyspaceName)
	if err != nil {
		return nil, err
	}

	tableMetadata, refreshNeeded, err := s.getTableFromSnapshot(keyspaceName, tableName, keyspaceMetadata, wasReloaded)
	if err != nil {
		return nil, err
	}
	if !refreshNeeded {
		return tableMetadata, nil
	}

	err = s.deduplicatedRefreshTable(keyspaceName, tableName)
	if err != nil {
		return nil, err
	}

	keyspaceMetadata, found := s.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalGetKeyspace(keyspaceName)
	if !found {
		return nil, tableNotFoundError(keyspaceName, tableName)
	}

	tableMetadata, found = keyspaceMetadata.Tables[tableName]
	if !found {
		return nil, tableNotFoundError(keyspaceName, tableName)
	}

	return tableMetadata, nil
}

func (s *InternalMetadataDescriber) getTablets() tablets.TabletInfoList {
	return s.InternalMetadata.privateStorage.InternalTabletsMetadata.Get()
}

func (s *InternalMetadataDescriber) getTableTablets(keyspace, table string) tablets.TabletEntryList {
	return s.InternalMetadata.privateStorage.InternalTabletsMetadata.GetTableTablets(keyspace, table)
}

func (s *InternalMetadataDescriber) forEachTablet(fn func(keyspace, table string, entries tablets.TabletEntryList) bool) {
	s.InternalMetadata.privateStorage.InternalTabletsMetadata.ForEach(fn)
}

func (s *InternalMetadataDescriber) AddTablet(tablet tablets.TabletInfo) {
	s.InternalMetadata.privateStorage.InternalTabletsMetadata.AddTablet(tablet)
}

// RemoveTabletsWithHost removes tablets that contains given host.
// to be used outside the metadataDescriber
func (s *InternalMetadataDescriber) RemoveTabletsWithHost(host *implEndpoint.HostInfo) {
	s.InternalMetadata.privateStorage.InternalTabletsMetadata.RemoveTabletsWithHost(tablets.HostUUID(implEndpoint.InternalHostInfoHostUUID(host)))
}

// RemoveTabletsWithKeyspace removes tablets for given keyspace.
// to be used outside the metadataDescriber
func (s *InternalMetadataDescriber) RemoveTabletsWithKeyspace(keyspace string) {
	s.InternalMetadata.privateStorage.InternalTabletsMetadata.RemoveTabletsWithKeyspace(keyspace)
}

// RemoveTabletsWithTable removes tablets for given table.
// to be used outside the metadataDescriber
func (s *InternalMetadataDescriber) RemoveTabletsWithTable(keyspace string, table string) {
	s.InternalMetadata.privateStorage.InternalTabletsMetadata.RemoveTabletsWithTable(keyspace, table)
}

// invalidateKeyspaceSchema clears the cached keyspace metadata
func (s *InternalMetadataDescriber) invalidateKeyspaceSchema(keyspaceName string) {
	s.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalRemoveKeyspace(keyspaceName)
}

func (s *InternalMetadataDescriber) invalidateTableSchema(keyspaceName, tableName string) {
	s.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalInvalidateTable(keyspaceName, tableName)
}

// deduplicatedRefreshKeyspace loads a keyspace that is missing from the cache,
// collapsing concurrent calls for the same keyspace into a single in-flight
// operation. Every caller found the keyspace absent; singleflight only joins
// calls that overlap, so a caller that decided just before another flight
// published and completed would start a second full refresh. The flight
// rechecks the current snapshot first and skips the queries when the keyspace
// is already there.
func (s *InternalMetadataDescriber) deduplicatedRefreshKeyspace(keyspaceName string) error {
	_, err, _ := s.keyspaceGroup.Do(keyspaceName, func() (any, error) {
		if _, ok := s.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalGetKeyspace(keyspaceName); ok {
			return nil, nil
		}
		return nil, s.refreshKeyspaceSchema(keyspaceName)
	})
	return err
}

// deduplicatedRefreshTable collapses concurrent refreshTableSchema calls
// for the same keyspace/table into a single in-flight operation.
func (s *InternalMetadataDescriber) deduplicatedRefreshTable(keyspaceName, tableName string) error {
	key := keyspaceName + "\x00" + tableName
	_, err, _ := s.tableGroup.Do(key, func() (any, error) {
		// A caller may arrive here after deciding to refresh an old snapshot but
		// after another flight already published and completed. Recheck the current
		// immutable snapshot inside this flight before issuing network queries.
		if current, ok := s.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalGetKeyspace(keyspaceName); ok {
			if _, present := current.Tables[tableName]; present {
				return nil, nil
			}
		}
		return nil, s.InternalRefreshTableSchema(keyspaceName, tableName)
	})
	return err
}

func (s *InternalMetadataDescriber) refreshAllSchema() error {
	return s.InternalSession.RefreshV2AllSchema(implMetadata.InternalV2SchemaContext(s.InternalSession.NewSchemaSource()))
}

// forcibly updates the current KeyspaceMetadata held by the schema describer
// for a given named keyspace.
//
// All system schema queries are issued concurrently since none of them
// depend on each other's results. The results are only combined in
// compileMetadata after all queries complete.
func (s *InternalMetadataDescriber) refreshKeyspaceSchema(keyspaceName string) error {
	return s.InternalSession.RefreshV2Keyspace(implMetadata.InternalV2SchemaContext(s.InternalSession.NewSchemaSource()), keyspaceName)
}

func (s *InternalMetadataDescriber) InternalRefreshTableSchema(keyspaceName, tableName string) error {
	p := s.InternalSession.Peripheral()
	p.InternalMu.Lock()
	epoch := p.schemaEpoch[keyspaceName]
	revision := p.schemaRevision[keyspaceName]
	tableKey := routing.InternalMetadataTable{InternalKeyspace: keyspaceName, InternalTable: tableName}
	tableRevision := p.tableRevision[tableKey]
	p.InternalMu.Unlock()
	_, found := s.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalGetKeyspace(keyspaceName)
	if !found {
		return s.deduplicatedRefreshKeyspace(keyspaceName)
	}

	// Perform network queries outside the lock.
	tables, err := getTableMetadataByName(s.InternalSession, keyspaceName, tableName)
	if err != nil {
		return err
	}

	columns, err := getColumnMetadataByTable(s.InternalSession, keyspaceName, tableName)
	if err != nil {
		return err
	}

	indexes, err := getIndexMetadataByTable(s.InternalSession, keyspaceName, tableName)
	if err != nil {
		return err
	}

	views, err := getViewMetadataByTable(s.InternalSession, keyspaceName, tableName)
	if err != nil {
		return err
	}

	ks, _ := s.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalGetKeyspace(keyspaceName)
	var types []implMetadata.TypeMetadata
	if ks != nil {
		for _, typ := range ks.Types {
			if typ != nil {
				types = append(types, *typ)
			}
		}
	}
	if err := implMetadata.InternalValidateV2SchemaParts(&implMetadata.KeyspaceMetadata{Name: keyspaceName}, tables, columns, types, nil, nil, indexes, views, s.InternalSession.v2MetadataDepthLimit()); err != nil {
		return err
	}
	// Atomically clone-and-swap the keyspace metadata to avoid data races
	// with concurrent readers.
	p.InternalMu.Lock()
	if p.schemaEpoch[keyspaceName] != epoch || p.schemaRevision[keyspaceName] != revision || p.tableRevision[tableKey] != tableRevision || p.schemaDropped[keyspaceName] {
		p.InternalMu.Unlock()
		s.InternalSession.scheduleV2SchemaRetry()
		return &implMetadata.PartialRefreshError{Generation: s.InternalSession.Metadata().Generation(), Keyspaces: map[string]error{keyspaceName: fmt.Errorf("schema changed during table refresh")}}
	}
	applied := s.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalUpdateKeyspace(keyspaceName, func(ks *implMetadata.KeyspaceMetadata) {
		if len(tables) == 0 {
			implMetadata.InternalKeyspaceMetadataRemoveTable(ks, tableName)
		} else {
			implMetadata.InternalCompileTableMetadata(ks, tables, columns, indexes, views)
			if implMetadata.KeyspaceData(ks).InternalTablesInvalidated != nil {
				delete(implMetadata.KeyspaceData(ks).InternalTablesInvalidated, tableName)
			}
		}
	})
	if applied {
		p.InternalBumpTableRevisionLocked(tableKey)
	}
	p.InternalMu.Unlock()
	if !applied {
		// Keyspace was removed between the initial check and the update.
		// Fall back to a full keyspace refresh to recover.
		return s.deduplicatedRefreshKeyspace(keyspaceName)
	}
	s.InternalSession.Peripheral().schemaGeneration.Add(1)
	s.InternalSession.PublishV2Metadata()
	return nil
}

func getKeyspaceMetadata(s *Core, keyspace string) (*implMetadata.KeyspaceMetadata, error) {
	return implMetadata.InternalGetV2KeyspaceMetadata(implMetadata.InternalV2SchemaContext(s.NewSchemaSource()), s.NewSchemaSource(), keyspace)
}
func getTableMetadataByName(s *Core, keyspace, table string) ([]implMetadata.TableMetadata, error) {
	return implMetadata.InternalGetV2TableMetadata(implMetadata.InternalV2SchemaContext(s.NewSchemaSource()), s.NewSchemaSource(), keyspace, table)
}
func getColumnMetadataByTable(s *Core, keyspace, table string) ([]implMetadata.ColumnMetadata, error) {
	return implMetadata.InternalGetV2ColumnMetadata(implMetadata.InternalV2SchemaContext(s.NewSchemaSource()), s.NewSchemaSource(), keyspace, table)
}
func getIndexMetadataByTable(s *Core, keyspace, table string) ([]implMetadata.IndexMetadata, error) {
	return implMetadata.InternalGetV2IndexMetadata(implMetadata.InternalV2SchemaContext(s.NewSchemaSource()), s.NewSchemaSource(), keyspace, table)
}
func getViewMetadataByTable(s *Core, keyspace, table string) ([]implMetadata.ViewMetadata, error) {
	return implMetadata.InternalGetV2ViewMetadata(implMetadata.InternalV2SchemaContext(s.NewSchemaSource()), s.NewSchemaSource(), keyspace, table)
}

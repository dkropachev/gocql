/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cluster

import (
	implCqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/protocol"

	"crypto/sha256"
	"fmt"
	"log"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/tests/mock"
	"github.com/scylladb/gocql/v2/tablets"
)

var (
	typeVarchar = cqltype.NewCustomType(4, implCqltype.TypeVarchar, "")
	typeBoolean = cqltype.NewCustomType(4, implCqltype.TypeBoolean, "")
	typeDouble  = cqltype.NewCustomType(4, implCqltype.TypeDouble, "")
	typeInt     = cqltype.NewCustomType(4, implCqltype.TypeInt, "")
	typeMapSS   = implCqltype.CollectionType{
		NativeType: cqltype.NewCustomType(4, implCqltype.TypeMap, ""),
		Key:        cqltype.NewCustomType(4, implCqltype.TypeVarchar, ""),
		Elem:       cqltype.NewCustomType(4, implCqltype.TypeVarchar, ""),
	}
	typeMapSB = implCqltype.CollectionType{
		NativeType: cqltype.NewCustomType(4, implCqltype.TypeMap, ""),
		Key:        cqltype.NewCustomType(4, implCqltype.TypeVarchar, ""),
		Elem:       cqltype.NewCustomType(4, implCqltype.TypeBlob, ""),
	}
	typeSetS = implCqltype.CollectionType{
		NativeType: cqltype.NewCustomType(4, implCqltype.TypeSet, ""),
		Elem:       cqltype.NewCustomType(4, implCqltype.TypeVarchar, ""),
	}
)

var keyspaceMeta = protocol.ResultMetadata{
	Columns: []implCqltype.ColumnInfo{
		{Name: "durable_writes", TypeInfo: typeBoolean},
		{Name: "replication", TypeInfo: typeMapSS},
	},
	ActualColCount: 2,
	ColCount:       2,
}

var tableMeta = protocol.ResultMetadata{
	Columns: []implCqltype.ColumnInfo{
		{Name: "table_name", TypeInfo: typeVarchar},
		{Name: "bloom_filter_fp_chance", TypeInfo: typeDouble},
		{Name: "caching", TypeInfo: typeMapSS},
		{Name: "comment", TypeInfo: typeVarchar},
		{Name: "compaction", TypeInfo: typeMapSS},
		{Name: "compression", TypeInfo: typeMapSS},
		{Name: "crc_check_chance", TypeInfo: typeDouble},
		{Name: "default_time_to_live", TypeInfo: typeInt},
		{Name: "gc_grace_seconds", TypeInfo: typeInt},
		{Name: "max_index_interval", TypeInfo: typeInt},
		{Name: "memtable_flush_period_in_ms", TypeInfo: typeInt},
		{Name: "min_index_interval", TypeInfo: typeInt},
		{Name: "speculative_retry", TypeInfo: typeVarchar},
		{Name: "flags", TypeInfo: typeSetS},
		{Name: "extensions", TypeInfo: typeMapSB},
		{Name: "id", TypeInfo: cqltype.NewCustomType(4, implCqltype.TypeUUID, "")},
	},
	ActualColCount: 16,
	ColCount:       16,
}

var columnMeta = protocol.ResultMetadata{
	Columns: []implCqltype.ColumnInfo{
		{Name: "table_name", TypeInfo: typeVarchar},
		{Name: "column_name", TypeInfo: typeVarchar},
		{Name: "clustering_order", TypeInfo: typeVarchar},
		{Name: "type", TypeInfo: typeVarchar},
		{Name: "kind", TypeInfo: typeVarchar},
		{Name: "position", TypeInfo: typeInt},
	},
	ActualColCount: 6,
	ColCount:       6,
}

func mustMarshal(info implCqltype.TypeInfo, value any) []byte {
	b, err := encodeFixtureCell(info, value)
	if err != nil {
		panic(fmt.Sprintf("mustMarshal(%v, %v): %v", info, value, err))
	}
	return b
}

func marshalRow(meta protocol.ResultMetadata, values []any) [][]byte {
	if len(meta.Columns) != len(values) {
		panic(fmt.Sprintf("marshalRow: column count %d != value count %d", len(meta.Columns), len(values)))
	}
	row := make([][]byte, len(values))
	for i, col := range meta.Columns {
		row[i] = mustMarshal(col.TypeInfo, values[i])
	}
	return row
}

func makeKeyspaceRow(durableWrites bool) [][]byte {
	replication := map[string]string{
		"class":              "org.apache.cassandra.locator.SimpleStrategy",
		"replication_factor": "1",
	}
	return marshalRow(keyspaceMeta, []any{durableWrites, replication})
}

func makeTableRow(tableName string) [][]byte {
	digest := sha256.Sum256([]byte(tableName))
	var id implCqltype.UUID
	copy(id[:], digest[:16])
	id[6] = id[6]&15 | 64
	id[8] = id[8]&63 | 128
	return marshalRow(tableMeta, []any{
		tableName,              // table_name
		float64(0.01),          // bloom_filter_fp_chance
		map[string]string(nil), // caching
		"",                     // comment
		map[string]string(nil), // compaction
		map[string]string(nil), // compression
		float64(0),             // crc_check_chance
		0,                      // default_time_to_live
		0,                      // gc_grace_seconds
		0,                      // max_index_interval
		0,                      // memtable_flush_period_in_ms
		0,                      // min_index_interval
		"",                     // speculative_retry
		[]string(nil),          // flags
		map[string][]byte(nil), // extensions
		id,                     // stable fixture table UUID
	})
}

func makeColumnRow(tableName, colName, kind string, position int) [][]byte {
	return marshalRow(columnMeta, []any{
		tableName, // table_name
		colName,   // column_name
		"none",    // clustering_order
		"int",     // type
		kind,      // kind
		position,  // position
	})
}

func makeIter(meta protocol.ResultMetadata, rows ...[][]byte) *nativeResultFixture {
	if len(rows) == 0 {
		return &nativeResultFixture{}
	}
	var allData [][]byte
	for _, row := range rows {
		allData = append(allData, row...)
	}
	return &nativeResultFixture{
		meta:    meta,
		framer:  &mock.MockFramer{Data: allData},
		numRows: len(rows),
	}
}

type tableInfo struct {
	name    string
	columns []columnInfo
}

type columnInfo struct {
	name     string
	kind     string // "partition_key", "clustering", "regular"
	position int
}

type schemaDataMock struct {
	fakeControlConn

	mu                        sync.Mutex
	awaitSchemaAgreementCalls int
	queries                   []queryRecord

	knownKeyspaces map[string][]tableInfo
	queryDelay     time.Duration
	queryError     error // if set, querySystem returns an nativeResultFixture with this error
	// iterFor, if set, is consulted first by query and querySystem; a non-nil
	// result is returned as the statement's nativeResultFixture.
	iterFor func(statement string) *nativeResultFixture
}

func (m *schemaDataMock) InternalAwaitSchemaAgreement() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.awaitSchemaAgreementCalls++
	return nil
}

func (m *schemaDataMock) query(statement string, values ...any) *nativeResultFixture {
	m.mu.Lock()
	m.queries = append(m.queries, queryRecord{method: "query", stmt: statement})
	delay := m.queryDelay
	iterFor := m.iterFor
	m.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}
	if iterFor != nil {
		if iter := iterFor(statement); iter != nil {
			return iter
		}
	}

	return &nativeResultFixture{}
}

func (m *schemaDataMock) querySystem(statement string, values ...any) *nativeResultFixture {
	m.mu.Lock()
	m.queries = append(m.queries, queryRecord{method: "querySystem", stmt: statement})
	delay := m.queryDelay
	queryErr := m.queryError
	iterFor := m.iterFor
	m.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}
	if iterFor != nil {
		if iter := iterFor(statement); iter != nil {
			return iter
		}
	}

	if queryErr != nil {
		return &nativeResultFixture{err: queryErr}
	}

	// Injection callbacks run outside the mutex so they can change fixture
	// topology. Read that topology under the same lock when building rows.
	m.mu.Lock()
	defer m.mu.Unlock()

	if statement == "SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces" {
		meta := protocol.ResultMetadata{
			Columns:        append([]implCqltype.ColumnInfo{{Name: "keyspace_name", TypeInfo: typeVarchar}}, keyspaceMeta.Columns...),
			ActualColCount: 3,
			ColCount:       3,
		}
		names := make([]string, 0, len(m.knownKeyspaces))
		for name := range m.knownKeyspaces {
			names = append(names, name)
		}
		sort.Strings(names)
		rows := make([][][]byte, 0, len(names))
		for _, name := range names {
			rows = append(rows, append([][]byte{[]byte(name)}, makeKeyspaceRow(true)...))
		}
		return makeIter(meta, rows...)
	}

	if strings.HasPrefix(statement, "SELECT durable_writes, replication FROM system_schema.keyspaces") {
		ksName, _ := values[0].(string)
		if _, ok := m.knownKeyspaces[ksName]; ok {
			return makeIter(keyspaceMeta, makeKeyspaceRow(true))
		}
		return &nativeResultFixture{}
	}

	if strings.HasPrefix(statement, "SELECT * FROM system_schema.tables WHERE keyspace_name = ?") &&
		!strings.Contains(statement, "AND table_name") {
		ksName, _ := values[0].(string)
		tables, ok := m.knownKeyspaces[ksName]
		if !ok || len(tables) == 0 {
			return &nativeResultFixture{}
		}
		var rows [][]byte
		for _, t := range tables {
			rows = append(rows, makeTableRow(t.name)...)
		}
		return &nativeResultFixture{
			meta:    tableMeta,
			framer:  &mock.MockFramer{Data: rows},
			numRows: len(tables),
		}
	}

	if strings.HasPrefix(statement, "SELECT * FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?") {
		ksName, _ := values[0].(string)
		tblName, _ := values[1].(string)
		tables, ok := m.knownKeyspaces[ksName]
		if ok {
			for _, t := range tables {
				if t.name == tblName {
					return makeIter(tableMeta, makeTableRow(t.name))
				}
			}
		}
		return &nativeResultFixture{}
	}

	if strings.HasPrefix(statement, "SELECT "+metadata.InternalColumnMetadataColumns+" FROM system_schema.columns WHERE keyspace_name = ?") &&
		!strings.Contains(statement, "AND table_name") {
		ksName, _ := values[0].(string)
		tables, ok := m.knownKeyspaces[ksName]
		if !ok {
			return &nativeResultFixture{}
		}
		var rows [][]byte
		count := 0
		for _, t := range tables {
			for _, c := range t.columns {
				rows = append(rows, makeColumnRow(t.name, c.name, c.kind, c.position)...)
				count++
			}
		}
		if count == 0 {
			return &nativeResultFixture{}
		}
		return &nativeResultFixture{
			meta:    columnMeta,
			framer:  &mock.MockFramer{Data: rows},
			numRows: count,
		}
	}

	if strings.HasPrefix(statement, "SELECT "+metadata.InternalColumnMetadataColumns+" FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?") {
		ksName, _ := values[0].(string)
		tblName, _ := values[1].(string)
		tables, ok := m.knownKeyspaces[ksName]
		if !ok {
			return &nativeResultFixture{}
		}
		var rows [][]byte
		count := 0
		for _, t := range tables {
			if t.name != tblName {
				continue
			}
			for _, c := range t.columns {
				rows = append(rows, makeColumnRow(t.name, c.name, c.kind, c.position)...)
				count++
			}
		}
		if count == 0 {
			return &nativeResultFixture{}
		}
		return &nativeResultFixture{
			meta:    columnMeta,
			framer:  &mock.MockFramer{Data: rows},
			numRows: count,
		}
	}

	return &nativeResultFixture{}
}

func (m *schemaDataMock) setQueryError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queryError = err
}

func (m *schemaDataMock) resetQueries() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queries = nil
}

func (m *schemaDataMock) getStatements() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	stmts := make([]string, len(m.queries))
	for i, q := range m.queries {
		stmts[i] = q.stmt
	}
	return stmts
}

func (m *schemaDataMock) getQueryCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.queries)
}

func (m *schemaDataMock) getAwaitSchemaAgreementCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.awaitSchemaAgreementCalls
}

func newSchemaEventTestSession(control InternalControlConnection, policy HostSelectionPolicy, keyspace string) *Session {
	s := func() *Session {
		opaqueStorageValue := new(Session)
		SessionData(opaqueStorageValue).InternalControl = control
		SessionData(opaqueStorageValue).InternalPolicy = policy
		SessionData(opaqueStorageValue).InternalLogger = log.Default()
		SessionData(opaqueStorageValue).InternalCfg = InternalClusterSettings{Keyspace: keyspace}
		return opaqueStorageValue
	}()
	SessionData(s).InternalHostSource = &InternalRingDescriber{InternalCfg: &SessionData(s).InternalCfg, InternalLogger: SessionData(s).InternalLogger}
	SessionData(s).InternalMetadataDescriber = &InternalMetadataDescriber{
		InternalSession: s,
		InternalMetadata: func() *Metadata {
			opaqueStorageValuex := new(Metadata)
			MetadataData(opaqueStorageValuex).InternalTabletsMetadata = tablets.NewCowTabletList()
			return opaqueStorageValuex
		}(),
	}
	runtime.SetFinalizer(s, func(s *Session) {
		s.Close()
	})
	return s
}

func newSchemaEventTestSessionWithMock(mockCtrl *schemaDataMock) *Session {
	s := newSchemaEventTestSession(mockCtrl, &trackingPolicy{}, "")
	SessionData(s).InternalUseSystemSchema = true
	SessionData(s).InternalHasAggregatesAndFunctions = false
	return s
}

func populateKeyspace(s *Session, ksName string, tableNames ...string) {
	ks := &metadata.KeyspaceMetadata{
		Name:          ksName,
		DurableWrites: true,
		Tables:        make(map[string]*metadata.TableMetadata),
	}
	for _, tbl := range tableNames {
		ks.Tables[tbl] = &metadata.TableMetadata{
			Keyspace: ksName,
			Name:     tbl,
			Columns: map[string]*metadata.ColumnMetadata{
				"id": {Keyspace: ksName, Table: tbl, Name: "id", Kind: metadata.ColumnPartitionKey},
			},
		}
	}
	MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalSet(ksName, ks)
}

type trackingPolicy struct {
	roundRobinHostPolicy
	mu                   sync.Mutex
	keyspaceChangedCalls []KeyspaceUpdateEvent
}

func (t *trackingPolicy) KeyspaceChanged(event KeyspaceUpdateEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.keyspaceChangedCalls = append(t.keyspaceChangedCalls, event)
}

func (t *trackingPolicy) getKeyspaceChangedCalls() []KeyspaceUpdateEvent {
	t.mu.Lock()
	defer t.mu.Unlock()
	dst := make([]KeyspaceUpdateEvent, len(t.keyspaceChangedCalls))
	copy(dst, t.keyspaceChangedCalls)
	return dst
}

type queryRecord struct {
	method string
	stmt   string
}

func addTestTablets(t *testing.T, session *Session, ksName, tblName string) {
	t.Helper()
	t1, err := tablets.TabletInfoBuilder{
		KeyspaceName: ksName,
		TableName:    tblName,
		FirstToken:   0,
		LastToken:    100,
	}.Build()
	if err != nil {
		t.Fatal(err)
	}
	t2, err := tablets.TabletInfoBuilder{
		KeyspaceName: ksName,
		TableName:    tblName,
		FirstToken:   101,
		LastToken:    200,
	}.Build()
	if err != nil {
		t.Fatal(err)
	}
	SessionData(session).InternalMetadataDescriber.AddTablet(t1)
	SessionData(session).InternalMetadataDescriber.AddTablet(t2)
	MetadataData(SessionData(session).InternalMetadataDescriber.InternalMetadata).InternalTabletsMetadata.Flush()
}

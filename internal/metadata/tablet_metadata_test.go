// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"context"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/wire"
	"github.com/scylladb/gocql/v2/metadata"
)

// scyllaKeyspacesSource answers system_schema.scylla_keyspaces with rows of
// keyspace_name and initial_tablets, a nil initial_tablets being null, and
// with a consistency column when consistency is not nil.
type scyllaKeyspacesSource struct {
	rows        map[string]*int32
	consistency map[string]string
	settings    QuerySettings
}

func (s scyllaKeyspacesSource) InternalSchemaSettings() QuerySettings  { return s.settings }
func (s scyllaKeyspacesSource) InternalSchemaContext() context.Context { return context.Background() }
func (s scyllaKeyspacesSource) InternalQuerySystem(_ context.Context, request *model.AttemptRequest) (*model.ResponseData, error) {
	if !strings.Contains(request.Statement, "scylla_keyspaces") {
		panic("unexpected query " + request.Statement)
	}
	var body []byte
	for name, initial := range s.rows {
		body = binary.BigEndian.AppendUint32(body, uint32(len(name)))
		body = append(body, name...)
		if initial == nil {
			body = binary.BigEndian.AppendUint32(body, 0xffffffff)
			continue
		}
		body = binary.BigEndian.AppendUint32(body, 4)
		body = binary.BigEndian.AppendUint32(body, uint32(*initial))
		if s.consistency != nil {
			if mode, ok := s.consistency[name]; ok {
				body = binary.BigEndian.AppendUint32(body, uint32(len(mode)))
				body = append(body, mode...)
			} else {
				body = binary.BigEndian.AppendUint32(body, 0xffffffff)
			}
		}
	}
	columns := []cql.ColumnSpec{
		{Keyspace: "system_schema", Table: "scylla_keyspaces", Name: "keyspace_name", Type: cql.TextType},
		{Keyspace: "system_schema", Table: "scylla_keyspaces", Name: "initial_tablets", Type: cql.IntType},
	}
	if s.consistency != nil {
		columns = append(columns, cql.ColumnSpec{Keyspace: "system_schema", Table: "scylla_keyspaces", Name: "consistency", Type: cql.TextType})
	}
	chain := wire.ChainOf(body)
	return &model.ResponseData{
		Owner:    wire.NewLease(chain, nil),
		Rows:     chain,
		RowCount: int32(len(s.rows)),
		Columns:  cql.NewColumns(columns...),
	}, nil
}

// TestReadV2ScyllaKeyspacesConsistency checks that a keyspace is strongly
// consistent ("global") only when the server reports it so, in any case,
// and the control connection negotiated TABLETS_ROUTING_V2, which is what
// gives the reported tablet leaders their meaning.
func TestReadV2ScyllaKeyspacesConsistency(t *testing.T) {
	n := func(v int32) *int32 { return &v }
	for _, v2 := range []bool{false, true} {
		keyspaces := map[string]*metadata.Keyspace{"global": {Name: "global"}, "upper": {Name: "upper"}, "eventual": {Name: "eventual"}, "null": {Name: "null"}, "local": {Name: "local"}}
		source := scyllaKeyspacesSource{
			rows:        map[string]*int32{"global": n(0), "upper": n(0), "eventual": n(0), "null": n(0), "local": n(0)},
			consistency: map[string]string{"global": "global", "upper": "GLOBAL", "eventual": "eventual", "local": "local"},
			settings:    QuerySettings{SystemSchema: true, Available: true, Scylla: true, Tablets: true, TabletsV2: v2},
		}
		if err := InternalReadV2ScyllaKeyspaces(context.Background(), source, keyspaces, ""); err != nil {
			t.Fatal(err)
		}
		for name, ks := range keyspaces {
			want := ""
			if v2 && (name == "global" || name == "upper") {
				want = "global"
			}
			if ks.ConsistencyMode != want {
				t.Errorf("TabletsV2 %v, %s: ConsistencyMode %q, want %q", v2, name, ks.ConsistencyMode, want)
			}
		}
	}
}

// TestReadV2ScyllaKeyspacesTablets checks what the loader records for
// KeyspaceToCQL's tablets option: tablets, their initial count, and whether
// the server takes the option at all.
func TestReadV2ScyllaKeyspacesTablets(t *testing.T) {
	n := func(v int32) *int32 { return &v }
	for _, tablets := range []bool{false, true} {
		keyspaces := map[string]*metadata.Keyspace{"vnodes": {Name: "vnodes"}, "auto": {Name: "auto"}, "eight": {Name: "eight"}, "nullrow": {Name: "nullrow"}}
		source := scyllaKeyspacesSource{
			rows:     map[string]*int32{"auto": n(0), "eight": n(8), "nullrow": nil, "absent": n(4)},
			settings: QuerySettings{SystemSchema: true, Available: true, Scylla: true, Tablets: tablets},
		}
		if err := InternalReadV2ScyllaKeyspaces(context.Background(), source, keyspaces, ""); err != nil {
			t.Fatal(err)
		}
		for name, want := range map[string]struct {
			based   bool
			initial int
		}{"vnodes": {}, "nullrow": {}, "auto": {true, 0}, "eight": {true, 8}} {
			ks := keyspaces[name]
			if ks.TabletBased != want.based || ks.InitialTablets != want.initial || KeyspaceData(ks).TabletsOption != tablets {
				t.Errorf("tablets %v, %s: TabletBased %v, InitialTablets %d, TabletsOption %v", tablets, name, ks.TabletBased, ks.InitialTablets, KeyspaceData(ks).TabletsOption)
			}
		}
	}
	// Not ScyllaDB: nothing to read, and no tablets option.
	ks := &metadata.Keyspace{Name: "ks"}
	if err := InternalReadV2ScyllaKeyspaces(context.Background(), scyllaKeyspacesSource{settings: QuerySettings{Tablets: true}}, map[string]*metadata.Keyspace{"ks": ks}, ""); err != nil || KeyspaceData(ks).TabletsOption {
		t.Fatalf("err %v, TabletsOption %v", err, KeyspaceData(ks).TabletsOption)
	}
}

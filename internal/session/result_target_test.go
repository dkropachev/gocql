// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"testing"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
)

// TestResultTarget checks that a result names the host of the attempt that
// produced it, and no shard for a host that is not shard-aware.
func TestResultTarget(t *testing.T) {
	s := loopbackTestSession(t, "pone")
	statement := binding.Statement[int32, cqltype.Bytes]{CQL: "SELECT value FROM ks.tbl WHERE k = ?", Params: codec.OneParam(codec.Int32), Rows: codec.OneColumn(codec.BlobCodec), BindMarkers: cqltype.NewColumns(cqltype.ColumnSpec{Name: "k", Type: cqltype.IntType})}
	p := codecTestMust(Prepare(context.Background(), s, statement, PrepareOptions{}))
	var dst cqltype.Bytes
	_, result, err := p.One(context.Background(), 1, &dst)
	if err != nil {
		t.Fatal(err)
	}
	if result.HostID == (cqltype.UUID{}) || result.ShardID.Set || result.Attempts != 1 {
		t.Fatalf("result host %v shard %v after %d attempts", result.HostID, result.ShardID, result.Attempts)
	}
	host := s.Metadata().Hosts()
	if len(host) == 0 || host[0].ID != result.HostID {
		t.Fatalf("result host %v is not the session's host %v", result.HostID, host)
	}
}

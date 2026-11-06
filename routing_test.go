// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/routing"
	"github.com/scylladb/gocql/v2/tablets"
)

func testHint(t testing.TB, first, last int64, version cql.Option[uint64], replicas ...routing.Target) tablets.TabletInfo {
	t.Helper()
	rs := make([]tablets.ReplicaInfo, len(replicas))
	for i, r := range replicas {
		rs[i] = tablets.NewReplicaInfo(tablets.HostUUID(r.HostID), int(r.ShardID.Value))
	}
	h, err := tablets.NewTabletInfo("ks", "table", first, last, rs)
	if err != nil {
		t.Fatal(err)
	}
	if version.Set {
		h = h.WithVersion(version.Value)
	}
	return h
}

func hTarget(id byte, shard uint32) routing.Target {
	return routing.Target{HostID: cql.UUID{15: id}, ShardID: cql.Option[uint32]{Value: shard, Set: true}}
}

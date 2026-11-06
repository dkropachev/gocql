// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"github.com/scylladb/gocql/v2/cql"
	implCqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	implModel "github.com/scylladb/gocql/v2/internal/model"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"

	routing "github.com/scylladb/gocql/v2/internal/routing"

	"testing"

	"github.com/scylladb/gocql/v2/tablets"
)

func step06Hint(t testing.TB, first, last int64, version cql.Option[uint64], replicas ...implModel.Target) tablets.TabletInfo {
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

func step06Session(t testing.TB) *Session {
	t.Helper()
	s := func() *Session {
		opaqueStorageValue := new(Session)
		return opaqueStorageValue
	}()
	SessionData(s).InternalMetadataDescriber = InternalNewMetadataDescriber(s)
	m := v2TestMetadata()
	routing.SnapshotData(m).InternalKeyspaces["ks"].Tables["table"].ID = "identity-1"
	routing.SnapshotData(m).InternalKeyspaces["ks"].TabletBased = true
	MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalSet("ks", routing.SnapshotData(m).InternalKeyspaces["ks"])
	p := InternalSessionPeripheral(s)
	p.InternalFeedbackOnce.Do(func() {})
	routing.SnapshotData(m).InternalPlacementSignal = &p.InternalPlacementEpoch
	routing.SnapshotData(m).InternalPlacements = implRouting.InternalPlacementPut(routing.SnapshotData(m).InternalPlacements, implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "table"}, implRouting.InternalTabletPlacement{InternalIdentity: "identity-1"}, implRouting.InternalComparePlacementTable)
	p.InternalSnapshot.Store(m)
	t.Cleanup(func() {
		InternalSessionCloseV2Peripheral(s)
		MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalTabletsMetadata.Close()
	})
	return s
}

func hTarget(id byte, shard uint32) implModel.Target {
	return implModel.Target{HostID: implCqltype.UUID{15: id}, ShardID: cql.Option[uint32]{Value: shard, Set: true}}
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	implCqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	implModel "github.com/scylladb/gocql/v2/internal/model"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"

	routing "github.com/scylladb/gocql/v2/internal/routing"

	"testing"

	"github.com/scylladb/gocql/v2/internal/tablets"
)

func testHint(t testing.TB, first, last int64, version implCqltype.Option[uint64], replicas ...implModel.Target) tablets.TabletInfo {
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

func testSession(t testing.TB) *Core {
	t.Helper()
	s := func() *Core {
		opaqueStorageValue := new(Core)
		return opaqueStorageValue
	}()
	SessionData(s).InternalMetadataDescriber = s.NewMetadataDescriber()
	m := v2TestMetadata()
	routing.SnapshotData(m).InternalKeyspaces["ks"].Tables["table"].ID = "identity-1"
	routing.SnapshotData(m).InternalKeyspaces["ks"].TabletBased = true
	MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalSet("ks", routing.SnapshotData(m).InternalKeyspaces["ks"])
	p := s.Peripheral()
	p.InternalFeedbackOnce.Do(func() {})
	routing.SnapshotData(m).InternalPlacementSignal = &p.InternalPlacementEpoch
	routing.SnapshotData(m).InternalPlacements = routing.SnapshotData(m).InternalPlacements.Put(implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "table"}, implRouting.InternalTabletPlacement{InternalIdentity: "identity-1"})
	p.InternalSnapshot.Store(m)
	t.Cleanup(func() {
		s.CloseV2Peripheral()
		MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalTabletsMetadata.Close()
	})
	return s
}

func hTarget(id byte, shard uint32) implModel.Target {
	return implModel.Target{HostID: implCqltype.UUID{15: id}, ShardID: implCqltype.Option[uint32]{Value: shard, Set: true}}
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"github.com/scylladb/gocql/v2/cql"
	implCqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	implModel "github.com/scylladb/gocql/v2/internal/model"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"

	routing "github.com/scylladb/gocql/v2/internal/routing"

	"encoding/binary"
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
func step06Payload(h tablets.TabletInfo) []byte {
	field := func(dst, b []byte) []byte { return append(binary.BigEndian.AppendUint32(dst, uint32(len(b))), b...) }
	var b []byte
	b = field(b, binary.BigEndian.AppendUint64(nil, uint64(h.FirstToken())))
	b = field(b, binary.BigEndian.AppendUint64(nil, uint64(h.LastToken())))
	list := binary.BigEndian.AppendUint32(nil, uint32(len(h.ReplicasUnsafe())))
	for _, r := range h.ReplicasUnsafe() {
		id := r.HostUUIDValue()
		tuple := field(nil, id[:])
		tuple = field(tuple, binary.BigEndian.AppendUint32(nil, uint32(r.ShardID())))
		list = field(list, tuple)
	}
	b = field(b, list)
	if v, ok := h.Version(); ok {
		b = field(b, binary.BigEndian.AppendUint64(nil, v))
	}
	return b
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

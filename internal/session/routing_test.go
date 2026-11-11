// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implCqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	implModel "github.com/scylladb/gocql/v2/internal/model"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/transport"

	routing "github.com/scylladb/gocql/v2/internal/routing"

	"encoding/binary"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/tablets"
	"github.com/scylladb/gocql/v2/internal/wire"
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
func testPayload(h tablets.TabletInfo) []byte {
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
func testSession(t testing.TB) *cluster.Core {
	t.Helper()
	s := func() *cluster.Core {
		opaqueStorageValue := new(cluster.Core)
		return opaqueStorageValue
	}()
	implCluster.SessionData(s).InternalMetadataDescriber = s.NewMetadataDescriber()
	m := v2TestMetadata()
	routing.SnapshotData(m).InternalKeyspaces["ks"].Tables["table"].ID = "identity-1"
	routing.SnapshotData(m).InternalKeyspaces["ks"].TabletBased = true
	implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalSet("ks", routing.SnapshotData(m).InternalKeyspaces["ks"])
	p := s.Peripheral()
	p.InternalFeedbackOnce.Do(func() {})
	routing.SnapshotData(m).InternalPlacementSignal = &p.InternalPlacementEpoch
	routing.SnapshotData(m).InternalPlacements = routing.SnapshotData(m).InternalPlacements.Put(implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "table"}, implRouting.InternalTabletPlacement{InternalIdentity: "identity-1"})
	p.InternalSnapshot.Store(m)
	t.Cleanup(func() {
		s.CloseV2Peripheral()
		implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalTabletsMetadata.Close()
	})
	return s
}
func testDrain(s *cluster.Core) {
	p := s.Peripheral()
	p.InternalFeedbackMu.Lock()
	updates := p.InternalFeedback
	p.InternalFeedback = make(map[cluster.InternalV2TabletFeedbackKey]cluster.InternalV2PendingTabletFeedback)
	resync := p.InternalFeedbackResync
	p.InternalFeedbackResync = false
	p.InternalFeedbackMu.Unlock()
	s.PublishTabletFeedback(p, updates, resync)
}
func hTarget(id byte, shard uint32) implModel.Target {
	return implModel.Target{HostID: implCqltype.UUID{15: id}, ShardID: implCqltype.Option[uint32]{Value: shard, Set: true}}
}

func FuzzTabletPayload(f *testing.F) {
	h, _ := tablets.NewTabletInfo("ks", "table", 0, 100, []tablets.ReplicaInfo{tablets.NewReplicaInfo(tablets.HostUUID{1}, 0)})
	f.Add(testPayload(h.WithVersion(7)), true)
	f.Fuzz(func(t *testing.T, data []byte, v2 bool) {
		h, err := transport.InternalParseTypedTabletPayload(implCqltype.BorrowBytes(data), "ks", "table", v2)
		if err == nil {
			if h.FirstToken() >= h.LastToken() {
				t.Fatal("invalid range")
			}
			for _, r := range h.ReplicasUnsafe() {
				if r.ShardID() < 0 {
					t.Fatal("invalid shard")
				}
			}
		}
	})
}

type capturedPartitioner struct {
	calls    int
	expected []byte
}

func (p *capturedPartitioner) Token(key implRouting.PartitionKey) (int64, error) {
	p.calls++
	err := key.ForEach(func(b implCqltype.Bytes) error {
		return cqltype.EachBytes(b, func(s wire.Span) error {
			if len(s.Bytes()) != len(p.expected) || len(p.expected) > 0 && &s.Bytes()[0] != &p.expected[0] {
				return implCqltype.TypeValidation("partitioner", "payload was reencoded or copied")
			}
			return nil
		})
	})
	return 77, err
}

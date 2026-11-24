// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"net"
	"net/netip"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/event"
	oldevents "github.com/scylladb/gocql/v2/internal/events"
	"github.com/scylladb/gocql/v2/internal/routing"
)

// TestNodeEventHostID checks that a node event carries the node's host ID:
// from the current snapshot, or, for a REMOVED_NODE the ring refresh has
// already dropped, from the snapshot before it.
func TestNodeEventHostID(t *testing.T) {
	t.Parallel()
	addr := netip.MustParseAddrPort("127.0.10.1:9042")
	id := cql.RandomUUID()
	prev := routing.NewMetadataSnapshot(routing.SnapshotState{InternalHosts: []routing.HostSnapshot{{Address: addr, HostID: id}}})

	for _, tc := range []struct {
		source oldevents.Event
		kind   event.Kind
	}{
		{&oldevents.TopologyChangeEvent{Change: "REMOVED_NODE", Host: net.ParseIP("127.0.10.1"), Port: 9042}, event.KindTopology},
		{&oldevents.StatusChangeEvent{Change: "DOWN", Host: net.ParseIP("127.0.10.1"), Port: 9042}, event.KindStatus},
	} {
		s := new(Core)
		sub, err := s.Subscribe(event.Filter{Capacity: 4})
		if err != nil {
			t.Fatal(err)
		}
		s.emitV2NodeEvent(tc.source, prev)
		got := <-sub.Events()
		if got.Kind != tc.kind || got.HostID != id {
			t.Errorf("%T: got %v %v, want %v %v", tc.source, got.Kind, got.HostID, tc.kind, id)
		}
		sub.Close()
		s.CloseV2Peripheral()
	}
	if got := eventHostID(addr, nil, prev); got != id {
		t.Errorf("eventHostID skipped a nil snapshot: %v", got)
	}
	if got := eventHostID(netip.MustParseAddrPort("127.0.10.2:9042"), prev); got != (cql.UUID{}) {
		t.Errorf("eventHostID of an unknown address: %v", got)
	}
}

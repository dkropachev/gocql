// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster_test

import (
	"sync"
	"testing"

	. "github.com/scylladb/gocql/v2"
	oldevents "github.com/scylladb/gocql/v2/events"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
)

func TestSessionAPI(t *testing.T) {
	t.Run("Subscribe", func(t *testing.T) {
		t.Run("ClosePublishRace", func(t *testing.T) {
			s := func() *implCluster.Core {
				opaqueStorageValuex := new(implCluster.Core)
				return opaqueStorageValuex
			}()

			sub, err := s.Subscribe(EventFilter{Capacity: 1})
			if err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				for i := 0; i < 100; i++ {
					implCluster.InternalSessionPublishV2Event(s, &oldevents.SchemaChangeKeyspaceEvent{Keyspace: "k"})
				}
			}()
			go func() { defer wg.Done(); sub.Close(); implCluster.InternalSessionCloseV2Peripheral(s) }()
			wg.Wait()
		})

		t.Run("FilterOverflowAndClose", func(t *testing.T) {
			s := func() *implCluster.Core {
				opaqueStorageValue := new(implCluster.Core)
				return opaqueStorageValue
			}()

			filter := EventFilter{Kinds: []EventKind{SchemaEvent}, Keyspace: "ks", Capacity: 1}
			sub, err := s.Subscribe(filter)
			if err != nil {
				t.Fatal(err)
			}
			filter.Kinds[0] = StatusEvent
			implCluster.InternalSessionPublishV2Event(s, &oldevents.SchemaChangeTableEvent{Change: "CREATED", Keyspace: "other", Table: "x"})
			select {
			case e := <-sub.Events():
				t.Fatal("keyspace filter ignored", e)
			default:
			}
			implCluster.InternalSessionPublishV2Event(s, &oldevents.SchemaChangeTableEvent{Change: "CREATED", Keyspace: "ks", Table: "a"})
			implCluster.InternalSessionPublishV2Event(s, &oldevents.SchemaChangeTableEvent{Change: "CREATED", Keyspace: "ks", Table: "b"})
			event := <-sub.Events()
			if event.Kind != OverflowEvent || event.Change != "RESYNC" || event.Generation != s.Metadata().Generation() {
				t.Fatal(event)
			}
			implCluster.InternalSessionPublishV2Event(s, &oldevents.SchemaChangeTableEvent{Change: "DROPPED", Keyspace: "ks", Table: "b"})
			if _, ok := <-sub.Events(); ok {
				t.Fatal("overflow subscription must close after terminal marker")
			}
			fresh, err := s.Subscribe(EventFilter{Kinds: []EventKind{SchemaEvent}, Keyspace: "ks", Capacity: 4})
			if err != nil {
				t.Fatal(err)
			}
			snapshot := s.Metadata()
			implCluster.InternalSessionPublishV2Event(s, &oldevents.SchemaChangeTableEvent{Change: "CREATED", Keyspace: "ks", Table: "c"})
			event = <-fresh.Events()
			if event.Generation <= snapshot.Generation() {
				t.Fatal("publication event did not follow subscribed snapshot", event)
			}
			fresh.Close()
			sub.Close()
			sub.Close()
			if _, ok := <-sub.Events(); ok {
				t.Fatal("closed subscription not closed")
			}
			implCluster.InternalSessionCloseV2Peripheral(s)
			if _, err := s.Subscribe(EventFilter{}); err == nil {
				t.Fatal("subscribe after close succeeded")
			}
		})
	})
}

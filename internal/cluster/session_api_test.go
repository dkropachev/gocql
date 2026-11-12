// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster_test

import (
	"sync"
	"testing"

	"github.com/scylladb/gocql/v2/event"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	oldevents "github.com/scylladb/gocql/v2/internal/events"
)

func TestSessionAPI(t *testing.T) {
	t.Run("Subscribe", func(t *testing.T) {
		t.Run("ClosePublishRace", func(t *testing.T) {
			s := func() *implCluster.Core {
				opaqueStorageValuex := new(implCluster.Core)
				return opaqueStorageValuex
			}()

			sub, err := s.Subscribe(event.Filter{Capacity: 1})
			if err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				for i := 0; i < 100; i++ {
					s.PublishV2Event(&oldevents.SchemaChangeKeyspaceEvent{Keyspace: "k"})
				}
			}()
			go func() { defer wg.Done(); sub.Close(); s.CloseV2Peripheral() }()
			wg.Wait()
		})

		t.Run("FilterOverflowAndClose", func(t *testing.T) {
			s := func() *implCluster.Core {
				opaqueStorageValue := new(implCluster.Core)
				return opaqueStorageValue
			}()

			filter := event.Filter{Kinds: []event.Kind{event.Schema}, Keyspace: "ks", Capacity: 1}
			sub, err := s.Subscribe(filter)
			if err != nil {
				t.Fatal(err)
			}
			filter.Kinds[0] = event.Status
			s.PublishV2Event(&oldevents.SchemaChangeTableEvent{Change: "CREATED", Keyspace: "other", Table: "x"})
			select {
			case e := <-sub.Events():
				t.Fatal("keyspace filter ignored", e)
			default:
			}
			s.PublishV2Event(&oldevents.SchemaChangeTableEvent{Change: "CREATED", Keyspace: "ks", Table: "a"})
			s.PublishV2Event(&oldevents.SchemaChangeTableEvent{Change: "CREATED", Keyspace: "ks", Table: "b"})
			got := <-sub.Events()
			if got.Kind != event.Overflow || got.Change != "RESYNC" || got.Generation != s.Metadata().Generation() {
				t.Fatal(got)
			}
			s.PublishV2Event(&oldevents.SchemaChangeTableEvent{Change: "DROPPED", Keyspace: "ks", Table: "b"})
			if _, ok := <-sub.Events(); ok {
				t.Fatal("overflow subscription must close after terminal marker")
			}
			fresh, err := s.Subscribe(event.Filter{Kinds: []event.Kind{event.Schema}, Keyspace: "ks", Capacity: 4})
			if err != nil {
				t.Fatal(err)
			}
			snapshot := s.Metadata()
			s.PublishV2Event(&oldevents.SchemaChangeTableEvent{Change: "CREATED", Keyspace: "ks", Table: "c"})
			got = <-fresh.Events()
			if got.Generation <= snapshot.Generation() {
				t.Fatal("publication event did not follow subscribed snapshot", got)
			}
			fresh.Close()
			sub.Close()
			sub.Close()
			if _, ok := <-sub.Events(); ok {
				t.Fatal("closed subscription not closed")
			}
			s.CloseV2Peripheral()
			if _, err := s.Subscribe(event.Filter{}); err == nil {
				t.Fatal("subscribe after close succeeded")
			}
		})
	})
}

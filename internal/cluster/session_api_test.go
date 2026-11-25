// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster_test

import (
	"slices"
	"sync"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
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

		t.Run("RejectsUnknownKind", func(t *testing.T) {
			s := new(implCluster.Core)
			for _, kind := range []event.Kind{event.KindUnknown, event.KindOverflow + 1} {
				if _, err := s.Subscribe(event.Filter{Kinds: []event.Kind{kind}}); err == nil {
					t.Errorf("Subscribe accepted event kind %v", kind)
				}
			}
		})

		t.Run("ClientRouteIDs", func(t *testing.T) {
			s := new(implCluster.Core)
			defer s.CloseV2Peripheral()
			sub, err := s.Subscribe(event.Filter{Kinds: []event.Kind{event.KindClientRoute}, Capacity: 4})
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Close()
			host := cql.RandomUUID()
			// A connection ID that looks like a UUID stays a connection ID,
			// and a host ID that does not parse is dropped.
			conn := cql.RandomUUID().String()
			s.PublishV2Event(&oldevents.ClientRoutesChangedEvent{
				ChangeType:    "UPDATED",
				ConnectionIDs: []string{"c1", conn},
				HostIDs:       []string{host.String(), "not-a-uuid"},
			})
			got := <-sub.Events()
			if got.Kind != event.KindClientRoute || got.Change != event.ChangeUpdated {
				t.Fatalf("event %+v", got)
			}
			if !slices.Equal(got.ConnectionIDs, []string{"c1", conn}) {
				t.Errorf("ConnectionIDs %v", got.ConnectionIDs)
			}
			if !slices.Equal(got.HostIDs, []cql.UUID{host}) {
				t.Errorf("HostIDs %v, want [%v]", got.HostIDs, host)
			}
			if got.ArgumentTypes != nil {
				t.Errorf("ArgumentTypes %v, want nil", got.ArgumentTypes)
			}
		})

		t.Run("FilterOverflowAndClose", func(t *testing.T) {
			s := func() *implCluster.Core {
				opaqueStorageValue := new(implCluster.Core)
				return opaqueStorageValue
			}()

			filter := event.Filter{Kinds: []event.Kind{event.KindSchema}, Keyspace: "ks", Capacity: 1}
			sub, err := s.Subscribe(filter)
			if err != nil {
				t.Fatal(err)
			}
			filter.Kinds[0] = event.KindStatus
			s.PublishV2Event(&oldevents.SchemaChangeTableEvent{Change: "CREATED", Keyspace: "other", Table: "x"})
			select {
			case e := <-sub.Events():
				t.Fatal("keyspace filter ignored", e)
			default:
			}
			s.PublishV2Event(&oldevents.SchemaChangeTableEvent{Change: "CREATED", Keyspace: "ks", Table: "a"})
			s.PublishV2Event(&oldevents.SchemaChangeTableEvent{Change: "CREATED", Keyspace: "ks", Table: "b"})
			got := <-sub.Events()
			if got.Kind != event.KindOverflow || got.Change != event.ChangeResync || got.Generation != s.Metadata().Generation() {
				t.Fatal(got)
			}
			// The subscription survives the overflow and keeps delivering.
			s.PublishV2Event(&oldevents.SchemaChangeTableEvent{Change: "DROPPED", Keyspace: "ks", Table: "b"})
			if got, ok := <-sub.Events(); !ok || got.Kind != event.KindSchema || got.Object != "b" || got.Change != event.ChangeDropped {
				t.Fatal("overflowed subscription stopped delivering", got, ok)
			}
			fresh, err := s.Subscribe(event.Filter{Kinds: []event.Kind{event.KindSchema}, Keyspace: "ks", Capacity: 4})
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
			// Close keeps what was queued; the queue then ends.
			for range cap(sub.Events()) + 1 {
				if _, ok := <-sub.Events(); !ok {
					break
				}
			}
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

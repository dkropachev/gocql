/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package cluster

import (
	"errors"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/metadata"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	routing "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/tablets"
)

func TestTablets(t *testing.T) {
	t.Run("Feedback", func(t *testing.T) {
		s := func() *Core {
			opaqueStorageValuexxx := new(Core)
			return opaqueStorageValuexxx
		}()
		SessionData(s).InternalMetadataDescriber = InternalNewMetadataDescriber(s)
		defer MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalTabletsMetadata.Close()
		defer InternalSessionCloseV2Peripheral(s)
		MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalSet("ks", &metadata.KeyspaceMetadata{Name: "ks", Tables: map[string]*metadata.TableMetadata{"t": {Name: "t"}}})
		p := InternalSessionPeripheral(s)
		p.InternalFeedbackOnce.Do(func() {}) // Hold the consumer while filling the bounded queue.
		sub, err := s.Subscribe(EventFilter{Kinds: []EventKind{OverflowEvent}, Capacity: 1})
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Close()
		for i := 0; i < 1025; i++ {
			hint, err := tablets.NewTabletInfo("ks", "t", int64(i), int64(i+1), []tablets.ReplicaInfo{tablets.NewReplicaInfo(tablets.HostUUID{1}, 0)})
			if err != nil {
				t.Fatal(err)
			}
			InternalSessionApplyV2TabletHint(s, hint)
		}
		p.InternalFeedbackMu.Lock()
		if len(p.InternalFeedback) > 1024 || !p.InternalFeedbackResync {
			t.Fatal("feedback did not bound and mark overflow")
		}
		p.InternalFeedbackMu.Unlock()
		// A newer schema event must win over every already queued hint.
		p.InternalMu.Lock()
		p.InternalBumpSchemaEpochLocked("ks")
		p.InternalMu.Unlock()
		go InternalSessionRunV2TabletFeedback(s, p)
		select {
		case event := <-sub.Events():
			if event.Kind != OverflowEvent || event.Change != "TABLET_RESYNC" {
				t.Fatal(event)
			}
		case <-time.After(time.Second):
			t.Fatal("feedback resync was not published")
		}
		if got := MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalTabletsMetadata.GetTableTablets("ks", "t"); len(got) != 0 {
			t.Fatal("stale hint restored tablet", got)
		}
		snapshot := s.Metadata()
		if _, known := routing.InternalMetadataSnapshotTabletRoot(snapshot, implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "t"}); !known {
			t.Fatal("resync lost tablet cache-miss marker")
		}
		if replicas, err := snapshot.Replicas("ks", "t", 4); err != nil || len(replicas) != 0 {
			t.Fatal("tablet cache miss invented vnode replicas", replicas, err)
		}
	})

	t.Run("ForEachTablet", func(t *testing.T) {
		t.Parallel()

		t.Run("HappyPath", func(t *testing.T) {
			t.Parallel()

			ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
			s := newSchemaEventTestSessionWithMock(ctrl)
			defer s.Close()
			SessionData(s).InternalIsInitialized = true
			SessionData(s).InternalTabletsRoutingV1 = true

			addTestTablets(t, s, "ks1", "tbl_a")
			addTestTablets(t, s, "ks2", "tbl_b")

			visited := make(map[string]int)
			err := InternalSessionForEachTablet(s, func(keyspace, table string, entries tablets.TabletEntryList) bool {
				visited[keyspace+"."+table] = len(entries)
				return true
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(visited) != 2 {
				t.Fatalf("expected 2 tables visited, got %d", len(visited))
			}
			if visited["ks1.tbl_a"] != 2 {
				t.Fatalf("expected 2 entries for ks1.tbl_a, got %d", visited["ks1.tbl_a"])
			}
			if visited["ks2.tbl_b"] != 2 {
				t.Fatalf("expected 2 entries for ks2.tbl_b, got %d", visited["ks2.tbl_b"])
			}
		})

		t.Run("EarlyStop", func(t *testing.T) {
			t.Parallel()

			ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
			s := newSchemaEventTestSessionWithMock(ctrl)
			defer s.Close()
			SessionData(s).InternalIsInitialized = true
			SessionData(s).InternalTabletsRoutingV1 = true

			addTestTablets(t, s, "ks1", "tbl_a")
			addTestTablets(t, s, "ks2", "tbl_b")

			count := 0
			err := InternalSessionForEachTablet(s, func(keyspace, table string, entries tablets.TabletEntryList) bool {
				count++
				return false
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if count != 1 {
				t.Fatalf("expected 1 callback invocation, got %d", count)
			}
		})

		t.Run("ClosedSession", func(t *testing.T) {
			t.Parallel()

			ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
			s := newSchemaEventTestSessionWithMock(ctrl)
			defer s.Close()
			SessionData(s).InternalIsInitialized = true
			SessionData(s).InternalTabletsRoutingV1 = true
			InternalSessionMarkClosed(s)

			err := InternalSessionForEachTablet(s, func(keyspace, table string, entries tablets.TabletEntryList) bool {
				t.Fatal("callback should not be called on closed session")
				return true
			})
			if !errors.Is(err, runtimeerrors.ErrSessionClosed) {
				t.Fatalf("expected ErrSessionClosed, got %v", err)
			}
		})

		t.Run("TabletsNotEnabled", func(t *testing.T) {
			t.Parallel()

			ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
			s := newSchemaEventTestSessionWithMock(ctrl)
			defer s.Close()
			SessionData(s).InternalIsInitialized = true

			err := InternalSessionForEachTablet(s, func(keyspace, table string, entries tablets.TabletEntryList) bool {
				t.Fatal("callback should not be called when tablets not enabled")
				return true
			})
			if !errors.Is(err, runtimeerrors.ErrTabletsNotUsed) {
				t.Fatalf("expected ErrTabletsNotUsed, got %v", err)
			}
		})

		t.Run("NilCallback", func(t *testing.T) {
			t.Parallel()

			ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
			s := newSchemaEventTestSessionWithMock(ctrl)
			defer s.Close()
			SessionData(s).InternalIsInitialized = true
			SessionData(s).InternalTabletsRoutingV1 = true

			addTestTablets(t, s, "ks", "tb")

			err := InternalSessionForEachTablet(s, nil)
			if err != nil {
				t.Fatalf("expected nil error for nil callback, got %v", err)
			}
		})
	})

	t.Run("TableTabletsMetadata", func(t *testing.T) {
		t.Parallel()

		t.Run("HappyPath", func(t *testing.T) {
			t.Parallel()

			ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
			s := newSchemaEventTestSessionWithMock(ctrl)
			defer s.Close()
			SessionData(s).InternalIsInitialized = true
			SessionData(s).InternalTabletsRoutingV1 = true

			addTestTablets(t, s, "test_ks", "tbl_a")

			entries, err := InternalSessionTableTabletsMetadata(s, "test_ks", "tbl_a")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(entries) != 2 {
				t.Fatalf("expected 2 tablet entries, got %d", len(entries))
			}
		})

		t.Run("ClosedSession", func(t *testing.T) {
			t.Parallel()

			ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
			s := newSchemaEventTestSessionWithMock(ctrl)
			defer s.Close()
			SessionData(s).InternalIsInitialized = true
			SessionData(s).InternalTabletsRoutingV1 = true
			InternalSessionMarkClosed(s)

			_, err := InternalSessionTableTabletsMetadata(s, "ks", "tb")
			if !errors.Is(err, runtimeerrors.ErrSessionClosed) {
				t.Fatalf("expected ErrSessionClosed, got %v", err)
			}
		})

		t.Run("NotReady", func(t *testing.T) {
			t.Parallel()

			ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
			s := newSchemaEventTestSessionWithMock(ctrl)
			defer s.Close()
			SessionData(s).InternalTabletsRoutingV1 = true

			_, err := InternalSessionTableTabletsMetadata(s, "ks", "tb")
			if !errors.Is(err, runtimeerrors.ErrSessionNotReady) {
				t.Fatalf("expected ErrSessionNotReady, got %v", err)
			}
		})

		t.Run("TabletsNotEnabled", func(t *testing.T) {
			t.Parallel()

			ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
			s := newSchemaEventTestSessionWithMock(ctrl)
			defer s.Close()
			SessionData(s).InternalIsInitialized = true

			_, err := InternalSessionTableTabletsMetadata(s, "ks", "tb")
			if !errors.Is(err, runtimeerrors.ErrTabletsNotUsed) {
				t.Fatalf("expected ErrTabletsNotUsed, got %v", err)
			}
		})

		t.Run("EmptyKeyspace", func(t *testing.T) {
			t.Parallel()

			ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
			s := newSchemaEventTestSessionWithMock(ctrl)
			defer s.Close()
			SessionData(s).InternalIsInitialized = true
			SessionData(s).InternalTabletsRoutingV1 = true

			_, err := InternalSessionTableTabletsMetadata(s, "", "tb")
			if !errors.Is(err, runtimeerrors.ErrNoKeyspace) {
				t.Fatalf("expected ErrNoKeyspace, got %v", err)
			}
		})

		t.Run("EmptyTable", func(t *testing.T) {
			t.Parallel()

			ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
			s := newSchemaEventTestSessionWithMock(ctrl)
			defer s.Close()
			SessionData(s).InternalIsInitialized = true
			SessionData(s).InternalTabletsRoutingV1 = true

			_, err := InternalSessionTableTabletsMetadata(s, "ks", "")
			if !errors.Is(err, runtimeerrors.ErrNoTable) {
				t.Fatalf("expected ErrNoTable, got %v", err)
			}
		})

		t.Run("NoData", func(t *testing.T) {
			t.Parallel()

			ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
			s := newSchemaEventTestSessionWithMock(ctrl)
			defer s.Close()
			SessionData(s).InternalIsInitialized = true
			SessionData(s).InternalTabletsRoutingV1 = true

			entries, err := InternalSessionTableTabletsMetadata(s, "ks", "nonexistent")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if entries != nil {
				t.Fatalf("expected nil for nonexistent table, got %d entries", len(entries))
			}
		})
	})
}

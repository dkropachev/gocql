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
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/event"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	routing "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/tablets"
	metadata2 "github.com/scylladb/gocql/v2/metadata"
)

func TestTablets(t *testing.T) {
	t.Run("Feedback", func(t *testing.T) {
		s := func() *Core {
			opaqueStorageValuexxx := new(Core)
			return opaqueStorageValuexxx
		}()
		SessionData(s).InternalMetadataDescriber = s.NewMetadataDescriber()
		defer MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalTabletsMetadata.Close()
		defer s.CloseV2Peripheral()
		MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalSet("ks", &metadata2.Keyspace{Name: "ks", Tables: map[string]*metadata2.Table{"t": {Name: "t"}}})
		p := s.Peripheral()
		p.InternalFeedbackOnce.Do(func() {}) // Hold the consumer while filling the bounded queue.
		sub, err := s.Subscribe(event.Filter{Kinds: []event.Kind{event.KindOverflow}, Capacity: 1})
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Close()
		for i := 0; i < 1025; i++ {
			hint, err := tablets.NewTabletInfo("ks", "t", int64(i), int64(i+1), []tablets.ReplicaInfo{tablets.NewReplicaInfo(tablets.HostUUID{1}, 0)})
			if err != nil {
				t.Fatal(err)
			}
			sessionApplyV2TabletHint(s, hint)
		}
		p.InternalFeedbackMu.Lock()
		if len(p.InternalFeedback) > 1024 || !p.InternalFeedbackResync {
			t.Fatal("feedback did not bound and mark overflow")
		}
		p.InternalFeedbackMu.Unlock()
		// A newer schema event must win over every already queued hint.
		p.InternalMu.Lock()
		p.bumpSchemaEpochLocked("ks")
		p.InternalMu.Unlock()
		go sessionRunV2TabletFeedback(s, p)
		select {
		case ev := <-sub.Events():
			if ev.Kind != event.KindOverflow || ev.Change != "TABLET_RESYNC" {
				t.Fatal(ev)
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
}

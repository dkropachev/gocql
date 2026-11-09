// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"

	oldevents "github.com/scylladb/gocql/v2/events"
	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/logging"
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/settings"
)

func TestSchemaMetadata(t *testing.T) {
	t.Run("Publication", func(t *testing.T) {
		t.Run("CannotCrossDrop", func(t *testing.T) {
			s, _ := refreshAllSchemaSession(t)
			before := s.Metadata()
			base := settings.DefaultConfig("localhost")
			policy := &blockingPolicy{RoutingPolicy: base.DefaultProfile.Routing, entered: make(chan struct{}), release: make(chan struct{})}
			profile := *base.DefaultProfile
			profile.Routing = policy
			base.DefaultProfile = &profile
			implCluster.InternalSessionConfigureV2Peripheral(s, base)
			policy.block.Store(true)
			done := make(chan struct{})
			go func() { defer close(done); implCluster.InternalSessionPublishV2Metadata(s) }()
			<-policy.entered
			implCluster.InternalSessionPublishV2Event(s, &oldevents.SchemaChangeTableEvent{Change: "DROPPED", Keyspace: "ks1", Table: "t1"})
			after := s.Metadata()
			close(policy.release)
			<-done
			if s.Metadata().Generation() < after.Generation() {
				t.Fatal("publication generation moved backwards")
			}
			if ks, _ := s.Metadata().Keyspace("ks1"); ks.Tables["t1"] != nil {
				t.Fatal("old builder resurrected dropped table")
			}
			if ks, _ := before.Keyspace("ks1"); ks.Tables["t1"] == nil {
				t.Fatal("retained snapshot mutated")
			}
		})

		t.Run("ContentionEmitsTerminalResync", func(t *testing.T) {
			s := func() *cluster.Session {
				opaqueStorageValuexxxxxxxxxxxxxx := new(cluster.Session)
				implCluster.SessionData(opaqueStorageValuexxxxxxxxxxxxxx).InternalHostSource = &cluster.InternalRingDescriber{}
				implCluster.SessionData(opaqueStorageValuexxxxxxxxxxxxxx).InternalLogger = logging.InternalNopLogger{}
				return opaqueStorageValuexxxxxxxxxxxxxx
			}()
			defer s.Close()
			s.Metadata()
			config := settings.DefaultConfig("localhost")
			profile := *config.DefaultProfile
			profile.Routing = unstablePolicy{RoutingPolicy: profile.Routing, session: s}
			config.DefaultProfile = &profile
			implCluster.InternalSessionConfigureV2Peripheral(s, config)
			sub, _ := s.Subscribe(cluster.EventFilter{Capacity: 4})
			implCluster.InternalSessionHandleNodeEvent(s, []protocol.Frame{&frm.StatusChangeEventFrame{Change: "DOWN", Host: net.IPv4(127, 0, 0, 1), Port: 9042}})
			event := <-sub.Events()
			if event.Kind != cluster.OverflowEvent || event.Change != "PUBLICATION_RESYNC" {
				t.Fatal("unpublished delta escaped", event)
			}
			if _, ok := <-sub.Events(); ok {
				t.Fatal("resync marker was not terminal")
			}
		})
	})

	t.Run("Refresh", func(t *testing.T) {
		t.Run("CapabilityAndScope", func(t *testing.T) {
			s, mock := refreshAllSchemaSession(t)
			implCluster.SessionData(s).InternalUseSystemSchema = false
			if err := implCluster.InternalSessionRefreshV2AllSchema(s, context.Background()); err == nil {
				t.Fatal("unsupported schema silently succeeded")
			} else {
				if _, ok := errors.AsType[*settings.UnsupportedSettingError](err); !ok {
					t.Fatal(err)
				}
			}
			implCluster.SessionData(s).InternalUseSystemSchema = true
			for _, mode := range []settings.MetadataMode{settings.MetadataDisabled, settings.MetadataMinimal, settings.MetadataFull} {
				config := settings.DefaultConfig("localhost")
				config.Metadata.Mode = mode
				config.Metadata.Keyspaces = []string{"ks1"}
				implCluster.InternalSessionConfigureV2Peripheral(s, config)
				mock.mu.Lock()
				mock.queries = nil
				mock.mu.Unlock()
				if err := implCluster.InternalSessionRefreshV2AllSchema(s, context.Background()); err != nil {
					t.Fatal(mode, err)
				}
				mock.mu.Lock()
				queries := append([]queryRecord(nil), mock.queries...)
				mock.mu.Unlock()
				if mode == settings.MetadataDisabled && len(queries) != 0 {
					t.Fatal("disabled schema issued optional queries", queries)
				}
				for _, q := range queries {
					if strings.Contains(q.stmt, "'ks2'") {
						t.Fatal("filtered keyspace queried", q)
					}
				}
				if mode == settings.MetadataMinimal {
					ks, _ := s.Metadata().Keyspace("ks1")
					if ks == nil || len(ks.Tables) != 0 {
						t.Fatal("minimal metadata expanded schema")
					}
				}
			}
		})

		t.Run("PartialBaseReadRetainsSnapshot", func(t *testing.T) {
			s, mock := refreshAllSchemaSession(t)
			mock.mu.Lock()
			mock.queryError = errors.New("base schema denied temporarily")
			mock.mu.Unlock()
			err := implCluster.InternalSessionRefreshV2Keyspace(s, context.Background(), "ks1")
			if _, ok := errors.AsType[*implMetadata.PartialRefreshError](err); !ok {
				t.Fatal("retained stale base schema lacks partial error", err)
			}
			if ks, _ := s.Metadata().Keyspace("ks1"); ks == nil || ks.Tables["t1"] == nil {
				t.Fatal("base read failure erased valid schema")
			}
			if len(s.Metadata().Diagnostics()) == 0 {
				t.Fatal("stale schema not observable")
			}
		})

		t.Run("TableCannotCrossDropAndFailure", func(t *testing.T) {
			s, mock := refreshAllSchemaSession(t)
			entered, release := make(chan struct{}), make(chan struct{})
			var gate atomic.Bool
			mock.iterFor = func(stmt string) *nativeResultFixture {
				if strings.Contains(stmt, "FROM system_schema.tables") && gate.CompareAndSwap(false, true) {
					close(entered)
					<-release
				}
				return nil
			}
			done := make(chan error, 1)
			go func() {
				done <- implCluster.SessionData(s).InternalMetadataDescriber.InternalRefreshTableSchema("ks1", "t1")
			}()
			<-entered
			implCluster.InternalSessionPublishV2Event(s, &oldevents.SchemaChangeTableEvent{Change: "DROPPED", Keyspace: "ks1", Table: "t1"})
			close(release)
			err := <-done
			if _, ok := errors.AsType[*implMetadata.PartialRefreshError](err); !ok {
				t.Fatal("stale table refresh accepted", err)
			}
			if ks, _ := s.Metadata().Keyspace("ks1"); ks.Tables["t1"] != nil {
				t.Fatal("stale table refresh resurrected table")
			}
			// A later failing keyspace read preserves the latest valid snapshot, including
			// the table tombstone, rather than the pre-drop retained schema.
			mock.mu.Lock()
			mock.queryError = errors.New("temporary failure")
			mock.mu.Unlock()
			_ = implCluster.InternalSessionRefreshV2Keyspace(s, context.Background(), "ks1")
			if ks, _ := s.Metadata().Keyspace("ks1"); ks.Tables["t1"] != nil {
				t.Fatal("failure fallback resurrected table")
			}
		})
	})

	t.Run("Retry", func(t *testing.T) {
		t.Run("RejectedAfterShutdown", func(t *testing.T) {
			s := func() *cluster.Session {
				opaqueStorageValuexxxxxxxxxxxxxxx := new(cluster.Session)
				return opaqueStorageValuexxxxxxxxxxxxxxx
			}()

			implCluster.InternalSessionCloseV2Peripheral(s)
			implCluster.InternalSessionStartV2SchemaRetry(s)
			if implCluster.InternalSessionPeripheral(s).InternalRetryScheduled.Load() {
				t.Fatal("denied worker retained a retry registration")
			}
			s.Close()
		})
	})
}

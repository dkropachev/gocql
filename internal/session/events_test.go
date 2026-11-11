/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package session

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestEvents(t *testing.T) {
	t.Run("Budget", func(t *testing.T) {
		t.Run("ChargeTransfersUntilConsumption", func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var freed atomic.Int32
			downstream := cluster.InternalNewEventDebouncer("owned", func([]protocol.Frame) { close(entered); <-release }, logging.InternalNopLogger{})
			conn, _ := typedTestConn(t, 4, nil)
			s := func() *cluster.Core {
				opaqueStorageValuexxxxxxxx := new(cluster.Core)
				implCluster.SessionData(opaqueStorageValuexxxxxxxx).InternalLogger = logging.InternalNopLogger{}
				implCluster.SessionData(opaqueStorageValuexxxxxxxx).InternalSchemaEvents = downstream
				return opaqueStorageValuexxxxxxxx
			}()
			conn.InternalHooks = s.NewConnectionHooks()
			if err := conn.InternalDispatchEventOwned(&frm.SchemaChangeTable{Change: "CREATED", Keyspace: "ks", Object: "t"}, func() { freed.Add(1) }); err != nil {
				t.Fatal(err)
			}
			<-entered
			if freed.Load() != 0 {
				t.Fatal("connection dispatcher released retained downstream event")
			}
			if !downstream.InternalDebounceOwned(&frm.SchemaChangeTable{}, func() { freed.Add(1) }) {
				t.Fatal("second owned event rejected")
			}
			closed := make(chan struct{})
			go func() { s.Close(); close(closed) }()
			select {
			case <-closed:
				t.Fatal("shutdown did not join stalled downstream consumer")
			default:
			}
			close(release)
			<-closed
			conn.InternalWaitEventDispatch()
			if freed.Load() != 2 {
				t.Fatal("downstream consumption/terminal drain did not release exactly once", freed.Load())
			}
			s.Close()
			if freed.Load() != 2 {
				t.Fatal("repeated closure released event twice")
			}
		})

		t.Run("MetadataAndTerminalAdmission", func(t *testing.T) {
			c, _ := typedTestConn(t, 4, nil)
			timer := time.NewTimer(time.Hour)
			defer timer.Stop()
			consumer := &cluster.InternalEventDebouncer{InternalTimer: timer, InternalLogger: logging.InternalNopLogger{}}
			consumer.InternalMu.Lock()
			session := func() *cluster.Core {
				opaqueStorageValuexxxxxxx := new(cluster.Core)
				implCluster.SessionData(opaqueStorageValuexxxxxxx).InternalCtx = c.InternalCtx
				implCluster.SessionData(opaqueStorageValuexxxxxxx).InternalLogger = logging.InternalNopLogger{}
				implCluster.SessionData(opaqueStorageValuexxxxxxx).InternalNodeEvents = consumer
				return opaqueStorageValuexxxxxxx
			}()
			c.InternalHooks = session.NewConnectionHooks()
			budget := &protocol.StreamMetadataBudget{Limit: 800}
			if err := budget.Reserve(400); err != nil {
				t.Fatal(err)
			}
			e := protocol.NewTypedEnvelope()
			e.Str("STATUS_CHANGE", false)
			e.Str("DOWN", false)
			e.U8(4)
			e.Chain(typedTestChain(t, []byte{127, 0, 0, 2}))
			e.U32(9042)
			body := typedTestBytes(t, codecTestMust(e.Freeze()))
			queued := 0
			for i := 0; i < 20; i++ {
				scope := codecTestMust(wire.NewStream(context.Background(), bytes.NewReader(body), int64(len(body)), wire.StreamLimits{}))
				cursor := &protocol.StreamMetadataCursor{Scope: scope, Budget: budget}
				p := &protocol.TypedParser{Cursor: cursor, Proto: 4}
				event := p.StreamEvent(frm.FrameHeader{Version: 0x84, Stream: -1, Op: frm.OpEvent})
				if p.Err != nil {
					cursor.Release()
					break
				}
				if err := scope.RequireEnd(); err != nil {
					t.Fatal(err)
				}
				if err := c.InternalDispatchEventOwned(event, cursor.Release); err != nil {
					t.Fatal(err)
				}
				queued++
			}
			if queued == 0 || queued > 3 || budget.Bytes() > 800 {
				t.Fatal("queued events escaped simultaneous budget", queued, budget.Bytes())
			}
			c.InternalCancel()
			consumer.InternalMu.Unlock()
			c.InternalWaitEventDispatch()
			budget.Release(400)
			if budget.Bytes() != 0 {
				t.Fatal("close did not release queued charges", budget.Bytes())
			}
			var released atomic.Int32
			if err := c.InternalDispatchEventOwned(&frm.ClientRoutesChanged{}, func() { released.Add(1) }); !errors.Is(err, runtimeerrors.ErrClosed) || released.Load() != 1 {
				t.Fatal("terminal enqueue retained ownership", err, released.Load())
			}
			// Event semantic consumption rejects bytes after a fully parsed event.
			trailing := append(append([]byte(nil), body...), 99)
			scope := codecTestMust(wire.NewStream(context.Background(), bytes.NewReader(trailing), int64(len(trailing)), wire.StreamLimits{}))
			cursor := &protocol.StreamMetadataCursor{Scope: scope, Budget: &protocol.StreamMetadataBudget{Limit: 4096}}
			p := &protocol.TypedParser{Cursor: cursor, Proto: 4}
			p.StreamEvent(frm.FrameHeader{})
			if p.Err != nil || scope.RequireEnd() == nil {
				t.Fatal("trailing event bytes accepted", p.Err)
			}
			cursor.Release()
		})
	})

	t.Run("Debouncer", func(t *testing.T) {
		t.Run("SerialBoundedAndJoined", func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var callbacks atomic.Int32
			var resync atomic.Int32
			d := cluster.InternalNewEventDebouncer("bounded", func([]protocol.Frame) {
				if callbacks.Add(1) == 1 {
					close(entered)
					<-release
				}
			}, logging.InternalNopLogger{})
			d.InternalResync = func() { resync.Add(1) }
			for i := 0; i < cluster.InternalEventBufferSize+1; i++ {
				d.InternalDebounce(&frm.StatusChangeEventFrame{})
			}
			d.InternalMu.Lock()
			if len(d.InternalEvents) != cluster.InternalEventBufferSize || !d.InternalOverflow {
				t.Fatal("unbounded queue or lost overflow")
			}
			d.InternalTimer.Reset(0)
			d.InternalMu.Unlock()
			<-entered
			stopped := make(chan struct{})
			go func() { d.InternalStop(); close(stopped) }()
			select {
			case <-stopped:
				t.Fatal("stop did not join callback")
			default:
			}
			close(release)
			<-stopped
			if callbacks.Load() != 1 {
				t.Fatal("callbacks overlapped")
			}
			d.InternalStop()
		})
	})

	t.Run("Overflow", func(t *testing.T) {
		t.Run("HighestGenerationAndIsolation", func(t *testing.T) {
			s := func() *cluster.Core {
				opaqueStorageValue := new(cluster.Core)
				return opaqueStorageValue
			}()

			defer s.Close()
			slow, _ := s.Subscribe(cluster.EventFilter{Capacity: 2})
			fast, _ := s.Subscribe(cluster.EventFilter{Capacity: 8})
			defer fast.Close()
			for _, generation := range []uint64{40, 99, 41} {
				s.PublishV2LocalEvent(cluster.Event{Kind: cluster.StatusEvent, Generation: generation})
			}
			marker, ok := <-slow.Events()
			if !ok || marker.Kind != cluster.OverflowEvent || marker.Generation != 99 {
				t.Fatal(marker, ok)
			}
			if _, ok := <-slow.Events(); ok {
				t.Fatal("overflow marker was not terminal")
			}
			for _, generation := range []uint64{40, 99, 41} {
				if e := <-fast.Events(); e.Kind != cluster.StatusEvent || e.Generation != generation {
					t.Fatal("one slow subscriber affected another", e)
				}
			}
			if len(s.Peripheral().InternalSubscribers) != 1 {
				t.Fatal("terminal subscriber leaked in registry")
			}
		})
	})

	t.Run("Register", func(t *testing.T) {
		t.Run("DisabledDoNotMutateMetadata", func(t *testing.T) {
			s := func() *cluster.Core {
				opaqueStorageValuexxxxxxx := new(cluster.Core)
				return opaqueStorageValuexxxxxxx
			}()

			defer s.Close()
			implCluster.SessionData(s).InternalCfg.Events.DisableSchemaEvents = true
			implCluster.SessionData(s).InternalCfg.Events.DisableTopologyEvents = true
			implCluster.SessionData(s).InternalCfg.Events.DisableNodeStatusEvents = true
			(&implCluster.SessionData(s).InternalCfg).Data().InternalDisableClientRouteEvents = true
			before := s.Metadata()
			for _, event := range []protocol.Frame{&frm.SchemaChangeTable{Change: "DROPPED", Keyspace: "ks", Object: "t"}, &frm.TopologyChangeEventFrame{}, &frm.StatusChangeEventFrame{}, &frm.ClientRoutesChanged{}} {
				s.HandleEvent(event)
			}
			if s.Metadata() != before {
				t.Fatal("disabled unsolicited event changed snapshot")
			}
		})

		t.Run("FlagsAndIdentityReachStartup", func(t *testing.T) {
			for _, enabled := range []string{"none", "topology", "status", "schema", "routes"} {
				t.Run(enabled, func(t *testing.T) {
					srv := newControlTestServer(t, func(srv *controlTestServer) {
						srv.queries = func(query string, stream int, response *protocol.Framer) bool {
							if !strings.Contains(query, "system.client_routes") {
								return false
							}
							columns := []rowsCol{{"connection_id", optNative(cqltype.TypeVarchar)}, {"host_id", optNative(cqltype.TypeUUID)}, {"address", optNative(cqltype.TypeVarchar)}, {"port", optNative(cqltype.TypeInt)}, {"tls_port", optNative(cqltype.TypeInt)}}
							writeRows(response, stream, columns, [][][]byte{{[]byte("route"), testLocalHostID[:], []byte("127.0.0.1"), binary.BigEndian.AppendUint32(nil, uint32(srv.port())), nil}})
							return true
						}
					})
					var starts atomic.Int32
					srv.setAuthHook(func(request *protocol.Framer, stream int, response *protocol.Framer) {
						actual := readStartupOptions(t, request)
						for key, want := range map[string]string{"DRIVER_NAME": "custom-driver", "DRIVER_VERSION": "17.2", "APPLICATION_NAME": "release-probe", "APPLICATION_VERSION": "17.3", "CLIENT_ID": "caller-id"} {
							if actual[key] != want {
								t.Errorf("STARTUP %s=%q want%q", key, actual[key], want)
							}
						}
						starts.Add(1)
						response.WriteHeader(0, frm.OpReady, stream)
					})
					config := settings.DefaultConfig(srv.Address)
					config.Protocol = 4
					config.KeepaliveInterval = 0
					config.Metadata.InitialDiscovery = false
					config.Metadata.Mode = settings.MetadataDisabled
					config.Metadata.PeriodicRefresh = 0
					config.ReportConfiguration = false
					config.Pool.ConnectionsPerHost = 1
					config.Pool.ShardAwarePort = false
					config.Metadata.TopologyEvents = enabled == "topology"
					config.Metadata.StatusEvents = enabled == "status"
					config.Metadata.SchemaEvents = enabled == "schema"
					config.Metadata.ClientRouteEvents = enabled == "routes"
					config.ClientRoutes = &settings.ClientRoutesOptions{ConnectionIDs: []string{"route"}, HostnameOverrides: map[string]string{"route": "127.0.0.1"}}
					config.Identity = settings.IdentityOptions{DriverName: "custom-driver", DriverVersion: "17.2", ApplicationName: "release-probe", ApplicationVersion: "17.3", ClientID: "caller-id"}
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					defer cancel()
					session, err := Connect(ctx, config)
					if err != nil {
						t.Fatal(err)
					}
					if err = session.Close(); err != nil {
						t.Fatal(err)
					}
					if starts.Load() == 0 {
						t.Fatal("no actual STARTUP observed")
					}
					registrations := srv.registered()
					if enabled == "none" {
						if len(registrations) != 0 {
							t.Fatal("disabled event flags sent REGISTER", registrations)
						}
						return
					}
					want := map[string]string{"topology": "TOPOLOGY_CHANGE", "status": "STATUS_CHANGE", "schema": "SCHEMA_CHANGE", "routes": "CLIENT_ROUTES_CHANGE"}[enabled]
					if len(registrations) == 0 {
						t.Fatal("enabled event omitted REGISTER")
					}
					for _, events := range registrations {
						if !slices.Equal(events, []string{want}) {
							t.Fatal("wrong native REGISTER events", events, want)
						}
					}
				})
			}
		})
	})

	t.Run("Shutdown", func(t *testing.T) {
		t.Run("JoinsWorkersAndRepeatedClosers", func(t *testing.T) {
			s := func() *cluster.Core {
				opaqueStorageValuex := new(cluster.Core)
				return opaqueStorageValuex
			}()

			p := s.Peripheral()
			canceled, release := make(chan struct{}), make(chan struct{})
			p.InternalStartWorker(func() { <-p.InternalDone; close(canceled); <-release })
			first, second := make(chan struct{}), make(chan struct{})
			go func() { s.Close(); close(first) }()
			<-canceled
			go func() { s.Close(); close(second) }()
			select {
			case <-first:
				t.Fatal("Close returned before admitted worker finished")
			default:
			}
			select {
			case <-second:
				t.Fatal("repeated Close did not join same shutdown")
			default:
			}
			close(release)
			<-first
			<-second
			if p.InternalStartWorker(func() { t.Error("worker admitted after close") }) {
				t.Fatal("worker admitted after close")
			}
		})
	})
}

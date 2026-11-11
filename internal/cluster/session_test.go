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
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"log/slog"

	"github.com/scylladb/gocql/v2/events"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/eventbus"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/pool"
	implPool "github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/protocol"
	implProtocol "github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func TestSession(t *testing.T) {
	t.Run("EventBus", func(t *testing.T) {
		s := func() *Core {
			opaqueStorageValue := new(Core)
			SessionData(opaqueStorageValue).eventBus = eventbus.New[events.Event](eventbus.EventBusConfig{
				InputEventsQueueSize: 1,
			}, nil)
			SessionData(opaqueStorageValue).InternalLogger = logging.Nop()
			return opaqueStorageValue
		}()

		if err := SessionData(s).eventBus.Start(); err != nil {
			t.Fatalf("starting event bus: %v", err)
		}
		defer SessionData(s).eventBus.Stop()

		sub := sessionSubscribeToEvents(s, "test", 1, nil)
		defer sub.Stop()

		ev := &events.StatusChangeEvent{
			Change: "UP",
			Host:   net.ParseIP("127.0.0.1"),
			Port:   9042,
		}
		sessionPublishEvent(s, ev)

		select {
		case received := <-sub.Events():
			if received != ev {
				t.Fatalf("unexpected event pointer: got %p want %p", received, ev)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for event")
		}
	})

	// TestSession/GetHostPoolByID pins that GetHostPoolByID hands back a nil
	// interface rather than a typed nil.
	//
	// getPoolByHostID reports absence through its ok result, not through the
	// pointer, so returning that pointer unconditionally boxes a nil
	// *hostConnPool into a non-nil HostPoolInfo. A caller guarding with
	// `if pool != nil` would then get past the guard and panic on the first
	// method call -- the failure lands at the call site, far from the cause.
	t.Run("GetHostPoolByID", func(t *testing.T) {
		t.Parallel()

		session := func() *Core {
			opaqueStorageValuexxxxxx := new(Core)
			SessionData(opaqueStorageValuexxxxxx).InternalLogger = logging.Nop()
			return opaqueStorageValuexxxxxx
		}()
		SessionData(session).InternalPool = &implPool.InternalPolicyConnPool{InternalServices: session.NewPoolServices(), InternalHostConnPools: map[cqltype.UUID]*implPool.InternalHostConnPool{}}

		present := func() *endpoint.HostInfo {
			opaqueStorageValuexxxxxxx := new(endpoint.HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValuexxxxxxx).InternalHostId = cqltype.MustRandomUUID()
			implEndpoint.HostInfoData(opaqueStorageValuexxxxxxx).InternalConnectAddress = net.ParseIP("127.0.40.1")
			return opaqueStorageValuexxxxxxx
		}()
		SessionData(session).InternalPool.InternalHostConnPools[implEndpoint.InternalHostInfoHostUUID(present)] = session.NewHostConnPool(present, 1, "")

		t.Run("present id returns the pool", func(t *testing.T) {
			info := sessionGetHostPoolByID(session, present.HostID())
			if info == nil {
				t.Fatal("pool for a known host was not returned")
			}
			if got := info.Host().ConnectAddress().String(); got != "127.0.40.1" {
				t.Errorf("pool belongs to %s, want 127.0.40.1", got)
			}
		})

		for _, tc := range []struct {
			name   string
			hostID string
		}{
			{"absent id", cqltype.MustRandomUUID().String()},
			{"malformed id", "not-a-uuid"},
			{"empty id", ""},
		} {
			t.Run(tc.name, func(t *testing.T) {
				info := sessionGetHostPoolByID(session, tc.hostID)
				if info != nil {
					// Not t.Fatal: calling through the interface is the point, and
					// it is what would panic before the fix.
					t.Errorf("got non-nil HostPoolInfo (%T) for %s", info, tc.name)
					t.Errorf("connection count reads %d", info.GetConnectionCount())
				}
			})
		}
	})

	// TestSession/IteratePool covers policyConnPool.iteratePool, the engine
	// behind the public Session.IterateHostPools. It had no coverage in either
	// lane.
	//
	// The behaviour worth pinning is the early exit: iteratePool holds a read lock
	// across the whole walk and breaks when the callback returns false, so a caller
	// can stop after finding what it wants. A refactor that dropped the break would
	// still pass a "visits everything" test.
	t.Run("IteratePool", func(t *testing.T) {
		t.Parallel()

		newPool := func(t *testing.T, ips ...string) *implPool.InternalPolicyConnPool {
			t.Helper()
			session := func() *Core {
				opaqueStorageValuexxxxx := new(Core)
				SessionData(opaqueStorageValuexxxxx).InternalLogger = logging.Nop()
				return opaqueStorageValuexxxxx
			}()
			p := &implPool.InternalPolicyConnPool{InternalHostConnPools: map[cqltype.UUID]*implPool.InternalHostConnPool{}}
			for _, ip := range ips {
				hp := newUndialedHostPool(t, session, ip)
				p.InternalHostConnPools[implEndpoint.InternalHostInfoHostUUID(hp.InternalHost)] = hp
			}
			return p
		}

		t.Run("visits every pool", func(t *testing.T) {
			t.Parallel()

			p := newPool(t, "127.0.20.1", "127.0.20.2", "127.0.20.3")

			seen := map[string]bool{}
			p.InternalIteratePool(func(info implPool.HostPoolInfo) bool {
				seen[info.Host().ConnectAddress().String()] = true
				return true
			})

			for _, want := range []string{"127.0.20.1", "127.0.20.2", "127.0.20.3"} {
				if !seen[want] {
					t.Errorf("pool for %s was never visited (saw %v)", want, seen)
				}
			}
		})

		t.Run("stops when the callback returns false", func(t *testing.T) {
			t.Parallel()

			p := newPool(t, "127.0.20.1", "127.0.20.2", "127.0.20.3")

			visits := 0
			p.InternalIteratePool(func(implPool.HostPoolInfo) bool {
				visits++
				return false
			})

			if visits != 1 {
				t.Errorf("callback ran %d times after returning false, want 1", visits)
			}
		})

		t.Run("empty pool never invokes the callback", func(t *testing.T) {
			t.Parallel()

			p := newPool(t)

			p.InternalIteratePool(func(implPool.HostPoolInfo) bool {
				t.Error("callback ran for a pool with no hosts")
				return true
			})
		})

		// iter is caller-supplied and runs under the pool's read lock, so it can
		// panic -- or a test callback can t.Fatal, which is a runtime.Goexit.
		// Either skips a bare RUnlock and strands the lock, and because the pool
		// never blocks on itself the symptom surfaces later and elsewhere: the next
		// addHost, removeHost or Close hangs, and the session silently stops
		// reacting to hosts coming up and going down.
		t.Run("a panicking callback does not strand the read lock", func(t *testing.T) {
			t.Parallel()

			p := newPool(t, "127.0.20.1")

			func() {
				defer func() { _ = recover() }()
				p.InternalIteratePool(func(implPool.HostPoolInfo) bool { panic("boom from the callback") })
			}()

			locked := make(chan struct{})
			go func() {
				p.InternalMu.Lock()
				p.InternalMu.Unlock()
				close(locked)
			}()

			select {
			case <-locked:
			case <-time.After(5 * time.Second):
				t.Fatal("pool is wedged: the read lock outlived a panic in the callback")
			}
		})
	})

	t.Run("Metrics", func(t *testing.T) {
		s := func() *Core {
			opaqueStorageValuexx := new(Core)
			return opaqueStorageValuexx
		}()

		for i := 1; i <= 100; i++ {
			s.RecordV2Latency(time.Duration(i) * time.Millisecond)
		}
		sessionRecordV2Orphan(s)
		s.Peripheral().publishLatencySummary()
		m := s.Metrics()
		if m.LatencyP50 != 50*time.Millisecond || m.LatencyP95 != 95*time.Millisecond || m.LatencyP99 != 99*time.Millisecond || m.Orphans != 1 {
			t.Fatal(m)
		}
		generation := sessionTypedSchemaGeneration(s)
		s.PublishV2Metadata()
		if sessionTypedSchemaGeneration(s) != generation {
			t.Fatal("topology invalidated prepared schema")
		}
	})

	// TestSession/NewSession covers an InitialReconnectionPolicy
	// that allows no attempts after Validate passed: connectSettings must return an
	// error, not go on without a control connection.
	t.Run("NewSession", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name    string
			allowed int32
			wantErr string
		}{
			// Validate's call passes, Session.init's sees zero.
			{name: "after Validate", allowed: 1, wantErr: "no connection attempted"},
			// Session.init must keep the limit it checked, not ask again.
			{name: "after the check", allowed: 2, wantErr: "no host reachable during protocol discovery: 127.0.0.1:1: "},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				cfg := InternalNewClusterSettings("127.0.0.1")
				cfg.Port = 1 // refused, so an attempt fails fast
				cfg.InitialReconnectionPolicy = &shrinkingRetryPolicy{allowed: tt.allowed}
				cfg.Logger = logging.Nop()

				s, err := connectSettings(*cfg)
				if err == nil {
					s.Close()
					t.Fatal("connectSettings() = nil error, want one")
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("connectSettings() = %v, want %q", err, tt.wantErr)
				}
				if tt.allowed > 1 {
					if _, ok := errors.AsType[*net.OpError](err); !errors.Is(err, ErrNoConnectionsStarted) || !ok {
						t.Fatalf("connectSettings() = %v, want ErrNoConnectionsStarted wrapping a dial error", err)
					}
				}
			})
		}
	})

	t.Run("NewSessionCommon", func(t *testing.T) {
		t.Run("LegacyPickerWhenTypedUnset", func(t *testing.T) {
			cfg := InternalNewClusterSettings("127.0.0.1")
			s, err := newSessionCommon(*cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if s.privateStorage.InternalPool.InternalServices.Config != nil {
				t.Fatal("unset configuration became a typed override")
			}
			p := s.privateStorage.InternalPool.InternalServices.NewHostPool(new(endpoint.HostInfo), 2, "")
			defer p.Close()
			if err := p.InternalInitConnPicker(new(transport.Conn)); err != nil {
				t.Fatal(err)
			}
			if _, ok := p.InternalConnPicker.(*pool.InternalDefaultConnPicker); !ok {
				t.Fatalf("unset picker=%T", p.InternalConnPicker)
			}
		})

		t.Run("TypedPoolSnapshot", func(t *testing.T) {
			for _, tc := range []struct {
				name              string
				host, shard, want int
			}{{"default-unset-host", 0, 1, 2}, {"host-override", 3, 1, 3}, {"shard-override", 0, 3, 6}} {
				t.Run(tc.name, func(t *testing.T) {
					typed := settings.DefaultConfig("127.0.0.1")
					typed.Pool.ConnectionsPerHost = tc.host
					typed.Pool.ConnectionsPerShard = tc.shard
					cfg := InternalNewClusterSettings("127.0.0.1")
					var captured *settings.Config
					s, err := newSessionCommonWithRuntime(*cfg, &typed, func(s *Core, c settings.Config) *settings.Config {
						if s.privateStorage.InternalPool != nil || s.privateStorage.eventBus != nil || s.privateStorage.InternalMetadataDescriber != nil {
							t.Fatal("typed settings attached after coordinator publication")
						}
						captured = &c
						return captured
					})
					if err != nil {
						t.Fatal(err)
					}
					defer s.Close()
					physical := s.privateStorage.InternalConnCfg.TypedConfig
					if captured == nil || physical == nil || s.privateStorage.InternalPool.InternalServices.Config != physical || physical.Pool != captured.Pool {
						t.Fatal("transport/pool did not capture the initialized snapshot")
					}
					// Mutating the supplied configuration cannot change the captured counts.
					typed.Pool.ConnectionsPerHost = 99
					typed.Pool.ConnectionsPerShard = 99
					features := endpoint.ScyllaConnectionFeatures{}
					endpoint.ScyllaHostFeaturesData(&features.ScyllaHostFeatures).InternalNrShards = 2
					endpoint.ScyllaHostFeaturesData(&features.ScyllaHostFeatures).InternalMsbIgnore = 12
					conn := &transport.Conn{InternalScyllaSupported: features}
					p := s.privateStorage.InternalPool.InternalServices.NewHostPool(new(endpoint.HostInfo), 17, "")
					defer p.Close()
					if err := p.InternalInitConnPicker(conn); err != nil {
						t.Fatal(err)
					}
					picker, ok := p.InternalConnPicker.(*pool.InternalTypedConnPicker)
					if !ok {
						t.Fatalf("typed bootstrap selected %T", p.InternalConnPicker)
					}
					if got := picker.InternalTarget(); got != tc.want {
						t.Fatalf("captured target=%d want=%d", got, tc.want)
					}
				})
			}
		})
	})

	t.Run("NewSessionID", func(t *testing.T) {
		logger := slog.Default()

		id1 := newSessionID(logger)
		id2 := newSessionID(logger)

		if id1 == "" || id2 == "" {
			t.Fatal("expected non-empty session ids")
		}
		if id1 == id2 {
			t.Errorf("expected distinct session ids, got %q twice", id1)
		}
	})

	t.Run("ObserveLifecycle", func(t *testing.T) {
		s := func() *Core {
			opaqueStorageValuex := new(Core)
			return opaqueStorageValuex
		}()

		p := s.Peripheral()
		p.InternalConfig.LifecycleObserver = panicLifecycle{}
		sessionObserveV2Lifecycle(s, settings.LifecycleEvent{Kind: "test", At: time.Now()})
		s.CloseV2Peripheral()
	})

	t.Run("ParseProtocolFromError", func(t *testing.T) {
		t.Parallel()

		tests := [...]struct {
			err   error
			proto int
		}{
			{
				err: &transport.InternalProtocolError{
					InternalFrame: &frm.ErrorFrame{
						Code:    0x10,
						Message: "Invalid or unsupported protocol version (5); the lowest supported version is 3 and the greatest is 4",
					},
				},
				proto: protocol.ProtoVersion4,
			},
			{
				err: &transport.InternalProtocolError{
					InternalFrame: func() *frm.ErrorFrame {
						f := frm.NewErrorFrame(frm.FrameHeader{Version: 0x83}, 0x10, "Invalid or unsupported protocol version: 5")
						return &f
					}(),
				},
				proto: 3,
			},
		}

		for i, test := range tests {
			if proto := parseProtocolFromError(test.err); proto != test.proto {
				t.Errorf("%d: exepcted proto %d got %d", i, test.proto, proto)
			}
		}
	})

	// TestSession/PseudoRandomUUID covers the fallback newSessionID uses if RandomUUID
	// ever reports an error. newSessionID cannot reach it on any supported Go
	// version, so the fallback is only exercised here: it must produce distinct,
	// well-formed random UUIDs.
	t.Run("PseudoRandomUUID", func(t *testing.T) {
		first := pseudoRandomUUID()
		second := pseudoRandomUUID()

		if first == second {
			t.Errorf("expected distinct uuids, got %q twice", first)
		}
		for _, u := range []cqltype.UUID{first, second} {
			if got := u.Version(); got != 4 {
				t.Errorf("expected uuid %q to be version 4, got %d", u, got)
			}
			if got := u.Variant(); got != cqltype.VariantIETF {
				t.Errorf("expected uuid %q to use the IETF variant, got %d", u, got)
			}
		}
	})

	t.Run("PublishMetadata", func(t *testing.T) {
		s := func() *Core {
			opaqueStorageValue := new(Core)
			return opaqueStorageValue
		}()

		first := s.Metadata()
		s.PublishV2Metadata()
		second := s.Metadata()
		if first == second || second.Generation() <= first.Generation() {
			t.Fatal("publication did not advance")
		}
		s.CloseV2Peripheral()
		if first.Generation() == 0 {
			t.Fatal("retained snapshot invalidated")
		}
	})

	t.Run("ResolveInitialEndpoint", func(t *testing.T) {
		t.Parallel()

		resolver := endpoint.NewSimpleDNSResolver(true)

		tests := [...]struct {
			addr string
			ip   net.IP
		}{
			{"127.0.0.1", net.IPv4(127, 0, 0, 1)},
			{"localhost", net.IPv4(127, 0, 0, 1)}, // TODO: this may be host dependant
		}

		for i, test := range tests {
			hosts, err := endpoint.InternalResolveInitialEndpoint(resolver, test.addr, 1)
			if err != nil {
				t.Errorf("%d: %v", i, err)
				continue
			}

			host := hosts[0]
			if !host.ConnectAddress().Equal(test.ip) {
				t.Errorf("expected ip %v got %v for addr %q", test.ip, host.ConnectAddress(), test.addr)
			}
		}
	})

	// TestSession/StartupSessionID checks what actually reaches the wire:
	// every connection of a session shares the same SESSION_ID, distinct
	// sessions get distinct ones, and every STARTUP frame carries one.
	t.Run("StartupSessionID", func(t *testing.T) {
		// Enough connections per session that "they all agree" is a claim about a
		// set rather than about a single sample. The default NumConns of 2 would
		// technically do, but only the first connection is opened during session
		// initialization, so a test that did not wait would routinely observe just
		// that one and assert nothing.
		const connsPerSession = 3

		tests := []struct {
			name             string
			sessionCount     int
			expectedSessions int
		}{
			{name: "single session", sessionCount: 1, expectedSessions: 1},
			{name: "multiple sessions", sessionCount: 2, expectedSessions: 2},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				var (
					mu         sync.Mutex
					sessionIDs = map[string]int{}
					startups   int
				)

				srv := newTestServerOpts{
					addr:     "127.0.0.1:0",
					protocol: implProtocol.ProtoVersion4,
					recvHook: func(f *implProtocol.Framer) {
						if f.Header.Op != frm.OpStartup {
							return
						}
						// Consuming the frame body here is only safe because the fake
						// server does not read the body of a STARTUP request.
						opts := readStartupOptions(t, f)

						mu.Lock()
						defer mu.Unlock()
						startups++
						if id, ok := opts[transport.InternalSessionIDStartupKey]; ok {
							sessionIDs[id]++
						}
					},
				}.newServer(t, ctx)
				defer srv.Stop()

				sessions := make([]*Core, 0, tt.sessionCount)
				for i := 0; i < tt.sessionCount; i++ {
					cluster := testCluster(implProtocol.ProtoVersion4, srv.Address)
					cluster.NumConns = connsPerSession
					session, err := cluster.Open()
					if err != nil {
						t.Fatal(err)
					}
					defer session.Close()
					sessions = append(sessions, session)
				}

				// open only guarantees one connection per host: the rest of
				// the pool is filled asynchronously when NumConns > 1, so without
				// this wait the snapshot below would capture an arbitrary prefix of
				// the STARTUP frames. Waiting on the pool is what makes the counts
				// deterministic, and a connection only joins the pool once its
				// handshake has completed, which is strictly after the server has
				// run recvHook on its STARTUP.
				for i, session := range sessions {
					if err := waitForPoolSize(session, connsPerSession, 10*time.Second); err != nil {
						t.Fatalf("session %d: %s", i, err)
					}
				}

				mu.Lock()
				defer mu.Unlock()

				if len(sessionIDs) != tt.expectedSessions {
					t.Errorf("expected %d distinct SESSION_ID, got %d: %v", tt.expectedSessions, len(sessionIDs), sessionIDs)
				}

				// Every connection of a session, not merely the first, must report
				// the id that session's own ID reports. Keying by ID rather than
				// iterating the observed ids also pins the two ends of the
				// correlation together: an accessor that returned anything other
				// than the value on the wire would leave an application unable to
				// match its logs to a system.clients row, which is the reason the
				// id exists. Without the count check the test would pass on a
				// driver that generated a fresh id per connection, as long as only
				// one connection per session had been observed.
				for i, session := range sessions {
					if count := sessionIDs[session.ID()]; count < connsPerSession {
						t.Errorf("session %d: expected SESSION_ID %q to be reported by all %d of its connections, got %d: %v",
							i, session.ID(), connsPerSession, count, sessionIDs)
					}
				}

				// Every STARTUP frame must carry a SESSION_ID: summing the
				// per-id counts and comparing against the unconditional STARTUP
				// count catches a frame that omits SESSION_ID entirely, which
				// indexing sessionIDs by the (possibly absent) id would hide.
				var reported int
				for _, count := range sessionIDs {
					reported += count
				}
				if reported != startups {
					t.Errorf("expected every STARTUP frame to carry SESSION_ID: got %d STARTUP frames but only %d reported one", startups, reported)
				}
			})
		}
	})
}

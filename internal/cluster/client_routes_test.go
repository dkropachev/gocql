/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"log/slog"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/eventbus"
	"github.com/scylladb/gocql/v2/internal/events"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/tests"
)

func TestClientRoutes(t *testing.T) {
	t.Run("GetHostPortMapping", func(t *testing.T) {
		t.Run("ConnectionIDsQuery", func(t *testing.T) {
			tcases := []struct {
				name          string
				connectionIDs []string
				expectedStmt  string
				expectedVals  []any
				expectedErr   bool
			}{
				{
					name:          "multiple-connections",
					connectionIDs: []string{"c1", "c2"},
					expectedStmt:  "select connection_id, host_id, address, port, tls_port from system.client_routes where connection_id in ?",
					expectedVals:  []any{[]string{"c1", "c2"}},
				},
				{
					name:          "single-connection",
					connectionIDs: []string{"c1"},
					expectedStmt:  "select connection_id, host_id, address, port, tls_port from system.client_routes where connection_id in ?",
					expectedVals:  []any{[]string{"c1"}},
				},
				{
					name:        "empty-slices",
					expectedErr: true,
				},
			}

			for _, tc := range tcases {
				t.Run(tc.name, func(t *testing.T) {
					ctrl := &fakeControlConn{}
					_, err := InternalGetHostPortMappingForConnectionIDs(ctrl, "system.client_routes", tc.connectionIDs, false)
					if tc.expectedErr {
						if err == nil {
							t.Fatal("expected error")
						}
						return
					}
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					if ctrl.statement != tc.expectedStmt {
						t.Fatalf("statement mismatch: got %q want %q", ctrl.statement, tc.expectedStmt)
					}
					if fmt.Sprint(ctrl.values) != fmt.Sprint(tc.expectedVals) {
						t.Fatalf("values mismatch: got %v want %v", ctrl.values, tc.expectedVals)
					}
				})
			}
		})

		t.Run("PairsFiltersCrossProductRows", func(t *testing.T) {
			meta := protocol.ResultMetadata{
				Columns: []cqltype.ColumnInfo{
					{Name: "connection_id", TypeInfo: typeVarchar},
					{Name: "host_id", TypeInfo: typeVarchar},
					{Name: "address", TypeInfo: typeVarchar},
					{Name: "port", TypeInfo: typeInt},
					{Name: "tls_port", TypeInfo: typeInt},
				},
				ActualColCount: 5,
				ColCount:       5,
			}

			ctrl := &fakeControlConn{
				iter: makeIter(meta,
					marshalRow(meta, []any{"c1", "h1", "10.0.0.1", 9042, 9142}),
					marshalRow(meta, []any{"c1", "h2", "10.0.0.2", 9043, 9143}),
					marshalRow(meta, []any{"c2", "h1", "10.0.0.3", 9044, 9144}),
					marshalRow(meta, []any{"c2", "h2", "10.0.0.4", 9045, 9145}),
				),
			}

			got, err := InternalGetHostPortMappingForPairs(ctrl, "system.client_routes", []InternalPair{
				{InternalConnectionID: "c1", InternalHostID: "h1"},
				{InternalConnectionID: "c2", InternalHostID: "h2"},
			}, false)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			want := []InternalClientRoute{
				{InternalConnectionID: "c1", InternalHostID: "h1", InternalAddress: "10.0.0.1", InternalPort: 9042},
				{InternalConnectionID: "c2", InternalHostID: "h2", InternalAddress: "10.0.0.4", InternalPort: 9045},
			}
			if len(got) != len(want) {
				t.Fatalf("unexpected row count: got %d want %d", len(got), len(want))
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("row #%d = %+v, want %+v", i, got[i], want[i])
				}
			}
		})

		t.Run("PairsQuery", func(t *testing.T) {
			tcases := []struct {
				name         string
				pairs        []InternalPair
				expectedStmt string
				expectedVals []any
				expectedErr  bool
			}{
				{
					name: "single-pair",
					pairs: []InternalPair{
						{InternalConnectionID: "c1", InternalHostID: "h1"},
					},
					expectedStmt: "select connection_id, host_id, address, port, tls_port from system.client_routes where connection_id in ? and host_id in ?",
					expectedVals: []any{[]string{"c1"}, []string{"h1"}},
				},
				{
					name: "multiple-pairs",
					pairs: []InternalPair{
						{InternalConnectionID: "c1", InternalHostID: "h1"},
						{InternalConnectionID: "c2", InternalHostID: "h2"},
					},
					expectedStmt: "select connection_id, host_id, address, port, tls_port from system.client_routes where connection_id in ? and host_id in ?",
					expectedVals: []any{[]string{"c1", "c2"}, []string{"h1", "h2"}},
				},
				{
					name:        "empty-slices",
					expectedErr: true,
				},
			}

			for _, tc := range tcases {
				t.Run(tc.name, func(t *testing.T) {
					ctrl := &fakeControlConn{}
					_, err := InternalGetHostPortMappingForPairs(ctrl, "system.client_routes", tc.pairs, false)
					if tc.expectedErr {
						if err == nil {
							t.Fatal("expected error")
						}
						return
					}
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					if ctrl.statement != tc.expectedStmt {
						t.Fatalf("statement mismatch: got %q want %q", ctrl.statement, tc.expectedStmt)
					}
					if fmt.Sprint(ctrl.values) != fmt.Sprint(tc.expectedVals) {
						t.Fatalf("values mismatch: got %v want %v", ctrl.values, tc.expectedVals)
					}
				})
			}
		})
	})

	t.Run("Initialize", func(t *testing.T) {
		s := func() *Core {
			opaqueStorageValuexxxxxxxxxxx := new(Core)
			SessionData(opaqueStorageValuexxxxxxxxxxx).InternalControl = &fakeControlConn{}
			SessionData(opaqueStorageValuexxxxxxxxxxx).eventBus = eventbus.New[events.Event](eventbus.EventBusConfig{
				InputEventsQueueSize: 1,
			}, nil)
			SessionData(opaqueStorageValuexxxxxxxxxxx).InternalLogger = logging.Nop()
			return opaqueStorageValuexxxxxxxxxxx
		}()

		if err := SessionData(s).eventBus.Start(); err != nil {
			t.Fatalf("starting event bus: %v", err)
		}
		defer SessionData(s).eventBus.Stop()

		h := NewClientRoutesAddressTranslator(ClientRoutesConfig{
			TableName: "system.client_routes",
			Endpoints: ClientRoutesEndpointList{{ConnectionID: "c1"}},
		}, nil, false, logging.Nop())
		defer h.Stop()

		if err := h.Initialize(s); err != nil {
			t.Fatalf("first Initialize call failed: %v", err)
		}
		if !ClientRoutesHandlerData(h).initialized.Load() {
			t.Fatal("expected handler to be marked initialized")
		}

		if err := h.Initialize(s); err == nil {
			t.Fatal("expected second Initialize call to fail")
		}
	})

	t.Run("NewSessionCommon", func(t *testing.T) {
		t.Parallel()

		testCases := []struct {
			name                  string
			clientRoutes          *ClientRoutesConfig
			disableShardAwarePort bool
			wantDisabled          bool
		}{
			{
				name:         "no client routes",
				wantDisabled: false,
			},
			{
				name: "client routes disable shard awareness by default",
				clientRoutes: &ClientRoutesConfig{
					Endpoints: ClientRoutesEndpointList{{ConnectionID: "connection-id"}},
				},
				wantDisabled: true,
			},
			{
				name: "client routes explicitly enable shard awareness",
				clientRoutes: &ClientRoutesConfig{
					Endpoints:            ClientRoutesEndpointList{{ConnectionID: "connection-id"}},
					EnableShardAwareness: true,
				},
				wantDisabled: false,
			},
			{
				name: "cluster-wide disable overrides client routes enable",
				clientRoutes: &ClientRoutesConfig{
					Endpoints:            ClientRoutesEndpointList{{ConnectionID: "connection-id"}},
					EnableShardAwareness: true,
				},
				disableShardAwarePort: true,
				wantDisabled:          true,
			},
		}

		for _, tt := range testCases {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				cfg := InternalNewClusterSettings("127.0.0.1")
				cfg.ClientRoutesConfig = tt.clientRoutes
				cfg.DisableShardAwarePort = tt.disableShardAwarePort
				session, err := newSessionCommon(*cfg)
				if err != nil {
					t.Fatalf("new session: %v", err)
				}
				t.Cleanup(func() { _ = session.Close() })

				tests.AssertEqual(t, "disable shard-aware port", tt.wantDisabled, SessionData(session).InternalCfg.DisableShardAwarePort)
			})
		}
	})

	t.Run("StartReadingEvents", func(t *testing.T) {
		t.Run("ConnectionIDsOnlySchedulesRefresh", func(t *testing.T) {
			h, bus := newClientRoutesEventHarness(t, []string{"c1"})

			bus.PublishEventBlocking(&events.ClientRoutesChangedEvent{
				ChangeType:    "UPDATED",
				ConnectionIDs: []string{"c1"},
				HostIDs:       []string{},
			})

			select {
			case task := <-ClientRoutesHandlerData(h).InternalUpdateTasks:
				if len(task.InternalConnectionIDs) != 1 || task.InternalConnectionIDs[0] != "c1" {
					t.Fatalf("unexpected connection IDs: %v", task.InternalConnectionIDs)
				}
				if task.InternalPairs != nil {
					t.Fatalf("expected connectionIDs-only refresh, got pairs: %v", task.InternalPairs)
				}
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for refresh task")
			}
		})

		t.Run("UsesCartesianPairs", func(t *testing.T) {
			h, bus := newClientRoutesEventHarness(t, []string{"c1", "c2"})

			bus.PublishEventBlocking(&events.ClientRoutesChangedEvent{
				ChangeType:    "UPDATED",
				ConnectionIDs: []string{"c1", "c2"},
				HostIDs:       []string{"h1", "h2"},
			})

			select {
			case task := <-ClientRoutesHandlerData(h).InternalUpdateTasks:
				want := []InternalPair{
					{InternalConnectionID: "c1", InternalHostID: "h1"},
					{InternalConnectionID: "c1", InternalHostID: "h2"},
					{InternalConnectionID: "c2", InternalHostID: "h1"},
					{InternalConnectionID: "c2", InternalHostID: "h2"},
				}
				if len(task.InternalPairs) != len(want) {
					t.Fatalf("unexpected pair count: got %d want %d", len(task.InternalPairs), len(want))
				}
				for i := range want {
					if task.InternalPairs[i] != want[i] {
						t.Fatalf("pair #%d = %+v, want %+v", i, task.InternalPairs[i], want[i])
					}
				}
				if task.InternalConnectionIDs != nil {
					t.Fatalf("expected pairs-only refresh, got connection IDs: %v", task.InternalConnectionIDs)
				}
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for refresh task")
			}
		})
	})

	t.Run("Stop", func(t *testing.T) {
		h := func() *ClientRoutesHandler {
			opaqueStorageValuexxxxxxxxx := new(ClientRoutesHandler)
			ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxx).log = slog.Default()
			ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxx).c = &fakeControlConn{}
			ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxx).InternalCloseChan = make(chan struct{})
			ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxx).InternalUpdateTasks = make(chan InternalUpdateTask, 1024)
			ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxx).InternalRouteCache = newClientRouteCache()
			ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxx).cfg = ClientRoutesConfig{
				TableName: "system.client_routes",
				Endpoints: ClientRoutesEndpointList{{ConnectionID: "c1"}},
			}
			return opaqueStorageValuexxxxxxxxx
		}()

		h.StartUpdateWorker()

		const goroutines = 200
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				h.UpdateHostPortMappingAsync(InternalUpdateTask{InternalConnectionIDs: []string{"c1"}})
			}()
		}
		close(start) // release all goroutines simultaneously
		h.Stop()     // race against the concurrent sends — must not panic
		wg.Wait()
	})

	t.Run("Translate", func(t *testing.T) {
		t.Run("ConnectionAddrOverride", func(t *testing.T) {
			addr := netip.MustParseAddrPort("1.1.1.1:9042")

			resolvedIPs := map[string]net.IP{
				"route-addr":    net.ParseIP("10.0.0.1"),
				"override-addr": net.ParseIP("10.0.0.99"),
			}
			resolver := dnsResolverFunc(func(host string) ([]net.IP, error) {
				if ip, ok := resolvedIPs[host]; ok {
					return []net.IP{ip}, nil
				}
				return nil, fmt.Errorf("unknown host %s", host)
			})
			newRoutes := func() clientRouteCache {
				return newCache(map[string]clientRouteCacheEntry{hostH1: newCacheEntry(InternalClientRoute{InternalConnectionID: "c1", InternalHostID: hostH1, InternalAddress: "route-addr", InternalPort: 9042})})
			}

			t.Run("no override uses route address", func(t *testing.T) {
				handler := func() *ClientRoutesHandler {
					opaqueStorageValuexxxxxxx := new(ClientRoutesHandler)
					ClientRoutesHandlerData(opaqueStorageValuexxxxxxx).addrOverrides = map[string]string{}
					ClientRoutesHandlerData(opaqueStorageValuexxxxxxx).resolver = resolver
					ClientRoutesHandlerData(opaqueStorageValuexxxxxxx).InternalRouteCache = newRoutes()
					return opaqueStorageValuexxxxxxx
				}()

				res, err := handler.Translate(context.Background(), routePeer(hostH1), addr)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !sameIP(res, resolvedIPs["route-addr"]) {
					t.Fatalf("expected route-addr IP %v, got %v", resolvedIPs["route-addr"], res.Addr())
				}
			})

			t.Run("override replaces route address", func(t *testing.T) {
				handler := func() *ClientRoutesHandler {
					opaqueStorageValuexxxxxxxx := new(ClientRoutesHandler)
					ClientRoutesHandlerData(opaqueStorageValuexxxxxxxx).addrOverrides = map[string]string{"c1": "override-addr"}
					ClientRoutesHandlerData(opaqueStorageValuexxxxxxxx).resolver = resolver
					ClientRoutesHandlerData(opaqueStorageValuexxxxxxxx).InternalRouteCache = newRoutes()
					return opaqueStorageValuexxxxxxxx
				}()

				res, err := handler.Translate(context.Background(), routePeer(hostH1), addr)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !sameIP(res, resolvedIPs["override-addr"]) {
					t.Fatalf("expected override-addr IP %v, got %v", resolvedIPs["override-addr"], res.Addr())
				}
			})
		})

		t.Run("CurrentRoute", func(t *testing.T) {
			addr := netip.MustParseAddrPort("1.1.1.1:9042")
			resolvedIPs := map[string]net.IP{
				"addr-c1": net.ParseIP("10.0.0.1"),
				"addr-c2": net.ParseIP("10.0.0.2"),
			}
			handler := func() *ClientRoutesHandler {
				opaqueStorageValuexxxx := new(ClientRoutesHandler)
				ClientRoutesHandlerData(opaqueStorageValuexxxx).pickTLSPorts = false
				ClientRoutesHandlerData(opaqueStorageValuexxxx).resolver = dnsResolverFunc(func(host string) ([]net.IP, error) {
					if ip, ok := resolvedIPs[host]; ok {
						return []net.IP{ip}, nil
					}
					return nil, fmt.Errorf("unknown host %s", host)
				})
				ClientRoutesHandlerData(opaqueStorageValuexxxx).InternalRouteCache = newCache(map[string]clientRouteCacheEntry{
					hostH1: newCacheEntry(
						InternalClientRoute{InternalConnectionID: "c1", InternalHostID: hostH1, InternalAddress: "addr-c1", InternalPort: 9042},
						InternalClientRoute{InternalConnectionID: "c2", InternalHostID: hostH1, InternalAddress: "addr-c2", InternalPort: 9042},
					),
				})
				return opaqueStorageValuexxxx

				// First call picks the first route in the record.
			}()

			res1, err := handler.Translate(context.Background(), routePeer(hostH1), addr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !sameIP(res1, resolvedIPs["addr-c1"]) {
				t.Fatalf("expected first route to resolve addr-c1, got %v", res1.Addr())
			}
			ClientRoutesHandlerData(handler).InternalRouteCache.ReplaceByConnectionIDs([]string{"c1"}, []InternalClientRoute{
				{InternalConnectionID: "c2", InternalHostID: hostH1, InternalAddress: "addr-c2", InternalPort: 9042},
			})

			res2, err := handler.Translate(context.Background(), routePeer(hostH1), addr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !sameIP(res2, resolvedIPs["addr-c2"]) {
				t.Fatalf("expected fallback route to resolve addr-c2, got %v", res2.Addr())
			}
		})

		t.Run("PreservesCurrentAcrossRefresh", func(t *testing.T) {
			addr := netip.MustParseAddrPort("1.1.1.1:9042")
			resolvedIPs := map[string]net.IP{
				"addr-c1-old": net.ParseIP("10.0.0.1"),
				"addr-c1-new": net.ParseIP("10.0.0.2"),
				"addr-c2":     net.ParseIP("10.0.0.3"),
			}
			resolver := dnsResolverFunc(func(host string) ([]net.IP, error) {
				if ip, ok := resolvedIPs[host]; ok {
					return []net.IP{ip}, nil
				}
				return nil, fmt.Errorf("unknown host %s", host)
			})

			newHandler := func() *ClientRoutesHandler {
				return func() *ClientRoutesHandler {
					opaqueStorageValuexxxxx := new(ClientRoutesHandler)
					ClientRoutesHandlerData(opaqueStorageValuexxxxx).resolver = resolver
					ClientRoutesHandlerData(opaqueStorageValuexxxxx).InternalRouteCache = newCache(map[string]clientRouteCacheEntry{
						hostH1: newCacheEntryWithCurrent(
							InternalClientRoute{InternalConnectionID: "c1", InternalHostID: hostH1, InternalAddress: "addr-c1-old", InternalPort: 9042},
							InternalClientRoute{InternalConnectionID: "c1", InternalHostID: hostH1, InternalAddress: "addr-c1-old", InternalPort: 9042},
							InternalClientRoute{InternalConnectionID: "c2", InternalHostID: hostH1, InternalAddress: "addr-c2", InternalPort: 9042},
						),
					})
					return opaqueStorageValuexxxxx
				}()

			}

			for _, tc := range []struct {
				name    string
				replace func(*clientRouteCache)
			}{
				{
					name: "connection-ids",
					replace: func(c *clientRouteCache) {
						c.ReplaceByConnectionIDs([]string{"c1"}, []InternalClientRoute{
							{InternalConnectionID: "c1", InternalHostID: hostH1, InternalAddress: "addr-c1-new", InternalPort: 9042},
						})
					},
				},
				{
					name: "pairs",
					replace: func(c *clientRouteCache) {
						c.ReplaceByPairs([]InternalPair{{InternalConnectionID: "c1", InternalHostID: hostH1}}, []InternalClientRoute{
							{InternalConnectionID: "c1", InternalHostID: hostH1, InternalAddress: "addr-c1-new", InternalPort: 9042},
						})
					},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					handler := newHandler()
					tc.replace(&ClientRoutesHandlerData(handler).InternalRouteCache)

					res, err := handler.Translate(context.Background(), routePeer(hostH1), addr)
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					if !sameIP(res, resolvedIPs["addr-c1-new"]) {
						t.Fatalf("expected preferred route to stay on c1 with updated address, got %v", res.Addr())
					}
				})
			}
		})

		t.Run("PrunesMissingHostAcrossRefresh", func(t *testing.T) {
			addr := netip.MustParseAddrPort("1.1.1.1:9042")
			resolvedIPs := map[string]net.IP{
				"addr-h1": net.ParseIP("10.0.1.1"),
				"addr-h2": net.ParseIP("10.0.1.2"),
				"addr-h3": net.ParseIP("10.0.1.3"),
			}
			handler := func() *ClientRoutesHandler {
				opaqueStorageValuexxxxxx := new(ClientRoutesHandler)
				ClientRoutesHandlerData(opaqueStorageValuexxxxxx).resolver = dnsResolverFunc(func(host string) ([]net.IP, error) {
					if ip, ok := resolvedIPs[host]; ok {
						return []net.IP{ip}, nil
					}
					return nil, fmt.Errorf("unknown host %s", host)
				})
				ClientRoutesHandlerData(opaqueStorageValuexxxxxx).InternalRouteCache = newCache(map[string]clientRouteCacheEntry{
					hostH1: newCacheEntryWithCurrent(
						InternalClientRoute{InternalConnectionID: "c1", InternalHostID: hostH1, InternalAddress: "addr-h1", InternalPort: 9042},
						InternalClientRoute{InternalConnectionID: "c1", InternalHostID: hostH1, InternalAddress: "addr-h1", InternalPort: 9042},
					),
					hostH2: newCacheEntry(InternalClientRoute{InternalConnectionID: "c1", InternalHostID: hostH2, InternalAddress: "addr-h2", InternalPort: 9042}),
					hostH3: newCacheEntry(InternalClientRoute{InternalConnectionID: "c1", InternalHostID: hostH3, InternalAddress: "addr-h3", InternalPort: 9042}),
				})
				return opaqueStorageValuexxxxxx
			}()

			if _, err := handler.Translate(context.Background(), routePeer(hostH3), addr); err != nil {
				t.Fatalf("unexpected error before refresh: %v", err)
			}
			ClientRoutesHandlerData(handler).InternalRouteCache.ReplaceByConnectionIDs([]string{"c1"}, []InternalClientRoute{
				{InternalConnectionID: "c1", InternalHostID: hostH1, InternalAddress: "addr-h1-new", InternalPort: 9042},
				{InternalConnectionID: "c1", InternalHostID: hostH2, InternalAddress: "addr-h2", InternalPort: 9042},
			})

			if _, err := handler.Translate(context.Background(), routePeer(hostH3), addr); err == nil {
				t.Fatal("expected h3 to be pruned after refresh")
			}
		})

		t.Run("Resolves", func(t *testing.T) {
			addr := netip.MustParseAddrPort("1.1.1.1:9042")
			noHost := routePeer("")
			missingHost := routePeer(hostMissing)

			resolver := dnsResolverFunc(func(host string) ([]net.IP, error) {
				return []net.IP{net.ParseIP("10.0.0.1")}, nil
			})

			handler := func() *ClientRoutesHandler {
				opaqueStorageValuex := new(ClientRoutesHandler)
				ClientRoutesHandlerData(opaqueStorageValuex).resolver = resolver
				ClientRoutesHandlerData(opaqueStorageValuex).InternalRouteCache = newClientRouteCache()
				return opaqueStorageValuex
			}()

			res, err := handler.Translate(context.Background(), noHost, addr)
			if err != nil {
				t.Fatalf("unexpected error for empty hostID: %v", err)
			}
			if res != addr {
				t.Fatalf("expected address to pass through when hostID is empty")
			}

			_, err = handler.Translate(context.Background(), missingHost, addr)
			if err == nil {
				t.Fatalf("expected error for missing host entry")
			}
			ClientRoutesHandlerData(handler).InternalRouteCache = newCache(map[string]clientRouteCacheEntry{
				hostH1: newCacheEntry(InternalClientRoute{InternalConnectionID: "c1", InternalHostID: hostH1, InternalPort: 9042}),
			})

			res, err = handler.Translate(context.Background(), routePeer(hostH1), addr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !sameIP(res, net.ParseIP("10.0.0.1")) {
				t.Fatalf("expected resolved IP 10.0.0.1, got %v", res.Addr())
			}
			if res.Port() != 9042 {
				t.Fatalf("expected port 9042, got %d", res.Port())
			}

			errorHandler := func() *ClientRoutesHandler {
				opaqueStorageValuexx := new(ClientRoutesHandler)
				ClientRoutesHandlerData(opaqueStorageValuexx).resolver = dnsResolverFunc(func(host string) ([]net.IP, error) {
					return nil, errors.New("lookup failed")
				})
				ClientRoutesHandlerData(opaqueStorageValuexx).InternalRouteCache = newCache(map[string]clientRouteCacheEntry{hostH2: newCacheEntry(InternalClientRoute{InternalConnectionID: "c2", InternalHostID: hostH2, InternalAddress: "host", InternalPort: 9042})})
				return opaqueStorageValuexx
			}()

			_, err = errorHandler.Translate(context.Background(), routePeer(hostH2), addr)
			if err == nil {
				t.Fatalf("expected resolver error to bubble up")
			}

			// Route with port 0 should return an error before attempting DNS.
			zeroPortHandler := func() *ClientRoutesHandler {
				opaqueStorageValuexxx := new(ClientRoutesHandler)
				ClientRoutesHandlerData(opaqueStorageValuexxx).resolver = dnsResolverFunc(func(host string) ([]net.IP, error) {
					t.Fatal("DNS should not be called when port is 0")
					return nil, nil
				})
				ClientRoutesHandlerData(opaqueStorageValuexxx).InternalRouteCache = newCache(map[string]clientRouteCacheEntry{hostH3: newCacheEntry(InternalClientRoute{InternalConnectionID: "c3", InternalHostID: hostH3, InternalAddress: "host", InternalPort: 0})})
				return opaqueStorageValuexxx
			}()

			_, err = zeroPortHandler.Translate(context.Background(), routePeer(hostH3), addr)
			if err == nil {
				t.Fatalf("expected error for zero port")
			}
		})

		t.Run("RetainsCurrentRouteAfterQueryError", func(t *testing.T) {
			addr := netip.MustParseAddrPort("1.1.1.1:9042")
			resolvedIPs := map[string]net.IP{
				"a1": net.ParseIP("10.0.0.1"),
			}
			h := func() *ClientRoutesHandler {
				opaqueStorageValuexxxxxxxxxxxx := new(ClientRoutesHandler)
				ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxxxx).log = slog.Default()
				ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxxxx).c = &fakeControlConn{iter: &nativeResultFixture{err: errors.New("query failed")}}
				ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxxxx).InternalCloseChan = make(chan struct{})
				ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxxxx).InternalUpdateTasks = make(chan InternalUpdateTask, 1024)
				ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxxxx).resolver = dnsResolverFunc(func(host string) ([]net.IP, error) {
					if ip, ok := resolvedIPs[host]; ok {
						return []net.IP{ip}, nil
					}
					return nil, fmt.Errorf("unknown host %s", host)
				})
				ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxxxx).InternalRouteCache = newCache(map[string]clientRouteCacheEntry{
					hostH1: newCacheEntryWithCurrent(
						InternalClientRoute{InternalConnectionID: "c1", InternalHostID: hostH1, InternalAddress: "a1", InternalPort: 9042},
						InternalClientRoute{InternalConnectionID: "c1", InternalHostID: hostH1, InternalAddress: "a1", InternalPort: 9042},
					),
				})
				ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxxxx).cfg = ClientRoutesConfig{
					TableName: "system.client_routes",
					Endpoints: ClientRoutesEndpointList{{ConnectionID: "c1"}},
				}
				return opaqueStorageValuexxxxxxxxxxxx
			}()

			h.StartUpdateWorker()
			defer h.Stop()

			before, err := h.Translate(context.Background(), routePeer(hostH1), addr)
			if err != nil {
				t.Fatalf("unexpected error before query failure: %v", err)
			}

			err = clientRoutesHandlerUpdateHostPortMappingSync(h, InternalUpdateTask{InternalConnectionIDs: []string{"c1"}})
			if err == nil {
				t.Fatal("expected query error")
			}

			after, err := h.Translate(context.Background(), routePeer(hostH1), addr)
			if err != nil {
				t.Fatalf("unexpected error after query failure: %v", err)
			}
			if after != before {
				t.Fatalf("expected route to remain stable after query failure, got %v want %v", after, before)
			}
		})
	})

	t.Run("UpdateHostPortMappingSync", func(t *testing.T) {
		h := func() *ClientRoutesHandler {
			opaqueStorageValuexxxxxxxxxx := new(ClientRoutesHandler)
			ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxx).log = slog.Default()
			ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxx).c = &fakeControlConn{}
			ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxx).InternalCloseChan = make(chan struct{})
			ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxx).InternalUpdateTasks = make(chan InternalUpdateTask, 1024)
			ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxx).InternalRouteCache = newClientRouteCache()
			ClientRoutesHandlerData(opaqueStorageValuexxxxxxxxxx).cfg = ClientRoutesConfig{
				TableName: "system.client_routes",
				Endpoints: ClientRoutesEndpointList{{ConnectionID: "c1"}},
			}
			return opaqueStorageValuexxxxxxxxxx
		}()

		h.StartUpdateWorker()
		defer h.Stop()

		done := make(chan error, 1)
		go func() {
			done <- clientRoutesHandlerUpdateHostPortMappingSync(h, InternalUpdateTask{InternalConnectionIDs: []string{"c1"}})
		}()

		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for synchronous update")
		}
	})

	t.Run("WithShardAwareness", func(t *testing.T) {
		t.Parallel()

		cfg := InternalNewClusterSettings("127.0.0.1")
		cfg.WithOptions(
			WithClientRoutes(
				WithEndpoints(ClientRoutesEndpoint{ConnectionID: "connection-id"}),
				WithShardAwareness(true),
			),
		)

		if cfg.ClientRoutesConfig == nil {
			t.Fatal("expected client routes config")
		}
		tests.AssertEqual(t, "client routes shard awareness", true, cfg.ClientRoutesConfig.EnableShardAwareness)
	})
}

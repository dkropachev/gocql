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
	"net"
	"strings"
	"testing"

	implCqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/tests"
	"github.com/scylladb/gocql/v2/internal/tests/mock"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func TestHostDiscovery(t *testing.T) {
	t.Parallel()

	t.Run("AddHostIfMissing", func(t *testing.T) {
		t.Parallel()

		t.Run("Existing", func(t *testing.T) {
			t.Parallel()

			ring := &InternalRingDescriber{}

			host := func() *endpoint.HostInfo {
				opaqueStorageValuexx := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuexx).InternalHostId = implCqltype.MustRandomUUID()
				implEndpoint.HostInfoData(opaqueStorageValuexx).InternalConnectAddress = net.IPv4(1, 1, 1, 1)
				return opaqueStorageValuexx
			}()
			ring.addHostIfMissing(host)

			h2 := func() *endpoint.HostInfo {
				opaqueStorageValuexxx := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuexxx).InternalHostId = implEndpoint.HostInfoData(host).InternalHostId
				implEndpoint.HostInfoData(opaqueStorageValuexxx).InternalConnectAddress = net.IPv4(2, 2, 2, 2)
				return opaqueStorageValuexxx
			}()

			h1, ok := ring.addHostIfMissing(h2)
			if !ok {
				t.Fatal("host was not reported as already existing")
			} else if !h1.Equal(host) {
				t.Fatalf("hosts not equal that are returned %v != %v", h1, host)
			} else if h1 != host {
				t.Fatalf("returned host same pointer: %p != %p", h1, host)
			}
		})

		t.Run("Missing", func(t *testing.T) {
			t.Parallel()

			ring := &InternalRingDescriber{}

			host := func() *endpoint.HostInfo {
				opaqueStorageValuex := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuex).InternalHostId = implCqltype.MustRandomUUID()
				implEndpoint.HostInfoData(opaqueStorageValuex).InternalConnectAddress = net.IPv4(1, 1, 1, 1)
				return opaqueStorageValuex
			}()
			h1, ok := ring.addHostIfMissing(host)
			if ok {
				t.Fatal("host was reported as already existing")
			} else if !h1.Equal(host) {
				t.Fatalf("hosts not equal that are returned %v != %v", h1, host)
			} else if h1 != host {
				t.Fatalf("returned host same pointer: %p != %p", h1, host)
			}
		})
	})

	t.Run("CheckSystemSchema", func(t *testing.T) {
		t.Parallel()

		t.Run("NilIterReturnsNoControl", func(t *testing.T) {
			ok, err := checkSystemSchema(&systemSchemaTestControl{})
			if err != metadata.InternalErrNoControl {
				t.Fatalf("expected errNoControl, got %v", err)
			}
			if ok {
				t.Fatal("expected system schema v2 detection to fail without a control iterator")
			}
		})

		t.Run("Success", func(t *testing.T) {
			framer := &trackingMockFramer{}
			ok, err := checkSystemSchema(&systemSchemaTestControl{
				iter: &nativeResultFixture{framer: framer},
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !ok {
				t.Fatal("expected system schema v2 detection to succeed")
			}
			if !framer.released {
				t.Fatal("expected iterator framer to be released")
			}
		})

		t.Run("SyntaxError", func(t *testing.T) {
			framer := &trackingMockFramer{}
			ok, err := checkSystemSchema(&systemSchemaTestControl{
				iter: &nativeResultFixture{
					err:    &frm.ErrorFrame{Code: protocol.ErrCodeSyntax},
					framer: framer,
				},
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok {
				t.Fatal("expected schema v2 detection to fall back on syntax error")
			}
			if !framer.released {
				t.Fatal("expected iterator framer to be released")
			}
		})
	})

	t.Run("GetClusterPeerInfo", func(t *testing.T) {
		t.Parallel()

		t.Run("ClosesIter", func(t *testing.T) {
			t.Parallel()

			row := []any{
				net.IPv4(192, 168, 100, 13),
				"datacenter1",
				implCqltype.MustParseUUID("b953309f-6e68-41f2-baf5-0e60da317a9c"),
				nil,
				"rack1",
				"3.0.8",
				net.IPv4(192, 168, 100, 13),
				implCqltype.MustParseUUID("b6ed5bde-b318-11ef-8f58-aeba19e31273"),
				[]string{"-1032311531684407545"},
			}
			framer := &trackingMockFramer{
				MockFramer: mock.MockFramer{Data: marshalMetadataMust(systemPeersResultMetadata, row)},
			}
			r := &InternalRingDescriber{InternalCfg: &InternalClusterSettings{Port: 9042}}

			peers, err := r.getClusterPeerInfo(context.Background(), &trackingRingConnection{
				iter: &nativeResultFixture{
					meta:    systemPeersResultMetadata,
					framer:  framer,
					numRows: 1,
				},
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(peers) != 1 {
				t.Fatalf("expected 1 peer, got %d", len(peers))
			}
			if !framer.released {
				t.Fatal("expected iterator framer to be released")
			}
		})

		t.Run("QueryRouting", func(t *testing.T) {
			t.Parallel()

			tests := []struct {
				name      string
				schemaV2  bool
				scylla    bool
				wantQuery string
			}{
				{"schema_v2", true, false, transport.InternalQrySystemPeersV2},
				{"scylla_peers", false, true, transport.InternalQrySystemPeers},
				{"cassandra_peers", false, false, transport.InternalQrySystemPeersCassandra},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					conn := &trackingRingConnection{
						schemaV2: tt.schemaV2,
						scylla:   tt.scylla,
					}
					r := &InternalRingDescriber{InternalCfg: &InternalClusterSettings{Port: 9042}}
					// iter is nil so getClusterPeerInfo returns errNoControl, but the query is still recorded
					_, _ = r.getClusterPeerInfo(context.Background(), conn)
					if conn.lastQuery != tt.wantQuery {
						t.Errorf("got query %q, want %q", conn.lastQuery, tt.wantQuery)
					}
				})
			}
		})

		t.Run("ZeroToken", func(t *testing.T) {
			t.Parallel()

			schema_version1 := implCqltype.MustParseUUID("af810386-a694-11ef-81fa-3aea73156247")

			peersRows := []map[string]any{
				{
					"data_center":     "datacenter1",
					"host_id":         implCqltype.MustParseUUID("b2035fd9-e0ca-4857-8c45-e63c00fb7c43"),
					"peer":            "127.0.0.3",
					"preferred_ip":    "127.0.0.3",
					"rack":            "rack1",
					"release_version": "3.0.8",
					"rpc_address":     "127.0.0.3",
					"schema_version":  schema_version1,
					"tokens":          []string{"-1296227678594315580994457470329811265"},
				},
				{
					"data_center":     "datacenter1",
					"host_id":         implCqltype.MustParseUUID("4b21ee4c-acea-4267-8e20-aaed5361a0dd"),
					"peer":            "127.0.0.2",
					"preferred_ip":    "127.0.0.2",
					"rack":            "rack1",
					"release_version": "3.0.8",
					"rpc_address":     "127.0.0.2",
					"schema_version":  schema_version1,
					"tokens":          []string{"-1129762924682054333"},
				},
				{
					"data_center":     "datacenter2",
					"host_id":         implCqltype.MustParseUUID("dfef4a22-b8d8-47e9-aee5-8c19d4b7a9e3"),
					"peer":            "127.0.0.5",
					"preferred_ip":    "127.0.0.5",
					"rack":            "rack1",
					"release_version": "3.0.8",
					"rpc_address":     "127.0.0.5",
					"schema_version":  schema_version1,
					"tokens":          []string{},
				},
			}

			var logger logging.StdLogger
			t.Run("OmitOneZeroTokenNode", func(t *testing.T) {
				peers, err := getPeersFromQuerySystemPeers(
					peersRows,
					9042,
					logger,
				)

				if err != nil {
					t.Fatalf("unable to get peers: %v", err)
				}
				tests.AssertEqual(t, "peers length", 2, len(peers))
			})

			t.Run("NoZeroTokenNodes", func(t *testing.T) {
				peersRows[2]["tokens"] = []string{"-1129762924682054333"}
				peers, err := getPeersFromQuerySystemPeers(
					peersRows,
					9042,
					logger,
				)

				if err != nil {
					t.Fatalf("unable to get peers: %v", err)
				}
				tests.AssertEqual(t, "peers length", 3, len(peers))
			})
		})
	})

	t.Run("GetHostsFromSystem", func(t *testing.T) {
		t.Parallel()

		r := &InternalRingDescriber{control: &mockControlConn{}, InternalCfg: &InternalClusterSettings{Port: 9042}}

		hosts, _, err := r.GetHostsFromSystem()
		if err != nil {
			t.Fatalf("unable to get hosts: %v", err)
		}

		// Zero-token hosts remain in topology and participate in schema agreement.
		tests.AssertEqual(t, "hosts length", 3, len(hosts))
		tests.AssertEqual(t, "host token length", 2, len(implEndpoint.HostInfoData(hosts[1]).InternalTokens))
	})

	t.Run("IsValidPeer", func(t *testing.T) {
		t.Parallel()

		host := func() *endpoint.HostInfo {
			opaqueStorageValue := new(endpoint.HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValue).InternalRpcAddress = net.ParseIP("0.0.0.0")
			implEndpoint.HostInfoData(opaqueStorageValue).InternalRack = "myRack"
			implEndpoint.HostInfoData(opaqueStorageValue).InternalHostId = tUUID(1)
			implEndpoint.HostInfoData(opaqueStorageValue).InternalDataCenter = "datacenter"
			implEndpoint.HostInfoData(opaqueStorageValue).InternalTokens = []string{"0", "1"}
			return opaqueStorageValue
		}()

		if !isValidPeer(host) {
			t.Errorf("expected %+v to be a valid peer", host)
		}
		implEndpoint.HostInfoData(host).InternalRack = ""
		if isValidPeer(host) {
			t.Errorf("expected %+v to NOT be a valid peer", host)
		}
	})

	t.Run("IsZeroToken", func(t *testing.T) {
		t.Parallel()

		host := func() *endpoint.HostInfo {
			opaqueStorageValuex := new(endpoint.HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValuex).InternalRpcAddress = net.ParseIP("0.0.0.0")
			implEndpoint.HostInfoData(opaqueStorageValuex).InternalRack = "myRack"
			implEndpoint.HostInfoData(opaqueStorageValuex).InternalHostId = tUUID(1)
			implEndpoint.HostInfoData(opaqueStorageValuex).InternalDataCenter = "datacenter"
			implEndpoint.HostInfoData(opaqueStorageValuex).InternalTokens = []string{"0", "1"}
			return opaqueStorageValuex
		}()

		if isZeroToken(host) {
			t.Errorf("expected %+v to NOT be a zero-token host", host)
		}
		implEndpoint.HostInfoData(host).InternalTokens = []string{}
		if !isZeroToken(host) {
			t.Errorf("expected %+v to be a zero-token host", host)
		}
	})

	t.Run("RefreshRing", func(t *testing.T) {
		t.Parallel()

		t.Run("AddsPeers", func(t *testing.T) {
			t.Parallel()

			p2 := newTestPeer("127.0.0.2")
			p3 := newTestPeer("127.0.0.3")
			rs := newRingTestServer(t, p2)
			s := rs.session(t, nil)

			if got, want := ringHosts(s), sortedKeys(localKey(), peerKey(p2)); strings.Join(got, " ") != strings.Join(want, " ") {
				t.Fatalf("initial ring = %v, want %v", got, want)
			}

			// A new peer joins.
			rs.setPeers(p2, p3)
			if err := s.RefreshRingNow(); err != nil {
				t.Fatal(err)
			}
			if got, want := ringHosts(s), sortedKeys(localKey(), peerKey(p2), peerKey(p3)); strings.Join(got, " ") != strings.Join(want, " ") {
				t.Fatalf("after join ring = %v, want %v", got, want)
			}
			if h := SessionData(s).InternalHostSource.InternalGetHost(p3.hostID.String()); h == nil || h.DataCenter() != "dc1" || h.Rack() != "rack1" {
				t.Fatalf("joined host = %v, want dc1/rack1", h)
			}

			// p2 keeps its host ID but moves to a new address.
			moved := p2
			moved.ip = net.ParseIP("127.0.0.4")
			rs.setPeers(moved, p3)
			if err := s.RefreshRingNow(); err != nil {
				t.Fatal(err)
			}
			if got, want := ringHosts(s), sortedKeys(localKey(), peerKey(moved), peerKey(p3)); strings.Join(got, " ") != strings.Join(want, " ") {
				t.Fatalf("after address change ring = %v, want %v", got, want)
			}

			// p3 leaves.
			rs.setPeers(moved)
			if err := s.RefreshRingNow(); err != nil {
				t.Fatal(err)
			}
			if got, want := ringHosts(s), sortedKeys(localKey(), peerKey(moved)); strings.Join(got, " ") != strings.Join(want, " ") {
				t.Fatalf("after leave ring = %v, want %v", got, want)
			}
		})

		// A failed lookup reports the error and leaves the ring as it was rather
		// than treating every peer as gone.
		t.Run("KeepsRingOnError", func(t *testing.T) {
			t.Parallel()

			p2 := newTestPeer("127.0.0.2")
			rs := newRingTestServer(t, p2)
			s := rs.session(t, nil)
			before := ringHosts(s)

			rs.failPeers("peers unavailable")
			err := s.RefreshRingNow()
			if err == nil || !strings.Contains(err.Error(), "peers unavailable") {
				t.Fatalf("refreshRingNow error = %v, want the peers error", err)
			}
			if got := ringHosts(s); strings.Join(got, " ") != strings.Join(before, " ") {
				t.Fatalf("ring after a failed refresh = %v, want %v", got, before)
			}
		})

		// Peers the driver cannot or should not use never enter the ring.
		t.Run("SkipsPeers", func(t *testing.T) {
			t.Parallel()

			good := newTestPeer("127.0.0.2")
			noRack := newTestPeer("127.0.0.3")
			noRack.rack = ""
			zeroToken := newTestPeer("127.0.0.4")
			zeroToken.tokens = nil
			otherDC := newTestPeer("127.0.0.5")
			otherDC.dc = "dc2"

			rs := newRingTestServer(t, good, noRack, zeroToken, otherDC)
			s := rs.session(t, func(cfg *InternalClusterSettings) { cfg.HostFilter = DataCenterHostFilter("dc1") })

			if got, want := ringHosts(s), sortedKeys(localKey(), peerKey(good), peerKey(zeroToken), peerKey(otherDC)); strings.Join(got, " ") != strings.Join(want, " ") {
				t.Fatalf("ring = %v, want valid topology including zero-token and disabled peers %v", got, want)
			}
			if err := s.RefreshRingNow(); err != nil {
				t.Fatal(err)
			}
			if got, want := ringHosts(s), sortedKeys(localKey(), peerKey(good), peerKey(zeroToken), peerKey(otherDC)); strings.Join(got, " ") != strings.Join(want, " ") {
				t.Fatalf("after refresh ring = %v, want %v", got, want)
			}
			for _, host := range s.Metadata().Hosts() {
				if host.ID == otherDC.hostID && host.Enabled {
					t.Fatal("filtered host must remain disabled")
				}
			}
			if pool, ok := SessionData(s).InternalPool.InternalGetPoolByHostID(otherDC.hostID.String()); ok && pool.Size() > 0 {
				t.Fatal("filtered host received connections")
			}
			if len(s.Metadata().Diagnostics()) == 0 {
				t.Fatal("skipped incomplete peer must be observable")
			}

		})
	})
}

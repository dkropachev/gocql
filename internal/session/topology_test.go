// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	oldevents "github.com/scylladb/gocql/v2/internal/events"
	"github.com/scylladb/gocql/v2/internal/logging"
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/metadata"
)

func TestTopology(t *testing.T) {
	t.Run("Contacts", func(t *testing.T) {
		t.Run("DistinctIdentities", func(t *testing.T) {
			ids := []cql.UUID{cql.RandomUUID(), cql.RandomUUID()}
			contacts := make([]string, 2)
			for i := range contacts {
				id := ids[i]
				srv := newControlTestServer(t, func(s *controlTestServer) {
					s.localRows = func(stream int, response *protocol.Framer) {
						row := systemLocalRow(net.IPv4(127, 0, 0, 1))
						row[0] = id[:]
						writeRows(response, stream, systemLocalCols, [][][]byte{row})
					}
				})
				contacts[i] = srv.Address
			}
			config := settings.DefaultConfig(contacts...)
			config.Protocol = 4
			config.Metadata.Mode = metadata.SchemaNone
			config.Metadata.InitialDiscovery = false
			config.Metadata.AutomaticAgreement = false
			config.Pool.AdvancedShardAwareness.Enabled = false
			config.Pool.ConnectionsPerHost = 1
			config.Logger = logging.Nop()
			s, err := Connect(context.Background(), config)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			hosts := s.Metadata().Hosts()
			if len(hosts) != 2 {
				t.Fatalf("contact identities collapsed: %+v", hosts)
			}
			for _, id := range ids {
				found := false
				for _, h := range hosts {
					if h.ID == id {
						found = true
					}
				}
				if !found {
					t.Fatal("missing actual UUID", id, hosts)
				}
			}
			if err := s.RefreshMetadata(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(s.Metadata().Hosts()) != 2 {
				t.Fatal("explicit refresh discovered unrelated peers or collapsed contacts")
			}
		})
		// A system.local row without any usable address (rpc_address
		// 0.0.0.0 and no broadcast address) keeps the dialed address.
		// HostInfo.ConnectAddress panics on a host without one, and
		// control setup reads it to apply the host filter and to report
		// a filtered host.
		t.Run("LocalHostWithoutAddress", func(t *testing.T) {
			for _, filtered := range []bool{false, true} {
				srv := newControlTestServer(t, func(s *controlTestServer) {
					s.localRows = func(stream int, response *protocol.Framer) {
						writeRows(response, stream, systemLocalCols, [][][]byte{systemLocalRow(net.IPv4zero)})
					}
				})
				config := settings.DefaultConfig(srv.Address)
				config.Protocol = 4
				config.Metadata.Mode = metadata.SchemaNone
				config.Metadata.InitialDiscovery = false
				config.Metadata.AutomaticAgreement = false
				config.Pool.AdvancedShardAwareness.Enabled = false
				config.Pool.ConnectionsPerHost = 1
				config.Logger = logging.Nop()
				if filtered {
					config.HostFilter = func(metadata.HostSnapshot) bool { return false }
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				s, err := Connect(ctx, config)
				cancel()
				if !filtered {
					if err != nil {
						t.Fatal(err)
					}
					_ = s.Close()
					continue
				}
				if err == nil {
					_ = s.Close()
					t.Fatal("Connect succeeded with every host filtered")
				}
				if !strings.Contains(err.Error(), "host was filtered: 127.0.0.1") {
					t.Fatalf("Connect = %v, want the dialed host filtered", err)
				}
			}
		})
	})

	t.Run("Refresh", func(t *testing.T) {
		t.Run("IncompletePeerRetainsTopology", func(t *testing.T) {
			good := newTestPeer("127.0.0.2")
			srv := newRingTestServer(t, good)
			s := srv.session(t, nil)
			before := ringHosts(s)
			incomplete := good
			incomplete.rack = ""
			srv.setPeers(incomplete)
			err := s.RefreshRingNow()
			if partial, ok := errors.AsType[*cqlerr.PartialRefreshError](err); !ok || partial.Topology == nil {
				t.Fatal("incomplete read was not a visible partial failure", err)
			}
			if got := ringHosts(s); !reflect.DeepEqual(got, before) {
				t.Fatal("incomplete read erased valid topology", before, got)
			}
		})
	})

	t.Run("InvalidPeers", func(t *testing.T) {
		// A peer row that does not parse is skipped like an incomplete
		// one, rather than failing the whole read.
		t.Run("NewPeerSkipped", func(t *testing.T) {
			good := newTestPeer("127.0.0.2")
			srv := newRingTestServer(t, good)
			s := srv.session(t, nil)
			bad := newTestPeer("127.0.0.3")
			bad.tokens = []string{"not-a-token"}
			incomplete := newTestPeer("127.0.0.4")
			incomplete.dc = ""
			srv.setPeers(good, bad, incomplete)
			if err := s.RefreshRingNow(); err != nil {
				t.Fatal("an invalid new peer failed the read:", err)
			}
			want := []string{good.hostID.String() + "@127.0.0.2", testLocalHostID.String() + "@127.0.0.1"}
			sort.Strings(want)
			if got := ringHosts(s); !reflect.DeepEqual(got, want) {
				t.Fatalf("ring %v, want %v", got, want)
			}
			diagnostics := fmt.Sprint(s.Metadata().Diagnostics())
			if !strings.Contains(diagnostics, "invalid peer "+bad.hostID.String()) || !strings.Contains(diagnostics, "incomplete peer "+incomplete.hostID.String()) {
				t.Fatal("skipped peers not in the diagnostics:", diagnostics)
			}
		})
		t.Run("KnownPeerRetainsTopology", func(t *testing.T) {
			good := newTestPeer("127.0.0.2")
			srv := newRingTestServer(t, good)
			s := srv.session(t, nil)
			before := ringHosts(s)
			bad := good
			bad.tokens = []string{"not-a-token"}
			srv.setPeers(bad)
			if err := s.RefreshRingNow(); err == nil {
				t.Fatal("a known node's invalid row was skipped")
			}
			if got := ringHosts(s); !reflect.DeepEqual(got, before) {
				t.Fatal("invalid read changed the topology", before, got)
			}
		})
	})

	t.Run("AddressChange", func(t *testing.T) {
		t.Run("Refresh", func(t *testing.T) {
			peer := newTestPeer("127.0.0.2")
			srv := newRingTestServer(t, peer)
			s := srv.session(t, nil)
			moved := peer
			moved.ip = net.ParseIP("127.0.0.3")
			srv.setPeers(moved)
			if err := s.RefreshRingNow(); err != nil {
				t.Fatal(err)
			}
			if h := implCluster.SessionData(s).InternalHostSource.InternalGetHost(peer.hostID.String()); h == nil || !h.ConnectAddress().Equal(moved.ip) {
				t.Fatal("moved node not at its new address:", ringHosts(s))
			}
		})
		// A node that changes address is removed at the old one, and the
		// tombstone that hides its old row must not hide it at the new one.
		t.Run("AfterRemovedNode", func(t *testing.T) {
			peer := newTestPeer("127.0.0.2")
			srv := newRingTestServer(t, peer)
			s := srv.session(t, nil)
			moved := peer
			moved.ip = net.ParseIP("127.0.0.3")
			s.RecordV2Topology(&oldevents.TopologyChangeEvent{Change: "REMOVED_NODE", Host: peer.ip, Port: 9042})
			s.RecordV2Topology(&oldevents.TopologyChangeEvent{Change: "NEW_NODE", Host: moved.ip, Port: 9042})
			srv.setPeers(moved)
			if err := s.RefreshRingNow(); err != nil {
				t.Fatal(err)
			}
			if h := implCluster.SessionData(s).InternalHostSource.InternalGetHost(peer.hostID.String()); h == nil || !h.ConnectAddress().Equal(moved.ip) {
				t.Fatal("moved node not at its new address:", ringHosts(s))
			}
		})
	})

	t.Run("Removal", func(t *testing.T) {
		// A removed node's row that a read skips, or that a read does not
		// list, does not end its tombstone: another read can list it again.
		t.Run("TombstoneSurvivesSkippedRow", func(t *testing.T) {
			peer := newTestPeer("127.0.0.2")
			srv := newRingTestServer(t, peer)
			s := srv.session(t, nil)
			s.RecordV2Topology(&oldevents.TopologyChangeEvent{Change: "REMOVED_NODE", Host: peer.ip, Port: 9042})
			incomplete := peer
			incomplete.rack = ""
			for _, peers := range [][]testPeer{{incomplete}, {}, {peer}} {
				srv.setPeers(peers...)
				// The incomplete row may fail the read, as the node was
				// known before; either way the tombstone stays.
				_ = s.RefreshRingNow()
			}
			if implCluster.SessionData(s).InternalHostSource.InternalGetHost(peer.hostID.String()) != nil {
				t.Fatal("removed node came back")
			}
		})
		t.Run("TombstoneAndRecreate", func(t *testing.T) {
			peer := newTestPeer("127.0.0.2")
			srv := newRingTestServer(t, peer)
			s := srv.session(t, nil)
			s.RecordV2Topology(&oldevents.TopologyChangeEvent{Change: "REMOVED_NODE", Host: peer.ip, Port: 9042})
			if implCluster.SessionData(s).InternalHostSource.InternalGetHost(peer.hostID.String()) != nil {
				t.Fatal("removed host still visible")
			}
			// Gossip still advertises its old row. A post-event fetch cannot resurrect it.
			if err := s.RefreshRingNow(); err != nil {
				t.Fatal(err)
			}
			if implCluster.SessionData(s).InternalHostSource.InternalGetHost(peer.hostID.String()) != nil {
				t.Fatal("stale gossip resurrected removed host")
			}
			s.RecordV2Topology(&oldevents.TopologyChangeEvent{Change: "NEW_NODE", Host: peer.ip, Port: 9042})
			if err := s.RefreshRingNow(); err != nil {
				t.Fatal(err)
			}
			if implCluster.SessionData(s).InternalHostSource.InternalGetHost(peer.hostID.String()) == nil {
				t.Fatal("new identity epoch did not allow rediscovery")
			}
		})
	})

	t.Run("Rows", func(t *testing.T) {
		t.Run("RejectsMalformedTokensAndPorts", func(t *testing.T) {
			for _, row := range []implMetadata.InternalV2TopologyRow{{InternalPort: -1}, {InternalPort: 65536}, {InternalPort: 9042, InternalTokens: []string{"not-a-token"}}} {
				if _, err := row.InternalHost(9042); err == nil {
					t.Fatal("malformed topology accepted", row)
				}
			}
		})
	})
}

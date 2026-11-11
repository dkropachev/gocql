// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"

	oldevents "github.com/scylladb/gocql/v2/events"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/logging"
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/settings"
)

func TestTopology(t *testing.T) {
	t.Run("Contacts", func(t *testing.T) {
		t.Run("DistinctIdentities", func(t *testing.T) {
			ids := []cqltype.UUID{cqltype.MustRandomUUID(), cqltype.MustRandomUUID()}
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
			config.Metadata.Mode = settings.MetadataDisabled
			config.Metadata.InitialDiscovery = false
			config.Metadata.AutomaticAgreement = false
			config.Pool.ShardAwarePort = false
			config.Pool.ConnectionsPerHost = 1
			config.Logger = logging.InternalNopLogger{}
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
			if partial, ok := errors.AsType[*implMetadata.PartialRefreshError](err); !ok || partial.Topology == nil {
				t.Fatal("incomplete read was not a visible partial failure", err)
			}
			if got := ringHosts(s); !reflect.DeepEqual(got, before) {
				t.Fatal("incomplete read erased valid topology", before, got)
			}
		})
	})

	t.Run("Removal", func(t *testing.T) {
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

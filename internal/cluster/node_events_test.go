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
	"net"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	implPool "github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/protocol"
)

func TestNodeEvents(t *testing.T) {
	t.Parallel()

	// TestNodeEvents/HandleNodeConnected covers Session.handleNodeConnected, which the pool
	// calls once a connection to a host is up. Its filter branch was the uncovered
	// part: a filtered host still gets its state set, but must not be announced to
	// the policy.
	t.Run("HandleNodeConnected", func(t *testing.T) {
		t.Parallel()

		t.Run("unfiltered host is marked up and announced", func(t *testing.T) {
			t.Parallel()

			f := newNodeEventFixture(t)
			host := f.addHost(t, "127.0.10.1")
			implEndpoint.
				// nodeState's zero value IS NodeUp, so a freshly built HostInfo already
				// reads as up and the assertion below would hold even if
				// handleNodeConnected never called setState. Start from NodeDown so the
				// transition is what is being observed.
				InternalHostInfoSetState(host, endpoint.NodeDown)
			sessionHandleNodeConnected(f.session, host)

			if state := host.State(); state != endpoint.NodeUp {
				t.Errorf("host state: got %v, want %v", state, endpoint.NodeUp)
			}
			assertStrings(t, "HostUp", f.policy.hostUpCalls(), []string{"127.0.10.1"})
		})

		t.Run("filtered host is marked up but not announced", func(t *testing.T) {
			t.Parallel()

			f := newNodeEventFixture(t, func(cfg *InternalClusterSettings) {
				cfg.HostFilter = HostFilterFunc(func(*endpoint.HostInfo) bool { return false })
			})
			host := f.addHost(t, "127.0.10.1")
			implEndpoint.InternalHostInfoSetState(host, endpoint.NodeDown)
			sessionHandleNodeConnected(f.session, host)

			if state := host.State(); state != endpoint.NodeUp {
				t.Errorf("host state: got %v, want %v", state, endpoint.NodeUp)
			}
			assertStrings(t, "HostUp", f.policy.hostUpCalls(), nil)
		})
	})

	// TestNodeEvents/HandleNodeDown covers the branch of Session.handleNodeDown a healthy
	// cluster does not reach: a DOWN for a host the HostFilter excludes.
	//
	// The asymmetry is deliberate to pin, because it is easy to "fix" in the wrong
	// direction: handleNodeDown sets the host's state before consulting the filter,
	// so a filtered host is still marked NodeDown, but it is not handed to
	// policy.HostDown. handleNodeUp does the opposite -- it drops a filtered host
	// before touching anything.
	t.Run("HandleNodeDown", func(t *testing.T) {
		t.Parallel()

		t.Run("unfiltered host is marked down and dispatched", func(t *testing.T) {
			t.Parallel()

			f := newNodeEventFixture(t)
			host := f.addHost(t, "127.0.10.1")
			SessionData(f.session).InternalPool.InternalHostConnPools = map[cqltype.UUID]*implPool.InternalHostConnPool{implEndpoint.InternalHostInfoHostUUID(host): f.session.NewHostConnPool(host, 1, "")}
			f.session.HandleNodeDown(net.ParseIP("127.0.10.1"), 9042)

			if state := host.State(); state != endpoint.NodeDown {
				t.Errorf("host state: got %v, want %v", state, endpoint.NodeDown)
			}
			assertStrings(t, "HostDown", f.policy.hostDownCalls(), []string{"127.0.10.1"})
			if _, ok := SessionData(f.session).InternalPool.InternalGetPoolByHostID(host.HostID()); ok {
				t.Error("host that went down still has a connection pool")
			}
		})

		t.Run("filtered host is marked down but not dispatched", func(t *testing.T) {
			t.Parallel()

			f := newNodeEventFixture(t, func(cfg *InternalClusterSettings) {
				cfg.HostFilter = HostFilterFunc(func(*endpoint.HostInfo) bool { return false })
			})
			host := f.addHost(t, "127.0.10.1")
			SessionData(f.session).InternalPool.InternalHostConnPools = map[cqltype.UUID]*implPool.InternalHostConnPool{implEndpoint.InternalHostInfoHostUUID(host): f.session.NewHostConnPool(host, 1, "")}
			f.session.HandleNodeDown(net.ParseIP("127.0.10.1"), 9042)

			// setState happens before the filter check, so it still lands.
			if state := host.State(); state != endpoint.NodeDown {
				t.Errorf("host state: got %v, want %v", state, endpoint.NodeDown)
			}
			assertStrings(t, "HostDown", f.policy.hostDownCalls(), nil)
			// The filter returns before both policy.HostDown and pool.removeHost,
			// so the pool survives -- the other half of the asymmetry this test
			// exists to pin.
			if _, ok := SessionData(f.session).InternalPool.InternalGetPoolByHostID(host.HostID()); !ok {
				t.Error("filtered host lost its connection pool")
			}
		})

		t.Run("unknown host is ignored", func(t *testing.T) {
			t.Parallel()

			// Unlike handleNodeUp, a DOWN for a host the driver does not know is
			// dropped outright -- there is nothing to mark down and no reason to
			// re-read the ring for a node that just left.
			f := newNodeEventFixture(t)
			f.session.HandleNodeDown(net.ParseIP("127.0.10.9"), 9042)

			assertStrings(t, "HostDown", f.policy.hostDownCalls(), nil)
			f.assertNoRefresh(t)
		})
	})

	t.Run("HandleNodeEvent", func(t *testing.T) {
		t.Parallel()

		// TestNodeEvents/HandleNodeEvent/CoalescesStatusChanges pins the per-host debouncing
		// documented on handleNodeEvent: a burst of status changes collapses to one
		// dispatch per host, and the last change for a host is the one that wins.
		//
		// Nothing asserted this before, and the collapse is easy to break -- the map in
		// handleNodeEvent is keyed by IP string, so a change to how the key is derived
		// would silently turn one dispatch into several.
		t.Run("CoalescesStatusChanges", func(t *testing.T) {
			t.Parallel()

			t.Run("repeated DOWN for one host dispatches once", func(t *testing.T) {
				t.Parallel()

				f := newNodeEventFixture(t)
				f.addHost(t, "127.0.10.1")
				f.session.HandleNodeEvent([]protocol.Frame{
					statusChange("DOWN", "127.0.10.1"),
					statusChange("DOWN", "127.0.10.1"),
					statusChange("DOWN", "127.0.10.1"),
				})

				assertStrings(t, "HostDown", f.policy.hostDownCalls(), []string{"127.0.10.1"})
			})

			t.Run("distinct hosts each dispatch", func(t *testing.T) {
				t.Parallel()

				f := newNodeEventFixture(t)
				f.addHost(t, "127.0.10.1")
				f.addHost(t, "127.0.10.2")
				f.session.HandleNodeEvent([]protocol.Frame{
					statusChange("DOWN", "127.0.10.1"),
					statusChange("DOWN", "127.0.10.2"),
				})

				got := f.policy.hostDownCalls()
				if len(got) != 2 {
					t.Fatalf("HostDown: got %v, want one call per host", got)
				}
				seen := map[string]bool{got[0]: true, got[1]: true}
				for _, want := range []string{"127.0.10.1", "127.0.10.2"} {
					if !seen[want] {
						t.Errorf("HostDown: %s never dispatched (got %v)", want, got)
					}
				}
			})

			t.Run("UP then DOWN leaves the host down", func(t *testing.T) {
				t.Parallel()

				f := newNodeEventFixture(t)
				host := f.addHost(t, "127.0.10.1")
				f.session.HandleNodeEvent([]protocol.Frame{
					statusChange("UP", "127.0.10.1"),
					statusChange("DOWN", "127.0.10.1"),
				})

				assertStrings(t, "HostDown", f.policy.hostDownCalls(), []string{"127.0.10.1"})
				assertStrings(t, "AddHost", f.policy.addHostCalls(), nil)
				if state := host.State(); state != endpoint.NodeDown {
					t.Errorf("host state: got %v, want %v", state, endpoint.NodeDown)
				}
			})

			t.Run("DOWN then UP leaves the host up", func(t *testing.T) {
				t.Parallel()

				// A known host the filter rejects. The filter is what keeps the UP from
				// reaching startPoolFill and dialing, and it leaves the host's state as
				// the one observable: handleNodeDown sets NodeDown *before* consulting
				// the filter, so a DOWN that was wrongly dispatched still shows up,
				// while handleNodeUp touches nothing for a filtered host.
				//
				// An unknown host does not work here. A DOWN for one is already a
				// no-op, so dispatching both events looks identical to dispatching only
				// the UP, and the assertion would hold with coalescing broken entirely.
				f := newNodeEventFixture(t, func(cfg *InternalClusterSettings) {
					cfg.HostFilter = HostFilterFunc(func(*endpoint.HostInfo) bool { return false })
				})
				host := f.addHost(t, "127.0.10.1")
				implEndpoint.InternalHostInfoSetState(host, endpoint.NodeUp)
				f.session.HandleNodeEvent([]protocol.Frame{
					statusChange("DOWN", "127.0.10.1"),
					statusChange("UP", "127.0.10.1"),
				})

				if state := host.State(); state != endpoint.NodeUp {
					t.Errorf("host state: got %v, want %v -- the superseded DOWN was dispatched", state, endpoint.NodeUp)
				}
				assertStrings(t, "HostDown", f.policy.hostDownCalls(), nil)
			})
		})

		// TestNodeEvents/HandleNodeEvent/DisableNodeStatusEvents pins the guard added for
		// apache/cassandra-gocql-driver#1591: a status change that arrives while node
		// status events are disabled is dropped rather than dispatched.
		t.Run("DisableNodeStatusEvents", func(t *testing.T) {
			t.Parallel()

			t.Run("enabled dispatches DOWN", func(t *testing.T) {
				t.Parallel()

				f := newNodeEventFixture(t)
				f.addHost(t, "127.0.10.1")
				f.session.HandleNodeEvent([]protocol.Frame{statusChange("DOWN", "127.0.10.1")})

				assertStrings(t, "HostDown", f.policy.hostDownCalls(), []string{"127.0.10.1"})
			})

			t.Run("disabled drops DOWN", func(t *testing.T) {
				t.Parallel()

				f := newNodeEventFixture(t, func(cfg *InternalClusterSettings) {
					cfg.Events.DisableNodeStatusEvents = true
				})
				host := f.addHost(t, "127.0.10.1")
				f.session.HandleNodeEvent([]protocol.Frame{statusChange("DOWN", "127.0.10.1")})

				assertStrings(t, "HostDown", f.policy.hostDownCalls(), nil)
				if state := host.State(); state == endpoint.NodeDown {
					t.Error("host was marked down even though node status events are disabled")
				}
			})

			t.Run("disabled drops UP", func(t *testing.T) {
				t.Parallel()

				// Unknown IP: were the UP dispatched, handleNodeUp would request a ring
				// refresh, so silence here means the event really was dropped.
				f := newNodeEventFixture(t, func(cfg *InternalClusterSettings) {
					cfg.Events.DisableNodeStatusEvents = true
				})
				f.session.HandleNodeEvent([]protocol.Frame{statusChange("UP", "127.0.10.9")})

				f.assertNoRefresh(t)
			})
		})

		// TestNodeEvents/HandleNodeEvent/TopologyChange covers the topology half of the dispatcher:
		// any topology frame in the batch collapses to a single ring refresh, and
		// DisableTopologyEvents suppresses it.
		t.Run("TopologyChange", func(t *testing.T) {
			t.Parallel()

			t.Run("topology change refreshes the ring", func(t *testing.T) {
				t.Parallel()

				f := newNodeEventFixture(t)
				f.session.HandleNodeEvent([]protocol.Frame{topologyChange("NEW_NODE", "127.0.10.1")})
				f.awaitRefresh(t)
			})

			t.Run("DisableTopologyEvents suppresses the refresh", func(t *testing.T) {
				t.Parallel()

				f := newNodeEventFixture(t, func(cfg *InternalClusterSettings) {
					cfg.Events.DisableTopologyEvents = true
				})
				f.session.HandleNodeEvent([]protocol.Frame{topologyChange("NEW_NODE", "127.0.10.1")})
				f.assertNoRefresh(t)
			})

			t.Run("status changes are still dispatched when topology events are off", func(t *testing.T) {
				t.Parallel()

				// The two switches are independent; turning topology events off must not
				// take status dispatch with it.
				f := newNodeEventFixture(t, func(cfg *InternalClusterSettings) {
					cfg.Events.DisableTopologyEvents = true
				})
				f.addHost(t, "127.0.10.1")
				f.session.HandleNodeEvent([]protocol.Frame{
					topologyChange("NEW_NODE", "127.0.10.1"),
					statusChange("DOWN", "127.0.10.1"),
				})

				assertStrings(t, "HostDown", f.policy.hostDownCalls(), []string{"127.0.10.1"})
			})
		})

		// TestNodeEvents/HandleNodeEvent/TopologySurvivesStatusForSameHost pins the reason
		// handleNodeEvent tracks topology events in a separate bool instead of putting
		// them in the per-host status map: a NEW_NODE must not be lost to a newer UP
		// for the same host in the same batch.
		//
		// The batch here is NEW_NODE + UP for one unknown IP, with status events
		// disabled. That combination is what makes the invariant observable at all.
		// With status events enabled the property is self-healing -- handleNodeUp
		// answers an unknown host with a ring refresh of its own, the same remedy the
		// topology branch asks for -- so a NEW_NODE swallowed by the UP would still end
		// up refreshing the ring and no test could tell. Disabling status events
		// removes that second source, leaving the topology branch as the only thing
		// that can produce a refresh.
		//
		// It catches the two refactors that would break the split: folding topology
		// frames into sEvents (the UP overwrites the NEW_NODE and the refresh is lost),
		// and gating the topology refresh on DisableNodeStatusEvents as well as
		// DisableTopologyEvents. Note it does not pin the textual order of the two
		// blocks in handleNodeEvent -- debounceRingRefresh only resets a trailing
		// timer, so moving the topology block below the status loop is unobservable
		// here.
		t.Run("TopologySurvivesStatusForSameHost", func(t *testing.T) {
			t.Parallel()

			f := newNodeEventFixture(t, func(cfg *InternalClusterSettings) {
				cfg.Events.DisableNodeStatusEvents = true
			})
			f.session.HandleNodeEvent([]protocol.Frame{
				topologyChange("NEW_NODE", "127.0.10.9"),
				statusChange("UP", "127.0.10.9"),
			})

			f.awaitRefresh(t)
			assertStrings(t, "AddHost", f.policy.addHostCalls(), nil)
		})
	})

	// TestNodeEvents/HandleNodeUp covers Session.handleNodeUp's two early exits directly. Both
	// are unreachable from a healthy cluster, which is why the integration lane
	// never touched them.
	t.Run("HandleNodeUp", func(t *testing.T) {
		t.Parallel()

		t.Run("unknown host triggers a ring refresh", func(t *testing.T) {
			t.Parallel()

			// An UP for a host the driver has never seen means the ring is stale,
			// so the driver re-reads it rather than guessing.
			f := newNodeEventFixture(t)
			sessionHandleNodeUp(f.session, net.ParseIP("127.0.10.9"), 9042)

			f.awaitRefresh(t)
			assertStrings(t, "AddHost", f.policy.addHostCalls(), nil)
		})

		t.Run("filtered host is ignored", func(t *testing.T) {
			t.Parallel()

			f := newNodeEventFixture(t, func(cfg *InternalClusterSettings) {
				cfg.HostFilter = HostFilterFunc(func(*endpoint.HostInfo) bool { return false })
			})
			f.addHost(t, "127.0.10.1")
			sessionHandleNodeUp(f.session, net.ParseIP("127.0.10.1"), 9042)

			// The host is known, so this is not the unknown-host path: a filtered
			// host must be dropped outright, with no refresh and no pool fill.
			assertStrings(t, "AddHost", f.policy.addHostCalls(), nil)
			f.assertNoRefresh(t)
		})
	})

	// TestNodeEvents/RemoveHost covers Session.removeHost, which the ring refresher
	// calls when a host leaves the ring (host_source.go). It had no coverage in
	// either lane.
	//
	// It has to retire the host from all three places that track it -- the
	// selection policy, the connection pool and the host source. Dropping any one
	// leaves the driver routing to a node that is gone, so each is asserted
	// separately.
	t.Run("RemoveHost", func(t *testing.T) {
		t.Parallel()

		// newNodeEventFixture (events_node_test.go) already assembles exactly the
		// trio removeHost touches: a recording policy, a connection pool and a
		// host source.
		f := newNodeEventFixture(t)
		host := f.addHost(t, "127.0.20.1")
		other := f.addHost(t, "127.0.20.2")

		// The fixture's pool is empty by default, and policyConnPool.removeHost
		// short-circuits on a missing host -- so with an empty pool the
		// s.pool.removeHost leg of removeHost is unobservable and dropping it
		// entirely would still pass. Populate it so that leg is actually pinned.
		// newHostConnPool is a pure constructor, so neither pool dials.
		hostPool, hostPicker := newSignallingHostPool(f.session, host)
		otherPool, otherPicker := newSignallingHostPool(f.session, other)
		SessionData(f.session).InternalPool.InternalHostConnPools = map[cqltype.UUID]*implPool.InternalHostConnPool{implEndpoint.InternalHostInfoHostUUID(host): hostPool, implEndpoint.InternalHostInfoHostUUID(other): otherPool}

		if _, ok := SessionData(f.session).InternalHostSource.getHostByIP("127.0.20.1"); !ok {
			t.Fatal("precondition: host is not in the host source")
		}
		if _, ok := SessionData(f.session).InternalPool.InternalGetPoolByHostID(host.HostID()); !ok {
			t.Fatal("precondition: host has no connection pool")
		}
		sessionRemoveHost(f.session, host)

		assertStrings(t, "RemoveHost", f.policy.removeHostCalls(), []string{"127.0.20.1"})

		// policyConnPool.removeHost does two things, and the map entry only proves
		// the first. Dropping the entry without closing the pool leaves the retired
		// host's connections open -- a leak on every topology change, and a
		// regression the entry check alone cannot see, since reducing removeHost to
		// a bare delete() still satisfies it.
		if _, ok := SessionData(f.session).InternalPool.InternalGetPoolByHostID(host.HostID()); ok {
			t.Error("removed host still has a connection pool")
		}
		awaitPoolClosed(t, hostPicker, "removed host")

		if _, ok := SessionData(f.session).InternalPool.InternalGetPoolByHostID(other.HostID()); !ok {
			t.Error("removeHost also dropped an unrelated host's connection pool")
		}
		// Safe as a non-blocking check: the wait above already established that the
		// goroutine removeHost started has run, and it is the only one that could
		// have closed anything.
		assertPoolNotClosed(t, otherPicker, "unrelated host")

		if _, ok := SessionData(f.session).InternalHostSource.getHostByIP("127.0.20.1"); ok {
			t.Error("removed host is still resolvable in the host source")
		}
		// ringDescriber keeps two indexes, and getHostByIP only proves one of them.
		// Its ok comes from hostIPToUUID alone, so a host left behind in the hosts
		// map is invisible to the check above -- and hosts is what getHostsList
		// reads, which is what controlConn.attemptReconnect dials. Assert the ID
		// index separately.
		if got := SessionData(f.session).InternalHostSource.InternalGetHost(host.HostID()); got != nil {
			t.Error("removed host is still in the host source by ID")
		}

		if _, ok := SessionData(f.session).InternalHostSource.getHostByIP("127.0.20.2"); !ok {
			t.Error("removeHost also dropped an unrelated host")
		}
		if got := SessionData(f.session).InternalHostSource.InternalGetHost(other.HostID()); got == nil {
			t.Error("unrelated host disappeared from the host source by ID")
		}
	})
}

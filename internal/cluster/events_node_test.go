/*
 * Copyright (C) 2026 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cluster

import (
	"net"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	endpoint2 "github.com/scylladb/gocql/v2/endpoint"
	"github.com/scylladb/gocql/v2/internal/debounce"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/logging"
	implPool "github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/protocol"
)

// Session.handleNodeEvent and Session.handleNodeUp had no unit coverage at all
// -- they were reached only by the integration lane, which drives them through a
// live cluster and therefore exercises the happy path and little else. The
// branches pinned here are the ones a live cluster does not reliably produce:
// the per-host coalescing of a burst of status changes, and the two config
// switches that suppress dispatch entirely.

const (
	// nodeEventRefreshInterval is the debounce window the fixture's ring
	// refresher runs with. Production uses debounce.RingRefreshDebounceTime
	// (1s); a test only needs it short enough not to dominate the run.
	nodeEventRefreshInterval = 2 * time.Millisecond

	// nodeEventRefreshTimeout bounds a positive "a refresh should happen"
	// assertion. Generous on purpose: exceeding it means a refresh never came,
	// not that the machine was briefly busy.
	nodeEventRefreshTimeout = 2 * time.Second

	// nodeEventQuietWindow bounds a negative "no refresh should happen"
	// assertion. It is ~75x nodeEventRefreshInterval, so a refresh that was
	// wrongly scheduled has ample time to land and fail the test.
	nodeEventQuietWindow = 150 * time.Millisecond
)

// nodeEventFixture is a Session cut down to exactly what the node-event path
// touches: a host source to resolve event IPs against, a ring refresher to observe debounced refreshes, and a connection
// pool.
//
// The pool starts as an empty &policyConnPool{}. policyConnPool.removeHost
// short-circuits on a missing host, so the DOWN path is safe, but addHost would
// build a real hostConnPool against a nil session and start dialing. Tests here
// therefore never let an UP event reach Session.startPoolFill -- they use either
// an unknown IP or a filtered host. startPoolFill is already covered by the
// integration lane.
//
// A test that needs to observe the pool being mutated has to populate
// hostConnPools itself, or the short-circuit makes the call unobservable; see
// TestNodeEvents/RemoveHost in node_events_test.go.
type nodeEventFixture struct {
	session   *Core
	refreshes chan struct{}
}

func newNodeEventFixture(t *testing.T, opts ...func(*InternalClusterSettings)) *nodeEventFixture {
	t.Helper()

	refreshes := make(chan struct{}, 16)

	s := func() *Core {
		opaqueStorageValue := new(Core)
		SessionData(opaqueStorageValue).InternalLogger = logging.Nop()
		return opaqueStorageValue
	}()
	SessionData(s).InternalCfg.ConvictionPolicy = &endpoint2.SimpleConvictionPolicy{}
	for _, opt := range opts {
		opt(&SessionData(s).InternalCfg)
	}
	SessionData(s).InternalPool = &implPool.InternalPolicyConnPool{InternalServices: s.NewPoolServices(), InternalHostConnPools: map[cql.UUID]*implPool.InternalHostConnPool{}}
	SessionData(s).InternalHostSource = &InternalRingDescriber{InternalCfg: &SessionData(s).InternalCfg, InternalLogger: SessionData(s).InternalLogger}
	SessionData(s).InternalRingRefresher = debounce.NewRefreshDebouncer(nodeEventRefreshInterval, func() error {
		select {
		case refreshes <- struct{}{}:
		default:
		}
		return nil
	})
	t.Cleanup(SessionData(s).InternalRingRefresher.Stop)

	return &nodeEventFixture{session: s, refreshes: refreshes}
}

// addHost registers a host with the fixture's host source so that an event
// carrying ip resolves to it.
//
// broadcastAddress is what ringDescriber.addHostIfMissing keys hostIPToUUID by
// (via HostInfo.nodeToNodeAddress), and getHostByIP looks up the event's IP in
// that map -- so it has to match the IP the event carries, not just
// connectAddress.
func (f *nodeEventFixture) addHost(t *testing.T, ip string) *endpoint.HostInfo {
	t.Helper()

	addr := net.ParseIP(ip)
	if addr == nil {
		t.Fatalf("addHost: %q is not an IP", ip)
	}
	host := func() *endpoint.HostInfo {
		opaqueStorageValuex := new(endpoint.HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuex).InternalHostId = cql.RandomUUID()
		implEndpoint.HostInfoData(opaqueStorageValuex).InternalConnectAddress = addr
		implEndpoint.HostInfoData(opaqueStorageValuex).InternalBroadcastAddress = addr
		implEndpoint.HostInfoData(opaqueStorageValuex).InternalPort = 9042
		return opaqueStorageValuex
	}()
	SessionData(f.session).InternalHostSource.InternalAddOrUpdate(host)

	// Give the host a pool that reports itself already full. The tests below
	// are written to keep UP events away from startPoolFill, but nothing
	// enforces that, and an unenforced convention should not be able to cost
	// the package its results: a stray fill would dial, and this session has no
	// connCfg, so it would panic and take the whole run down -- including the
	// sibling test that correctly diagnoses whatever caused the stray UP.
	// hostConnPool.fill returns before connecting when the picker asks for no
	// more connections, so this makes a stray startPoolFill a no-op instead.
	pool := f.session.NewHostConnPool(host, 1, "")
	pool.InternalConnPicker = satedConnPicker{}
	SessionData(f.session).InternalPool.InternalHostConnPools[implEndpoint.InternalHostInfoHostUUID(host)] = pool

	return host
}

// satedConnPicker reports one live connection and no demand for more, which is
// what makes hostConnPool.fill return early.
type satedConnPicker struct {
	nopConnPicker
}

func (satedConnPicker) Size() (int, int) { return 1, 0 }

func (f *nodeEventFixture) awaitRefresh(t *testing.T) {
	t.Helper()
	select {
	case <-f.refreshes:
	case <-time.After(nodeEventRefreshTimeout):
		t.Fatal("timed out waiting for a ring refresh")
	}
}

func (f *nodeEventFixture) assertNoRefresh(t *testing.T) {
	t.Helper()
	select {
	case <-f.refreshes:
		t.Fatal("ring refresh happened but should have been suppressed")
	case <-time.After(nodeEventQuietWindow):
	}
}

func statusChange(change, ip string) protocol.Frame {
	return &frm.StatusChangeEventFrame{Change: change, Host: net.ParseIP(ip), Port: 9042}
}

func topologyChange(change, ip string) protocol.Frame {
	return &frm.TopologyChangeEventFrame{Change: change, Host: net.ParseIP(ip), Port: 9042}
}

func assertStrings(t *testing.T, what string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d call(s) %v, want %d %v", what, len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: call %d was %q, want %q (got %v)", what, i, got[i], want[i], got)
		}
	}
}

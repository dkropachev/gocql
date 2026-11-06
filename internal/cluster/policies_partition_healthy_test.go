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
	"fmt"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/transport"
)

// stubConnPicker reports a fixed InFlight and panics on everything else, so a
// test that accidentally exercises more of the pool than partitionHealthy
// needs fails loudly rather than on a nil dereference.
type stubConnPicker struct {
	inFlight int
}

func (s *stubConnPicker) InFlight() int { return s.inFlight }

func (s *stubConnPicker) Put(*transport.Conn) error { panic("stubConnPicker.Put: not expected") }
func (s *stubConnPicker) Remove(*transport.Conn)    { panic("stubConnPicker.Remove: not expected") }
func (s *stubConnPicker) Size() (int, int)          { panic("stubConnPicker.Size: not expected") }
func (s *stubConnPicker) Close()                    { panic("stubConnPicker.Close: not expected") }
func (s *stubConnPicker) NextShard() (int, int) {
	panic("stubConnPicker.NextShard: not expected")
}
func (s *stubConnPicker) GetConnectionCount() int {
	panic("stubConnPicker.GetConnectionCount: not expected")
}
func (s *stubConnPicker) GetExcessConnectionCount() int {
	panic("stubConnPicker.GetExcessConnectionCount: not expected")
}
func (s *stubConnPicker) GetShardCount() int {
	panic("stubConnPicker.GetShardCount: not expected")
}

// sequenceConnPicker reports a different InFlight on successive calls, holding
// the last value once the sequence runs out. It exists to make a host look
// healthy when counted and busy when placed, which is the race
// partitionHealthy's snapshot is there to survive.
type sequenceConnPicker struct {
	stubConnPicker // the panicking methods; its InFlight is shadowed below
	counts         []int
	calls          int
}

func (s *sequenceConnPicker) InFlight() int {
	n := s.calls
	s.calls++
	if n >= len(s.counts) {
		n = len(s.counts) - 1
	}
	return s.counts[n]
}

// hostLabels names hosts by the index they started at, so a failure message
// reads as an order ("h1 h0") rather than as a pair of UUIDs. HostInfo.HostID
// renders the UUID, which is not what these tests are about, so the mapping is
// kept on the side and keyed by pointer.
type hostLabels map[*endpoint.HostInfo]string

func (l hostLabels) names(replicas []*endpoint.HostInfo) []string {
	out := make([]string, len(replicas))
	for i, h := range replicas {
		name, ok := l[h]
		if !ok {
			name = fmt.Sprintf("<unknown %p>", h)
		}
		out[i] = name
	}
	return out
}

// requireUsableThreshold fails fast when MAX_IN_FLIGHT_THRESHOLD cannot express
// a healthy host, which is the one way these fixtures break without this file
// changing.
//
// IsBusy is `InFlight() >= MAX_IN_FLIGHT_THRESHOLD`, so at or below zero every
// host counts as busy and no in-flight value means "idle". AvoidSlowReplicas
// assigns the global when its option is applied, and a parallel test resumes
// only once every sequential test has finished -- so a single sequential test
// building a policy with AvoidSlowReplicas(0) leaves every test here failing
// for a reason visible nowhere in this file. Reading the global rather than
// writing it keeps this file from being the cause; it does not make it immune.
func requireUsableThreshold(t *testing.T) {
	t.Helper()

	if MAX_IN_FLIGHT_THRESHOLD < 1 {
		t.Fatalf("MAX_IN_FLIGHT_THRESHOLD is %d, so IsBusy reports every host busy "+
			"and these fixtures cannot express a healthy one. Some other test in "+
			"this package applied AvoidSlowReplicas with that value; pass the "+
			"existing MAX_IN_FLIGHT_THRESHOLD back the way driver_config_test.go "+
			"does, rather than a literal.", MAX_IN_FLIGHT_THRESHOLD)
	}
}

// hostsWithPickers builds replicas backed by the given pickers, the session
// those lookups resolve against, and labels naming each host by its input
// position.
func hostsWithPickers(t *testing.T, pickers []pool.ConnPicker) ([]*endpoint.HostInfo, *Session, hostLabels) {
	t.Helper()
	requireUsableThreshold(t)

	pools := make(map[cqltype.UUID]*pool.InternalHostConnPool, len(pickers))
	replicas := make([]*endpoint.HostInfo, len(pickers))
	labels := make(hostLabels, len(pickers))

	for i, picker := range pickers {
		// A distinct valid UUID per host: getPool keys on it, so two hosts
		// sharing one would silently share a pool.
		host := endpoint.HostInfoBuilder{HostId: fmt.Sprintf("00000000-0000-0000-0000-%012d", i)}.Build()
		replicas[i] = &host
		labels[&host] = fmt.Sprintf("h%d", i)

		pools[implEndpoint.InternalHostInfoHostUUID(&host)] = &pool.InternalHostConnPool{
			InternalHost:       &host,
			InternalConnPicker: picker,
		}
	}

	return replicas, func() *Session {
		opaqueStorageValue := new(Session)
		SessionData(opaqueStorageValue).InternalPool = &pool.InternalPolicyConnPool{InternalHostConnPools: pools}
		return opaqueStorageValue

		// hostsWithInFlight builds replicas whose pools report the given, constant
		// in-flight counts.
	}(), labels
}

func hostsWithInFlight(t *testing.T, inFlight []int) ([]*endpoint.HostInfo, *Session, hostLabels) {
	t.Helper()

	pickers := make([]pool.ConnPicker, len(inFlight))
	for i, n := range inFlight {
		pickers[i] = &stubConnPicker{inFlight: n}
	}
	return hostsWithPickers(t, pickers)
}

// busyHosts builds replicas whose IsBusy(session) follows busy, the session
// those calls resolve against, and labels naming each host by its input
// position.
//
// IsBusy is `ok && h != nil && pool.InFlight() >= MAX_IN_FLIGHT_THRESHOLD`, so
// a healthy host gets a pool reporting 0 in flight and a busy one a pool well
// above the threshold. Tests that care about the threshold itself use
// hostsWithInFlight directly. The threshold is a package global; this file only
// reads it, and requireUsableThreshold above covers what that does not.
func busyHosts(t *testing.T, busy []bool) ([]*endpoint.HostInfo, *Session, hostLabels) {
	t.Helper()

	inFlight := make([]int, len(busy))
	for i, isBusy := range busy {
		if isBusy {
			inFlight[i] = MAX_IN_FLIGHT_THRESHOLD * 10
		}
	}
	return hostsWithInFlight(t, inFlight)
}

func equalNames(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (s *stubConnPicker) Pick(endpoint.Token) *transport.Conn {
	panic("stubConnPicker.Pick: not expected")
}

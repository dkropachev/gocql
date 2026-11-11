// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
	"fmt"
	"net"
	"testing"

	"log/slog"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/pool"
)

func TestHostSelectionPolicy(t *testing.T) {
	t.Run("COWList", func(t *testing.T) {
		t.Parallel()

		t.Run("Add", func(t *testing.T) {
			t.Parallel()

			var cow cowHostList

			toAdd := [...]net.IP{net.IPv4(10, 0, 0, 1), net.IPv4(10, 0, 0, 2), net.IPv4(10, 0, 0, 3)}

			for _, addr := range toAdd {
				if !cow.add(func() *endpoint.HostInfo {
					opaqueStorageValue := new(endpoint.HostInfo)
					implEndpoint.HostInfoData(opaqueStorageValue).InternalConnectAddress = addr
					return opaqueStorageValue
				}()) {
					t.Fatal("did not add peer which was not in the set")
				}
			}

			hosts := cow.get().allHosts()
			if len(hosts) != len(toAdd) {
				t.Fatalf("expected to have %d hosts got %d", len(toAdd), len(hosts))
			}

			set := make(map[string]bool)
			for _, host := range hosts {
				set[string(host.ConnectAddress())] = true
			}

			for _, addr := range toAdd {
				if !set[string(addr)] {
					t.Errorf("addr was not in the host list: %q", addr)
				}
			}
		})

		t.Run("AddAll", func(t *testing.T) {
			t.Parallel()

			var cow cowHostList
			hosts := makeHostInfoListTestHosts(hostInfoListMapThreshold)

			if !cow.addAll(hosts) {
				t.Fatal("did not add hosts which were not in the set")
			}
			if got := cow.get().InternalLen(); got != len(hosts) {
				t.Fatalf("len() = %d, want %d", got, len(hosts))
			}
			if cow.get().hostsByID == nil {
				t.Fatal("expected host ID map at threshold")
			}
			for _, host := range hosts {
				if got := cow.get().hostByID(implEndpoint.InternalHostInfoHostUUID(host)); got != host {
					t.Fatalf("hostByID(%s) = %v, want %v", host.HostID(), got, host)
				}
			}
			if cow.addAll(hosts) {
				t.Fatal("added duplicate hosts")
			}
		})
	})

	t.Run("HostInfoList", func(t *testing.T) {
		t.Parallel()

		t.Run("AllHostsClipsCapacity", func(t *testing.T) {
			t.Parallel()

			original := makeHostInfoListTestHosts(2)
			withSpareCapacity := make([]*endpoint.HostInfo, len(original), len(original)+1)
			copy(withSpareCapacity, original)

			hosts := newHostInfoList(withSpareCapacity)
			snapshot := hosts.allHosts()
			appended := append(snapshot, func() *endpoint.HostInfo {
				opaqueStorageValuex := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuex).InternalHostId = tUUID(100)
				return opaqueStorageValuex
			}())

			if len(snapshot) != len(original) {
				t.Fatalf("len(allHosts()) = %d, want %d", len(snapshot), len(original))
			}
			if len(appended) != len(original)+1 {
				t.Fatalf("len(appended) = %d, want %d", len(appended), len(original)+1)
			}
			if &snapshot[0] == &appended[0] {
				t.Fatal("append reused allHosts backing array")
			}
		})

		t.Run("HostByID", func(t *testing.T) {
			t.Parallel()

			tests := []struct {
				name    string
				hosts   []*endpoint.HostInfo
				wantMap bool
			}{
				{
					name:    "linear below threshold",
					hosts:   makeHostInfoListTestHosts(hostInfoListMapThreshold - 1),
					wantMap: false,
				},
				{
					name:    "map at threshold",
					hosts:   makeHostInfoListTestHosts(hostInfoListMapThreshold),
					wantMap: true,
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()

					hosts := newHostInfoList(tt.hosts)
					if (hosts.hostsByID != nil) != tt.wantMap {
						t.Fatalf("hostsByID presence = %v, want %v", hosts.hostsByID != nil, tt.wantMap)
					}
					if tt.wantMap {
						if hosts.hostIDs != nil {
							t.Fatalf("hostIDs = %v, want nil when hostsByID is used", hosts.hostIDs)
						}
					} else if len(hosts.hostIDs) != len(tt.hosts) {
						t.Fatalf("len(hostIDs) = %d, want %d", len(hosts.hostIDs), len(tt.hosts))
					}

					for _, host := range tt.hosts {
						if got := hosts.hostByID(implEndpoint.InternalHostInfoHostUUID(host)); got != host {
							t.Fatalf("hostByID(%s) = %v, want %v", host.HostID(), got, host)
						}
					}

					if got := hosts.hostByID(tUUID(9999)); got != nil {
						t.Fatalf("hostByID(missing) = %v, want nil", got)
					}
					if got := hosts.hostByID(cqltype.UUID{}); got != nil {
						t.Fatalf("hostByID(zero) = %v, want nil", got)
					}
				})
			}
		})
	})

	t.Run("HostSet", func(t *testing.T) {
		t.Run("Inline", func(t *testing.T) {
			var s hostSet
			hosts := make([]*endpoint.HostInfo, 9)
			for i := range hosts {
				hosts[i] = func() *endpoint.HostInfo {
					opaqueStorageValuexxxxxx := new(endpoint.HostInfo)
					return opaqueStorageValuexxxxxx
				}()

				s.add(hosts[i])
			}
			// All 9 should be tracked inline (no overflow map).
			if s.overflow != nil {
				t.Fatal("expected inline-only storage for 9 hosts")
			}
			for i, h := range hosts {
				if !s.contains(h) {
					t.Fatalf("host %d not found in inline set", i)
				}
			}
			// Unknown host should not be found.
			if s.contains(func() *endpoint.HostInfo {
				opaqueStorageValuexxxxxxx := new(endpoint.HostInfo)
				return opaqueStorageValuexxxxxxx
			}(),
			) {
				t.Fatal("unexpected contains=true for unknown host")
			}
		})

		t.Run("Overflow", func(t *testing.T) {
			var s hostSet
			hosts := make([]*endpoint.HostInfo, 15) // exceeds inline capacity of 9
			for i := range hosts {
				hosts[i] = func() *endpoint.HostInfo {
					opaqueStorageValuexxxxxxxx := new(endpoint.HostInfo)
					return opaqueStorageValuexxxxxxxx
				}()

				s.add(hosts[i])
			}
			// Should have spilled to map.
			if s.overflow == nil {
				t.Fatal("expected overflow map for 15 hosts")
			}
			// Every host must be found, including those added before and after spill.
			for i, h := range hosts {
				if !s.contains(h) {
					t.Fatalf("host %d not found after overflow", i)
				}
			}
			// Unknown host should not be found.
			if s.contains(func() *endpoint.HostInfo {
				opaqueStorageValuexxxxxxxxx := new(endpoint.HostInfo)
				return opaqueStorageValuexxxxxxxxx
			}(),
			) {
				t.Fatal("unexpected contains=true for unknown host in overflow mode")
			}
		})

		t.Run("OverflowPreservesInline", func(t *testing.T) {
			var s hostSet
			// Fill inline storage exactly.
			inline := make([]*endpoint.HostInfo, 9)
			for i := range inline {
				inline[i] = func() *endpoint.HostInfo {
					opaqueStorageValuexxxxxxxxxx := new(endpoint.HostInfo)
					return opaqueStorageValuexxxxxxxxxx
				}()

				s.add(inline[i])
			}
			// Add one more to trigger spill.
			extra := func() *endpoint.HostInfo {
				opaqueStorageValuexxxxxxxxxxx := new(endpoint.HostInfo)
				return opaqueStorageValuexxxxxxxxxxx
			}()

			s.add(extra)

			if s.overflow == nil {
				t.Fatal("expected overflow map after 10th add")
			}
			// Inline entries must be findable via the map path.
			for i, h := range inline {
				if !s.contains(h) {
					t.Fatalf("inline host %d lost after spill", i)
				}
			}
			if !s.contains(extra) {
				t.Fatal("extra host not found after spill")
			}
		})
	})

	t.Run("PartitionHealthy", func(t *testing.T) {
		t.Parallel()

		// TestHostSelectionPolicy/PartitionHealthy/AcrossBufferBoundary walks every size either side of the
		// stack buffer with the same alternating pattern. A fixed table can state the
		// boundary is covered; this states that nothing between 1 and 20 behaves
		// differently, which is what the two allocation paths are supposed to
		// guarantee.
		t.Run("AcrossBufferBoundary", func(t *testing.T) {
			t.Parallel()

			for n := 1; n <= 20; n++ {
				t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
					t.Parallel()

					busy := make([]bool, n)
					for i := range busy {
						busy[i] = i%2 == 0 // even indices busy, odd healthy
					}

					replicas, session, labels := busyHosts(t, busy)
					partitionHealthy(replicas, session)

					var want []string
					for i := range busy { // healthy, in input order
						if !busy[i] {
							want = append(want, fmt.Sprintf("h%d", i))
						}
					}
					for i := range busy { // then busy, in input order
						if busy[i] {
							want = append(want, fmt.Sprintf("h%d", i))
						}
					}

					if got := labels.names(replicas); !equalNames(got, want) {
						t.Errorf("partitionHealthy(n=%d) order = %v, want %v", n, got, want)
					}
				})
			}
		})

		// TestHostSelectionPolicy/PartitionHealthy/BusyThreshold pins the comparison the partition rests
		// on. AvoidSlowReplicas documents a replica as avoided when it has "equal or
		// more than MAX_IN_FLIGHT_THRESHOLD requests in flight", and IsBusy implements
		// that as `>=`. Nothing else pins which side of the boundary the threshold
		// itself falls on: the other tests in this file use 0 and 10x the threshold,
		// so relaxing `>=` to `>` leaves the whole unit suite green.
		//
		// Each case puts the host under test first and a plainly healthy or busy host
		// second, so a misclassification changes the resulting order. An arrangement
		// that happens to leave the input order intact could not tell the two
		// comparisons apart.
		t.Run("BusyThreshold", func(t *testing.T) {
			t.Parallel()

			threshold := MAX_IN_FLIGHT_THRESHOLD

			tests := []struct {
				name     string
				inFlight []int
				want     []string
			}{
				{
					// The boundary itself: at exactly the threshold h0 is busy, so it
					// sorts behind the idle h1. Under `>` it would stay in front.
					name:     "exactly at the threshold counts as busy",
					inFlight: []int{threshold, 0},
					want:     []string{"h1", "h0"},
				},
				{
					// One short of the threshold is healthy, so it sorts ahead of the
					// saturated h0.
					name:     "one below the threshold counts as healthy",
					inFlight: []int{threshold * 10, threshold - 1},
					want:     []string{"h1", "h0"},
				},
				{
					name:     "one above the threshold counts as busy",
					inFlight: []int{threshold + 1, 0},
					want:     []string{"h1", "h0"},
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()

					replicas, session, labels := hostsWithInFlight(t, tt.inFlight)
					partitionHealthy(replicas, session)

					if got := labels.names(replicas); !equalNames(got, tt.want) {
						t.Errorf("partitionHealthy() with in-flight %v order = %v, want %v",
							tt.inFlight, got, tt.want)
					}
				})
			}
		})

		// TestHostSelectionPolicy/PartitionHealthy/EmptyPool covers the path where no host resolves to a
		// pool at all: getPool misses, IsBusy is false for everyone, and the early
		// return for "all one category" leaves the slice untouched. This is the state
		// a freshly built session is in before any pool is filled.
		t.Run("EmptyPool", func(t *testing.T) {
			t.Parallel()

			replicas, _, labels := busyHosts(t, []bool{false, false, false, false})
			session := func() *Core {
				opaqueStorageValuex := new(Core)
				SessionData(opaqueStorageValuex).InternalPool = &pool.InternalPolicyConnPool{InternalHostConnPools: map[cqltype.UUID]*pool.InternalHostConnPool{}}
				return opaqueStorageValuex
			}()

			partitionHealthy(replicas, session)

			want := []string{"h0", "h1", "h2", "h3"}
			if got := labels.names(replicas); !equalNames(got, want) {
				t.Errorf("partitionHealthy() with empty pool order = %v, want %v", got, want)
			}
		})

		// TestHostSelectionPolicy/PartitionHealthy/Order pins the two properties partitionHealthy promises:
		// every healthy host precedes every busy one, and within each group the input
		// order survives. The sizes straddle the 9-element stack buffers the function
		// keeps for busy and tmp -- 9 takes the buffer, 10 takes the heap -- because
		// that boundary is the one place the two paths can disagree.
		t.Run("Order", func(t *testing.T) {
			t.Parallel()

			const bufSize = 9 // len(busyBuf)/len(buf) in partitionHealthy

			tests := []struct {
				name string
				busy []bool
				want []string
			}{
				{
					name: "empty",
					busy: nil,
					want: []string{},
				},
				{
					name: "single healthy",
					busy: []bool{false},
					want: []string{"h0"},
				},
				{
					name: "single busy",
					busy: []bool{true},
					want: []string{"h0"},
				},
				{
					name: "all healthy keeps order",
					busy: []bool{false, false, false},
					want: []string{"h0", "h1", "h2"},
				},
				{
					name: "all busy keeps order",
					busy: []bool{true, true, true},
					want: []string{"h0", "h1", "h2"},
				},
				{
					name: "busy first moves to back",
					busy: []bool{true, false},
					want: []string{"h1", "h0"},
				},
				{
					name: "interleaved is stable in both groups",
					busy: []bool{true, false, true, false, true, false},
					want: []string{"h1", "h3", "h5", "h0", "h2", "h4"},
				},
				{
					name: "only the last is healthy",
					busy: []bool{true, true, true, false},
					want: []string{"h3", "h0", "h1", "h2"},
				},
				{
					name: "only the first is busy at buffer size",
					busy: append([]bool{true}, make([]bool, bufSize-1)...),
					want: []string{"h1", "h2", "h3", "h4", "h5", "h6", "h7", "h8", "h0"},
				},
				{
					name: "one past the buffer takes the heap path",
					busy: []bool{true, false, true, false, true, false, true, false, true, false},
					want: []string{"h1", "h3", "h5", "h7", "h9", "h0", "h2", "h4", "h6", "h8"},
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()

					replicas, session, labels := busyHosts(t, tt.busy)
					partitionHealthy(replicas, session)

					if got := labels.names(replicas); !equalNames(got, tt.want) {
						t.Errorf("partitionHealthy() order = %v, want %v", got, tt.want)
					}
				})
			}
		})

		// TestHostSelectionPolicy/PartitionHealthy/PreservesSet guards the copy-back loop: replicas must
		// come out a permutation of what went in, with nothing dropped, duplicated or
		// left nil. The two indices hi and ui advance independently over one shared
		// slice, so an off-by-one there overwrites an entry instead of moving it --
		// which a pure order assertion on a correct healthyCount would not notice.
		t.Run("PreservesSet", func(t *testing.T) {
			t.Parallel()

			busy := []bool{true, false, false, true, true, false, true, false, false, true, true}
			replicas, session, labels := busyHosts(t, busy)

			before := make(map[string]int, len(replicas))
			for _, h := range replicas {
				before[labels[h]]++
			}

			partitionHealthy(replicas, session)

			after := make(map[string]int, len(replicas))
			for i, h := range replicas {
				if h == nil {
					t.Fatalf("partitionHealthy() left replicas[%d] nil", i)
				}
				after[labels[h]]++
			}

			if len(after) != len(before) {
				t.Fatalf("partitionHealthy() host set size = %d, want %d", len(after), len(before))
			}
			for name, count := range before {
				if after[name] != count {
					t.Errorf("partitionHealthy() count for %s = %d, want %d", name, after[name], count)
				}
			}
		})

		// TestHostSelectionPolicy/PartitionHealthy/UsesBusySnapshot pins the snapshot itself. partitionHealthy
		// samples IsBusy once per replica up front and the placement loop reads that
		// array, rather than asking again while it moves hosts around. Every other test
		// here uses pickers with a constant InFlight, under which a live re-read behaves
		// identically -- so replacing `busy[i]` with `h.IsBusy(s)` in the placement loop
		// leaves the whole unit suite green.
		//
		// h1 flips: idle when counted, saturated when placed. That is exactly the race
		// the snapshot exists for. healthyCount is fixed at 2 during counting, so ui
		// starts at index 2; a live re-read would classify h1 busy on the second pass
		// and push ui past the end of the slice.
		t.Run("UsesBusySnapshot", func(t *testing.T) {
			t.Parallel()

			flip := &sequenceConnPicker{counts: []int{0, MAX_IN_FLIGHT_THRESHOLD}}
			replicas, session, labels := hostsWithPickers(t, []pool.ConnPicker{
				&stubConnPicker{inFlight: MAX_IN_FLIGHT_THRESHOLD * 10}, // h0, busy throughout
				flip,                         // h1, healthy when counted, busy when placed
				&stubConnPicker{inFlight: 0}, // h2, healthy throughout
			})

			partitionHealthy(replicas, session)

			// The snapshot counted h1 as healthy, so it stays in the healthy group and
			// keeps its position ahead of h2. Only h0 moves.
			want := []string{"h1", "h2", "h0"}
			if got := labels.names(replicas); !equalNames(got, want) {
				t.Errorf("partitionHealthy() order = %v, want %v", got, want)
			}

			// The invariant stated directly: each replica's busyness is sampled once.
			// A second call means the placement loop consulted IsBusy again, which is
			// the bug even when the cursors happen not to run off the end.
			if flip.calls != 1 {
				t.Errorf("IsBusy sampled %d times for one replica, want 1; the placement "+
					"loop must read the snapshot rather than re-check", flip.calls)
			}
		})
	})

	// TestHostSelectionPolicy/SelectedHostTokenInt64 verifies the unboxed routing token: TokenInt64()
	// returns the raw int64, and Token() lazily boxes to a non-nil value so the
	// shard-aware invariant (a nil token would disable shard-aware picking) holds
	// for any caller, not just the raw fast path.
	t.Run("SelectedHostTokenInt64", func(t *testing.T) {
		t.Parallel()

		sh := int64SelectedHost{info: func() *endpoint.HostInfo {
			opaqueStorageValuexxxx := new(endpoint.HostInfo)
			return opaqueStorageValuexxxx
		}(), tokenCasted: endpoint.InternalInt64Token(42)}
		tok, ok := sh.TokenInt64()
		if !ok || tok != endpoint.InternalInt64Token(42) {
			t.Fatalf("TokenInt64() = %d, %v; want 42, true", tok, ok)
		}
		if got := sh.Token(); got == nil || got != endpoint.InternalInt64Token(42) {
			t.Fatalf("Token() = %v; want boxed int64Token(42)", got)
		}
		if got := sh.Token(); got == nil || got != endpoint.InternalInt64Token(42) {
			t.Fatalf("Token() (again) = %v; want boxed int64Token(42)", got)
		}

		shBoxed := selectedHost{info: func() *endpoint.HostInfo {
			opaqueStorageValuexxxxx := new(endpoint.HostInfo)
			return opaqueStorageValuexxxxx
		}(), token: endpoint.InternalInt64Token(7)}
		if tok, ok := shBoxed.TokenInt64(); ok || tok != 0 {
			t.Fatalf("TokenInt64() = %d, %v; want 0, false for a boxed-only host", tok, ok)
		}
		if got := shBoxed.Token(); got != endpoint.InternalInt64Token(7) {
			t.Fatalf("Token() = %v; want boxed int64Token(7)", got)
		}
	})

	t.Run("TokenAwareReset", func(t *testing.T) {
		t.Parallel()

		policy := TokenAwareHostPolicy(
			RackAwareRoundRobinPolicy("local", "b"),
			NonLocalReplicasFallback(),
		)
		policyInternal := policy.(*tokenAwareHostPolicy)

		if tokenAwareHostPolicyData(policyInternal).fallback == nil {
			t.Fatal("fallback is nil")
		}
		if !tokenAwareHostPolicyData(policyInternal).nonLocalReplicasFallback {
			t.Fatal("nonLocalReplicasFallback is false")
		}

		policy.Init(func() *Core {
			opaqueStorageValuexxx := new(Core)
			SessionData(opaqueStorageValuexxx).InternalLogger = slog.Default()
			return opaqueStorageValuexxx
		}())
		if tokenAwareHostPolicyData(policyInternal).getKeyspaceMetadata == nil {
			t.Fatal("keyspace metatadata fn is nil")
		}
		if tokenAwareHostPolicyData(policyInternal).getKeyspaceName == nil {
			t.Fatal("keyspace name fn is nil")
		}
		if tokenAwareHostPolicyData(policyInternal).logger == nil {
			t.Fatal("logger is nil")
		}

		// Reset - should reset fields that were set in Init
		policy.Reset()

		if tokenAwareHostPolicyData(policyInternal).fallback == nil { // we don't touch fallback
			t.Fatal("fallback is nil")
		}
		if !tokenAwareHostPolicyData(policyInternal).nonLocalReplicasFallback { // we don't touch nonLocalReplicasFallback
			t.Fatal("nonLocalReplicasFallback is false")
		}
		if tokenAwareHostPolicyData(policyInternal).getKeyspaceMetadata != nil {
			t.Fatal("keyspace metatadata fn is not nil")
		}
		if tokenAwareHostPolicyData(policyInternal).getKeyspaceName != nil {
			t.Fatal("keyspace name fn is not nil")
		}
		if tokenAwareHostPolicyData(policyInternal).logger != nil {
			t.Fatal("logger is nil")
		}
	})
}

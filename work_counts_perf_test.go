// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

//go:build gocql_perf

package gocql

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implCodec "github.com/scylladb/gocql/v2/internal/codec"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/paging"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/wire"
	"github.com/scylladb/gocql/v2/internal/wirecompress"
	routing "github.com/scylladb/gocql/v2/routing"
)

func TestWorkCountsPerf(t *testing.T) {
	t.Run("BuildTypedRequest", func(t *testing.T) {
		t.Run("CompressedFrameIdentity", func(t *testing.T) {
			data := bytes.Repeat([]byte{'q'}, 64<<10)
			source := codecTestMust(wire.NewChain(wire.Borrow(data[:7]), wire.Borrow(data[7:])))
			wirecompress.ResetPerfCounters()
			frame := codecTestMust(segment.EncodeSpans(source, true, true))
			calls, input, size, ptr := wirecompress.ReadPerfCounters()
			found := false
			frame.Each(func(s wire.Span) error {
				if uintptr(unsafe.Pointer(unsafe.SliceData(s.Bytes()))) == ptr && int64(s.Len()) == size {
					found = true
				}
				return nil
			})
			if !found || calls != 1 || input != int64(len(data)) {
				t.Fatal("segment copied compressor output", calls, input, size, found)
			}
			t.Logf("v5 transform_input=%d transformed_output=%d same_output_backing=true", input, size)
			for _, name := range []string{"lz4", "snappy"} {
				t.Run(name, func(t *testing.T) {
					c := &driverConn{InternalVersion: 4}
					if name == "lz4" {
						c.InternalCompressor = wirecompress.LZ4Compressor{}
					} else {
						c.InternalCompressor = wirecompress.SnappyCompressor{}
					}
					c.InternalInitFramerCache()
					values := typedTestChain(t, binary.BigEndian.AppendUint32(nil, uint32(len(data))), data)
					wirecompress.ResetPerfCounters()
					wire.ResetPerfCounters()
					frame := codecTestMust(c.InternalBuildTypedRequest(&attemptRequest{Statement: "INSERT INTO ks.tbl(v)VALUES(?)", Values: values, ValueCount: 1, Consistency: cql.One}, 0))
					calls, input, size, ptr := wirecompress.ReadPerfCounters()
					found := false
					frame.Each(func(s wire.Span) error {
						p := s.Bytes()
						start := uintptr(unsafe.Pointer(unsafe.SliceData(p)))
						if ptr >= start && ptr+uintptr(size) <= start+uintptr(len(p)) {
							found = true
						}
						return nil
					})
					if calls != 1 || !found || wire.ReadPerfCounters().CopyBytes != 0 {
						t.Fatal(fmt.Sprint(calls, input, size, found, wire.ReadPerfCounters()))
					}
					t.Logf("transform_input=%d transformed_output=%d same_output_backing=true source_gather_bytes=0", input, size)
				})
			}
		})
	})

	t.Run("PagingHistory", func(t *testing.T) {
		for _, count := range []int{1, 1000, 10000} {
			t.Run(fmt.Sprint(count), func(t *testing.T) {
				h := pagingHistory{InternalMaxCount: count + 4, InternalMaxBytes: 1 << 20}
				tokens := make([]PageState, count)
				for i := range tokens {
					tokens[i] = model.PageStateOwned(binary.BigEndian.AppendUint64(nil, uint64(i)), 1)
					if e := h.InternalAdd(tokens[i]); e != nil {
						t.Fatal(e)
					}
				}
				for _, index := range []int{0, count - 1, count / 2} {
					paging.ResetPerfCounters()
					if h.InternalAdd(tokens[index]) != cqlerr.ErrPagingNoProgress {
						t.Fatal("cycle")
					}
					c := paging.ReadPerfCounters()
					if c.HashBytes != 8 || c.EqualityChecks != 1 || c.Insertions != 0 {
						t.Fatal(c)
					}
					t.Logf("history=%d token=%d counts=%+v", count, index, c)
				}
				empty := model.PageStateOwned([]byte{}, 1)
				if e := h.InternalAdd(empty); e != nil {
					t.Fatal(e)
				}
				if h.InternalAdd(empty) != cqlerr.ErrPagingNoProgress {
					t.Fatal("empty")
				}
				collision := model.PageStateOwned([]byte("collision"), 1)
				h.InternalSpill()
				hash := maphash.Bytes(h.InternalSeed, model.PageToken(collision))
				h.InternalEntries[hash] = pagingHistoryEntry{InternalState: tokens[0]}
				paging.ResetPerfCounters()
				if e := h.InternalAdd(collision); e != nil {
					t.Fatal(e)
				}
				if h.InternalAdd(collision) != cqlerr.ErrPagingNoProgress {
					t.Fatal("collision equality")
				}
				c := paging.ReadPerfCounters()
				if c.HashBytes != 18 || c.EqualityChecks != 2 || c.Insertions != 1 {
					t.Fatal(c)
				}
				entry := h.InternalEntries[hash]
				if &model.PageToken(entry.InternalState)[0] != &model.PageToken(collision)[0] {
					t.Fatal("copied history token")
				}
				t.Logf("collision=%+v token_payload_bytes=%d", c, h.InternalBytes)
			})
		}
	})

	t.Run("Query", func(t *testing.T) {
		t.Run("OptionalWork", func(t *testing.T) {
			for _, mode := range []string{"disabled", "enabled", "default"} {
				enabled := mode == "enabled"
				t.Run(mode, func(t *testing.T) {
					s, conns, peers := runtimeTestSession(t, 1)
					observer := new(testObserver)
					profile := defaultProfile()
					profile.Prefetch = 0
					if enabled {
						profile.Speculation = speculation{}
					}
					handle := new(profile)
					options := Options{Profile: handle, Idempotent: true}
					if mode == "default" {
						options = Options{}
					}
					if enabled {
						options.Observer = observer
					}
					bound := runtimeQuery(t)
					defer bound.Close()
					done := make(chan struct{})
					go func() {
						defer close(done)
						for page := 0; page < 2; page++ {
							id := typedTestReadRequest(t, conns[0], peers[0])
							var state []byte
							if page == 0 {
								state = []byte("next")
							}
							peers[0].Write(typedTestResponse(t, 4, id, runtimeRowsBody(state, "value")))
						}
					}()
					implExecution.ResetPerfCounters()
					wire.ResetPerfCounters()
					rows := codecTestMust(Run(context.Background(), s, bound, bindOptions(s, options)))
					var row cql.Bytes
					for rows.NextInto(&row) {
					}
					if e := rows.Err(); e != nil {
						t.Fatal(e)
					}
					rows.Close()
					<-done
					c := implExecution.ReadPerfCounters()
					if !enabled && (c.RequestEvents != 0 || c.AttemptCallbacks != 0 || c.SpeculationSnapshots != 0 || c.SpeculationTimers != 0 || mode != "default" && c.PrefetchWorkers != 0) {
						t.Fatal(c)
					}
					if enabled && (c.RequestEvents != 1 || c.AttemptCallbacks != 2 || c.SpeculationSnapshots != 2 || c.SpeculationTimers != 2 || observer.requests.Load() != 1 || observer.attempts.Load() != 2) {
						t.Fatal(c)
					}
					if mode == "default" && c.PrefetchWorkers != 1 {
						t.Fatal("default prefetch demand", c)
					}
					t.Logf("mode=%s scheduler=%+v owner_transfers=%+v", mode, c, wire.ReadPerfCounters())
				})
			}
			// Warm prepared lookup remains usable while the discovery LRU lock is held;
			// unrelated schema events leave the captured table generation current.
			s := (*Session)(implExecution.InternalWrapSession(new(implCluster.Core)))
			implExecution.InternalSessionData((*implExecution.Session)(s)).InternalLogger = nopLogger{}
			cols := cql.NewColumns(cql.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "v", Type: cql.BlobType})
			_, hp := testPreparedState(t, s, cols)
			for i := 0; i < 1000; i++ {
				implCluster.InternalSessionInvalidatePreparedSchema(implExecution.InternalCore((*implExecution.Session)(s)), Event{Keyspace: "unrelated", Object: fmt.Sprint(i), Target: "TABLE"})
			}
			if !hp.InternalEpoch.InternalCurrent() {
				t.Fatal("unrelated invalidation")
			}
		})
	})

	t.Run("ResultPlanCache", func(t *testing.T) {
		t.Run("WarmAndChanged", func(t *testing.T) {
			cols := cql.NewColumns(cql.ColumnSpec{Name: "v", Type: cql.BlobType})
			entry := &typedResultMetadata{InternalColumns: cols}
			response := &responsePage{InternalMetadata: entry, InternalResponseData: responseData{Columns: cols}}
			cache := &resultPlanCache[cql.Bytes]{}
			var calls atomic.Int32
			decoder := countingDecoder{calls: &calls}
			if _, e := cache.InternalSelectPlan(response, decoder); e != nil {
				t.Fatal(e)
			}
			implCodec.ResetPerfCounters()
			for i := 0; i < 10000; i++ {
				if _, e := cache.InternalSelectPlan(response, decoder); e != nil {
					t.Fatal(e)
				}
			}
			comp, maps := implCodec.ReadPerfCounters()
			if comp != 0 || maps != 0 || calls.Load() != 1 || implCodec.ReadPerfTypeChecks() != 0 {
				t.Fatal(comp, maps, calls.Load())
			}
			changed := cql.NewColumns(cql.ColumnSpec{Name: "changed", Type: cql.BlobType})
			second := &responsePage{InternalMetadata: &typedResultMetadata{InternalColumns: changed}, InternalResponseData: responseData{Columns: changed}}
			if _, e := cache.InternalSelectPlan(second, decoder); e != nil {
				t.Fatal(e)
			}
			for i := 0; i < 10000; i++ {
				if _, e := cache.InternalSelectPlan(second, decoder); e != nil {
					t.Fatal(e)
				}
			}
			if calls.Load() != 2 || implCodec.ReadPerfTypeChecks() != 1 {
				t.Fatal("changed entry compiled repeatedly", calls.Load())
			}
			t.Logf("warm pages=10000 compiles=0 name_maps=0 changed_candidate_compiles=%d", calls.Load()-1)
		})
	})

	t.Run("Routing", func(t *testing.T) {
		t.Run("HostScaling", func(t *testing.T) {
			for _, hosts := range []int{3, 30, 300} {
				for _, kind := range []string{"ordinary", "pinned", "conditional", "leader", "latency", "fallback"} {
					t.Run(fmt.Sprintf("hosts%d/%s", hosts, kind), func(t *testing.T) {
						m := v2TestMetadata()
						storage := implRouting.SnapshotData(m)
						storage.InternalHosts = make([]routing.HostSnapshot, hosts)
						for i := range storage.InternalHosts {
							h := v2TestHost(0, "a", "r1")
							binary.BigEndian.PutUint64(h.ID[8:], uint64(i+1))
							storage.InternalHosts[i] = h
						}
						storage.InternalHostIndex = buildRoutingHostIndex(storage.InternalHosts)
						hint := testHint(t, 0, 100, cql.Option[uint64]{Set: true, Value: 1}, hTarget(1, 0), hTarget(2, 0), hTarget(3, 0))
						root, _ := updateTabletRange(nil, hint)
						storage.InternalPlacements = placementPut(storage.InternalPlacements, metadataTable{InternalKeyspace: "ks", InternalTable: "table"}, tabletPlacement{InternalRanges: root}, comparePlacementTable)
						info := routing.Info{Keyspace: "ks", Table: "table", Token: cql.Option[int64]{Set: true, Value: 1}, Consistency: cql.Quorum, Kind: cql.OrdinaryStatement}
						opts := routing.Options{TokenAware: true, Datacenter: "a", Rack: "r1", LeaderAware: kind == "leader"}
						if kind == "pinned" {
							info.HostID = cql.Option[cql.UUID]{Set: true, Value: storage.InternalHosts[0].ID}
						}
						if kind == "conditional" {
							info.Conditional = true
						}
						if kind == "leader" {
							storage.InternalKeyspaces["ks"].ConsistencyMode = "global"
						}
						if kind == "fallback" {
							for i := 0; i < 3; i++ {
								storage.InternalHosts[i].Connected = false
							}
						}
						if kind == "latency" {
							opts.Latency.Enabled = true
						}
						policy := codecTestMust(routing.NewPolicy(opts))
						if kind == "latency" {
							policy.(*routingPolicy).InternalDecisions.Store(&routingDecisions{InternalSlow: map[cql.UUID]time.Time{storage.InternalHosts[2].ID: time.Now().Add(time.Hour)}})
						}
						implRouting.ResetPerfCounters()
						plan := routing.NewPlan(policy, info, m)
						target, ok := plan.Next()
						if !ok {
							t.Fatal("no target")
						}
						c := implRouting.ReadPerfCounters()
						if c.HostIndexVisits != 0 || kind != "fallback" && c.FallbackVisits != 0 || kind != "fallback" && c.HostLookups > 3 {
							t.Fatal(c)
						}
						t.Logf("first=%v counters=%+v", target, c)
					})
				}
			}
		})
	})

	t.Run("Rows", func(t *testing.T) {
		// All workers retain exactly one old page and consume two fresh pages. Ordinary
		// row views add no owner transfers. Prefetch is one demand-bounded extra page.
		t.Run("RetainedPrefetchCharges", func(t *testing.T) {
			for _, workers := range []int{1, 4} {
				for _, prefetch := range []int{0, 1} {
					for _, cells := range []int{1, 1024} {
						t.Run(fmt.Sprintf("workers%d/prefetch%d/cells%d", workers, prefetch, cells), func(t *testing.T) {
							wire.ResetPerfCounters()
							wire.ResetPerfMemoryCounters()
							implExecution.ResetPerfCounters()
							errors := make(chan error, workers)
							done := make(chan struct{})
							var finished atomic.Int32
							for worker := 0; worker < workers; worker++ {
								go func() {
									s, cs, ps := runtimeTestSession(t, 1)
									bound := runtimeQuery(t)
									defer bound.Close()
									profile := defaultProfile()
									profile.Prefetch = prefetch
									options := Options{Profile: new(profile)}
									values := make([]string, cells)
									for i := range values {
										values[i] = "q"
									}
									peerDone := make(chan struct{})
									go func() {
										defer close(peerDone)
										for page := 0; page < 3; page++ {
											id := typedTestReadRequest(t, cs[0], ps[0])
											var token []byte
											if page < 2 {
												token = []byte{byte(page)}
											}
											ps[0].Write(typedTestResponse(t, 4, id, runtimeRowsBody(token, values...)))
										}
									}()
									rows, e := Run(context.Background(), s, bound, bindOptions(s, options))
									if e != nil {
										errors <- e
										return
									}
									var view cql.Bytes
									if !rows.NextInto(&view) {
										errors <- rows.Err()
										return
									}
									lease, e := rows.Retain()
									if e != nil {
										errors <- e
										return
									}
									count := 1
									var next cql.Bytes
									for rows.NextInto(&next) {
										count++
									}
									if rows.Err() != nil || count != 3*cells {
										errors <- fmt.Errorf("count %d err %v", count, rows.Err())
										return
									}
									rows.Close()
									if value, e := view.At(0); e != nil || value != 'q' {
										errors <- fmt.Errorf("retained page changed %d %v", value, e)
										return
									}
									lease.Close()
									<-peerDone
									errors <- nil
									if finished.Add(1) == int32(workers) {
										close(done)
									}
								}()
							}
							for i := 0; i < workers; i++ {
								if e := <-errors; e != nil {
									t.Fatal(e)
								}
							}
							<-done
							live, peak, slab := wire.ReadPerfMemoryCounters()
							c := wire.ReadPerfCounters()
							scheduler := implExecution.ReadPerfCounters()
							if live != 0 || c.Retains != int64(4*workers) {
								t.Fatal("per-cell owner churn or leak", live, c)
							}
							// Three received pages per worker plus one retained handle; prefetch never
							// exceeds the same total received pages and cannot pin arbitrary demand.
							maxBytes := int64(workers * 3 * (cells*5 + 128))
							if peak > maxBytes {
								t.Fatal("unbounded leased charge", peak, maxBytes)
							}
							if scheduler.PrefetchWorkers != int64(prefetch*workers) {
								t.Fatal(scheduler)
							}
							t.Logf("live_lease_charge=%d peak_lease_charge=%d scalar_slab_allocated=%d transfers=%+v optional=%+v", live, peak, slab, c, scheduler)
						})
					}
				}
			}
		})
	})
}

type testObserver struct{ requests, attempts atomic.Int64 }

func (o *testObserver) ObserveRequest(RequestEvent) { o.requests.Add(1) }
func (o *testObserver) ObserveAttempt(AttemptEvent) { o.attempts.Add(1) }

type speculation struct{}

func (speculation) Delay(int, routing.MetricsSnapshot) (time.Duration, bool) {
	return time.Hour, true
}

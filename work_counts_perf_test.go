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
	"github.com/scylladb/gocql/v2/event"
	"github.com/scylladb/gocql/v2/execopt"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/cqlperf"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/paging"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/segment"
	implSession "github.com/scylladb/gocql/v2/internal/session"
	"github.com/scylladb/gocql/v2/internal/wire"
	"github.com/scylladb/gocql/v2/internal/wirecompress"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/observe"
	"github.com/scylladb/gocql/v2/retry"
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
				tokens := make([]cql.PageState, count)
				for i := range tokens {
					tokens[i] = cqltype.PageStateOwned(binary.BigEndian.AppendUint64(nil, uint64(i)))
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
				empty := cqltype.PageStateOwned([]byte{})
				if e := h.InternalAdd(empty); e != nil {
					t.Fatal(e)
				}
				if h.InternalAdd(empty) != cqlerr.ErrPagingNoProgress {
					t.Fatal("empty")
				}
				collision := cqltype.PageStateOwned([]byte("collision"))
				h.InternalSpill()
				hash := maphash.Bytes(h.InternalSeed, cqltype.PageToken(collision))
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
				if &cqltype.PageToken(entry.InternalState)[0] != &cqltype.PageToken(collision)[0] {
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
					profile.Prefetch = false
					if enabled {
						profile.Speculation = speculation{}
					}
					handle := new(profile)
					options := execopt.Options{Profile: handle, Idempotent: cql.Some(true)}
					if mode == "default" {
						options = execopt.Options{}
					}
					if enabled {
						options.Observer = observe.Hooks{Request: observer.StartRequest, Attempt: observer.StartAttempt}
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
					implSession.ResetPerfCounters()
					wire.ResetPerfCounters()
					rows := codecTestMust(runtimeRun(s, bound, s.MustCompileOptions(options)))
					var row cql.Bytes
					for rows.NextInto(&row) {
					}
					if e := rows.Err(); e != nil {
						t.Fatal(e)
					}
					rows.Close()
					<-done
					c := implSession.ReadPerfCounters()
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
			s := (*testSession)(implSession.InternalWrapSession(new(implCluster.Core)))
			(*implSession.Session)(s).Data().InternalLogger = logging.Nop()
			cols := cql.NewColumns(cql.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "v", Type: cql.BlobType})
			hp := testHostPrepared(t, s, cols)
			for i := 0; i < 1000; i++ {
				(*implSession.Session)(s).Core().InvalidatePreparedSchema(event.Event{Keyspace: "unrelated", Object: fmt.Sprint(i), ObjectKind: event.ObjectTable})
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
			response := &responsePage{InternalMetadata: entry, ResponseData: responseData{Columns: cols}}
			cache := &resultPlanCache[cql.Bytes]{}
			var calls atomic.Int32
			decoder := countingDecoder{calls: &calls}
			if _, e := cache.InternalSelectPlan(response, decoder); e != nil {
				t.Fatal(e)
			}
			cqlperf.Reset()
			for i := 0; i < 10000; i++ {
				if _, e := cache.InternalSelectPlan(response, decoder); e != nil {
					t.Fatal(e)
				}
			}
			comp, maps := cqlperf.Counters()
			if comp != 0 || maps != 0 || calls.Load() != 1 || cqlperf.TypeChecks() != 0 {
				t.Fatal(comp, maps, calls.Load())
			}
			changed := cql.NewColumns(cql.ColumnSpec{Name: "changed", Type: cql.BlobType})
			second := &responsePage{InternalMetadata: &typedResultMetadata{InternalColumns: changed}, ResponseData: responseData{Columns: changed}}
			if _, e := cache.InternalSelectPlan(second, decoder); e != nil {
				t.Fatal(e)
			}
			for i := 0; i < 10000; i++ {
				if _, e := cache.InternalSelectPlan(second, decoder); e != nil {
					t.Fatal(e)
				}
			}
			if calls.Load() != 2 || cqlperf.TypeChecks() != 1 {
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
						storage := implRouting.SnapshotData(implRouting.SnapshotOf(m))
						storage.InternalHosts = make([]metadata.HostSnapshot, hosts)
						for i := range storage.InternalHosts {
							h := v2TestHost(0, "a", "r1")
							binary.BigEndian.PutUint64(h.HostID[8:], uint64(i+1))
							storage.InternalHosts[i] = h
						}
						storage.InternalHostIndex = buildRoutingHostIndex(storage.InternalHosts)
						hint := testHint(t, 0, 100, cql.Option[uint64]{Set: true, Value: 1}, hTarget(1, 0), hTarget(2, 0), hTarget(3, 0))
						root, _ := updateTabletRange(nil, hint)
						storage.InternalPlacements = storage.InternalPlacements.Put(metadataTable{InternalKeyspace: "ks", InternalTable: "table"}, tabletPlacement{InternalRanges: root})
						info := routing.Info{Keyspace: "ks", Table: "table", Token: cql.Option[int64]{Set: true, Value: 1}, Consistency: cql.Quorum, Kind: cql.OrdinaryStatement}
						opts := routing.Options{Datacenter: "a", Rack: "r1", NoLeaderAware: kind != "leader", FixedReplicaOrder: true}
						if kind == "pinned" {
							info.HostID = cql.Option[cql.UUID]{Set: true, Value: storage.InternalHosts[0].HostID}
						}
						if kind == "conditional" {
							info.Kind = cql.ConditionalStatement
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
						policy := routing.NewPolicy(opts)
						if kind == "latency" {
							builtinPolicy(policy).InternalDecisions.Store(&routingDecisions{InternalSlow: map[cql.UUID]time.Time{storage.InternalHosts[2].HostID: time.Now().Add(time.Hour)}})
						}
						implRouting.ResetPerfCounters()
						plan := implRouting.PlanFor(policy, info, m)
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
							implSession.ResetPerfCounters()
							errors := make(chan error, workers)
							done := make(chan struct{})
							var finished atomic.Int32
							for worker := 0; worker < workers; worker++ {
								go func() {
									s, cs, ps := runtimeTestSession(t, 1)
									bound := runtimeQuery(t)
									defer bound.Close()
									profile := defaultProfile()
									profile.Prefetch = prefetch == 1
									options := execopt.Options{Profile: new(profile)}
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
									rows, e := runtimeRun(s, bound, s.MustCompileOptions(options))
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
							scheduler := implSession.ReadPerfCounters()
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

func (o *testObserver) ObserveRequest(observe.RequestEvent) { o.requests.Add(1) }
func (o *testObserver) ObserveAttempt(routing.AttemptEvent) { o.attempts.Add(1) }

func (o *testObserver) StartRequest(ctx context.Context, _ observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
	return ctx, o.ObserveRequest
}
func (o *testObserver) StartAttempt(context.Context, routing.AttemptStart) func(routing.AttemptEvent) {
	return o.ObserveAttempt
}

type speculation struct{}

func (speculation) Delay(retry.Speculation) (time.Duration, bool) {
	return time.Hour, true
}

// runtimeQuery binds an unprepared read of ks.tbl. The fake peers answer it
// without a PREPARE exchange, which the public API always makes.
func runtimeQuery(t testing.TB) *implSession.Bound[cql.Bytes] {
	t.Helper()
	statement := cql.Statement[cql.EmptyParams, cql.Bytes]{Kind: cql.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: cql.NoParams, Rows: cql.OneColumn(cql.BytesViewCodec)}
	b, err := implSession.BindUnpreparedBorrowed(internalStatement(statement), cql.NewColumns(), cql.EmptyParams{})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func runtimeRun(s *testSession, bound *implSession.Bound[cql.Bytes], options execopt.CompiledOptions) (implSession.Rows[cql.Bytes], error) {
	return implSession.Run(context.Background(), (*implSession.Session)(s).Core(), bound, implSession.CompiledOptions(options))
}

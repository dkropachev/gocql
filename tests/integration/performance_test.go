//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	implSession "github.com/scylladb/gocql/v2/internal/session"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cql/stream"
	"github.com/scylladb/gocql/v2/endpoint"
)

func TestPerformance(t *testing.T) {
	// This diagnostic is deliberately separate from the unchanged normal latency,
	// CPU and baseline harness. Every worker is parked after its first complete
	// validated final output while its cursor still owns its current response. A
	// fail-safe context releases and joins every worker before owned schema cleanup.
	t.Run("ConcurrentMemory", func(t *testing.T) {
		out := os.Getenv("GOCQL_PERF_OUTPUT")
		if out == "" {
			t.Skip("explicit performance diagnostic runner only")
		}
		if e := os.MkdirAll(out, 0755); e != nil {
			t.Fatal(e)
		}
		runtime.GC()
		initial := mem().HeapAlloc
		config := createV2Cluster(func(c *gocql.ConfigBuilder) {
			c.WithPoolSize(gocql.PerHost(8)).WithHeartbeat(0, 0).WithMetadataRefresh(0)
		})
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()
		s, e := gocql.Connect[testDialect](ctx, mustBuild(t, config))
		if e != nil {
			t.Fatal(e)
		}
		defer s.Close()
		ks := testKeyspaceName(t, fmt.Sprintf("%x", time.Now().UnixNano()))
		exec := func(stmt string) {
			t.Helper()
			if _, e := s.UnpreparedExec(ctx, cql.Statement[cql.EmptyParams, cql.EmptyRow]{CQL: stmt, Kind: cql.OrdinaryStatement, Params: cql.EmptyParamsEncoder, Rows: cql.EmptyRowDecoder}, cql.EmptyParams{}); e != nil {
				t.Fatal(e)
			}
		}
		exec("CREATE KEYSPACE " + ks + " WITH replication={'class':'NetworkTopologyStrategy','replication_factor':1}")
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			_, e := s.UnpreparedExec(cleanup, cql.Statement[cql.EmptyParams, cql.EmptyRow]{CQL: "DROP KEYSPACE " + ks, Kind: cql.OrdinaryStatement, Params: cql.EmptyParamsEncoder, Rows: cql.EmptyRowDecoder}, cql.EmptyParams{})
			if e != nil {
				t.Error(e)
			}
		}()
		exec("CREATE TABLE " + ks + ".records(cell int,id int,payload blob,pieces list<blob>,PRIMARY KEY(cell,id))")
		insert := codecTestMust(s.Prepare(ctx, cql.Statement[testInput, cql.EmptyRow]{CQL: "INSERT INTO " + ks + ".records(cell,id,payload,pieces)VALUES(?,?,?,?)", Kind: cql.OrdinaryStatement, Params: cql.Params(cql.Param("cell", cql.Int32Codec, func(v testInput) int32 { return v.Cell }), cql.Param("id", cql.Int32Codec, func(v testInput) int32 { return v.ID }), cql.Param("payload", cql.ByteSliceCodec, func(v testInput) []byte { return v.Payload }), cql.Param("pieces", cql.ListEncoder(cql.ByteSliceCodec), func(v testInput) [][]byte { return v.Pieces })), Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{}))
		// Only one 4KiB source exists locally. All source writes drain before the
		// fixed-session baseline, and source payload/nested buffers are then released.
		func() {
			payload := bytes.Repeat([]byte{0x71}, 4096)
			for id := int32(0); id < 128; id++ {
				if _, e := insert.Exec(ctx, testInput{4096, id, payload, [][]byte{[]byte("first"), []byte("second")}}); e != nil {
					t.Fatal(e)
				}
			}
		}()
		decoder := cql.Row(cql.Column("id", cql.Int32Codec, func(v *testBorrowed) *int32 { return &v.ID }), cql.Column("payload", cql.BlobCodec, func(v *testBorrowed) *cql.Bytes { return &v.Payload }), cql.Column("pieces", cql.ListDecoder(cql.BlobCodec), func(v *testBorrowed) *[]cql.Bytes { return &v.Pieces }))
		prepared := codecTestMust(s.Prepare(ctx, cql.Statement[cql.EmptyParams, testBorrowed]{CQL: "SELECT id,payload,pieces FROM " + ks + ".records WHERE cell=4096 AND id<128", Kind: cql.OrdinaryStatement, Params: cql.EmptyParamsEncoder, Rows: decoder}, gocql.PrepareOptions{}))
		streamDecoder := stream.RowInto(func(*testOutput) {}, stream.Column("id", stream.Int32, func(v *testOutput) *int32 { return &v.ID }), stream.Column("payload", stream.Blob, func(v *testOutput) *[]byte { return &v.Payload }), stream.Column("pieces", stream.List(stream.Blob), func(v *testOutput) *[][]byte { return &v.Pieces }))
		streamPrepared := codecTestMust(s.PrepareStream(ctx, stream.Statement[cql.EmptyParams, testOutput]{CQL: "SELECT id,payload,pieces FROM " + ks + ".records WHERE cell=4096 AND id<128", Kind: cql.OrdinaryStatement, Params: cql.EmptyParamsEncoder, Rows: streamDecoder}))
		runtime.GC()
		fixed := mem().HeapAlloc
		records := []concurrentMemory{}
		for _, workers := range []int{1, 4, 8} {
			for _, api := range []string{"into", "stream"} {
				if api == "stream" && *flagProto < 5 && *flagCompressTest != "no-compression" && *flagCompressTest != "" {
					continue
				}
				t.Run(fmt.Sprintf("%s/workers%d", api, workers), func(t *testing.T) {
					caseCtx, caseCancel := context.WithTimeout(ctx, 30*time.Second)
					defer caseCancel()
					release := make(chan struct{})
					var releaseOnce sync.Once
					unpark := func() { releaseOnce.Do(func() { close(release) }) }
					defer unpark()
					ready := make(chan error, workers)
					done := make(chan error, workers)
					outputs := make([]testOutput, workers)
					for i := range outputs {
						outputs[i] = testOutput{Payload: make([]byte, 0, 4096), Pieces: [][]byte{make([]byte, 0, 5), make([]byte, 0, 6)}}
					}
					runtime.GC()
					before := mem()
					r := concurrentMemory{API: api, Workers: workers, Rows: 128, Cell: 4096, Page: 128, RuntimeHeapBeforeConnect: initial, FixedSessionHeap: fixed, IdleHeapWithApplication: before.HeapAlloc, ApplicationStorageBytes: int64(workers * (4096 + 11))}
					r.IdleCacheBytes, r.IdleCacheObjects = (*implSession.Session)(s).Data().InternalConnCfg.InternalReceiveCache.Storage()
					var peak atomic.Uint64
					monitorStop := make(chan struct{})
					monitorDone := make(chan struct{})
					go func() {
						defer close(monitorDone)
						ticker := time.NewTicker(100 * time.Microsecond)
						defer ticker.Stop()
						for {
							select {
							case <-monitorStop:
								return
							case <-ticker.C:
								m := mem()
								for old := peak.Load(); m.HeapAlloc > old; old = peak.Load() {
									if peak.CompareAndSwap(old, m.HeapAlloc) {
										break
									}
								}
							}
						}
					}()
					for worker := range outputs {
						go func(worker int) {
							parked := false
							count := 0
							dst := &outputs[worker]
							complete := func() error {
								if dst.ID != int32(count) || len(dst.Payload) != 4096 || bytes.Count(dst.Payload, []byte{0x71}) != 4096 || len(dst.Pieces) != 2 || string(dst.Pieces[0]) != "first" || string(dst.Pieces[1]) != "second" {
									return fmt.Errorf("unequal final output worker%d row%d", worker, count)
								}
								count++
								if !parked {
									parked = true
									ready <- nil
									select {
									case <-release:
									case <-caseCtx.Done():
										return caseCtx.Err()
									}
								}
								return nil
							}
							run := func() error {
								options := gocql.Options{PageSize: cql.Option[int32]{Set: true, Value: 128}, Prefetch: cql.Option[int]{Set: true, Value: 0}}
								if api == "stream" {
									query, e := streamPrepared.Query(gocql.StreamOptions{Incremental: true, Options: options})
									if e != nil {
										return e
									}
									defer query.Close()
									cursor, e := query.Run(caseCtx, cql.EmptyParams{})
									if e != nil {
										return e
									}
									defer cursor.Close()
									for cursor.NextInto(dst) {
										if e = complete(); e != nil {
											return e
										}
									}
									if e = cursor.Err(); e != nil {
										return e
									}
									return cursor.Close()
								}
								rows, e := prepared.Run(caseCtx, cql.EmptyParams{}, bindOptions(s, options))
								if e != nil {
									return e
								}
								defer rows.Close()
								var view testBorrowed
								for rows.NextInto(&view) {
									dst.ID = view.ID
									dst.Payload = dst.Payload[:4096]
									if _, e = view.Payload.CopyTo(dst.Payload); e != nil {
										return e
									}
									index := 0
									for _, v := range view.Pieces {
										if index >= 2 {
											return fmt.Errorf("extra nested output")
										}
										dst.Pieces[index] = dst.Pieces[index][:int(v.Len())]
										if _, e = v.CopyTo(dst.Pieces[index]); e != nil {
											return e
										}
										index++
									}
									if index != 2 {
										return fmt.Errorf("short nested output")
									}
									if e = complete(); e != nil {
										return e
									}
								}
								if e = rows.Err(); e != nil {
									return e
								}
								return rows.Close()
							}
							e := run()
							if e == nil && count != 128 {
								e = fmt.Errorf("worker%d rows=%d", worker, count)
							}
							if !parked {
								ready <- e
							}
							done <- e
						}(worker)
					}
					var firstErr error
					for i := 0; i < workers; i++ {
						select {
						case e := <-ready:
							if e != nil && firstErr == nil {
								firstErr = e
							}
						case <-caseCtx.Done():
							firstErr = caseCtx.Err()
						}
						if firstErr != nil {
							caseCancel()
							break
						}
					}
					if firstErr == nil {
						runtime.GC()
						parked := mem()
						r.AllParkedPostGCHeap = parked.HeapAlloc
						r.ParkedCacheBytes, r.ParkedCacheObjects = (*implSession.Session)(s).Data().InternalConnCfg.InternalReceiveCache.Storage()
						r.AllParkedStackBytes = parked.StackInuse
						r.WorkerAllocatedBytes = parked.TotalAlloc - before.TotalAlloc
						r.WorkerAllocations = parked.Mallocs - before.Mallocs
					}
					unpark()
					// Completion is joined even after a failed park; context cancellation closes
					// in-progress socket reads and prevents a goroutine from pinning fixture data.
					for i := 0; i < workers; i++ {
						select {
						case e := <-done:
							if e != nil && firstErr == nil {
								firstErr = e
							}
						case <-time.After(35 * time.Second):
							t.Fatal("diagnostic worker did not join")
						}
					}
					close(monitorStop)
					<-monitorDone
					r.PeakSampledHeap = peak.Load()
					runtime.GC()
					r.PostClosePostGCHeap = mem().HeapAlloc
					r.PostCloseCacheBytes, r.PostCloseCacheObjects = (*implSession.Session)(s).Data().InternalConnCfg.InternalReceiveCache.Storage()
					r.InclusiveDriverAndHarnessBytes = int64(r.AllParkedPostGCHeap) - int64(initial) - r.ApplicationStorageBytes
					r.IncrementalOverWarmIdleBytes = int64(r.AllParkedPostGCHeap) - int64(r.IdleHeapWithApplication)
					if firstErr != nil {
						t.Fatal(firstErr)
					}
					for i := range outputs {
						if outputs[i].ID != 127 || bytes.Count(outputs[i].Payload, []byte{0x71}) != 4096 {
							t.Fatal("output changed after join")
						}
					}
					records = append(records, r)
					data, _ := json.Marshal(r)
					t.Log(string(data))
					runtime.KeepAlive(outputs)
				})
			}
		}
		data, e := json.MarshalIndent(records, "", "  ")
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(out, "concurrent-memory.json"), append(data, '\n'), 0644); e != nil {
			t.Fatal(e)
		}
	})

	// This gate performs identical application work: every API fills the same
	// reusable output shape, validates all payload bytes/nested elements and scalar
	// ordinals. Borrowed APIs' explicit application copies are counted separately.
	// Latency/CPU runs never force GC. A separate diagnostic forces GC while the
	// cursor remains live, and samples HeapAlloc at 100us; it reports both metrics
	// without labeling uncollected garbage as retained/live driver memory.
	t.Run("Latency", func(t *testing.T) {
		outputDir := os.Getenv("GOCQL_PERF_OUTPUT")
		if outputDir == "" {
			t.Skip("explicit performance runner only")
		}
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			t.Fatal(err)
		}
		stats := new(testIO)
		// Quiescent endpoint calibration: the first snapshot precedes connection
		// creation, so these process syscall deltas contain only counter/harness I/O.
		cr, cw, _, _ := stats.snapshot()
		calCPU := testCPU()
		calMem := mem()
		calStart := time.Now()
		for i := 0; i < 64; i++ {
			stats.snapshot()
		}
		calEnd := mem()
		calibration := map[string]int64{"iterations": 64, "wall_ns": time.Since(calStart).Nanoseconds(), "cpu_ns": testCPU() - calCPU, "allocated_bytes": int64(calEnd.TotalAlloc - calMem.TotalAlloc)}
		ar, aw, _, _ := stats.snapshot()
		calibration["process_read_syscalls"] = ar - cr
		calibration["process_write_syscalls"] = aw - cw
		calibrationBytes, _ := json.MarshalIndent(calibration, "", "  ")
		if e := os.WriteFile(filepath.Join(outputDir, "counter-calibration.json"), append(calibrationBytes, '\n'), 0644); e != nil {
			t.Fatal(e)
		}
		config := createV2Cluster(func(c *gocql.ConfigBuilder) {
			c.WithPoolSize(gocql.PerShard(1)).
				WithAdvancedShardAwareness(32768, 65535).
				WithHeartbeat(30*time.Second, 30*time.Second).
				WithMetadataRefresh(0)
			c.WithDialer(endpoint.DialerFunc(func(ctx context.Context, target endpoint.DialTarget) (net.Conn, error) {
				conn, err := (endpoint.DefaultDialer{}).DialContext(ctx, target)
				if err != nil {
					return nil, err
				}
				stats.add(conn)
				return conn, nil
			}))
		})
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
		defer cancel()
		runtime.GC()
		baseRuntime := mem().HeapAlloc
		s, err := gocql.Connect[testDialect](ctx, mustBuild(t, config))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		time.Sleep(1100 * time.Millisecond)
		runtime.GC()
		sessionHeap := mem().HeapAlloc
		ks := testKeyspaceName(t, fmt.Sprintf("%x", time.Now().UnixNano()))
		exec := func(stmt string) {
			t.Helper()
			if _, e := s.UnpreparedExec(ctx, cql.Statement[cql.EmptyParams, cql.EmptyRow]{CQL: stmt, Kind: cql.OrdinaryStatement, Params: cql.EmptyParamsEncoder, Rows: cql.EmptyRowDecoder}, cql.EmptyParams{}); e != nil {
				t.Fatal(e)
			}
		}
		exec("CREATE KEYSPACE " + ks + " WITH replication={'class':'NetworkTopologyStrategy','replication_factor':1}")
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			_, e := s.UnpreparedExec(cleanup, cql.Statement[cql.EmptyParams, cql.EmptyRow]{CQL: "DROP KEYSPACE " + ks, Kind: cql.OrdinaryStatement, Params: cql.EmptyParamsEncoder, Rows: cql.EmptyRowDecoder}, cql.EmptyParams{})
			if e != nil {
				t.Error("owned cleanup", e)
			}
		}()
		exec("CREATE TABLE " + ks + ".records(cell int,id int,payload blob,pieces list<blob>,PRIMARY KEY(cell,id))")
		insert := codecTestMust(s.Prepare(ctx, cql.Statement[testInput, cql.EmptyRow]{CQL: "INSERT INTO " + ks + ".records(cell,id,payload,pieces)VALUES(?,?,?,?)", Kind: cql.OrdinaryStatement, Params: cql.Params(cql.Param("cell", cql.Int32Codec, func(v testInput) int32 { return v.Cell }), cql.Param("id", cql.Int32Codec, func(v testInput) int32 { return v.ID }), cql.Param("payload", cql.ByteSliceCodec, func(v testInput) []byte { return v.Payload }), cql.Param("pieces", cql.ListEncoder(cql.ByteSliceCodec), func(v testInput) [][]byte { return v.Pieces })), Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{}))
		for _, size := range []int{32, 4096, 256 << 10, 8 << 20} {
			count := 128
			if size >= 256<<10 {
				count = 1
			}
			payload := bytes.Repeat([]byte{0x71}, size)
			for i := 0; i < count; i++ {
				if _, e := insert.Exec(ctx, testInput{int32(size), int32(i), payload, [][]byte{[]byte("first"), []byte("second")}}); e != nil {
					t.Fatal(e)
				}
			}
		}
		borrowedDecoder := cql.Row(cql.Column("id", cql.Int32Codec, func(v *testBorrowed) *int32 { return &v.ID }), cql.Column("payload", cql.BlobCodec, func(v *testBorrowed) *cql.Bytes { return &v.Payload }), cql.Column("pieces", cql.ListDecoder(cql.BlobCodec), func(v *testBorrowed) *[]cql.Bytes { return &v.Pieces }))
		prepared := codecTestMust(s.Prepare(ctx, cql.Statement[testQuery, testBorrowed]{CQL: "SELECT id,payload,pieces FROM " + ks + ".records WHERE cell=? AND id<?", Kind: cql.OrdinaryStatement, Params: cql.Params(cql.Param("cell", cql.Int32Codec, func(v testQuery) int32 { return v.Cell }), cql.Param("id", cql.Int32Codec, func(v testQuery) int32 { return v.Rows })), Rows: borrowedDecoder}, gocql.PrepareOptions{}))
		streamDecoder := stream.RowInto(func(*testOutput) {}, stream.Column("id", stream.Int32, func(v *testOutput) *int32 { return &v.ID }), stream.Column("payload", stream.Blob, func(v *testOutput) *[]byte { return &v.Payload }), stream.Column("pieces", stream.List(stream.Blob), func(v *testOutput) *[][]byte { return &v.Pieces }))
		streamPrepared := codecTestMust(s.PrepareStream(ctx, stream.Statement[testQuery, testOutput]{CQL: "SELECT id,payload,pieces FROM " + ks + ".records WHERE cell=? AND id<?", Kind: cql.OrdinaryStatement, Params: cql.Params(cql.Param("cell", cql.Int32Codec, func(v testQuery) int32 { return v.Cell }), cql.Param("id", cql.Int32Codec, func(v testQuery) int32 { return v.Rows })), Rows: streamDecoder}))
		shapes := []testShape{{"small", 1, 32, 1, 1, false}, {"page1", 128, 4096, 1, 1, false}, {"page32", 128, 4096, 32, 1, false}, {"rows1", 1, 4096, 128, 1, false}, {"rows128", 128, 4096, 128, 1, false}, {"cell256K", 1, 256 << 10, 1, 1, false}, {"cell8M", 1, 8 << 20, 1, 1, false}, {"concurrent", 128, 4096, 32, 4, false}, {"slow", 128, 4096, 32, 1, true}}
		runtime.GC()
		setupHeap := mem().HeapAlloc
		results := []testResult{}
		var measureMu sync.Mutex
		for _, shape := range shapes {
			for _, api := range []string{"into", "stream"} {
				if api == "stream" && *flagProto < 5 && *flagCompressTest != "no-compression" && *flagCompressTest != "" {
					continue
				}
				t.Run(shape.Name+"/"+api, func(t *testing.T) {
					value := testQuery{int32(shape.Cell), int32(shape.Rows)}
					outputs := make([]testOutput, shape.Concurrency)
					for i := range outputs {
						outputs[i] = testOutput{Payload: make([]byte, 0, shape.Cell), Pieces: [][]byte{make([]byte, 0, 5), make([]byte, 0, 6)}}
					}
					options := gocql.Options{PageSize: cql.Option[int32]{Set: true, Value: int32(shape.Page)}, Prefetch: cql.Option[int]{Set: true, Value: 0}}
					run := func(worker int, sample func()) (int64, int64, error) {
						dst := &outputs[worker]
						backing := unsafe.SliceData(dst.Payload[:cap(dst.Payload)])
						start := time.Now()
						first := int64(0)
						n := 0
						validate := func() error {
							if dst.ID != int32(n) || len(dst.Payload) != shape.Cell || bytes.Count(dst.Payload, []byte{0x71}) != shape.Cell || len(dst.Pieces) != 2 || string(dst.Pieces[0]) != "first" || string(dst.Pieces[1]) != "second" || unsafe.SliceData(dst.Payload) != backing {
								return fmt.Errorf("unequal output id=%d row=%d payload=%d", dst.ID, n, len(dst.Payload))
							}
							n++
							if n == 1 {
								first = time.Since(start).Nanoseconds()
							}
							if sample != nil {
								sample()
							}
							if shape.Slow {
								time.Sleep(time.Millisecond)
							}
							return nil
						}
						if api == "stream" {
							query, e := streamPrepared.Query(gocql.StreamOptions{Incremental: true, Options: options})
							if e != nil {
								return 0, 0, e
							}
							defer query.Close()
							cursor, e := query.Run(ctx, value)
							if e != nil {
								return 0, 0, e
							}
							defer cursor.Close()
							for cursor.NextInto(dst) {
								if e = validate(); e != nil {
									return 0, 0, e
								}
							}
							if cursor.Err() != nil {
								return 0, 0, cursor.Err()
							}
							if e = cursor.Close(); e != nil {
								return 0, 0, e
							}
						} else {
							rows, e := prepared.Run(ctx, value, bindOptions(s, options))
							if e != nil {
								return 0, 0, e
							}
							defer rows.Close()
							var view testBorrowed
							for rows.NextInto(&view) {
								dst.ID = view.ID
								dst.Payload = dst.Payload[:shape.Cell]
								if _, e = view.Payload.CopyTo(dst.Payload); e != nil {
									return 0, 0, e
								}
								index := 0
								for _, v := range view.Pieces {
									if index >= 2 {
										return 0, 0, fmt.Errorf("extra nested value")
									}
									dst.Pieces[index] = dst.Pieces[index][:int(v.Len())]
									if _, e = v.CopyTo(dst.Pieces[index]); e != nil {
										return 0, 0, e
									}
									index++
								}
								if index != 2 {
									return 0, 0, fmt.Errorf("short nested list: %d", index)
								}
								if e = validate(); e != nil {
									return 0, 0, e
								}
							}
							if rows.Err() != nil {
								return 0, 0, rows.Err()
							}
							if e = rows.Close(); e != nil {
								return 0, 0, e
							}
						}
						if n != shape.Rows {
							return 0, 0, fmt.Errorf("rows=%d wanted=%d", n, shape.Rows)
						}
						return first, time.Since(start).Nanoseconds(), nil
					}
					for worker := range outputs {
						for warm := 0; warm < 2; warm++ {
							if _, _, e := run(worker, nil); e != nil {
								t.Fatal(e)
							}
						}
					}
					perWorker := 32
					if shape.Slow {
						perWorker = 8
					}
					r := testResult{Shape: shape, API: api, Operations: perWorker * shape.Concurrency, Protocol: *flagProto, TLS: *flagRunSslTest, Compression: *flagCompressTest, ApplicationStorageBytes: int64(shape.Concurrency * (shape.Cell + 11)), Sampling: "100us HeapAlloc during separate diagnostic; post-GC at every row and post-Close; inclusive driver+harness delta uses pre-Connect runtime baseline and separately seeded output; warm idle retained pools included; no process syscall overhead subtraction"}
					if api != "stream" {
						r.ApplicationCopyBytes = int64(r.Operations * shape.Rows * (shape.Cell + 11))
					}
					runtime.GC()
					reads, writes, rb, wb := stats.snapshot()
					before := mem()
					startCPU := testCPU()
					start := time.Now()
					firsts, ends := make([]int64, 0, r.Operations), make([]int64, 0, r.Operations)
					var wg sync.WaitGroup
					for worker := range outputs {
						wg.Add(1)
						go func(worker int) {
							defer wg.Done()
							for j := 0; j < perWorker; j++ {
								f, e, err := run(worker, nil)
								if err != nil {
									t.Error(err)
									return
								}
								measureMu.Lock()
								firsts = append(firsts, f)
								ends = append(ends, e)
								measureMu.Unlock()
							}
						}(worker)
					}
					wg.Wait()
					r.WallNS = time.Since(start).Nanoseconds()
					r.CPUNS = testCPU() - startCPU
					after := mem()
					r.AllocatedBytes = int64(after.TotalAlloc - before.TotalAlloc)
					r.Allocations = int64(after.Mallocs - before.Mallocs)
					ar, aw, arb, awb := stats.snapshot()
					r.ProcessReadSyscalls = ar - reads
					r.ProcessWriteSyscalls = aw - writes
					r.TCPReceivedBytes = arb - rb
					r.TCPSentBytes = awb - wb
					if len(firsts) != r.Operations {
						t.Fatal("incomplete measurements")
					}
					r.FirstNS = quantiles(firsts)
					r.EndNS = quantiles(ends)
					if shape.Name == "rows128" || shape.Name == "concurrent" {
						f, e := os.Create(filepath.Join(outputDir, shape.Name+"-"+api+"-cpu.pprof"))
						if e != nil {
							t.Fatal(e)
						}
						runtime.SetMutexProfileFraction(1)
						runtime.SetBlockProfileRate(1)
						if e = pprof.StartCPUProfile(f); e != nil {
							t.Fatal(e)
						}
						deadline := time.Now().Add(time.Second)
						var profileWorkers sync.WaitGroup
						for worker := range outputs {
							profileWorkers.Add(1)
							go func(worker int) {
								defer profileWorkers.Done()
								for time.Now().Before(deadline) {
									if _, _, e := run(worker, nil); e != nil {
										t.Error(e)
										return
									}
								}
							}(worker)
						}
						profileWorkers.Wait()
						pprof.StopCPUProfile()
						runtime.SetMutexProfileFraction(0)
						runtime.SetBlockProfileRate(0)
						f.Close()
					}
					runtime.GC()
					r.IdleHeapBytes = mem().HeapAlloc
					r.IdleReceiveCacheBytes, r.IdleReceiveCacheObjects = (*implSession.Session)(s).Data().InternalConnCfg.InternalReceiveCache.Storage()
					r.Negotiated = testNegotiated(s)
					var peak atomic.Uint64
					stop := make(chan struct{})
					joined := make(chan struct{})
					go func() {
						defer close(joined)
						ticker := time.NewTicker(100 * time.Microsecond)
						defer ticker.Stop()
						for {
							select {
							case <-ticker.C:
								m := mem()
								for old := peak.Load(); m.HeapAlloc > old; old = peak.Load() {
									if peak.CompareAndSwap(old, m.HeapAlloc) {
										break
									}
								}
							case <-stop:
								return
							}
						}
					}()
					// Diagnostic retains source/final storage established before idle baseline.
					if _, _, e := run(0, func() {
						runtime.GC()
						m := mem()
						r.PeakPostGCHeapBytes = max(r.PeakPostGCHeapBytes, m.HeapAlloc)
					}); e != nil {
						t.Error(e)
					}
					close(stop)
					<-joined
					r.PeakSampledHeapAllocBytes = peak.Load()
					runtime.GC()
					r.PostCloseHeapBytes = mem().HeapAlloc
					r.RuntimeHeapBeforeConnect = baseRuntime
					r.SessionHeapAfterConnect = sessionHeap
					r.HeapAfterSetup = setupHeap
					r.PercentileSamples = len(firsts)
					r.WarmIdleDriverAndHarnessBytes = int64(r.IdleHeapBytes) - int64(baseRuntime) - r.ApplicationStorageBytes
					r.InclusivePeakDriverAndHarnessBytes = int64(r.PeakPostGCHeapBytes) - int64(baseRuntime) - r.ApplicationStorageBytes
					results = append(results, r)
					data, _ := json.Marshal(r)
					t.Log(string(data))
					runtime.KeepAlive(outputs)
				})
			}
		}
		data, e := json.MarshalIndent(results, "", "  ")
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(outputDir, "results.json"), append(data, '\n'), 0644); e != nil {
			t.Fatal(e)
		}
		// Profiles are explicitly separate from latency runs and use the same payload,
		// API and concurrency shape; profile output is owned by this mode directory.
		for _, name := range []string{"heap", "allocs", "mutex", "block", "goroutine"} {
			f, e := os.Create(filepath.Join(outputDir, name+".pprof"))
			if e != nil {
				t.Fatal(e)
			}
			if e = pprof.Lookup(name).WriteTo(f, 0); e != nil {
				t.Error(e)
			}
			f.Close()
		}
		runtime.SetMutexProfileFraction(0)
		runtime.SetBlockProfileRate(0)
	})
}

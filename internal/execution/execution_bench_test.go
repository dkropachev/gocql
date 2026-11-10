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
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package execution

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	implProtocol "github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/settings"
)

func BenchmarkExecution(b *testing.B) {
	b.Run("DurationDecode", func(b *testing.B) {
		reader := scalarReader(b, cqltype.TypeDuration, cqltype.Present, []byte{2, 4, 6})
		b.ReportAllocs()
		for b.Loop() {
			if _, err := codec.DurationCodec.Decode(reader); err != nil {
				b.Fatal(err)
			}
		}
	})

	// BenchmarkExecution/NewOptionTemplates encodes the templates of resolved options, as
	// NewCompiledOptions and a query created with options for another session do.
	b.Run("NewOptionTemplates", func(b *testing.B) {
		profile := settings.InternalDefaultProfile()
		profile.PageSize = 5000
		for _, c := range []struct {
			name   string
			values Options
		}{{"plain", Options{}}, {"timestamp", Options{Timestamp: cqltype.Option[int64]{Value: 1, Set: true}}}} {
			b.Run(c.name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if newOptionTemplates(&profile, &c.values, "ks") == nil {
						b.Fatal("no templates")
					}
				}
			})
		}
	})

	// BenchmarkExecution/OneShotLoopback compares the ways to execute a prepared statement
	// once per call from many goroutines: a point write (Exec) and a point read
	// (One). bind binds each call; query keeps one Query per worker; userpool
	// keeps queries in a sync.Pool; prepared calls Prepared.Exec and One. ns/op
	// is wall time per completed operation.
	b.Run("OneShotLoopback", func(b *testing.B) {
		options := testOptions(Options{Timeout: cqltype.Option[time.Duration]{Set: true, Value: 10 * time.Second}})
		writeStmt := binding.Statement[loopbackRow, codec.EmptyRecord]{Kind: cqltype.OrdinaryStatement, CQL: "INSERT INTO ks.tbl(k, value) VALUES(?, ?)", Params: codec.Params(codec.ParamAt(0, codec.Int32, func(r loopbackRow) int32 { return r.k }), codec.ParamAt(1, codec.BytesEncoder, func(r loopbackRow) []byte { return r.value })), Rows: codec.EmptyRowDecoder}
		readStmt := binding.Statement[int32, []byte]{Kind: cqltype.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl WHERE k = ?", Params: codec.OneParam(codec.Int32), Rows: codec.RowInto(func(*[]byte) {}, codec.Column("value", codec.OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
		variants := []string{"bind", "bindborrowed", "query", "userpool", "prepared"}
		for _, op := range []string{"exec", "one"} {
			workload := map[string]string{"exec": "pwrite", "one": "pone"}[op]
			for _, variant := range variants {
				for _, workers := range []int{1, 8, 64} {
					b.Run(fmt.Sprintf("%s/%s/w%d", op, variant, workers), func(b *testing.B) {
						s := loopbackTestSession(b, workload)
						// One-shot calls take options bound to their session, as
						// the public API requires.
						bound := codecTestMust(InternalBindOptions(s, options.Values()))
						var wp *Prepared[loopbackRow, codec.EmptyRecord]
						var rp *Prepared[int32, []byte]
						var wpool, rpool sync.Pool
						if op == "exec" {
							wp = codecTestMust(Prepare(context.Background(), s, writeStmt, PrepareOptions{}))
							wpool.New = func() any { return codecTestMust(NewQuery(wp, options.Values())) }
						} else {
							rp = codecTestMust(Prepare(context.Background(), s, readStmt, PrepareOptions{}))
							rpool.New = func() any { return codecTestMust(NewQuery(rp, options.Values())) }
						}
						run := make([]func(context.Context) error, workers)
						for i := range run {
							input := loopbackRow{k: int32(i), value: []byte(loopbackValue)}
							key := int32(i)
							dst := make([]byte, 0, len(loopbackValue))
							check := func(found bool, _ model.Result, err error) error {
								if err != nil {
									return err
								}
								if !found || len(dst) != len(loopbackValue) {
									return fmt.Errorf("bad diagnostic row")
								}
								return nil
							}
							var wq *Query[loopbackRow, codec.EmptyRecord]
							var rq *Query[int32, []byte]
							if variant == "query" && op == "exec" {
								wq = codecTestMust(NewQuery(wp, options.Values()))
								b.Cleanup(func() { _ = wq.Close(context.Background()) })
							} else if variant == "query" {
								rq = codecTestMust(NewQuery(rp, options.Values()))
								b.Cleanup(func() { _ = rq.Close(context.Background()) })
							}
							switch variant + "/" + op {
							case "bind/exec", "bindborrowed/exec":
								borrowed := variant == "bindborrowed"
								run[i] = func(ctx context.Context) error {
									var bound *Bound[codec.EmptyRecord]
									var err error
									if borrowed {
										bound, err = BindBorrowed(wp, input)
									} else {
										bound, err = BindOwned(wp, input)
									}
									if err != nil {
										return err
									}
									_, err = Exec(ctx, s, bound, options)
									return err
								}
							case "bind/one", "bindborrowed/one":
								borrowed := variant == "bindborrowed"
								run[i] = func(ctx context.Context) error {
									var bound *Bound[[]byte]
									var err error
									if borrowed {
										bound, err = BindBorrowed(rp, key)
									} else {
										bound, err = BindOwned(rp, key)
									}
									if err != nil {
										return err
									}
									return check(RunOne(ctx, s, bound, options, &dst))
								}
							case "query/exec":
								run[i] = func(ctx context.Context) error { _, err := wq.Exec(ctx, input); return err }
							case "query/one":
								run[i] = func(ctx context.Context) error { return check(rq.One(ctx, key, &dst)) }
							case "userpool/exec":
								run[i] = func(ctx context.Context) error {
									q := wpool.Get().(*Query[loopbackRow, codec.EmptyRecord])
									_, err := q.Exec(ctx, input)
									wpool.Put(q)
									return err
								}
							case "userpool/one":
								run[i] = func(ctx context.Context) error {
									q := rpool.Get().(*Query[int32, []byte])
									found, result, err := q.One(ctx, key, &dst)
									rpool.Put(q)
									return check(found, result, err)
								}
							case "prepared/exec":
								run[i] = func(ctx context.Context) error { _, err := wp.Exec(ctx, input, bound); return err }
							case "prepared/one":
								run[i] = func(ctx context.Context) error { return check(rp.One(ctx, key, &dst, bound)) }
							}
						}
						ctx := context.Background()
						for range 2 { // warm plans, caches and pools
							for _, fn := range run {
								if err := fn(ctx); err != nil {
									b.Fatal(err)
								}
							}
						}
						var remaining atomic.Int64
						remaining.Store(int64(b.N))
						var failed atomic.Pointer[error]
						var wg sync.WaitGroup
						b.ReportAllocs()
						cpuStart := processCPUTime()
						b.ResetTimer()
						for _, fn := range run {
							wg.Add(1)
							go func() {
								defer wg.Done()
								for remaining.Add(-1) >= 0 {
									if err := fn(ctx); err != nil {
										failed.CompareAndSwap(nil, &err)
										return
									}
								}
							}()
						}
						wg.Wait()
						b.StopTimer()
						b.ReportMetric(float64(processCPUTime()-cpuStart)/float64(b.N), "cpu-ns/op")
						if err := failed.Load(); err != nil {
							b.Fatal(*err)
						}
					})
				}
			}
		}
	})

	b.Run("SharedAdmission", func(b *testing.B) {
		for _, parallelism := range []int{1, 4, 16} {
			b.Run(fmt.Sprint(parallelism), func(b *testing.B) {
				s := func() *cluster.Core {
					opaqueStorageValue := new(cluster.Core)
					return opaqueStorageValue
				}()

				bound := runtimeQuery(b)
				b.ReportAllocs()
				old := runtime.GOMAXPROCS(parallelism)
				defer runtime.GOMAXPROCS(old)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						ctx, cancel, request, err := InternalAdmitExecution(context.Background(), s, bound, CompiledOptions{})
						if err != nil {
							b.Error(err)
							return
						}
						_ = ctx
						request.InternalLogical.InternalFinish(model.Result{}, nil)
						cancel()
						request.InternalLogical.releaseInput()
					}
				})
				b.StopTimer()
				bound.Close()
				bound.Wait(context.Background())
			})
		}
	})

	b.Run("SingleConn", func(b *testing.B) {
		srv := NewTestServer(b, 3, context.Background())
		defer srv.Stop()

		cluster := testCluster(implProtocol.ProtoVersion3, srv.Address)
		// Set the timeout arbitrarily low so that the query hits the timeout in a
		// timely manner.
		cluster.Timeout = 500 * time.Millisecond
		cluster.NumConns = 1
		db, err := implCluster.InternalClusterSettingsOpen(cluster)
		if err != nil {
			b.Fatalf("newClusterSettings: %v", err)
		}
		defer db.Close()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				err := execTestStatement(context.Background(), db, "void", CompiledOptions{})
				if err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}

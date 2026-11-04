// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	implExecution "github.com/scylladb/gocql/v2/internal/execution"
)

// BenchmarkOneShotLoopback compares the ways to execute a prepared statement
// once per call from many goroutines: a point write (Exec) and a point read
// (One). bind binds each call; query keeps one Query per worker; userpool
// keeps queries in a sync.Pool; prepared calls Prepared.Exec and One. ns/op
// is wall time per completed operation.
func BenchmarkOneShotLoopback(b *testing.B) {
	options := testOptions(OptionValues{Timeout: Option[time.Duration]{Set: true, Value: 10 * time.Second}})
	writeStmt := Statement[loopbackRow, Unit]{Kind: OrdinaryStatement, CQL: "INSERT INTO ks.tbl(k, value) VALUES(?, ?)", Params: Params(ParamAt(0, Int32, func(r loopbackRow) int32 { return r.k }), ParamAt(1, BytesEncoder, func(r loopbackRow) []byte { return r.value })), Rows: NoRows}
	readStmt := Statement[int32, []byte]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl WHERE k = ?", Params: OneParam(Int32), Rows: RowInto(func(*[]byte) {}, Column("value", OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
	variants := []string{"bind", "bindborrowed", "query", "userpool", "prepared"}
	for _, op := range []string{"exec", "one"} {
		workload := map[string]string{"exec": "pwrite", "one": "pone"}[op]
		for _, variant := range variants {
			for _, workers := range []int{1, 8, 64} {
				b.Run(fmt.Sprintf("%s/%s/w%d", op, variant, workers), func(b *testing.B) {
					s := loopbackTestSession(b, workload)
					var wp *Prepared[loopbackRow, Unit]
					var rp *Prepared[int32, []byte]
					var wpool, rpool sync.Pool
					if op == "exec" {
						wp = codecTestMust(Prepare(context.Background(), s, writeStmt, PrepareOptions{}))
						wpool.New = func() any { return codecTestMust(NewQuery(wp, options)) }
					} else {
						rp = codecTestMust(Prepare(context.Background(), s, readStmt, PrepareOptions{}))
						rpool.New = func() any { return codecTestMust(NewQuery(rp, options)) }
					}
					run := make([]func(context.Context) error, workers)
					for i := range run {
						input := loopbackRow{k: int32(i), value: []byte(loopbackValue)}
						key := int32(i)
						dst := make([]byte, 0, len(loopbackValue))
						check := func(found bool, err error) error {
							if err != nil {
								return err
							}
							if !found || len(dst) != len(loopbackValue) {
								return fmt.Errorf("bad diagnostic row")
							}
							return nil
						}
						var wq *Query[loopbackRow, Unit]
						var rq *Query[int32, []byte]
						if variant == "query" && op == "exec" {
							wq = codecTestMust(NewQuery(wp, options))
							b.Cleanup(wq.Close)
						} else if variant == "query" {
							rq = codecTestMust(NewQuery(rp, options))
							b.Cleanup(rq.Close)
						}
						switch variant + "/" + op {
						case "bind/exec", "bindborrowed/exec":
							borrowed := variant == "bindborrowed"
							run[i] = func(ctx context.Context) error {
								var bound *Bound[Unit]
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
								found, _, err := implExecution.RunOne(ctx, s, bound, options, &dst)
								return check(found, err)
							}
						case "query/exec":
							run[i] = func(ctx context.Context) error { return wq.Exec(ctx, input) }
						case "query/one":
							run[i] = func(ctx context.Context) error { return check(rq.One(ctx, key, &dst)) }
						case "userpool/exec":
							run[i] = func(ctx context.Context) error {
								q := wpool.Get().(*Query[loopbackRow, Unit])
								err := q.Exec(ctx, input)
								wpool.Put(q)
								return err
							}
						case "userpool/one":
							run[i] = func(ctx context.Context) error {
								q := rpool.Get().(*Query[int32, []byte])
								found, err := q.One(ctx, key, &dst)
								rpool.Put(q)
								return check(found, err)
							}
						case "prepared/exec":
							run[i] = func(ctx context.Context) error { _, err := wp.Exec(ctx, input, options); return err }
						case "prepared/one":
							run[i] = func(ctx context.Context) error { return check(rp.One(ctx, key, &dst, options)) }
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
}

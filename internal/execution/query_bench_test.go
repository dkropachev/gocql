// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/scylladb/gocql/v2/internal/binding"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"

	"io"

	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	implCqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

// These diagnostics isolate native paging and decoding from application SHA-256.
// Canonical comparison observations always retain full payload hashing.
func BenchmarkReusableQueryDecoding(b *testing.B) {
	for _, size := range []struct {
		name        string
		rows, bytes int
	}{{"point", 1, 1024}, {"partition", 128, 4096}} {
		for _, mode := range []string{"buffered", "stream", "each"} {
			stream, each := mode == "stream", mode == "each"
			if each && size.rows == 1 {
				continue
			}
			b.Run(size.name+"/"+mode, func(b *testing.B) {
				s, conns, peers := runtimeTestSession(b, 1)
				value := strings.Repeat("x", size.bytes)
				var bodies [][]byte
				if size.rows == 1 {
					bodies = [][]byte{runtimeRowsBody(nil, value)}
				} else {
					values := make([]string, 32)
					for i := range values {
						values[i] = value
					}
					for page := 1; page <= 4; page++ {
						bodies = append(bodies, runtimeRowsBody([]byte{byte(page)}, values...))
					}
					bodies = append(bodies, runtimeRowsBody(nil))
				}
				served := make(chan error, 1)
				go func() {
					var header [9]byte
					for range b.N {
						for _, body := range bodies {
							if _, err := io.ReadFull(peers[0], header[:]); err != nil {
								served <- err
								return
							}
							if _, err := io.CopyN(io.Discard, peers[0], int64(binary.BigEndian.Uint32(header[5:]))); err != nil {
								served <- err
								return
							}
							header[0], header[1], header[4] = 0x84, 0, 8
							binary.BigEndian.PutUint32(header[5:], uint32(len(body)))
							if _, err := peers[0].Write(header[:]); err != nil {
								served <- err
								return
							}
							if _, err := peers[0].Write(body); err != nil {
								served <- err
								return
							}
						}
					}
					served <- nil
				}()
				options := testOptions(OptionValues{PageSize: cqltype.Option[int32]{Set: true, Value: 32}, Prefetch: cqltype.Option[int]{Set: true, Value: 0}, Timeout: cqltype.Option[time.Duration]{Set: true, Value: 10 * time.Second}})
				statement := binding.Statement[codec.EmptyRecord, []byte]{Kind: cqltype.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: codec.NoParams, Rows: codec.RowInto(func(*[]byte) {}, codec.Column("value", codec.OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
				var query *Query[codec.EmptyRecord, []byte]
				var streaming *StreamQuery[codec.EmptyRecord, []byte]
				if stream {
					streaming = codecTestMust(NewUnpreparedStreamQuery(s, statement, cqltype.NewColumns(), codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Options: options}))
					defer streaming.Close()
				} else {
					query = codecTestMust(NewUnpreparedQuery(s, statement, cqltype.NewColumns(), options))
					defer query.Close()
				}
				dst := make([]byte, 0, size.bytes)
				b.ReportAllocs()
				b.SetBytes(int64(size.rows * size.bytes))
				b.ResetTimer()
				for range b.N {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					count := 0
					if each {
						err := query.Each(ctx, codec.EmptyRecord{}, &dst, func(row *[]byte) error {
							count++
							if len(*row) != size.bytes || (*row)[0] != 'x' || (*row)[len(*row)-1] != 'x' {
								b.Fatal("bad diagnostic payload")
							}
							return nil
						})
						if err != nil {
							b.Fatal(err)
						}
					} else if stream {
						cursor, err := streaming.Run(ctx, codec.EmptyRecord{})
						if err != nil {
							b.Fatal(err)
						}
						for cursor.NextInto(&dst) {
							count++
							if len(dst) != size.bytes || dst[0] != 'x' || dst[len(dst)-1] != 'x' {
								b.Fatal("bad diagnostic payload")
							}
						}
						if err := cursor.Err(); err != nil {
							b.Fatal(err)
						}
						if err := cursor.Close(); err != nil {
							b.Fatal(err)
						}
						if err := streaming.Wait(ctx); err != nil {
							b.Fatal(err)
						}
					} else {
						cursor, err := query.Run(ctx, codec.EmptyRecord{})
						if err != nil {
							b.Fatal(err)
						}
						for cursor.NextInto(&dst) {
							count++
							if len(dst) != size.bytes || dst[0] != 'x' || dst[len(dst)-1] != 'x' {
								b.Fatal("bad diagnostic payload")
							}
						}
						if err := cursor.Err(); err != nil {
							b.Fatal(err)
						}
						if err := cursor.Close(); err != nil {
							b.Fatal(err)
						}
						if err := query.Wait(ctx); err != nil {
							b.Fatal(err)
						}
					}
					cancel()
					if count != size.rows {
						b.Fatal("incomplete diagnostic partition", count)
					}
				}
				b.StopTimer()
				if err := <-served; err != nil {
					b.Fatal(err)
				}
				_ = conns
			})
		}
	}
}

// BenchmarkOneShotLoopback compares the ways to execute a prepared statement
// once per call from many goroutines: a point write (Exec) and a point read
// (One). bind binds each call; query keeps one Query per worker; userpool
// keeps queries in a sync.Pool; prepared calls Prepared.Exec and One. ns/op
// is wall time per completed operation.
func BenchmarkOneShotLoopback(b *testing.B) {
	options := testOptions(OptionValues{Timeout: cqltype.Option[time.Duration]{Set: true, Value: 10 * time.Second}})
	writeStmt := binding.Statement[loopbackRow, codec.EmptyRecord]{Kind: cqltype.OrdinaryStatement, CQL: "INSERT INTO ks.tbl(k, value) VALUES(?, ?)", Params: codec.Params(codec.ParamAt(0, codec.Int32, func(r loopbackRow) int32 { return r.k }), codec.ParamAt(1, codec.BytesEncoder, func(r loopbackRow) []byte { return r.value })), Rows: codec.NoRows}
	readStmt := binding.Statement[int32, []byte]{Kind: cqltype.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl WHERE k = ?", Params: codec.OneParam(codec.Int32), Rows: codec.RowInto(func(*[]byte) {}, codec.Column("value", codec.OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
	variants := []string{"bind", "bindborrowed", "query", "userpool", "prepared"}
	for _, op := range []string{"exec", "one"} {
		workload := map[string]string{"exec": "pwrite", "one": "pone"}[op]
		for _, variant := range variants {
			for _, workers := range []int{1, 8, 64} {
				b.Run(fmt.Sprintf("%s/%s/w%d", op, variant, workers), func(b *testing.B) {
					s := loopbackTestSession(b, workload)
					var wp *Prepared[loopbackRow, codec.EmptyRecord]
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
						var wq *Query[loopbackRow, codec.EmptyRecord]
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
								found, _, err := RunOne(ctx, s, bound, options, &dst)
								return check(found, err)
							}
						case "query/exec":
							run[i] = func(ctx context.Context) error { return wq.Exec(ctx, input) }
						case "query/one":
							run[i] = func(ctx context.Context) error { return check(rq.One(ctx, key, &dst)) }
						case "userpool/exec":
							run[i] = func(ctx context.Context) error {
								q := wpool.Get().(*Query[loopbackRow, codec.EmptyRecord])
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

// BenchmarkReusableQueryLoopback measures per-request fixed cost under
// concurrency. ns/op is wall time per completed operation across all workers.
func BenchmarkReusableQueryLoopback(b *testing.B) {
	options := testOptions(OptionValues{Timeout: cqltype.Option[time.Duration]{Set: true, Value: 10 * time.Second}})
	value := loopbackValue
	for _, workload := range []string{"read", "write", "pread", "pwrite", "pone", "preadctx", "page", "pagectx", "pagepto", "pageb"} {
		workerCounts := []int{1, 8, 64}
		if strings.HasPrefix(workload, "page") {
			workerCounts = []int{1, 4}
		}
		for _, workers := range workerCounts {
			b.Run(fmt.Sprintf("%s/w%d", workload, workers), func(b *testing.B) {
				s := loopbackTestSession(b, workload)
				run := make([]func(context.Context) error, workers)
				for i := range run {
					switch workload {
					case "pageb":
						// Borrowed column values, valid until the callback returns: no
						// per-row copy. Measurement only; master's client copies.
						pageOptions := testOptions(OptionValues{PageSize: cqltype.Option[int32]{Set: true, Value: loopbackPageRows}, Prefetch: cqltype.Option[int]{Set: true, Value: 0}, Timeout: cqltype.Option[time.Duration]{Set: true, Value: 10 * time.Second}})
						statement := binding.Statement[codec.EmptyRecord, implCqltype.Bytes]{Kind: implCqltype.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: codec.NoParams, Rows: codec.OneColumn(codec.BlobCodec)}
						query := codecTestMust(NewUnpreparedQuery(s, statement, implCqltype.NewColumns(), pageOptions))
						b.Cleanup(query.Close)
						var dst implCqltype.Bytes
						run[i] = func(ctx context.Context) error {
							count := 0
							err := query.Each(ctx, codec.EmptyRecord{}, &dst, func(row *implCqltype.Bytes) error {
								if row.Len() != int64(len(loopbackPageValue)) {
									return fmt.Errorf("bad diagnostic row")
								}
								count++
								return nil
							})
							if err != nil {
								return err
							}
							if count != loopbackPageCount*loopbackPageRows {
								return fmt.Errorf("incomplete diagnostic partition: %d rows", count)
							}
							return nil
						}
					case "page", "pagectx", "pagepto":
						pageValues := OptionValues{PageSize: cqltype.Option[int32]{Set: true, Value: loopbackPageRows}, Prefetch: cqltype.Option[int]{Set: true, Value: 0}, Timeout: cqltype.Option[time.Duration]{Set: true, Value: 10 * time.Second}}
						if workload == "pagepto" {
							pageValues.PageTimeout = cqltype.Option[time.Duration]{Set: true, Value: 2 * time.Second}
						}
						pageOptions := testOptions(pageValues)
						statement := binding.Statement[codec.EmptyRecord, []byte]{Kind: implCqltype.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: codec.NoParams, Rows: codec.RowInto(func(*[]byte) {}, codec.Column("value", codec.OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
						query := codecTestMust(NewUnpreparedQuery(s, statement, implCqltype.NewColumns(), pageOptions))
						b.Cleanup(query.Close)
						dst := make([]byte, 0, len(loopbackPageValue))
						perOp := workload == "pagectx"
						run[i] = func(ctx context.Context) error {
							if perOp {
								var cancel context.CancelFunc
								ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
								defer cancel()
							}
							count := 0
							err := query.Each(ctx, codec.EmptyRecord{}, &dst, func(row *[]byte) error {
								if len(*row) != len(loopbackPageValue) {
									return fmt.Errorf("bad diagnostic row")
								}
								count++
								return nil
							})
							if err != nil {
								return err
							}
							if count != loopbackPageCount*loopbackPageRows {
								return fmt.Errorf("incomplete diagnostic partition: %d rows", count)
							}
							return nil
						}
					case "pread", "preadctx":
						statement := binding.Statement[int32, []byte]{Kind: implCqltype.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl WHERE k = ?", Params: codec.OneParam(codec.Int32), Rows: codec.RowInto(func(*[]byte) {}, codec.Column("value", codec.OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
						prepared := codecTestMust(Prepare(context.Background(), s, statement, PrepareOptions{}))
						query := codecTestMust(NewQuery(prepared, options))
						b.Cleanup(query.Close)
						dst := make([]byte, 0, len(value))
						key := int32(i)
						perOp := workload == "preadctx"
						run[i] = func(ctx context.Context) error {
							if perOp {
								var cancel context.CancelFunc
								ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
								defer cancel()
							}
							rows, err := query.Run(ctx, key)
							if err != nil {
								return err
							}
							count := 0
							for rows.NextInto(&dst) {
								count++
							}
							if err := rows.Close(); err != nil {
								return err
							}
							if count != 1 || len(dst) != len(value) {
								return fmt.Errorf("bad diagnostic row")
							}
							return query.Wait(ctx)
						}
					case "pone":
						statement := binding.Statement[int32, []byte]{Kind: implCqltype.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl WHERE k = ?", Params: codec.OneParam(codec.Int32), Rows: codec.RowInto(func(*[]byte) {}, codec.Column("value", codec.OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
						prepared := codecTestMust(Prepare(context.Background(), s, statement, PrepareOptions{}))
						query := codecTestMust(NewQuery(prepared, options))
						b.Cleanup(query.Close)
						dst := make([]byte, 0, len(value))
						key := int32(i)
						run[i] = func(ctx context.Context) error {
							found, err := query.One(ctx, key, &dst)
							if err != nil {
								return err
							}
							if !found || len(dst) != len(value) {
								return fmt.Errorf("bad diagnostic row")
							}
							return nil
						}
					case "pwrite":
						statement := binding.Statement[loopbackRow, codec.EmptyRecord]{Kind: implCqltype.OrdinaryStatement, CQL: "INSERT INTO ks.tbl(k, value) VALUES(?, ?)", Params: codec.Params(codec.ParamAt(0, codec.Int32, func(r loopbackRow) int32 { return r.k }), codec.ParamAt(1, codec.BytesEncoder, func(r loopbackRow) []byte { return r.value })), Rows: codec.NoRows}
						prepared := codecTestMust(Prepare(context.Background(), s, statement, PrepareOptions{}))
						query := codecTestMust(NewQuery(prepared, options))
						b.Cleanup(query.Close)
						input := loopbackRow{k: int32(i), value: []byte(value)}
						run[i] = func(ctx context.Context) error {
							err := query.Exec(ctx, input)
							return err
						}
					case "read":
						statement := binding.Statement[codec.EmptyRecord, []byte]{Kind: implCqltype.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: codec.NoParams, Rows: codec.RowInto(func(*[]byte) {}, codec.Column("value", codec.OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
						query := codecTestMust(NewUnpreparedQuery(s, statement, implCqltype.NewColumns(), options))
						b.Cleanup(query.Close)
						dst := make([]byte, 0, len(value))
						run[i] = func(ctx context.Context) error {
							rows, err := query.Run(ctx, codec.EmptyRecord{})
							if err != nil {
								return err
							}
							count := 0
							for rows.NextInto(&dst) {
								count++
							}
							if err := rows.Close(); err != nil {
								return err
							}
							if count != 1 || len(dst) != len(value) {
								return fmt.Errorf("bad diagnostic row")
							}
							return query.Wait(ctx)
						}
					default:
						typ := codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))
						statement := binding.Statement[[]byte, codec.EmptyRecord]{Kind: implCqltype.OrdinaryStatement, CQL: "INSERT INTO ks.tbl(value)VALUES(?)", Params: codec.OneParam(codec.BytesEncoder), Rows: codec.NoRows}
						query := codecTestMust(NewUnpreparedQuery(s, statement, implCqltype.NewColumns(implCqltype.ColumnSpec{Name: "value", Type: typ}), options))
						b.Cleanup(query.Close)
						input := []byte(value)
						run[i] = func(ctx context.Context) error {
							err := query.Exec(ctx, input)
							return err
						}
					}
				}
				ctx := context.Background()
				for _, fn := range run { // warm prepared plans, caches and pools
					if err := fn(ctx); err != nil {
						b.Fatal(err)
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
				// Client CPU per operation excludes time spent preempted by other
				// load, so it stays comparable on a busy host.
				b.ReportMetric(float64(processCPUTime()-cpuStart)/float64(b.N), "cpu-ns/op")
				if err := failed.Load(); err != nil {
					b.Fatal(*err)
				}
			})
		}
	}
}

// processCPUTime returns this process's user and system CPU time. The fake
// server is a separate process and is not included.
func processCPUTime() time.Duration {
	var u syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &u); err != nil {
		return 0
	}
	return time.Duration(u.Utime.Nano() + u.Stime.Nano())
}

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

package session

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/binding"
)

func BenchmarkReusableQuery(b *testing.B) {
	// This isolates admission/capture from transport, decoding, and application
	// hashing. Canonical comparisons remain necessary for throughput decisions.
	b.Run("Admission", func(b *testing.B) {
		q := testOwnedQuery(b)
		ctx := context.Background()
		value := []byte("owned parameter")
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := q.capture(ctx, value); err != nil {
				b.Fatal(err)
			}
			_, cancel, request, err := InternalAdmitExecution(ctx, q.state.runtime.Core, &q.bound, CompiledOptions{})
			if err != nil {
				b.Fatal(err)
			}
			cancel()
			request.InternalLogical.InternalFinish(cql.Result{}, nil)
			request.InternalLogical.releaseInput()
			q.state.owner.InternalRelease()
		}
	})

	// These diagnostics isolate native paging and decoding from application SHA-256.
	// Canonical comparison observations always retain full payload hashing.
	b.Run("Decoding", func(b *testing.B) {
		for _, size := range []struct {
			name        string
			rows, bytes int
		}{{"point", 1, 1024}, {"partition", 128, 4096}} {
			for _, mode := range []string{"buffered", "stream", "each", "all"} {
				stream, each, all := mode == "stream", mode == "each", mode == "all"
				if (each || all) && size.rows == 1 {
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
					options := testOptions(Options{PageSize: 32, Prefetch: cql.Option[int]{Set: true, Value: 0}, Timeout: cql.Option[time.Duration]{Set: true, Value: 10 * time.Second}})
					statement := binding.Statement[cql.EmptyRow, []byte]{Kind: cql.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: cql.EmptyParamsEncoder, Rows: cql.RowInto(func(*[]byte) {}, cql.Column("value", cql.ByteSliceCodec, func(dst *[]byte) *[]byte { return dst }))}
					var query *Query[cql.EmptyRow, []byte]
					var streaming *StreamQuery[cql.EmptyRow, []byte]
					if stream {
						streaming = codecTestMust(unpreparedStreamQuery(s, StreamStatement[cql.EmptyRow, []byte]{Kind: statement.Kind, CQL: statement.CQL, Params: statement.Params, Rows: cql.StreamOneColumn(cql.StreamBlob)}, StreamOptions{Options: options.Values()}))
						defer streaming.Close()
					} else {
						query = codecTestMust(unpreparedQuery(s, statement, options.Values()))
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
							_, err := query.Each(ctx, cql.EmptyRow{}, &dst, func(row *[]byte) error {
								count++
								if len(*row) != size.bytes || (*row)[0] != 'x' || (*row)[len(*row)-1] != 'x' {
									b.Fatal("bad diagnostic payload")
								}
								return nil
							})
							if err != nil {
								b.Fatal(err)
							}
						} else if all {
							for row, err := range query.All(ctx, cql.EmptyRow{}, &dst) {
								if err != nil {
									b.Fatal(err)
								}
								count++
								if len(*row) != size.bytes || (*row)[0] != 'x' || (*row)[len(*row)-1] != 'x' {
									b.Fatal("bad diagnostic payload")
								}
							}
						} else if stream {
							cursor, err := streaming.Run(ctx, cql.EmptyRow{})
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
						} else {
							cursor, err := query.Run(ctx, cql.EmptyRow{})
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
	})

	// BenchmarkReusableQuery/Loopback measures per-request fixed cost under
	// concurrency. ns/op is wall time per completed operation across all workers.
	b.Run("Loopback", func(b *testing.B) {
		options := testOptions(Options{Timeout: cql.Option[time.Duration]{Set: true, Value: 10 * time.Second}})
		value := loopbackValue
		for _, workload := range []string{"read", "write", "pread", "pwrite", "pone", "preadctx", "page", "pageall", "pagectx", "pagepto", "pageb"} {
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
							pageOptions := testOptions(Options{PageSize: loopbackPageRows, Prefetch: cql.Option[int]{Set: true, Value: 0}, Timeout: cql.Option[time.Duration]{Set: true, Value: 10 * time.Second}})
							statement := binding.Statement[cql.EmptyRow, cql.Bytes]{Kind: cql.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: cql.EmptyParamsEncoder, Rows: cql.OneColumn(cql.BytesCodec)}
							query := codecTestMust(unpreparedQuery(s, statement, pageOptions.Values()))
							b.Cleanup(query.Close)
							var dst cql.Bytes
							run[i] = func(ctx context.Context) error {
								count := 0
								_, err := query.Each(ctx, cql.EmptyRow{}, &dst, func(row *cql.Bytes) error {
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
						case "pageall":
							pageOptions := testOptions(Options{PageSize: loopbackPageRows, Prefetch: cql.Option[int]{Set: true, Value: 0}, Timeout: cql.Option[time.Duration]{Set: true, Value: 10 * time.Second}})
							statement := binding.Statement[cql.EmptyRow, []byte]{Kind: cql.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: cql.EmptyParamsEncoder, Rows: cql.RowInto(func(*[]byte) {}, cql.Column("value", cql.ByteSliceCodec, func(dst *[]byte) *[]byte { return dst }))}
							query := codecTestMust(unpreparedQuery(s, statement, pageOptions.Values()))
							b.Cleanup(query.Close)
							dst := make([]byte, 0, len(loopbackPageValue))
							run[i] = func(ctx context.Context) error {
								count := 0
								for row, err := range query.All(ctx, cql.EmptyRow{}, &dst) {
									if err != nil {
										return err
									}
									if len(*row) != len(loopbackPageValue) {
										return fmt.Errorf("bad diagnostic row")
									}
									count++
								}
								if count != loopbackPageCount*loopbackPageRows {
									return fmt.Errorf("incomplete diagnostic partition: %d rows", count)
								}
								return nil
							}
						case "page", "pagectx", "pagepto":
							pageValues := Options{PageSize: loopbackPageRows, Prefetch: cql.Option[int]{Set: true, Value: 0}, Timeout: cql.Option[time.Duration]{Set: true, Value: 10 * time.Second}}
							if workload == "pagepto" {
								pageValues.PageTimeout = cql.Option[time.Duration]{Set: true, Value: 2 * time.Second}
							}
							pageOptions := testOptions(pageValues)
							statement := binding.Statement[cql.EmptyRow, []byte]{Kind: cql.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: cql.EmptyParamsEncoder, Rows: cql.RowInto(func(*[]byte) {}, cql.Column("value", cql.ByteSliceCodec, func(dst *[]byte) *[]byte { return dst }))}
							query := codecTestMust(unpreparedQuery(s, statement, pageOptions.Values()))
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
								_, err := query.Each(ctx, cql.EmptyRow{}, &dst, func(row *[]byte) error {
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
							statement := binding.Statement[int32, []byte]{Kind: cql.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl WHERE k = ?", Params: cql.OneParam(cql.Int32Codec), Rows: cql.RowInto(func(*[]byte) {}, cql.Column("value", cql.ByteSliceCodec, func(dst *[]byte) *[]byte { return dst }))}
							prepared := codecTestMust(Prepare(context.Background(), s, statement, PrepareOptions{}))
							query := codecTestMust(prepared.Query(options.Values()))
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
								return nil
							}
						case "pone":
							statement := binding.Statement[int32, []byte]{Kind: cql.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl WHERE k = ?", Params: cql.OneParam(cql.Int32Codec), Rows: cql.RowInto(func(*[]byte) {}, cql.Column("value", cql.ByteSliceCodec, func(dst *[]byte) *[]byte { return dst }))}
							prepared := codecTestMust(Prepare(context.Background(), s, statement, PrepareOptions{}))
							query := codecTestMust(prepared.Query(options.Values()))
							b.Cleanup(query.Close)
							dst := make([]byte, 0, len(value))
							key := int32(i)
							run[i] = func(ctx context.Context) error {
								found, _, err := query.One(ctx, key, &dst)
								if err != nil {
									return err
								}
								if !found || len(dst) != len(value) {
									return fmt.Errorf("bad diagnostic row")
								}
								return nil
							}
						case "pwrite":
							statement := binding.Statement[loopbackRow, cql.EmptyRow]{Kind: cql.OrdinaryStatement, CQL: "INSERT INTO ks.tbl(k, value) VALUES(?, ?)", Params: cql.Params(cql.ParamAt(0, cql.Int32Codec, func(r loopbackRow) int32 { return r.k }), cql.ParamAt(1, cql.ByteSliceCodec, func(r loopbackRow) []byte { return r.value })), Rows: cql.EmptyRowDecoder}
							prepared := codecTestMust(Prepare(context.Background(), s, statement, PrepareOptions{}))
							query := codecTestMust(prepared.Query(options.Values()))
							b.Cleanup(query.Close)
							input := loopbackRow{k: int32(i), value: []byte(value)}
							run[i] = func(ctx context.Context) error {
								_, err := query.Exec(ctx, input)
								return err
							}
						case "read":
							statement := binding.Statement[cql.EmptyRow, []byte]{Kind: cql.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: cql.EmptyParamsEncoder, Rows: cql.RowInto(func(*[]byte) {}, cql.Column("value", cql.ByteSliceCodec, func(dst *[]byte) *[]byte { return dst }))}
							query := codecTestMust(unpreparedQuery(s, statement, options.Values()))
							b.Cleanup(query.Close)
							dst := make([]byte, 0, len(value))
							run[i] = func(ctx context.Context) error {
								rows, err := query.Run(ctx, cql.EmptyRow{})
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
								return nil
							}
						default:
							typ := codecTestMust(cql.ScalarType(cql.TypeBlob))
							statement := binding.Statement[[]byte, cql.EmptyRow]{Kind: cql.OrdinaryStatement, CQL: "INSERT INTO ks.tbl(value)VALUES(?)", Params: cql.OneParam(cql.ByteSliceCodec), Rows: cql.EmptyRowDecoder, BindMarkers: cql.NewColumns(cql.ColumnSpec{Name: "value", Type: typ})}
							query := codecTestMust(unpreparedQuery(s, statement, options.Values()))
							b.Cleanup(query.Close)
							input := []byte(value)
							run[i] = func(ctx context.Context) error {
								_, err := query.Exec(ctx, input)
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
	})
}

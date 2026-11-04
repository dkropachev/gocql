// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	transporttestkit "github.com/scylladb/gocql/v2/internal/testkit/transport"
)

// loopbackTestSession is runtimeTestSession over a real loopback TCP socket
// with the production shared reader, so concurrent workers exercise socket
// batching, the submission writer and shared-connection admission.
//
// The fake server runs in a child process pinned by GOCQL_LOOPBACK_SERVER_CPUS
// (default 6-7), so client CPU time and scheduling are not shared with it.
func loopbackTestSession(t testing.TB, workload string) *Session {
	t.Helper()
	cpus := os.Getenv("GOCQL_LOOPBACK_SERVER_CPUS")
	if cpus == "" {
		cpus = "6-7"
	}
	cmd := exec.Command("taskset", "-c", cpus, os.Args[0], "-test.run=^TestLoopbackServerProcess$")
	cmd.Env = append(os.Environ(), "GOCQL_LOOPBACK_SERVER="+workload)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	line, err := bufio.NewReader(stdout).ReadString('\n')
	addr, ok := strings.CutPrefix(strings.TrimSpace(line), "ADDR ")
	if err != nil || !ok {
		t.Fatalf("loopback server did not start: %q %v", line, err)
	}
	s := new(Session)
	implCluster.SessionData(s).InternalLogger = nopLogger{}
	implCluster.SessionData(s).InternalHostSource = &ringDescriber{InternalHosts: map[string]*HostInfo{}}
	implCluster.SessionData(s).InternalPool = &policyConnPool{InternalHostConnPools: map[UUID]*hostConnPool{}}
	conn := transporttestkit.NewDialedConn(t, 4, addr)
	id := UUID{15: 1}
	host := new(HostInfo)
	implEndpoint.HostInfoData(host).InternalHostId = id
	implEndpoint.HostInfoData(host).InternalState.Store(NodeUp)
	conn.InternalHost = host
	conn.InternalHooks = newConnectionHooks(s)
	implCluster.SessionData(s).InternalHostSource.InternalHosts[id.String()] = host
	picker := newDefaultConnPicker(1)
	picker.Put(conn)
	implCluster.SessionData(s).InternalPool.InternalHostConnPools[id] = &hostConnPool{InternalHost: host, InternalConnPicker: picker}
	go conn.InternalServe(conn.InternalCtx)
	return s
}

// TestLoopbackServerProcess is the loopback benchmark's fake server. It runs
// only as a child of loopbackTestSession.
func TestLoopbackServerProcess(t *testing.T) {
	workload := os.Getenv("GOCQL_LOOPBACK_SERVER")
	if workload == "" {
		t.Skip("benchmark helper process")
	}
	body := []byte{0, 0, 0, frm.ResultKindVoid}
	if workload == "read" || workload == "pread" || workload == "pone" || workload == "preadctx" {
		body = runtimeRowsBody(nil, loopbackValue)
	}
	choose := func([]byte) []byte { return body }
	if strings.HasPrefix(workload, "page") {
		choose = loopbackPages()
	}
	key := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "k", Type: codecTestMust(ScalarType(TypeInt))})
	value := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	prepared := step03PreparedBody([]byte("loopback"), key, value, 0)
	if workload == "pwrite" {
		prepared = step03PreparedBody([]byte("loopback"), NewColumns(append(cqltype.ColumnSlice(key), cqltype.ColumnSlice(value)...)...), NewColumns(), 0)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("ADDR %s\n", listener.Addr())
	peer, err := listener.Accept()
	listener.Close()
	if err != nil {
		t.Fatal(err)
	}
	serveLoopback(peer, choose, prepared)
}

var loopbackValue = strings.Repeat("x", 1024)

type loopbackRow struct {
	k     int32
	value []byte
}

// Paged loopback workloads read loopbackPageCount pages of loopbackPageRows
// rows of loopbackPageValue, then an empty final page, as a server does when
// the last full page still reported more.
const (
	loopbackPageCount = 4
	loopbackPageRows  = 32
)

var loopbackPageValue = strings.Repeat("x", 4096)

// loopbackPages returns a chooser that answers an unprepared v4 QUERY with
// the page its paging state names: no state is page 1, and page n carries the
// one-byte token n.
func loopbackPages() func([]byte) []byte {
	values := make([]string, loopbackPageRows)
	for i := range values {
		values[i] = loopbackPageValue
	}
	pages := make([][]byte, loopbackPageCount+1)
	for n := range loopbackPageCount {
		pages[n] = runtimeRowsBody([]byte{byte(n + 1)}, values...)
	}
	pages[loopbackPageCount] = runtimeRowsBody(nil)
	return func(request []byte) []byte {
		token := loopbackPagingState(request)
		if len(token) != 1 || int(token[0]) > loopbackPageCount {
			return pages[0]
		}
		return pages[token[0]]
	}
}

// loopbackPagingState extracts the paging state of a v4 QUERY body, or nil.
func loopbackPagingState(b []byte) []byte {
	take := func(n int) []byte {
		if n < 0 || n > len(b) {
			b = nil
			return nil
		}
		v := b[:n]
		b = b[n:]
		return v
	}
	u32 := func() int {
		v := take(4)
		if v == nil {
			return -1
		}
		return int(int32(binary.BigEndian.Uint32(v)))
	}
	take(u32()) // query
	take(2)     // consistency
	flags := take(1)
	if flags == nil {
		return nil
	}
	if flags[0]&0x01 != 0 {
		count := take(2)
		if count == nil {
			return nil
		}
		for range int(binary.BigEndian.Uint16(count)) {
			if n := u32(); n > 0 {
				take(n)
			}
		}
	}
	if flags[0]&0x04 != 0 {
		take(4)
	}
	if flags[0]&0x08 == 0 {
		return nil
	}
	return take(u32())
}

// serveLoopback answers PREPARE with prepared and every other request frame
// with choose(request body), preserving its stream. Like a server, it flushes
// once no further request bytes are buffered.
func serveLoopback(peer net.Conn, choose func([]byte) []byte, prepared []byte) {
	r := bufio.NewReaderSize(peer, 64<<10)
	w := bufio.NewWriterSize(peer, 64<<10)
	var header [9]byte
	var request []byte
	for {
		if _, err := io.ReadFull(r, header[:]); err != nil {
			return
		}
		n := int(binary.BigEndian.Uint32(header[5:]))
		if cap(request) < n {
			request = make([]byte, n)
		}
		request = request[:n]
		if _, err := io.ReadFull(r, request); err != nil {
			return
		}
		var response []byte
		if header[4] == byte(frm.OpPrepare) {
			response = prepared
		} else {
			response = choose(request)
		}
		header[0], header[1], header[4] = 0x84, 0, byte(frm.OpResult)
		binary.BigEndian.PutUint32(header[5:], uint32(len(response)))
		if _, err := w.Write(header[:]); err != nil {
			return
		}
		if _, err := w.Write(response); err != nil {
			return
		}
		if r.Buffered() == 0 {
			if err := w.Flush(); err != nil {
				return
			}
		}
	}
}

// BenchmarkReusableQueryLoopback measures per-request fixed cost under
// concurrency. ns/op is wall time per completed operation across all workers.
func BenchmarkReusableQueryLoopback(b *testing.B) {
	options := testOptions(OptionValues{Timeout: Option[time.Duration]{Set: true, Value: 10 * time.Second}})
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
						pageOptions := testOptions(OptionValues{PageSize: Option[int32]{Set: true, Value: loopbackPageRows}, Prefetch: Option[int]{Set: true, Value: 0}, Timeout: Option[time.Duration]{Set: true, Value: 10 * time.Second}})
						statement := Statement[Unit, Bytes]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: NoParams, Rows: OneColumn(BlobCodec)}
						query := codecTestMust(NewUnpreparedQuery(s, statement, NewColumns(), pageOptions))
						b.Cleanup(query.Close)
						var dst Bytes
						run[i] = func(ctx context.Context) error {
							count := 0
							err := query.Each(ctx, Unit{}, &dst, func(row *Bytes) error {
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
						pageValues := OptionValues{PageSize: Option[int32]{Set: true, Value: loopbackPageRows}, Prefetch: Option[int]{Set: true, Value: 0}, Timeout: Option[time.Duration]{Set: true, Value: 10 * time.Second}}
						if workload == "pagepto" {
							pageValues.PageTimeout = Option[time.Duration]{Set: true, Value: 2 * time.Second}
						}
						pageOptions := testOptions(pageValues)
						statement := Statement[Unit, []byte]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: NoParams, Rows: RowInto(func(*[]byte) {}, Column("value", OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
						query := codecTestMust(NewUnpreparedQuery(s, statement, NewColumns(), pageOptions))
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
							err := query.Each(ctx, Unit{}, &dst, func(row *[]byte) error {
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
						statement := Statement[int32, []byte]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl WHERE k = ?", Params: OneParam(Int32), Rows: RowInto(func(*[]byte) {}, Column("value", OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
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
						statement := Statement[int32, []byte]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl WHERE k = ?", Params: OneParam(Int32), Rows: RowInto(func(*[]byte) {}, Column("value", OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
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
						statement := Statement[loopbackRow, Unit]{Kind: OrdinaryStatement, CQL: "INSERT INTO ks.tbl(k, value) VALUES(?, ?)", Params: Params(ParamAt(0, Int32, func(r loopbackRow) int32 { return r.k }), ParamAt(1, BytesEncoder, func(r loopbackRow) []byte { return r.value })), Rows: NoRows}
						prepared := codecTestMust(Prepare(context.Background(), s, statement, PrepareOptions{}))
						query := codecTestMust(NewQuery(prepared, options))
						b.Cleanup(query.Close)
						input := loopbackRow{k: int32(i), value: []byte(value)}
						run[i] = func(ctx context.Context) error {
							err := query.Exec(ctx, input)
							return err
						}
					case "read":
						statement := Statement[Unit, []byte]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: NoParams, Rows: RowInto(func(*[]byte) {}, Column("value", OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
						query := codecTestMust(NewUnpreparedQuery(s, statement, NewColumns(), options))
						b.Cleanup(query.Close)
						dst := make([]byte, 0, len(value))
						run[i] = func(ctx context.Context) error {
							rows, err := query.Run(ctx, Unit{})
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
						typ := codecTestMust(ScalarType(TypeBlob))
						statement := Statement[[]byte, Unit]{Kind: OrdinaryStatement, CQL: "INSERT INTO ks.tbl(value)VALUES(?)", Params: OneParam(BytesEncoder), Rows: NoRows}
						query := codecTestMust(NewUnpreparedQuery(s, statement, NewColumns(ColumnSpec{Name: "value", Type: typ}), options))
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

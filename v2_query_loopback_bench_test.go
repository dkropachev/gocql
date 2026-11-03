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
	"testing"
	"time"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
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
	implEndpoint.HostInfoData(host).InternalState = NodeUp
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
	if workload == "read" {
		body = runtimeRowsBody(nil, loopbackValue)
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
	serveLoopback(peer, body)
}

var loopbackValue = strings.Repeat("x", 1024)

// serveLoopback answers every request frame with body, preserving its stream.
// Like a server, it flushes once no further request bytes are buffered.
func serveLoopback(peer net.Conn, body []byte) {
	r := bufio.NewReaderSize(peer, 64<<10)
	w := bufio.NewWriterSize(peer, 64<<10)
	var header [9]byte
	for {
		if _, err := io.ReadFull(r, header[:]); err != nil {
			return
		}
		if _, err := r.Discard(int(binary.BigEndian.Uint32(header[5:]))); err != nil {
			return
		}
		header[0], header[1], header[4] = 0x84, 0, byte(frm.OpResult)
		binary.BigEndian.PutUint32(header[5:], uint32(len(body)))
		if _, err := w.Write(header[:]); err != nil {
			return
		}
		if _, err := w.Write(body); err != nil {
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
	options := Options{Timeout: Option[time.Duration]{Set: true, Value: 10 * time.Second}}
	value := loopbackValue
	for _, workload := range []string{"read", "write"} {
		for _, workers := range []int{1, 8, 64} {
			b.Run(fmt.Sprintf("%s/w%d", workload, workers), func(b *testing.B) {
				s := loopbackTestSession(b, workload)
				run := make([]func(context.Context) error, workers)
				for i := range run {
					if workload == "read" {
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
					} else {
						typ := codecTestMust(ScalarType(TypeBlob))
						statement := Statement[[]byte, Unit]{Kind: OrdinaryStatement, CQL: "INSERT INTO ks.tbl(value)VALUES(?)", Params: OneParam(BytesEncoder), Rows: NoRows}
						query := codecTestMust(NewUnpreparedQuery(s, statement, NewColumns(ColumnSpec{Name: "value", Type: typ}), options))
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
				if err := failed.Load(); err != nil {
					b.Fatal(*err)
				}
			})
		}
	}
}

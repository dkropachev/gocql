// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	implCqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/routing"
	transporttestkit "github.com/scylladb/gocql/v2/internal/testkit/transport"
)

func testOwnedQuery(t testing.TB) *Query[[]byte, EmptyRecord] {
	t.Helper()
	s, _ := attachmentSession()
	typ, err := cqltype.ScalarType(cqltype.TypeBlob)
	if err != nil {
		t.Fatal(err)
	}
	q, err := NewUnpreparedQuery(s, Statement[[]byte, EmptyRecord]{Kind: OrdinaryStatement, CQL: "INSERT INTO ks.t(v) VALUES(?)", Params: codec.OneParam(codec.BytesEncoder), Rows: codec.EmptyRowDecoder}, cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: typ}), Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(q.Close)
	return q
}

func TestUninitializedQueryReturnsValidationErrors(t *testing.T) {
	var q Query[EmptyRecord, EmptyRecord]
	if err := q.Exec(context.Background(), EmptyRecord{}); err == nil {
		t.Fatal("zero query executed")
	}
	q.Close()
	if err := q.Wait(context.Background()); err == nil {
		t.Fatal("zero query wait succeeded")
	}
}

func TestQueryOwnsInputAndRejectsOverlapUntilWriterDrain(t *testing.T) {
	q := testOwnedQuery(t)
	input := []byte("original")
	if err := q.capture(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	input[0] = 'X'
	cell := q.bound.privateStorage.InternalRequest.InternalValues.Cells[0].Body
	got := make([]byte, cell.Len())
	_, _ = cell.CopyTo(got)
	if string(got) != "original" {
		t.Fatal(string(got))
	}
	if err := q.capture(context.Background(), input); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	writer := q.state.owner.InternalWriterLease(q.bound.privateStorage.InternalRequest.InternalValues.Framed)
	q.state.owner.InternalRelease() // execution returned after cancellation
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := q.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := q.capture(context.Background(), input); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	writer.Release()
	if err := q.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := q.capture(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	q.state.owner.InternalRelease()
	q.Close()
	if err := q.capture(context.Background(), input); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestQueryConcurrentAdmissionAndClose(t *testing.T) {
	q := testOwnedQuery(t)
	var wg sync.WaitGroup
	start, winner := make(chan struct{}), make(chan struct{}, 1)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := q.capture(context.Background(), []byte("capture"))
			if err == nil {
				winner <- struct{}{}
			} else if !errors.Is(err, ErrBusy) {
				t.Error(err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if len(winner) != 1 {
		t.Fatal("admissions", len(winner))
	}
	q.Close()
	q.state.owner.InternalRelease()
	if err := q.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestClosedQueryRemainsRegisteredUntilAcceptedWritesDrain(t *testing.T) {
	q := testOwnedQuery(t)
	if err := q.capture(context.Background(), []byte("pending")); err != nil {
		t.Fatal(err)
	}
	writer := q.state.owner.InternalWriterLease(q.bound.privateStorage.InternalRequest.InternalValues.Framed)
	q.state.owner.InternalRelease()
	q.Close()
	q.state.runtime.InternalMu.Lock()
	_, registered := q.state.runtime.queries[&q.state]
	q.state.runtime.InternalMu.Unlock()
	if !registered {
		t.Fatal("session lost a closed query's pending transport storage")
	}
	drain := q.state.runtime.StopActive()
	done := make(chan struct{})
	go func() { drain.Wait(); close(done) }()
	select {
	case <-done:
		t.Fatal("session drain bypassed accepted write")
	default:
	}
	writer.Release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("session did not join transport drain")
	}
}

func TestQueryRetiresInlineExecutionCaptures(t *testing.T) {
	q := testOwnedQuery(t)
	if err := q.capture(context.Background(), []byte("owned")); err != nil {
		t.Fatal(err)
	}
	_, cancel, request, err := InternalAdmitExecution(context.Background(), q.state.runtime.Session, &q.bound, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if request.InternalLogical != &q.state.inlineLogical {
		t.Fatal("ordinary execution was not embedded")
	}
	request.InternalLogical.installStop(stopFunc(func() { t.Error("retired stop callback invoked") }))
	cancel()
	request.InternalLogical.InternalFinish(Result{}, nil)
	request.InternalLogical.InternalReleaseInput()
	q.state.owner.InternalRelease()
	if q.state.inlineLogical.lifetime.stop != nil || q.state.request.InternalValues.Framed.Len() != 0 || q.state.request.InternalLogical != nil {
		t.Fatal("idle execution retained captures")
	}
}

func TestQueryIdleRetentionAndBatchCapture(t *testing.T) {
	q := testOwnedQuery(t)
	input := bytes.Repeat([]byte{37}, 128<<10)
	if err := q.capture(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	q.state.owner.InternalRelease()
	if q.buffer.RetainedBytes() > 64<<10 {
		t.Fatal("oversized idle storage")
	}
	input = []byte("batch entry")
	entry, err := q.BatchEntry(input)
	if err != nil {
		t.Fatal(err)
	}
	input[0] = 'X'
	if err := q.capture(context.Background(), []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	q.state.owner.InternalRelease()
	body := entry.privateStorage.InternalRequest.InternalValues.Cells[0].Body
	got := make([]byte, body.Len())
	_, _ = body.CopyTo(got)
	if string(got) != "batch entry" {
		t.Fatal(string(got))
	}
	if entry.privateStorage.InternalRequest.query != nil {
		t.Fatal("batch retained reusable query")
	}
	if len(q.state.runtime.InternalExecutions) != 0 || len(q.state.runtime.queries) != 1 {
		t.Fatal("per-operation registration")
	}
}

type slicePlan []Target

func (p *slicePlan) Next() (Route, bool) {
	if len(*p) == 0 {
		return Route{}, false
	}
	t := (*p)[0]
	*p = (*p)[1:]
	return routing.InternalRouteOf(t), true
}

func TestSkipHostPlanOmitsPinnedHost(t *testing.T) {
	a, b, c := UUID{15: 1}, UUID{15: 2}, UUID{15: 3}
	plan := &skipHostPlan{plan: &slicePlan{{HostID: b}, {HostID: a}, {HostID: b}, {HostID: c}}, host: b}
	var got []UUID
	for {
		target, ok := plan.Next()
		if !ok {
			break
		}
		got = append(got, target.HostID())
	}
	if len(got) != 2 || got[0] != a || got[1] != c {
		t.Fatal(got)
	}
}

// loopbackTestSession is runtimeTestSession over a real loopback TCP socket
// with the production shared reader, so concurrent workers exercise socket
// batching, the submission writer and shared-connection admission.
//
// The fake server runs in a child process pinned by GOCQL_LOOPBACK_SERVER_CPUS
// (default 6-7), so client CPU time and scheduling are not shared with it.
func loopbackTestSession(t testing.TB, workload string) *cluster.Session {
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
	s := new(cluster.Session)
	implCluster.SessionData(s).InternalLogger = logging.InternalNopLogger{}
	implCluster.SessionData(s).InternalHostSource = &cluster.InternalRingDescriber{InternalHosts: map[string]*endpoint.HostInfo{}}
	implCluster.SessionData(s).InternalPool = &pool.InternalPolicyConnPool{InternalHostConnPools: map[implCqltype.UUID]*pool.InternalHostConnPool{}}
	conn := transporttestkit.NewDialedConn(t, 4, addr)
	id := implCqltype.UUID{15: 1}
	host := new(endpoint.HostInfo)
	implEndpoint.HostInfoData(host).InternalHostId = id
	implEndpoint.HostInfoData(host).InternalState.Store(endpoint.NodeUp)
	conn.InternalHost = host
	conn.InternalHooks = cluster.InternalNewConnectionHooks(s)
	implCluster.SessionData(s).InternalHostSource.InternalHosts[id.String()] = host
	picker := pool.InternalNewDefaultConnPicker(1)
	picker.Put(conn)
	implCluster.SessionData(s).InternalPool.InternalHostConnPools[id] = &pool.InternalHostConnPool{InternalHost: host, InternalConnPicker: picker}
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
	key := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "k", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeInt))})
	value := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))})
	prepared := preparedBody([]byte("loopback"), key, value, 0)
	if workload == "pwrite" {
		prepared = preparedBody([]byte("loopback"), implCqltype.NewColumns(append(cqltype.ColumnSlice(key), cqltype.ColumnSlice(value)...)...), implCqltype.NewColumns(), 0)
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

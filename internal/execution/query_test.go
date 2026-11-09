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

	"github.com/scylladb/gocql/v2/internal/binding"
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
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/testkit"
	transporttestkit "github.com/scylladb/gocql/v2/internal/testkit/transport"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wirecompress"
)

func testOwnedQuery(t testing.TB) *Query[[]byte, EmptyRecord] {
	t.Helper()
	s, _ := attachmentSession()
	typ := cqltype.BlobType
	q, err := NewUnpreparedQuery(s, Statement[[]byte, EmptyRecord]{Kind: OrdinaryStatement, CQL: "INSERT INTO ks.t(v) VALUES(?)", Params: codec.OneParam(codec.BytesEncoder), Rows: codec.EmptyRowDecoder, BindMarkers: cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: typ})}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close(context.Background()) })
	return q
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

// loopbackTestSession is runtimeTestSession over a real loopback TCP socket
// with the production shared reader, so concurrent workers exercise socket
// batching, the submission writer and shared-connection admission.
//
// The fake server runs in a child process pinned by GOCQL_LOOPBACK_SERVER_CPUS
// (default 6-7), so client CPU time and scheduling are not shared with it.
func loopbackTestSession(t testing.TB, workload string) *cluster.Core {
	t.Helper()
	cpus := os.Getenv("GOCQL_LOOPBACK_SERVER_CPUS")
	if cpus == "" {
		cpus = "6-7"
	}
	cmd := exec.Command("taskset", "-c", cpus, os.Args[0], "-test.run=^TestQuery$/^LoopbackServerProcess$")
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
	s := new(cluster.Core)
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

func TestQuery(t *testing.T) {
	t.Run("Capture", func(t *testing.T) {
		t.Run("IdleRetentionAndBatch", func(t *testing.T) {
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
			var batch Batch
			if err := q.AddToBatch(&batch, input); err != nil {
				t.Fatal(err)
			}
			input[0] = 'X'
			if err := q.capture(context.Background(), []byte("replacement")); err != nil {
				t.Fatal(err)
			}
			q.state.owner.InternalRelease()
			entry := batch.entries[0]
			// The values block is the length-prefixed value.
			if got := string(batch.values[entry.start+4 : entry.end]); got != "batch entry" {
				t.Fatal(got)
			}
			if len(q.state.runtime.InternalExecutions) != 0 || len(q.state.runtime.queries) != 1 {
				t.Fatal("per-operation registration")
			}
		})

		t.Run("OwnsInputAndRejectsOverlap", func(t *testing.T) {
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
			// The execution returns after cancellation while the transport
			// still holds an accepted write of its input.
			writer := q.state.owner.InternalWriterLease(q.bound.privateStorage.InternalRequest.InternalValues.Framed)
			_, cancelExec, request, err := InternalAdmitExecution(context.Background(), q.state.runtime.Core, &q.bound, BoundOptions{})
			if err != nil {
				t.Fatal(err)
			}
			cancelExec()
			request.InternalLogical.InternalFinish(Result{}, context.Canceled)
			request.InternalLogical.InternalReleaseInput()
			q.state.owner.InternalRelease()
			// The next execution waits for the drain within its context; calls
			// without a context still report the overlap.
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
			defer cancel()
			if err := q.capture(ctx, input); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			if _, err := q.Routing(input); !errors.Is(err, ErrBusy) {
				t.Fatal(err)
			}
			waited := make(chan error, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				waited <- q.capture(ctx, input)
			}()
			select {
			case err := <-waited:
				t.Fatal("execution did not wait for the accepted write", err)
			case <-time.After(10 * time.Millisecond):
			}
			writer.Release()
			if err := <-waited; err != nil {
				t.Fatal(err)
			}
			q.state.owner.InternalRelease()
			if err := q.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := q.capture(context.Background(), input); !errors.Is(err, ErrClosed) {
				t.Fatal(err)
			}
		})

		t.Run("RetiresInlineExecution", func(t *testing.T) {
			q := testOwnedQuery(t)
			if err := q.capture(context.Background(), []byte("owned")); err != nil {
				t.Fatal(err)
			}
			_, cancel, request, err := InternalAdmitExecution(context.Background(), q.state.runtime.Core, &q.bound, BoundOptions{})
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
		})
	})

	t.Run("Close", func(t *testing.T) {
		t.Run("ConcurrentAdmission", func(t *testing.T) {
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
			// Close stops the query at once but waits for the admitted capture.
			short, cancel := context.WithTimeout(context.Background(), time.Millisecond)
			defer cancel()
			if err := q.Close(short); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			q.state.owner.InternalRelease()
			if err := q.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("RemainsRegisteredUntilWritesDrain", func(t *testing.T) {
			q := testOwnedQuery(t)
			if err := q.capture(context.Background(), []byte("pending")); err != nil {
				t.Fatal(err)
			}
			writer := q.state.owner.InternalWriterLease(q.bound.privateStorage.InternalRequest.InternalValues.Framed)
			q.state.owner.InternalRelease()
			short, cancel := context.WithTimeout(context.Background(), time.Millisecond)
			defer cancel()
			if err := q.Close(short); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("Close returned before the accepted write drained", err)
			}
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
		})
	})

	t.Run("CloseWaits", func(t *testing.T) {
		t.Run("JoinsAcceptedWrites", func(t *testing.T) {
			q := testOwnedQuery(t)
			if err := q.capture(context.Background(), []byte("value")); err != nil {
				t.Fatal(err)
			}
			writer := q.state.owner.InternalWriterLease(q.bound.privateStorage.InternalRequest.InternalValues.Framed)
			q.state.owner.InternalRelease()
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
			defer cancel()
			if err := q.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Close with a held write = %v, want deadline exceeded", err)
			}
			writer.Release()
			if err := q.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err := q.Exec(context.Background(), []byte("value")); err == nil {
				t.Fatal("closed query executed")
			}
		})
	})

	t.Run("Each", func(t *testing.T) {
		t.Run("CallbackErrorStops", func(t *testing.T) {
			q, serve := eachTestQuery(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			served := serve(runtimeRowsBody([]byte{1}, "a", "b"))
			stop := errors.New("stop")
			calls := 0
			dst := make([]byte, 0, 8)
			_, err := q.Each(ctx, codec.EmptyRecord{}, &dst, func(*[]byte) error { calls++; return stop })
			<-served
			if !errors.Is(err, stop) || calls != 1 {
				t.Fatal(calls, err)
			}
			served = serve(runtimeRowsBody(nil, "next"))
			if found, _, err := q.One(ctx, codec.EmptyRecord{}, &dst); err != nil || !found || string(dst) != "next" {
				t.Fatal("query not reusable after a callback error", found, err)
			}
			<-served
		})

		t.Run("DecodesEveryPageAndLeavesIdle", func(t *testing.T) {
			q, serve := eachTestQuery(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			served := serve(runtimeRowsBody([]byte{1}, "a", "b"), runtimeRowsBody(nil, "c"))
			var got []string
			dst := make([]byte, 0, 8)
			result, err := q.Each(ctx, codec.EmptyRecord{}, &dst, func(row *[]byte) error {
				got = append(got, string(*row))
				return nil
			})
			<-served
			if err != nil || strings.Join(got, ",") != "a,b,c" || result.Rows != 3 {
				t.Fatal(got, result.Rows, err)
			}
			// The query is reusable at once: no Close or Wait obligation.
			served = serve(runtimeRowsBody(nil, "d"))
			found, _, err := q.One(ctx, codec.EmptyRecord{}, &dst)
			<-served
			if err != nil || !found || string(dst) != "d" {
				t.Fatal(found, string(dst), err)
			}
		})

		// A page that outlives PageTimeout fails with a deadline while the
		// execution timeout still holds, and the next execution's pages get a fresh
		// page budget from the same reused context.
		t.Run("PageTimeoutPerPage", func(t *testing.T) {
			q, peer := eachPageTimeoutQuery(t)
			answer := func(body []byte) {
				_, stream, _, err := readRequest(peer)
				if err == nil {
					_, err = peer.Write(typedTestResponse(t, 4, stream, body))
				}
				if err != nil {
					t.Error(err)
				}
			}
			served := make(chan struct{})
			go func() {
				defer close(served)
				answer(runtimeRowsBody([]byte{1}, "a"))
				if _, _, _, err := readRequest(peer); err != nil { // page 2: never answered
					t.Error(err)
				}
				// The next run: four pages, each within the page budget but
				// together well beyond it.
				for page := byte(2); page <= 4; page++ {
					time.Sleep(60 * time.Millisecond)
					answer(runtimeRowsBody([]byte{page}, string('a'+page)))
				}
				time.Sleep(60 * time.Millisecond)
				answer(runtimeRowsBody(nil, "f"))
			}()
			ctx := context.Background()
			dst := make([]byte, 0, 8)
			started := time.Now()
			_, err := q.Each(ctx, codec.EmptyRecord{}, &dst, func(*[]byte) error { return nil })
			if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 2*time.Second {
				t.Fatal("page timeout", err, time.Since(started))
			}
			var got string
			_, err = q.Each(ctx, codec.EmptyRecord{}, &dst, func(row *[]byte) error { got += string(*row); return nil })
			<-served
			if err != nil || got != "cdef" {
				t.Fatal("each page should get a fresh budget", got, err)
			}
		})

		// Cancelling the caller's context interrupts a retry delay.
		t.Run("ParentCancelInterruptsRetryDelay", func(t *testing.T) {
			s, _, peers := runtimeTestSession(t, 1)
			statement := binding.Statement[codec.EmptyRecord, []byte]{Kind: cqltype.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: codec.EmptyParamsEncoder, Rows: codec.RowInto(func(*[]byte) {}, codec.Column("value", codec.OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
			profile := settings.InternalDefaultProfile()
			profile.Retry = eachRetryAfterDelay{}
			q := codecTestMust(NewUnpreparedQuery(s, statement, Options{Profile: new(profile), Timeout: cqltype.Option[time.Duration]{Set: true, Value: 5 * time.Minute}}))
			t.Cleanup(func() { _ = q.Close(context.Background()) })
			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				_, stream, _, err := readRequest(peers[0])
				if err == nil {
					_, err = peers[0].Write(errorResponse(t, stream, protocol.ErrCodeServer, nil))
				}
				if err != nil {
					t.Error(err)
				}
				time.Sleep(20 * time.Millisecond)
				cancel()
			}()
			dst := make([]byte, 0, 8)
			started := time.Now()
			_, _, err := q.One(ctx, codec.EmptyRecord{}, &dst)
			if !errors.Is(err, context.Canceled) || time.Since(started) > 2*time.Second {
				t.Fatal("parent cancellation did not end the retry delay", err, time.Since(started))
			}
		})

		// Cancelling the caller's context interrupts a blocked response wait
		// although the execution context registers no watcher on it.
		t.Run("ParentCancelInterruptsWait", func(t *testing.T) {
			s, _, peers := runtimeTestSession(t, 1)
			statement := binding.Statement[codec.EmptyRecord, []byte]{Kind: cqltype.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: codec.EmptyParamsEncoder, Rows: codec.RowInto(func(*[]byte) {}, codec.Column("value", codec.OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
			q := codecTestMust(NewUnpreparedQuery(s, statement, Options{Timeout: cqltype.Option[time.Duration]{Set: true, Value: 5 * time.Second}}))
			t.Cleanup(func() { _ = q.Close(context.Background()) })
			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				if _, _, _, err := readRequest(peers[0]); err != nil {
					t.Error(err)
				}
				cancel()
			}()
			dst := make([]byte, 0, 8)
			started := time.Now()
			found, _, err := q.One(ctx, codec.EmptyRecord{}, &dst)
			if found || !errors.Is(err, context.Canceled) || time.Since(started) > time.Second {
				t.Fatal("parent cancellation did not end the wait", found, err, time.Since(started))
			}
		})

		// Cancelling the caller's context interrupts a page running under the
		// reused page context.
		t.Run("ParentCancelReachesPageContext", func(t *testing.T) {
			q, peer := eachPageTimeoutQuery(t)
			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				_, stream, _, err := readRequest(peer)
				if err == nil {
					_, err = peer.Write(typedTestResponse(t, 4, stream, runtimeRowsBody([]byte{1}, "a")))
				}
				if err == nil {
					_, _, _, err = readRequest(peer)
				}
				if err != nil {
					t.Error(err)
				}
				cancel()
			}()
			dst := make([]byte, 0, 8)
			started := time.Now()
			_, err := q.Each(ctx, codec.EmptyRecord{}, &dst, func(*[]byte) error { return nil })
			if !errors.Is(err, context.Canceled) || time.Since(started) > 90*time.Millisecond {
				t.Fatal("parent cancellation did not reach the page", err, time.Since(started))
			}
		})

		// Each lends the query's response storage to every page. Paging tokens of
		// different lengths and per-page row counts must survive that reuse, and a
		// token the history already holds must still be detected after the storage
		// that carried it was overwritten.
		t.Run("ReusedStorageKeepsTokens", func(t *testing.T) {
			q, serve := eachTestQuery(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			served := serve(runtimeRowsBody([]byte{1}, "a"), runtimeRowsBody([]byte{2, 2, 2}, "b", "c", "d"), runtimeRowsBody([]byte{3}), runtimeRowsBody(nil, "e"))
			var got []string
			dst := make([]byte, 0, 8)
			result, err := q.Each(ctx, codec.EmptyRecord{}, &dst, func(row *[]byte) error {
				got = append(got, string(*row))
				return nil
			})
			<-served
			if err != nil || strings.Join(got, ",") != "a,b,c,d,e" || result.Rows != 5 {
				t.Fatal(got, result.Rows, err)
			}
			got = got[:0]
			served = serve(runtimeRowsBody([]byte{1}, "a"), runtimeRowsBody([]byte{2, 2}, "b"), runtimeRowsBody([]byte{1}, "c"))
			_, err = q.Each(ctx, codec.EmptyRecord{}, &dst, func(row *[]byte) error {
				got = append(got, string(*row))
				return nil
			})
			<-served
			if !errors.Is(err, runtimeerrors.ErrPagingNoProgress) || strings.Join(got, ",") != "a,b,c" {
				t.Fatal(got, err)
			}
			served = serve(runtimeRowsBody(nil, "next"))
			if found, _, err := q.One(ctx, codec.EmptyRecord{}, &dst); err != nil || !found || string(dst) != "next" {
				t.Fatal("query not reusable after a paging failure", found, err)
			}
			<-served
		})

		// An UNPREPARED continuation page is reprepared and re-executed in the
		// query's lent response storage, and the released first response does not
		// leak into the repaired page.
		t.Run("UnpreparedContinuationRepairs", func(t *testing.T) {
			s, _, peers := runtimeTestSession(t, 1)
			implCluster.SessionData(s).InternalCfg.Keyspace = "ks"
			typ := cqltype.BlobType
			columns := cqltype.NewColumns(cqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: typ})
			state, old := preparedState(t, s, columns)
			state.InternalParameters = columns
			state.InternalInfo.PartitionKeyIndexes = []int{0}
			handle := new(Prepared[[]byte, cqltype.Bytes])
			PreparedData(handle).InternalState = state
			PreparedData(handle).InternalParams = codecTestMust(codec.OneParam(codec.BytesEncoder).Compile(columns))
			PreparedData(handle).InternalRows = codec.OneColumn(codec.BlobCodec)
			PreparedData(handle).InternalPlans = &InternalResultPlanCache[cqltype.Bytes]{}
			q := codecTestMust(NewQuery(handle, Options{}))
			defer func() { _ = q.Close(context.Background()) }()
			serverDone := make(chan error, 1)
			go func() {
				for step := 0; step < 4; step++ {
					op, stream, _, err := readRequest(peers[0])
					if err != nil {
						serverDone <- err
						return
					}
					switch {
					case step == 2 && op == 9:
						_, err = peers[0].Write(typedTestResponse(t, 4, stream, preparedBody([]byte("new"), columns, columns, 0)))
					case step == 2:
						err = fmt.Errorf("wanted PREPARE got %d", op)
					case step == 0:
						_, err = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody([]byte("a"), "one", "uno")))
					case step == 1:
						_, err = peers[0].Write(errorResponse(t, stream, protocol.ErrCodeUnprepared, old.InternalId))
					default:
						_, err = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "two")))
					}
					if err != nil {
						serverDone <- err
						return
					}
				}
				serverDone <- nil
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			var dst cqltype.Bytes
			var values []string
			result, err := q.Each(ctx, []byte("key"), &dst, func(row *cqltype.Bytes) error {
				values = append(values, string(row.OwnedBytes()))
				return nil
			})
			if err != nil || fmt.Sprint(values) != "[one uno two]" || result.Rows != 3 {
				t.Fatal(values, result.Rows, err)
			}
			if err := <-serverDone; err != nil {
				t.Fatal(err)
			}
		})
	})

	t.Run("Exec", func(t *testing.T) {
		t.Run("BlockedObserverKeepsBusy", func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			entered, release := make(chan struct{}), make(chan struct{})
			observer := &execVoidObserver{onAttempt: func(routing.AttemptEvent) { close(entered); <-release }}
			query := codecTestMust(NewUnpreparedQuery(s, binding.Statement[codec.EmptyRecord, codec.EmptyRecord]{Kind: cqltype.OrdinaryStatement, CQL: "INSERT INTO ks.tbl(k)VALUES(1)", Params: codec.EmptyParamsEncoder, Rows: codec.EmptyRowDecoder}, Options{Observer: observer}))
			defer func() { _ = query.Close(context.Background()) }()
			served := make(chan struct{})
			go func() {
				defer close(served)
				for range 2 {
					stream := typedTestReadRequest(t, conns[0], peers[0])
					if _, err := peers[0].Write(typedTestResponse(t, 4, stream, []byte{0, 0, 0, frm.ResultKindVoid})); err != nil {
						t.Error(err)
						return
					}
				}
			}()
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { _, err := query.Exec(ctx, codec.EmptyRecord{}); done <- err }()
			<-entered
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			// The blocked observer keeps the finished execution draining: the
			// next execution waits for it within its context.
			wait, stop := context.WithTimeout(context.Background(), time.Millisecond)
			defer stop()
			if _, err := query.Exec(wait, codec.EmptyRecord{}); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			if _, err := query.Routing(codec.EmptyRecord{}); !errors.Is(err, ErrBusy) {
				t.Fatal(err)
			}
			observer.mu.Lock()
			observer.onAttempt = nil
			observer.mu.Unlock()
			close(release)
			joined, finish := context.WithTimeout(context.Background(), time.Second)
			defer finish()
			if _, err := query.Exec(joined, codec.EmptyRecord{}); err != nil {
				t.Fatal(err)
			}
			<-served
		})

		t.Run("CancelledAcceptedWriteStaysBusy", func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			entered := make(chan struct{})
			conns[0].InternalW = &transport.InternalDeadlineContextWriter{InternalW: &acceptedWriter{Conn: conns[0].InternalR.(*transport.InternalConnReader).InternalConn, entered: entered}, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
			typ := cqltype.BlobType
			q := codecTestMust(NewUnpreparedQuery(s, binding.Statement[[]byte, codec.EmptyRecord]{Kind: cqltype.OrdinaryStatement, CQL: "INSERT INTO ks.tbl(value)VALUES(?)", Params: codec.OneParam(codec.BytesEncoder), Rows: codec.EmptyRowDecoder, BindMarkers: cqltype.NewColumns(cqltype.ColumnSpec{Name: "value", Type: typ})}, Options{}))
			defer func() { _ = q.Close(context.Background()) }()
			input := bytes.Repeat([]byte{31}, 128<<10)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { _, err := q.Exec(ctx, input); done <- err }()
			<-entered
			input[0] = 99 // execution has captured an owned snapshot
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("cancellation joined a blocked writer")
			}
			if _, err := q.Routing(input); !errors.Is(err, ErrBusy) {
				t.Fatal(err)
			}
			short, stopShort := context.WithTimeout(context.Background(), time.Millisecond)
			defer stopShort()
			if _, err := q.Exec(short, []byte("reuse")); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("next execution did not wait for the accepted write", err)
			}
			served := make(chan struct{})
			go func() {
				defer close(served)
				for range 2 {
					stream := typedTestReadRequest(t, conns[0], peers[0])
					if _, err := peers[0].Write(typedTestResponse(t, 4, stream, []byte{0, 0, 0, frm.ResultKindVoid})); err != nil {
						t.Error(err)
						return
					}
				}
			}()
			wait, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			// The next execution waits for the accepted write to drain.
			if _, err := q.Exec(wait, []byte("reuse")); err != nil {
				t.Fatal(err)
			}
			<-served
		})
	})

	// TestQuery/LoopbackServerProcess is the loopback benchmark's fake server. It runs
	// only as a child of loopbackTestSession.
	t.Run("LoopbackServerProcess", func(t *testing.T) {
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
	})

	t.Run("NewUnpreparedQuery", func(t *testing.T) {
		t.Run("DefaultsEmptyCodecs", func(t *testing.T) {
			s, _ := attachmentSession()
			q, err := NewUnpreparedQuery(s, Statement[EmptyRecord, EmptyRecord]{Kind: OrdinaryStatement, CQL: "TRUNCATE ks.t"}, Options{})
			if err != nil {
				t.Fatal(err)
			}
			if err := q.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err := NewUnpreparedQuery(s, Statement[[]byte, EmptyRecord]{Kind: OrdinaryStatement, CQL: "INSERT INTO ks.t(v) VALUES(?)"}, Options{}); err == nil {
				t.Fatal("typed parameters without an encoder accepted")
			}
		})
	})

	t.Run("One", func(t *testing.T) {
		t.Run("NotFoundClearsDestination", func(t *testing.T) {
			q, serve := eachTestQuery(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			served := serve(runtimeRowsBody(nil))
			dst := []byte("stale")
			found, _, err := q.One(ctx, codec.EmptyRecord{}, &dst)
			<-served
			if err != nil || found || dst != nil {
				t.Fatal(found, dst, err)
			}
		})

		t.Run("StopsAfterFirstRow", func(t *testing.T) {
			q, serve := eachTestQuery(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// Only one page is served: One must not request the continuation.
			served := serve(runtimeRowsBody([]byte{1}, "first", "second"))
			dst := make([]byte, 0, 8)
			found, _, err := q.One(ctx, codec.EmptyRecord{}, &dst)
			<-served
			if err != nil || !found || string(dst) != "first" {
				t.Fatal(found, string(dst), err)
			}
		})
	})

	t.Run("Pin", func(t *testing.T) {
		// Continuation pages go to the connection that served the first page, while
		// first pages still follow the routing plan across hosts.
		t.Run("ContinuationPages", func(t *testing.T) {
			s, _, peers := runtimeTestSession(t, 2)
			var serve pinServe
			serve.start(t, peers, func(_ int, token string, stream int) []byte { return pinPages(t, stream, token) })
			q := pinTestQuery(t, s, BoundOptions{})
			firsts := map[byte]bool{}
			for range 8 {
				pinEach(t, q)
				seen := serve.take()
				if len(seen) != 3 || seen[1][0] != seen[0][0] || seen[2][0] != seen[0][0] {
					t.Fatal("continuation left the first page's connection", seen)
				}
				firsts[seen[0][0]] = true
			}
			if len(firsts) != 2 {
				t.Fatal("first pages did not follow the plan across hosts", firsts)
			}
		})

		// When the pinned host goes down between pages, the next page is planned
		// again and served elsewhere.
		t.Run("RePlansWhenHostDown", func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 2)
			var serve pinServe
			serve.start(t, peers, func(peer int, token string, stream int) []byte {
				if token == "b" {
					implEndpoint.HostInfoData(conns[peer].InternalHost).InternalState.Store(endpoint.NodeDown)
				}
				return pinPages(t, stream, token)
			})
			q := pinTestQuery(t, s, BoundOptions{})
			pinEach(t, q)
			seen := serve.take()
			if len(seen) != 3 || seen[1][0] != seen[0][0] || seen[2][0] == seen[1][0] {
				t.Fatal("page after the pinned host went down did not move", seen)
			}
		})

		// retry.Next on a pinned continuation moves to another host, as it would
		// have moved past the plan's first target, and the page after it pins to
		// the new host. Each run fails its first continuation attempt.
		// TestQuery/Pin/SkipHostPlanOmitsPinnedHost covers the skip itself, which round-robin
		// plans alone do not expose.
		t.Run("RetryNextMovesHost", func(t *testing.T) {
			s, _, peers := runtimeTestSession(t, 2)
			var serve pinServe
			var mu sync.Mutex
			failNext := false
			serve.start(t, peers, func(_ int, token string, stream int) []byte {
				mu.Lock()
				fail := token == "b" && failNext
				if fail {
					failNext = false
				}
				mu.Unlock()
				if fail {
					return errorResponse(t, stream, protocol.ErrCodeServer, nil)
				}
				return pinPages(t, stream, token)
			})
			profile := settings.InternalDefaultProfile()
			profile.Retry = pinRetryNext{}
			q := pinTestQuery(t, s, testOptions(Options{Profile: new(profile)}))
			for range 16 {
				mu.Lock()
				failNext = true
				mu.Unlock()
				pinEach(t, q)
				seen := serve.take()
				if len(seen) != 4 || seen[1][0] != seen[0][0] || seen[2][0] == seen[1][0] || seen[3][0] != seen[2][0] {
					t.Fatal("retry did not move off the pinned host, or the next page did not pin to the new one", seen)
				}
			}
		})

		t.Run("SkipHostPlanOmitsPinnedHost", func(t *testing.T) {
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
		})
	})

	t.Run("Rows", func(t *testing.T) {
		// TestQuery/Rows/HandleGoesStaleOnNextExecution checks that a reusable query's
		// Rows handle stops working once the query runs again: its storage now
		// serves the new execution, which the stale handle must neither read nor
		// close.
		t.Run("HandleGoesStaleOnNextExecution", func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			q := codecTestMust(NewUnpreparedQuery(s, binding.Statement[codec.EmptyRecord, cqltype.Bytes]{Kind: cqltype.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: codec.EmptyParamsEncoder, Rows: codec.OneColumn(codec.BlobCodec)}, Options{}))
			defer func() { _ = q.Close(context.Background()) }()
			served := make(chan struct{})
			go func() {
				defer close(served)
				for _, value := range []string{"first", "second"} {
					stream := typedTestReadRequest(t, conns[0], peers[0])
					if _, err := peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, value))); err != nil {
						t.Error(err)
					}
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			first := codecTestMust(q.Run(ctx, codec.EmptyRecord{}))
			var row cqltype.Bytes
			if !first.NextInto(&row) || string(row.OwnedBytes()) != "first" {
				t.Fatal(first.Err())
			}
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}
			// The next execution waits for the closed one to drain.
			second := codecTestMust(q.Run(ctx, codec.EmptyRecord{}))
			// The stale handle reports nothing and leaves the new execution alone.
			if first.NextInto(new(cqltype.Bytes)) {
				t.Fatal("stale handle advanced")
			}
			if !errors.Is(first.Err(), ErrStaleRows) {
				t.Fatal("stale handle error", first.Err())
			}
			if _, err := first.Clone(); !errors.Is(err, runtimeerrors.ErrNoRows) {
				t.Fatal("stale handle cloned a row", err)
			}
			if _, err := first.Retain(); !errors.Is(err, runtimeerrors.ErrNoRows) {
				t.Fatal("stale handle retained a page", err)
			}
			if first.Columns().Len() != 0 || first.Info().Attempts != 0 {
				t.Fatal("stale handle exposed the new execution")
			}
			if err := first.Close(); err != nil {
				t.Fatal("stale close", err)
			}
			if !second.NextInto(&row) || string(row.OwnedBytes()) != "second" {
				t.Fatal("stale close disturbed the new execution", second.Err())
			}
			if second.NextInto(&row) || second.Err() != nil {
				t.Fatal(second.Err())
			}
			if err := second.Close(); err != nil {
				t.Fatal(err)
			}
			<-served
			var zero Rows[cqltype.Bytes]
			if zero.NextInto(&row) || zero.Err() != nil || zero.Close() != nil {
				t.Fatal("zero Rows is not empty")
			}
		})

		t.Run("OwnedPreserveDestination", func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			type output struct{ Value []byte }
			decoder := codec.RowInto(func(*output) {}, codec.Column("value", codec.OwnedBlobCodec, func(v *output) *[]byte { return &v.Value }))
			q := codecTestMust(NewUnpreparedQuery(s, binding.Statement[codec.EmptyRecord, output]{Kind: cqltype.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: codec.EmptyParamsEncoder, Rows: decoder}, Options{}))
			defer func() { _ = q.Close(context.Background()) }()
			served := make(chan struct{})
			go func() {
				defer close(served)
				stream := typedTestReadRequest(t, conns[0], peers[0])
				_, err := peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "one", "two")))
				if err != nil {
					t.Error(err)
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			rows := codecTestMust(q.Run(ctx, codec.EmptyRecord{}))
			defer rows.Close()
			dst := output{Value: make([]byte, 0, 8)}
			backing := &dst.Value[:cap(dst.Value)][0]
			for _, want := range []string{"one", "two"} {
				if !rows.NextInto(&dst) || string(dst.Value) != want || &dst.Value[0] != backing {
					t.Fatal(rows.Err(), dst)
				}
			}
			if rows.NextInto(&dst) || rows.Err() != nil || string(dst.Value) != "two" {
				t.Fatal(rows.Err(), dst)
			}
			<-served
		})
	})

	t.Run("Stream", func(t *testing.T) {
		t.Run("BufferedProtocolTLSMatrix", func(t *testing.T) {
			modes := append([]fixtureMode(nil), fixtureModes...)
			modes = append(modes, fixtureMode{"v3lz4frame", 3, wirecompress.LZ4Compressor{}, false}, fixtureMode{"v4lz4frame", 4, wirecompress.LZ4Compressor{}, false}, fixtureMode{"v4snappyframe", 4, wirecompress.SnappyCompressor{}, false}, fixtureMode{"v4lz4frametls", 4, wirecompress.LZ4Compressor{}, true})
			for _, mode := range modes {
				t.Run(mode.name, func(t *testing.T) {
					s, c, peer := fixtureStreamSession(t, mode)
					query := codecTestMust(NewUnpreparedStreamQuery(s, binding.Statement[codec.EmptyRecord, codec.EmptyRecord]{Kind: cqltype.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: codec.EmptyParamsEncoder}, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{}))
					defer func() { _ = query.Close(context.Background()) }()
					served := make(chan struct{})
					go func() {
						defer close(served)
						stream := typedTestReadRequest(t, c, peer)
						packet := fixturePacket(mode, testkit.BlobRows(mode.version, uint16(stream), []byte("complete")))
						if mode.version < 5 && mode.compression != nil {
							compressed, err := mode.compression.Encode(packet[9:])
							if err != nil {
								t.Error(err)
								return
							}
							packet[1] |= frm.FlagCompress
							binary.BigEndian.PutUint32(packet[5:9], uint32(len(compressed)))
							packet = append(packet[:9], compressed...)
						}
						for i := 0; i < len(packet); i++ {
							if _, err := peer.Write(packet[i : i+1]); err != nil {
								t.Error(err)
								return
							}
						}
					}()
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					cursor := codecTestMust(query.Run(ctx, codec.EmptyRecord{}))
					defer cursor.Close()
					dst := make([]byte, 0, 16)
					if !cursor.NextInto(&dst) || string(dst) != "complete" || cursor.NextInto(&dst) || cursor.Err() != nil {
						t.Fatal(string(dst), cursor.Err())
					}
					<-served
				})
			}
		})

		t.Run("MultiplexAndReuseSeededStorage", func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			statement := binding.Statement[codec.EmptyRecord, codec.EmptyRecord]{Kind: cqltype.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: codec.EmptyParamsEncoder}
			q := codecTestMust(NewUnpreparedStreamQuery(s, statement, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{}))
			other := codecTestMust(NewUnpreparedStreamQuery(s, statement, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{}))
			defer func() { _ = q.Close(context.Background()) }()
			defer func() { _ = other.Close(context.Background()) }()
			served := make(chan struct{})
			go func() {
				defer close(served)
				for _, body := range [][]byte{runtimeRowsBody([]byte{1}, "one"), runtimeRowsBody(nil, "other"), runtimeRowsBody(nil, "two"), runtimeRowsBody(nil, "again")} {
					stream := typedTestReadRequest(t, conns[0], peers[0])
					if _, err := peers[0].Write(typedTestResponse(t, 4, stream, body)); err != nil {
						t.Error(err)
						return
					}
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			first := codecTestMust(q.Run(ctx, codec.EmptyRecord{}))
			if _, err := q.Run(ctx, codec.EmptyRecord{}); !errors.Is(err, ErrBusy) {
				t.Fatal(err)
			}
			second := codecTestMust(other.Run(ctx, codec.EmptyRecord{})) // shares the one connection
			output := make([]byte, 0, 16)
			backing := &output[:cap(output)][0]
			if !first.NextInto(&output) || string(output) != "one" {
				t.Fatal(first.Err(), string(output))
			}
			var otherOutput = make([]byte, 0, 16)
			if !second.NextInto(&otherOutput) || string(otherOutput) != "other" || second.NextInto(&otherOutput) {
				t.Fatal(second.Err())
			}
			if err := second.Close(); err != nil {
				t.Fatal(err)
			}
			if !first.NextInto(&output) || string(output) != "two" || first.NextInto(&output) {
				t.Fatal(first.Err())
			}
			if &output[0] != backing {
				t.Fatal("seeded destination was replaced")
			}
			// The exhausted first stream finished its execution; the next Run
			// waits for its drain although first is not closed yet.
			reused := codecTestMust(q.Run(ctx, codec.EmptyRecord{}))
			if err := first.Close(); err != nil {
				t.Fatal(err)
			} // stale cursor cannot cancel reuse
			if first.NextInto(&output) {
				t.Fatal("stale cursor advanced")
			}
			if !reused.NextInto(&output) || string(output) != "again" || reused.NextInto(&output) {
				t.Fatal(reused.Err())
			}
			if err := reused.Close(); err != nil {
				t.Fatal(err)
			}
			if info := reused.Info(); info.DeliveredRows != 1 || info.Pages != 1 {
				t.Fatal(info)
			}
			<-served
		})
	})

	t.Run("Validation", func(t *testing.T) {
		t.Run("Uninitialized", func(t *testing.T) {
			var q Query[EmptyRecord, EmptyRecord]
			if _, err := q.Exec(context.Background(), EmptyRecord{}); err == nil {
				t.Fatal("zero query executed")
			}
			if err := q.Close(context.Background()); err == nil {
				t.Fatal("zero query close succeeded")
			}
		})
	})
}

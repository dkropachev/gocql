// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	implExecution "github.com/scylladb/gocql/v2/internal/execution"
	frm "github.com/scylladb/gocql/v2/internal/frame"
)

// oneShotRequest is an EXECUTE the fake server received.
type oneShotRequest struct {
	stream      int
	consistency Consistency
	pageSize    int32 // -1 without a page size
	key         int32
}

// oneShotServer answers PREPARE on peer with a statement of one int
// parameter and one blob column, and passes each EXECUTE to requests. It
// returns when peer closes.
func oneShotServer(t *testing.T, peer net.Conn, requests chan<- oneShotRequest) {
	key := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "k", Type: codecTestMust(ScalarType(TypeInt))})
	value := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	prepared := step03PreparedBody([]byte("oneshot"), key, value, 0)
	go func() {
		defer close(requests)
		for {
			op, stream, body, err := step03ReadRequest(peer)
			if err != nil {
				return
			}
			if op == byte(frm.OpPrepare) {
				if _, err := peer.Write(typedTestResponse(t, 4, stream, prepared)); err != nil {
					return
				}
				continue
			}
			requests <- parseOneShotExecute(stream, body)
		}
	}()
}

// parseOneShotExecute decodes a v4 EXECUTE body of one int value.
func parseOneShotExecute(stream int, b []byte) oneShotRequest {
	r := oneShotRequest{stream: stream, pageSize: -1}
	b = b[2+int(binary.BigEndian.Uint16(b)):] // statement id
	r.consistency = Consistency(binary.BigEndian.Uint16(b))
	flags := b[2]
	b = b[3:]
	if flags&0x01 != 0 {
		b = b[2:] // value count
		n := int(int32(binary.BigEndian.Uint32(b)))
		if n == 4 {
			r.key = int32(binary.BigEndian.Uint32(b[4:]))
		}
		b = b[4+max(n, 0):]
	}
	if flags&0x04 != 0 {
		r.pageSize = int32(binary.BigEndian.Uint32(b))
	}
	return r
}

func oneShotStatement() Statement[int32, []byte] {
	return Statement[int32, []byte]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl WHERE k = ?", Params: OneParam(Int32), Rows: RowInto(func(*[]byte) {}, Column("value", OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
}

func oneShotPrepare(t *testing.T, s *Session, peer net.Conn, requests chan oneShotRequest) *Prepared[int32, []byte] {
	t.Helper()
	oneShotServer(t, peer, requests)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p, err := Prepare(ctx, s, oneShotStatement(), PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func waitIdle(t *testing.T, p *Prepared[int32, []byte], want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for implExecution.InternalPreparedPoolIdle(p) != want {
		if time.Now().After(deadline) {
			t.Fatalf("idle queries %d, want %d", implExecution.InternalPreparedPoolIdle(p), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestPreparedOneShotOptionsPerCall checks that each call's options reach the
// wire while calls reuse one pooled query.
func TestPreparedOneShotOptionsPerCall(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	defer s.Close()
	defer peers[0].Close()
	requests := make(chan oneShotRequest, 1)
	p := oneShotPrepare(t, s, peers[0], requests)
	quorum := testOptions(OptionValues{Consistency: Option[Consistency]{Set: true, Value: Quorum}, PageSize: Option[int32]{Set: true, Value: 7}})
	two := testOptions(OptionValues{Consistency: Option[Consistency]{Set: true, Value: Two}})
	defaults := defaultProfile()
	for i, tc := range []struct {
		options     Options
		consistency Consistency
		pageSize    int32
	}{
		{quorum, Quorum, 7},
		{two, Two, defaults.PageSize},
		{Options{}, defaults.Consistency, defaults.PageSize},
		{quorum, Quorum, 7},
	} {
		errs := make(chan error, 1)
		var dst []byte
		go func() {
			found, err := p.One(context.Background(), int32(i), &dst, tc.options)
			if err == nil && (!found || string(dst) != "row") {
				err = fmt.Errorf("found %t, row %q", found, dst)
			}
			errs <- err
		}()
		r := <-requests
		if r.consistency != tc.consistency || r.pageSize != tc.pageSize || r.key != int32(i) {
			t.Fatalf("call %d: request %+v, want consistency %v page size %d", i, r, tc.consistency, tc.pageSize)
		}
		if _, err := peers[0].Write(typedTestResponse(t, 4, r.stream, runtimeRowsBody(nil, "row"))); err != nil {
			t.Fatal(err)
		}
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		waitIdle(t, p, 1)
	}
}

// TestPreparedOneShotCancelledCallReuse checks that a query whose call was
// cancelled before its answer serves the next call, and that the late answer
// does not reach that call.
func TestPreparedOneShotCancelledCallReuse(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	defer s.Close()
	defer peers[0].Close()
	requests := make(chan oneShotRequest, 2)
	p := oneShotPrepare(t, s, peers[0], requests)
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		var dst []byte
		_, err := p.One(ctx, 1, &dst, Options{})
		first <- err
	}()
	r1 := <-requests
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled call: %v", err)
	}
	waitIdle(t, p, 1)
	second := make(chan error, 1)
	go func() {
		var dst []byte
		found, err := p.One(context.Background(), 2, &dst, Options{})
		if err == nil && (!found || string(dst) != "fresh") {
			err = fmt.Errorf("found %t, row %q", found, dst)
		}
		second <- err
	}()
	r2 := <-requests
	if r2.key != 2 {
		t.Fatalf("second request key %d", r2.key)
	}
	if _, err := peers[0].Write(typedTestResponse(t, 4, r1.stream, runtimeRowsBody(nil, "stale"))); err != nil {
		t.Fatal(err)
	}
	if _, err := peers[0].Write(typedTestResponse(t, 4, r2.stream, runtimeRowsBody(nil, "fresh"))); err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	waitIdle(t, p, 1)
}

// TestPreparedOneShotSessionClose checks that closing the session stops an
// unanswered call and fails later ones.
func TestPreparedOneShotSessionClose(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	defer peers[0].Close()
	requests := make(chan oneShotRequest, 1)
	p := oneShotPrepare(t, s, peers[0], requests)
	inflight := make(chan error, 1)
	go func() {
		var dst []byte
		_, err := p.One(context.Background(), 1, &dst, Options{})
		inflight <- err
	}()
	<-requests
	closed := make(chan struct{})
	go func() { s.Close(); close(closed) }()
	select {
	case err := <-inflight:
		if err == nil {
			t.Fatal("unanswered call succeeded after close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("close did not stop the unanswered call")
	}
	<-closed
	if _, err := p.Exec(context.Background(), 2, Options{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("call after close: %v", err)
	}
}

// TestPreparedOneShotPoolCollected checks that the session does not keep an
// unused statement's pool alive.
func TestPreparedOneShotPoolCollected(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	defer s.Close()
	defer peers[0].Close()
	requests := make(chan oneShotRequest, 1)
	use := func() {
		p := oneShotPrepare(t, s, peers[0], requests)
		errs := make(chan error, 1)
		go func() {
			var dst []byte
			_, err := p.One(context.Background(), 1, &dst, Options{})
			errs <- err
		}()
		r := <-requests
		if _, err := peers[0].Write(typedTestResponse(t, 4, r.stream, runtimeRowsBody(nil, "row"))); err != nil {
			t.Fatal(err)
		}
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		waitIdle(t, p, 1)
		if n := implExecution.InternalQueryPools(s); n != 1 {
			t.Fatalf("pools %d, want 1", n)
		}
	}
	use()
	deadline := time.Now().Add(5 * time.Second)
	for implExecution.InternalQueryPools(s) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("unused pool still registered")
		}
		runtime.GC()
		time.Sleep(time.Millisecond)
	}
}

// TestPreparedOneShotConcurrent runs Exec, One and Each from many goroutines
// over a real socket.
func TestPreparedOneShotConcurrent(t *testing.T) {
	for _, workload := range []string{"pwrite", "pone"} {
		t.Run(workload, func(t *testing.T) {
			s := loopbackTestSession(t, workload)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var wg sync.WaitGroup
			errs := make(chan error, 32)
			if workload == "pwrite" {
				statement := Statement[loopbackRow, Unit]{Kind: OrdinaryStatement, CQL: "INSERT INTO ks.tbl(k, value) VALUES(?, ?)", Params: Params(ParamAt(0, Int32, func(r loopbackRow) int32 { return r.k }), ParamAt(1, BytesEncoder, func(r loopbackRow) []byte { return r.value })), Rows: NoRows}
				p := codecTestMust(Prepare(ctx, s, statement, PrepareOptions{}))
				for w := range 32 {
					wg.Go(func() {
						for i := range 100 {
							if _, err := p.Exec(ctx, loopbackRow{k: int32(w*100 + i), value: []byte(loopbackValue)}, Options{}); err != nil {
								errs <- err
								return
							}
						}
					})
				}
			} else {
				p := codecTestMust(Prepare(ctx, s, oneShotStatement(), PrepareOptions{}))
				for w := range 32 {
					wg.Go(func() {
						dst := make([]byte, 0, len(loopbackValue))
						for i := range 100 {
							var err error
							if i%2 == 0 {
								var found bool
								found, err = p.One(ctx, int32(w), &dst, Options{})
								if err == nil && (!found || string(dst) != loopbackValue) {
									err = fmt.Errorf("bad row")
								}
							} else {
								rows := 0
								err = p.Each(ctx, int32(w), &dst, func(row *[]byte) error {
									if string(*row) != loopbackValue {
										return fmt.Errorf("bad row")
									}
									rows++
									return nil
								}, Options{})
								if err == nil && rows != 1 {
									err = fmt.Errorf("%d rows", rows)
								}
							}
							if err != nil {
								errs <- err
								return
							}
						}
					})
				}
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Fatal(err)
			}
		})
	}
}

// TestPreparedOneShotIdleCap checks that after a burst the pool keeps at most
// its idle cap of queries and closes the rest.
func TestPreparedOneShotIdleCap(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1)) // cap 16
	const burst = 40
	s, _, peers := runtimeTestSession(t, 1)
	defer s.Close()
	defer peers[0].Close()
	requests := make(chan oneShotRequest, burst)
	p := oneShotPrepare(t, s, peers[0], requests)
	errs := make(chan error, burst)
	for i := range burst {
		go func() {
			var dst []byte
			_, err := p.One(context.Background(), int32(i), &dst, Options{})
			errs <- err
		}()
	}
	pending := make([]oneShotRequest, 0, burst)
	for range burst {
		pending = append(pending, <-requests)
	}
	if n := implExecution.InternalPreparedPoolTracked(p); n != burst {
		t.Fatalf("tracked %d queries during the burst, want %d", n, burst)
	}
	for _, r := range pending {
		if _, err := peers[0].Write(typedTestResponse(t, 4, r.stream, runtimeRowsBody(nil, "row"))); err != nil {
			t.Fatal(err)
		}
	}
	for range burst {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	waitIdle(t, p, 16)
	if n := implExecution.InternalPreparedPoolTracked(p); n != 16 {
		t.Fatalf("tracked %d queries after the burst, want 16", n)
	}
}

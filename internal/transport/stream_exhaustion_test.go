// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/connectivity"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/streams"
)

// A connection whose streams are all in use, orphans included, fails a new
// request at once with ErrNoStreams and stays open. A late response frees its
// orphan's stream without reaching the request that reuses it.
func TestStreamExhaustion(t *testing.T) {
	c, server := invPair(t, 4)
	c.InternalStreams = streams.NewLimited(2)
	if err := server.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	go c.InternalServe(c.InternalCtx)

	type result struct {
		value []byte
		err   error
	}
	execute := func(ctx context.Context) <-chan result {
		done := make(chan result, 1)
		go func() {
			page, err := c.InternalExecuteTyped(ctx, &model.AttemptRequest{Statement: "SELECT value", Consistency: cql.One})
			var r result
			r.err = err
			if page != nil {
				// One row of one cell: [int length][value].
				rows := make([]byte, page.Rows.Len())
				if _, err := page.Rows.CopyTo(rows); err == nil && len(rows) >= 4 {
					r.value = rows[4:]
				}
				page.Owner.Release()
			}
			done <- r
		}()
		return done
	}
	expectNoStreams := func(what string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		r := receiveWithin(t, execute(ctx), what)
		if !errors.Is(r.err, cqlerr.ErrNoStreams) {
			t.Fatalf("%s: %v, want ErrNoStreams", what, r.err)
		}
		if c.Closed() {
			t.Fatalf("%s closed the connection", what)
		}
	}
	readRequest := func() int {
		t.Helper()
		stream, err := invReadRequest(server)
		if err != nil {
			t.Fatal(err)
		}
		return stream
	}
	answer := func(stream int, value string) {
		t.Helper()
		if _, err := server.Write(invFrame(4, stream, typedTestRowsBody([]byte(value)))); err != nil {
			t.Fatal(err)
		}
	}

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	a := execute(ctxA)
	streamA := readRequest()
	b := execute(context.Background())
	streamB := readRequest()
	expectNoStreams("a request with every stream in flight")

	// A's caller leaves; its stream stays held until the server answers.
	cancelA()
	if r := receiveWithin(t, a, "the cancelled request"); !errors.Is(r.err, context.Canceled) {
		t.Fatalf("cancelled request: %v", r.err)
	}
	if s, calls, orphans, _ := connCounters(c); s != 2 || calls != 2 || orphans != 1 {
		t.Fatalf("after cancel: streams %d, calls %d, orphans %d; want 2, 2, 1", s, calls, orphans)
	}
	expectNoStreams("a request while an orphan holds a stream")

	answer(streamA, "late")
	for deadline := time.Now().Add(5 * time.Second); c.InternalStreams.InUse() != 1; {
		if time.Now().After(deadline) {
			t.Fatal("the late response did not free its stream")
		}
		time.Sleep(time.Millisecond)
	}
	d := execute(context.Background())
	if streamD := readRequest(); streamD != streamA {
		t.Fatalf("new request used stream %d, want the freed stream %d", streamD, streamA)
	}
	answer(streamA, "d")
	if r := receiveWithin(t, d, "the request on the reused stream"); r.err != nil || string(r.value) != "d" {
		t.Fatalf("request on the reused stream: %q, %v; want \"d\"", r.value, r.err)
	}
	answer(streamB, "b")
	if r := receiveWithin(t, b, "the long request"); r.err != nil || string(r.value) != "b" {
		t.Fatalf("long request: %q, %v; want \"b\"", r.value, r.err)
	}
	if s, calls, orphans, shared := connCounters(c); s != 0 || calls != 0 || orphans != 0 || shared != 0 {
		t.Fatalf("counters: streams %d, calls %d, orphans %d, shared %d; want zero", s, calls, orphans, shared)
	}
}

// A heartbeat that finds every stream in use counts as a failure only when
// nothing arrived since the previous beat: a connection saturated by callers
// it keeps answering is alive, and closing it would fail every one of them,
// while one that answers nothing is dead even if its callers never time out.
func TestHeartbeatOnExhaustedStreams(t *testing.T) {
	setup := func(t *testing.T, interval time.Duration) (*Conn, net.Conn) {
		c, server := invPair(t, 4)
		c.InternalStreams = streams.NewLimited(1)
		if err := server.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			t.Fatal(err)
		}
		c.InternalCfg.TypedConfig = &connectivity.Config{KeepaliveInterval: interval, KeepaliveTimeout: 5 * time.Second}
		c.InternalCfg.TypedConfig.Pool.MaxOrphanedStreams = InternalDefaultOrphanBudget
		go c.InternalServe(c.InternalCtx)
		return c, server
	}

	t.Run("Busy", func(t *testing.T) {
		c, server := setup(t, 5*time.Millisecond)
		// The server answers every request at once: queries with a row,
		// heartbeats with SUPPORTED.
		go func() {
			for {
				var header [9]byte
				if _, err := io.ReadFull(server, header[:]); err != nil {
					return
				}
				if _, err := io.CopyN(io.Discard, server, int64(binary.BigEndian.Uint32(header[5:]))); err != nil {
					return
				}
				stream := int(binary.BigEndian.Uint16(header[2:4]))
				frame := invFrame(4, stream, typedTestRowsBody(nil))
				if frm.Op(header[4]) == frm.OpOptions {
					frame = []byte{0x84, 0, byte(stream >> 8), byte(stream), byte(frm.OpSupported), 0, 0, 0, 2, 0, 0}
				}
				if _, err := server.Write(frame); err != nil {
					return
				}
			}
		}()
		// One caller keeps the only stream busy: each request goes out as
		// soon as the previous one is answered.
		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				select {
				case <-stop:
					return
				default:
				}
				page, err := c.InternalExecuteTyped(context.Background(), &model.AttemptRequest{Statement: "SELECT value", Consistency: cql.One})
				if page != nil {
					page.Owner.Release()
				}
				if err != nil && !errors.Is(err, cqlerr.ErrNoStreams) {
					return
				}
			}
		}()
		c.InternalHeartBeat(c.InternalCtx)
		// Far more beats than the failure bound, most finding the stream busy.
		time.Sleep(200 * time.Millisecond)
		close(stop)
		receiveWithin(t, done, "the caller")
		if c.Closed() {
			t.Fatal("heartbeats closed a busy connection that answers its requests")
		}
	})

	t.Run("Silent", func(t *testing.T) {
		c, server := setup(t, time.Millisecond)
		done := make(chan error, 1)
		go func() {
			// No timeout: this call never becomes an orphan.
			page, err := c.InternalExecuteTyped(context.Background(), &model.AttemptRequest{Statement: "SELECT value", Consistency: cql.One})
			if page != nil {
				page.Owner.Release()
			}
			done <- err
		}()
		if _, err := invReadRequest(server); err != nil {
			t.Fatal(err)
		}
		c.InternalHeartBeat(c.InternalCtx)
		receiveWithin(t, c.InternalCtx.Done(), "heartbeats to close a connection that answers nothing")
		if err := receiveWithin(t, done, "the request"); err == nil {
			t.Fatal("request on a closed connection succeeded")
		}
	})
}

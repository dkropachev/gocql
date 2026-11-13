// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/settings"
)

// TestInlineSpeculation checks the first attempt that a speculating
// execution runs on the caller's goroutine: a speculative attempt that
// answers first ends that wait and its rows are the page, a zero delay
// speculates while the first attempt waits, and a failed first attempt keeps
// the speculation schedule it started.
func TestInlineSpeculation(t *testing.T) {
	t.Run("SpeculativeWinnerEndsInlineWait", func(t *testing.T) {
		s, conns, peers := runtimeTestSession(t, 2)
		type request struct{ peer, stream int }
		requests := make(chan request, 4)
		done := make(chan struct{})
		t.Cleanup(func() { close(done) })
		for i := range peers {
			go func() {
				for {
					stream := readRequestStream(peers[i])
					if stream < 0 {
						return
					}
					select {
					case requests <- request{peer: i, stream: stream}:
					case <-done:
						return
					}
				}
			}()
		}
		profile := settings.InternalDefaultProfile()
		// The delay orders the requests: the first attempt reaches its peer
		// well before the speculative one, so the second request read is
		// the speculative attempt's.
		profile.Speculation = scheduleSpeculation{}
		if !speculationOnly(&InternalExecutionRequest{InternalLogical: &logicalExecution{}, options: &InternalResolvedOptions{InternalProfile: profile}}) {
			t.Fatal("the default routing with speculation must run its first attempt inline")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		next := func() (request, bool) {
			select {
			case r := <-requests:
				return r, true
			case <-ctx.Done():
				return request{}, false
			}
		}
		// The first attempt gets no answer until the speculative one won,
		// whose traced response names trace.
		trace := cqltype.UUID{15: 7}
		first, winner := make(chan request, 1), make(chan int, 1)
		go func() {
			slow, ok := next()
			if !ok {
				return
			}
			first <- slow
			fast, ok := next()
			if !ok {
				return
			}
			winner <- fast.peer
			frame := typedTestResponse(t, 4, fast.stream, append(append([]byte{}, trace[:]...), runtimeRowsBody(nil, "winner")...))
			frame[1] = frm.FlagTracing
			_, _ = peers[fast.peer].Write(frame)
		}()
		bound := runtimeQuery(t)
		page, err := RunPage(ctx, s, bound, model.StartPage(), 0, testOptions(Options{Profile: &profile, Idempotent: true}))
		if err != nil {
			t.Fatal(err)
		}
		var row cqltype.Bytes
		if !page.NextInto(&row) || string(row.OwnedBytes()) != "winner" {
			t.Fatal("page is not the speculative winner's", page.Err())
		}
		traces := page.Info().CQLTraces()
		page.Close()
		host := implEndpoint.InternalHostInfoHostUUID(conns[<-winner].InternalHost)
		if len(traces) != 1 || traces[0] != (model.CQLTraceRef{ID: trace, HostID: host, Attempt: 2, Speculative: true}) {
			t.Fatalf("traces %+v, want the speculative winner's ref from %v", traces, host)
		}
		bound.Close()
		var slow request
		select {
		case slow = <-first:
		case <-ctx.Done():
			t.Fatal("the first attempt never reached a peer")
		}
		_, _ = peers[slow.peer].Write(typedTestResponse(t, 4, slow.stream, runtimeRowsBody(nil, "loser")))
		if err := bound.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	})

	// A zero speculation delay starts speculative attempts while the first
	// is being sent and waits.
	t.Run("ZeroDelay", func(t *testing.T) {
		s, _, peers := runtimeTestSession(t, 2)
		for i := range peers {
			go func() {
				for {
					stream := readRequestStream(peers[i])
					if stream < 0 {
						return
					}
					// A slow reply keeps the first attempt waiting while
					// the speculation timer fires.
					time.Sleep(time.Millisecond)
					_, _ = peers[i].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "row")))
				}
			}()
		}
		profile := settings.InternalDefaultProfile()
		profile.Speculation = zeroSpeculation{}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for range 50 {
			bound := runtimeQuery(t)
			page, err := RunPage(ctx, s, bound, model.StartPage(), 0, testOptions(Options{Profile: &profile, Idempotent: true}))
			if err != nil {
				t.Fatal(err)
			}
			page.Close()
			bound.Close()
			if err := bound.Wait(ctx); err != nil {
				t.Fatal(err)
			}
		}
		if InternalRuntimeFor(s).speculations.Load() == 0 {
			t.Fatal("no speculative attempt started")
		}
	})

	// A first attempt that fails while its retry waits out a long delay
	// leaves the speculation timer it started running: the speculative
	// attempt comes on that schedule, before the retry.
	t.Run("FailedInlineKeepsSchedule", func(t *testing.T) {
		s, _, peers := runtimeTestSession(t, 2)
		type request struct {
			peer, stream int
			at           time.Time
		}
		requests := make(chan request, 4)
		done := make(chan struct{})
		t.Cleanup(func() { close(done) })
		for i := range peers {
			go func() {
				for {
					stream := readRequestStream(peers[i])
					if stream < 0 {
						return
					}
					select {
					case requests <- request{peer: i, stream: stream, at: time.Now()}:
					case <-done:
						return
					}
				}
			}()
		}
		profile := settings.InternalDefaultProfile()
		profile.Speculation = scheduleSpeculation{}
		profile.Retry = runtimeDelayedRetry{delay: time.Second}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		next := func() request {
			select {
			case r := <-requests:
				return r
			case <-ctx.Done():
				return request{peer: -1}
			}
		}
		served := make(chan time.Duration, 1)
		go func() {
			defer close(served)
			first := next()
			if first.peer < 0 {
				return
			}
			_, _ = peers[first.peer].Write(errorResponse(t, first.stream, protocol.CodeServer, nil))
			second := next()
			if second.peer < 0 {
				return
			}
			_, _ = peers[second.peer].Write(typedTestResponse(t, 4, second.stream, runtimeRowsBody(nil, "row")))
			served <- second.at.Sub(first.at)
		}()
		bound := runtimeQuery(t)
		page, err := RunPage(ctx, s, bound, model.StartPage(), 0, testOptions(Options{Profile: &profile, Idempotent: true}))
		if err != nil {
			t.Fatal(err)
		}
		page.Close()
		bound.Close()
		gap, ok := <-served
		if !ok {
			t.Fatal("the second attempt never reached a peer")
		}
		if gap >= 500*time.Millisecond {
			t.Fatalf("second attempt came %v after the first, not on the speculation schedule", gap)
		}
	})
}

// scheduleSpeculation starts one speculative attempt 50ms after the first.
type scheduleSpeculation struct{}

func (scheduleSpeculation) Delay(attempt int) (time.Duration, bool) {
	return 50 * time.Millisecond, attempt == 1
}

// zeroSpeculation starts up to two speculative attempts at once.
type zeroSpeculation struct{}

func (zeroSpeculation) Delay(attempt int) (time.Duration, bool) { return 0, attempt <= 2 }

// readRequestStream reads one request frame and returns its stream, or -1
// once the peer is closed.
func readRequestStream(peer net.Conn) int {
	var header [9]byte
	if _, err := io.ReadFull(peer, header[:]); err != nil {
		return -1
	}
	if _, err := io.CopyN(io.Discard, peer, int64(binary.BigEndian.Uint32(header[5:]))); err != nil {
		return -1
	}
	return int(binary.BigEndian.Uint16(header[2:4]))
}

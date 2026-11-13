package session

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/settings"
)

type countingObserver struct{ requests atomic.Uint64 }

func (o *countingObserver) ObserveRequest(settings.RequestEvent) { o.requests.Add(1) }
func (o *countingObserver) ObserveAttempt(routing.AttemptEvent)  {}

func (o *countingObserver) StartRequest(ctx context.Context, _ settings.RequestStart) (context.Context, func(settings.RequestEvent)) {
	return ctx, o.ObserveRequest
}
func (o *countingObserver) StartAttempt(context.Context, routing.AttemptStart) func(routing.AttemptEvent) {
	return o.ObserveAttempt
}

// TestRequestCount checks that Requests, derived from the request ids minus
// the refused registrations, counts exactly the admitted requests.
func TestRequestCount(t *testing.T) {
	// Refused: a session marked closing refuses the registration after the
	// admission took its id; that id is not a request.
	t.Run("Refused", func(t *testing.T) {
		s, _, peers := runtimeTestSession(t, 1)
		defer s.Close()
		void := binary.BigEndian.AppendUint32(nil, 1)
		go serveLoopback(peers[0], func([]byte) []byte { return void }, nil)
		b := runtimeQuery(t)
		defer b.Close()
		if _, err := Exec(context.Background(), s, b, CompiledOptions{}); err != nil {
			t.Fatal(err)
		}
		// Hold every registry shard so that the second Exec takes its id and
		// then waits to register while Close marks the session closing.
		rt := InternalRuntimeFor(s)
		release := holdRegistry(rt)
		defer release()
		refused := make(chan error, 1)
		go func() { _, err := Exec(context.Background(), s, b, CompiledOptions{}); refused <- err }()
		spinUntil(t, "Exec took its request id", func() bool { return rt.requestID.Load() == 2 })
		closed := make(chan struct{})
		go func() { _ = s.Close(); close(closed) }()
		spinUntil(t, "Close marked the session", s.Closed)
		release()
		if err := receiveWithin(t, "Exec", refused); !errors.Is(err, runtimeerrors.ErrClosed) {
			t.Fatalf("Exec on a closing session: %v", err)
		}
		receiveWithin(t, "Close", closed)
		if refused := rt.refusedRegistrations.Load(); refused != 1 || rt.requestID.Load() != 2 {
			t.Fatalf("refused %d of %d ids, want 1 of 2", refused, rt.requestID.Load())
		}
		if m := s.Metrics(); m.Requests != 1 || m.InFlight != 0 {
			t.Fatalf("Requests = %d, in flight %d; want 1, 0", m.Requests, m.InFlight)
		}
	})

	// CloseRegistration: a registration holds a request id and waits for its
	// registry shard while Close marks the session closing and starts listing
	// executions. Close marked the session before the shard is released, so
	// the registration, which checks the mark under the shard lock, must be
	// refused with ErrClosed; it must never be registered after the mark,
	// where Close's listing of its shard could miss it.
	t.Run("CloseRegistration", func(t *testing.T) {
		for range 20 {
			s, _, peers := runtimeTestSession(t, 1)
			void := binary.BigEndian.AppendUint32(nil, 1)
			go serveLoopback(peers[0], func([]byte) []byte { return void }, nil)
			b := runtimeQuery(t)
			rt := InternalRuntimeFor(s)
			release := holdRegistry(rt)
			result := make(chan error, 1)
			go func() { _, err := Exec(context.Background(), s, b, CompiledOptions{}); result <- err }()
			spinUntil(t, "Exec took its request id", func() bool { return rt.requestID.Load() == 1 })
			closed := make(chan error, 1)
			go func() { closed <- s.Close() }()
			spinUntil(t, "Close marked the session", s.Closed)
			release()
			// A registration that checked the mark before waiting for the
			// lock would be registered after Close marked, and is listed only
			// if Close reaches its shard after it: that is the miss.
			if err := receiveWithin(t, "Exec", result); !errors.Is(err, runtimeerrors.ErrClosed) {
				t.Fatalf("Exec on a session Close was stopping: %v", err)
			}
			if refused := rt.refusedRegistrations.Load(); refused != 1 {
				t.Fatalf("refused %d registrations, want 1: the execution was registered", refused)
			}
			if cerr := receiveWithin(t, "Close", closed); cerr != nil {
				t.Fatal(cerr)
			}
			if n := rt.activeExecutions(); n != 0 {
				t.Fatalf("%d executions still registered after Close", n)
			}
			if m := s.Metrics(); m.InFlight != 0 {
				t.Fatalf("in flight %d after Close, want 0", m.InFlight)
			}
			b.Close()
		}
	})

	// CloseRace: Close races with admission from 8 goroutines; the observer
	// sees one event per admitted request.
	t.Run("CloseRace", func(t *testing.T) {
		for range 20 {
			requestCountRound(t)
		}
	})
}

func requestCountRound(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	observer := new(countingObserver)
	InternalRuntimeFor(s).config.Observer = observer
	void := binary.BigEndian.AppendUint32(nil, 1)
	go serveLoopback(peers[0], func([]byte) []byte { return void }, nil)
	b := runtimeQuery(t)
	defer b.Close()
	var wg, started sync.WaitGroup
	for range 8 {
		wg.Add(1)
		started.Add(1)
		go func() {
			defer wg.Done()
			first := true
			for {
				_, err := Exec(context.Background(), s, b, CompiledOptions{})
				if first {
					started.Done()
					first = false
				}
				if err != nil && s.Closed() {
					return
				}
			}
		}()
	}
	started.Wait()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	m := s.Metrics()
	if m.Requests == 0 || m.Requests != observer.requests.Load() || m.InFlight != 0 {
		t.Fatalf("Requests = %d, observed %d, in flight %d", m.Requests, observer.requests.Load(), m.InFlight)
	}
}

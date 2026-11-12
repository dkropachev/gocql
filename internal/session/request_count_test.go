package session

import (
	"context"
	"encoding/binary"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/settings"
)

type countingObserver struct{ requests atomic.Uint64 }

func (o *countingObserver) ObserveRequest(settings.RequestEvent) { o.requests.Add(1) }
func (o *countingObserver) ObserveAttempt(routing.AttemptEvent)  {}

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
		for i := range rt.registry.shards {
			rt.registry.shards[i].mu.Lock()
		}
		refused := make(chan error, 1)
		go func() { _, err := Exec(context.Background(), s, b, CompiledOptions{}); refused <- err }()
		for rt.requestID.Load() != 2 {
			runtime.Gosched()
		}
		closed := make(chan struct{})
		go func() { _ = s.Close(); close(closed) }()
		for !s.Closed() {
			runtime.Gosched()
		}
		for i := range rt.registry.shards {
			rt.registry.shards[i].mu.Unlock()
		}
		if err := <-refused; !errors.Is(err, runtimeerrors.ErrClosed) {
			t.Fatalf("Exec on a closing session: %v", err)
		}
		<-closed
		if refused := rt.refusedRegistrations.Load(); refused != 1 || rt.requestID.Load() != 2 {
			t.Fatalf("refused %d of %d ids, want 1 of 2", refused, rt.requestID.Load())
		}
		if m := s.Metrics(); m.Requests != 1 || m.InFlight != 0 {
			t.Fatalf("Requests = %d, in flight %d; want 1, 0", m.Requests, m.InFlight)
		}
	})

	// CloseRegistration: a registration holds a request id and waits for its
	// registry shard while Close marks the session closing and starts listing
	// executions. Once the shard is released the execution must be refused with
	// ErrClosed, or be listed and stopped by Close; it must never be registered
	// after Close listed its shard and so run on unstopped.
	t.Run("CloseRegistration", func(t *testing.T) {
		for range 20 {
			s, _, peers := runtimeTestSession(t, 1)
			void := binary.BigEndian.AppendUint32(nil, 1)
			go serveLoopback(peers[0], func([]byte) []byte { return void }, nil)
			b := runtimeQuery(t)
			rt := InternalRuntimeFor(s)
			for i := range rt.registry.shards {
				rt.registry.shards[i].mu.Lock()
			}
			result := make(chan error, 1)
			go func() { _, err := Exec(context.Background(), s, b, CompiledOptions{}); result <- err }()
			for rt.requestID.Load() != 1 {
				runtime.Gosched()
			}
			closed := make(chan error, 1)
			go func() { closed <- s.Close() }()
			for !s.Closed() {
				runtime.Gosched()
			}
			for i := range rt.registry.shards {
				rt.registry.shards[i].mu.Unlock()
			}
			var err error
			select {
			case err = <-result:
			case <-time.After(10 * time.Second):
				t.Fatal("Exec did not return after Close: it was registered and missed")
			}
			// Close marked the session before the shard was released, so the
			// registration, which checks the mark under the shard lock, is
			// refused. A registration that checked before waiting for the lock
			// would instead be registered after Close marked, and is listed
			// only if Close reaches its shard after it: that is the miss.
			if !errors.Is(err, runtimeerrors.ErrClosed) {
				t.Fatalf("Exec on a session Close was stopping: %v", err)
			}
			if refused := rt.refusedRegistrations.Load(); refused != 1 {
				t.Fatalf("refused %d registrations, want 1: the execution was registered", refused)
			}
			if cerr := <-closed; cerr != nil {
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

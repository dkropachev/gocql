package session

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
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
		data := implCluster.SessionData(s)
		data.InternalSessionStateMu.Lock()
		data.InternalIsClosing = true
		data.InternalSessionStateMu.Unlock()
		if _, err := Exec(context.Background(), s, b, CompiledOptions{}); !errors.Is(err, runtimeerrors.ErrClosed) {
			t.Fatalf("Exec on a closing session: %v", err)
		}
		data.InternalSessionStateMu.Lock()
		data.InternalIsClosing = false
		data.InternalSessionStateMu.Unlock()
		rt := InternalRuntimeFor(s)
		if refused := rt.refusedRegistrations.Load(); refused != 1 || rt.requestID.Load() != 2 {
			t.Fatalf("refused %d of %d ids, want 1 of 2", refused, rt.requestID.Load())
		}
		if m := s.Metrics(); m.Requests != 1 || m.InFlight != 0 {
			t.Fatalf("Requests = %d, in flight %d; want 1, 0", m.Requests, m.InFlight)
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

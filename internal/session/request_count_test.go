package session

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/settings"
)

type countingObserver struct{ requests atomic.Uint64 }

func (o *countingObserver) ObserveRequest(settings.RequestEvent) { o.requests.Add(1) }
func (o *countingObserver) ObserveAttempt(routing.AttemptEvent)  {}

// TestRequestCount checks that Requests, derived from the request ids minus
// the requests refused during Close, counts exactly the admitted requests when
// Close races with admission. The observer sees one event per admitted request.
func TestRequestCount(t *testing.T) {
	// A refusal needs Close to land between an Exec's early closed check and
	// its registration, so rounds repeat until one did.
	for round := 0; round < 200; round++ {
		if requestCountRound(t) {
			return
		}
	}
	t.Log("no request raced with Close in 200 rounds")
}

func requestCountRound(t *testing.T) (raced bool) {
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
	return InternalRuntimeFor(s).refusedDuringClose.Load() != 0
}

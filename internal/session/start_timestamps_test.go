package session

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/settings"
)

func TestStartTimestamps(t *testing.T) {
	t.Run("Concurrent", func(t *testing.T) {
		// Every goroutine passes the same start time, so all but the first
		// call must raise it; values must be unique and each goroutine's
		// strictly increasing.
		var timestamps startTimestamps
		started := time.Now()
		const workers, each = 16, 1000
		results := make([][]int64, workers)
		var wg sync.WaitGroup
		for w := range workers {
			wg.Go(func() {
				for range each {
					results[w] = append(results[w], timestamps.next(started))
				}
			})
		}
		wg.Wait()
		seen := make(map[int64]bool, workers*each)
		for _, values := range results {
			for i, v := range values {
				if seen[v] {
					t.Fatalf("duplicate timestamp %d", v)
				}
				seen[v] = true
				if i > 0 && v <= values[i-1] {
					t.Fatalf("timestamp %d after %d", v, values[i-1])
				}
				if v < started.UnixMicro() {
					t.Fatalf("timestamp %d before start %d", v, started.UnixMicro())
				}
			}
		}
	})

	t.Run("FollowsStart", func(t *testing.T) {
		var timestamps startTimestamps
		started := time.UnixMicro(1_000_000)
		if got := timestamps.next(started); got != 1_000_000 {
			t.Fatalf("got %d, want the start time", got)
		}
		if got := timestamps.next(started.Add(time.Second)); got != 2_000_000 {
			t.Fatalf("got %d, want the later start time", got)
		}
	})

	// Admission with the default generator takes timestamps from its start
	// time without calling the generator; across 10 000 concurrent requests
	// they are unique and strictly increasing per caller.
	t.Run("DefaultGenerator", func(t *testing.T) {
		s, _, peers := runtimeTestSession(t, 1)
		defer s.Close()
		var calls atomic.Int64
		rt := InternalRuntimeFor(s)
		rt.config.TimestampGenerator = func() int64 { calls.Add(1); return 1 }
		rt.config.DefaultTimestamps = true
		var mu sync.Mutex
		sent := make(map[int64]bool)
		void := binary.BigEndian.AppendUint32(nil, 1)
		go serveLoopback(peers[0], func(body []byte) []byte {
			at := 4 + int(binary.BigEndian.Uint32(body[:4])) + 2
			if body[at]&0x20 == 0 {
				t.Error("request without a timestamp")
				return void
			}
			mu.Lock()
			sent[int64(binary.BigEndian.Uint64(body[len(body)-8:]))] = true
			mu.Unlock()
			return void
		}, nil)
		b := runtimeQuery(t)
		defer b.Close()
		const workers, each = 16, 625
		var wg sync.WaitGroup
		before := time.Now().UnixMicro()
		for range workers {
			wg.Go(func() {
				for range each {
					if _, err := Exec(context.Background(), s, b, CompiledOptions{}); err != nil {
						t.Error(err)
						return
					}
				}
			})
		}
		wg.Wait()
		if calls.Load() != 0 {
			t.Fatalf("the default generator was called %d times", calls.Load())
		}
		mu.Lock()
		defer mu.Unlock()
		if len(sent) != workers*each {
			t.Fatalf("%d distinct timestamps for %d requests", len(sent), workers*each)
		}
		for v := range sent {
			if v < before || v > rt.timestamps.last.Load() {
				t.Fatalf("timestamp %d outside [%d, %d]", v, before, rt.timestamps.last.Load())
			}
		}
	})

	t.Run("UserGenerator", func(t *testing.T) {
		// DefaultConfig marks its generator; one set through the builder
		// clears the mark (TestConfigBuilder), and an unmarked generator is
		// called (TestConfig/TimestampGenerator/OnceAcrossRetryAndPages).
		if !settings.DefaultConfig("localhost").DefaultTimestamps {
			t.Fatal("DefaultConfig does not mark its timestamp generator")
		}
	})
}

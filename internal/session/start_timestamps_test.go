package session

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
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

	// Admission with driver timestamps takes them from its start time:
	// across 10 000 concurrent requests they are unique, and each caller's,
	// in the order the server received them, strictly increasing.
	t.Run("DriverTimestamps", func(t *testing.T) {
		const workers, each = 16, 625
		s, _, peers := runtimeTestSession(t, 1)
		defer s.Close()
		rt := InternalRuntimeFor(s)
		rt.config.Timestamps = settings.DriverTimestamps()
		var mu sync.Mutex
		sent := make(map[string][]int64, workers)
		void := binary.BigEndian.AppendUint32(nil, 1)
		go serveLoopback(peers[0], func(body []byte) []byte {
			n := int(binary.BigEndian.Uint32(body[:4]))
			if body[4+n+2]&0x20 == 0 {
				t.Error("request without a timestamp")
				return void
			}
			mu.Lock()
			cql := string(body[4 : 4+n])
			sent[cql] = append(sent[cql], int64(binary.BigEndian.Uint64(body[len(body)-8:])))
			mu.Unlock()
			return void
		}, nil)
		before := time.Now().UnixMicro()
		var wg sync.WaitGroup
		for w := range workers {
			b := timestampQuery(t, fmt.Sprintf("SELECT value FROM ks.tbl /* caller %d */", w))
			defer b.Close()
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
		mu.Lock()
		defer mu.Unlock()
		seen := make(map[int64]bool, workers*each)
		for cql, values := range sent {
			if len(values) != each {
				t.Fatalf("%s: %d requests, want %d", cql, len(values), each)
			}
			for i, v := range values {
				if seen[v] {
					t.Fatalf("duplicate timestamp %d", v)
				}
				seen[v] = true
				if v < before {
					t.Fatalf("%s: timestamp %d before the test started at %d", cql, v, before)
				}
				if i > 0 && v <= values[i-1] {
					t.Fatalf("%s: timestamp %d after %d", cql, v, values[i-1])
				}
			}
		}
		if len(sent) != workers {
			t.Fatalf("%d callers seen, want %d", len(sent), workers)
		}
	})

	// A user generator's value is sent as is.
	t.Run("UserGenerator", func(t *testing.T) {
		s, _, peers := runtimeTestSession(t, 1)
		defer s.Close()
		InternalRuntimeFor(s).config.Timestamps = settings.GeneratedTimestamps(func() int64 { return 4242 })
		got := make(chan int64, 1)
		void := binary.BigEndian.AppendUint32(nil, 1)
		go serveLoopback(peers[0], func(body []byte) []byte {
			at := 4 + int(binary.BigEndian.Uint32(body[:4])) + 2
			if body[at]&0x20 == 0 {
				got <- -1
			} else {
				got <- int64(binary.BigEndian.Uint64(body[len(body)-8:]))
			}
			return void
		}, nil)
		b := runtimeQuery(t)
		defer b.Close()
		if _, err := Exec(context.Background(), s, b, CompiledOptions{}); err != nil {
			t.Fatal(err)
		}
		if v := <-got; v != 4242 {
			t.Fatalf("sent timestamp %d, want the generator's 4242", v)
		}
		if settings.DefaultConfig("localhost").Timestamps.Mode() != settings.DriverTimestampMode {
			t.Fatal("DefaultConfig does not use driver timestamps")
		}
	})
}

func timestampQuery(t testing.TB, cql string) *Bound[cqltype.Bytes] {
	t.Helper()
	b, err := BindUnpreparedBorrowed(binding.Statement[codec.EmptyRecord, cqltype.Bytes]{Kind: cqltype.OrdinaryStatement, CQL: cql, Params: codec.EmptyParamsEncoder, Rows: codec.OneColumn(codec.BlobCodec)}, cqltype.NewColumns(), codec.EmptyRecord{})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

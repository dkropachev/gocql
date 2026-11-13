// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/retry"
)

type nopObserver struct{}

func (nopObserver) StartRequest(ctx context.Context, _ settings.RequestStart) (context.Context, func(settings.RequestEvent)) {
	return ctx, func(settings.RequestEvent) {}
}

func (nopObserver) StartAttempt(context.Context, routing.AttemptStart) func(routing.AttemptEvent) {
	return func(routing.AttemptEvent) {}
}

// constantSpeculation starts each speculative attempt delay after the last.
type constantSpeculation struct{ delay time.Duration }

func (p constantSpeculation) Delay(int) (time.Duration, bool) { return p.delay, true }

// delayRetry retries every failure on the same host after delay.
type delayRetry struct{ delay time.Duration }

func (p delayRetry) Decide(retry.Attempt) retry.Decision {
	return retry.Decision{Action: retry.Same, Delay: p.delay}
}

// BenchmarkGoroutines measures request paths that started goroutines besides
// the caller, the connection reader and the submission writer: observers,
// delayed retries and speculation. plain is the direct path for reference.
func BenchmarkGoroutines(b *testing.B) {
	value := loopbackValue
	for _, variant := range []string{"plain", "observer", "retrydelay", "speculate"} {
		for _, workers := range []int{1, 64} {
			b.Run(fmt.Sprintf("%s/w%d", variant, workers), func(b *testing.B) {
				workload := "pone"
				if variant == "retrydelay" {
					workload = "poneerr"
				}
				s := loopbackTestSession(b, workload)
				values := Options{Timeout: cqltype.Option[time.Duration]{Set: true, Value: 10 * time.Second}}
				switch variant {
				case "observer":
					values.Observer = nopObserver{}
				case "speculate":
					// A delay no loopback read reaches: the timer never fires.
					profile := settings.InternalDefaultProfile()
					profile.Speculation = constantSpeculation{delay: 50 * time.Millisecond}
					values.Profile = &profile
					values.Idempotent = true
				case "retrydelay":
					// Delayed retries wait in the general execution loop.
					values.Profile = generalPathProfile()
					values.Retry = delayRetry{delay: 20 * time.Microsecond}
				}
				options := testOptions(values)
				statement := binding.Statement[int32, []byte]{Kind: cqltype.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl WHERE k = ?", Params: codec.OneParam(codec.Int32), Rows: codec.RowInto(func(*[]byte) {}, codec.Column("value", codec.ByteSliceCodec, func(dst *[]byte) *[]byte { return dst }))}
				prepared := codecTestMust(Prepare(context.Background(), s, statement, PrepareOptions{}))
				run := make([]func(context.Context) error, workers)
				for i := range run {
					query := codecTestMust(prepared.Query(options.Values()))
					b.Cleanup(query.Close)
					dst := make([]byte, 0, len(value))
					key := int32(i)
					run[i] = func(ctx context.Context) error {
						found, _, err := query.One(ctx, key, &dst)
						if err != nil {
							return err
						}
						if !found || len(dst) != len(value) {
							return fmt.Errorf("unexpected row")
						}
						return nil
					}
				}
				goroutineBenchRun(b, run)
			})
		}
	}
}

func goroutineBenchRun(b *testing.B, run []func(context.Context) error) {
	ctx := context.Background()
	for _, fn := range run {
		if err := fn(ctx); err != nil {
			b.Fatal(err)
		}
	}
	var remaining atomic.Int64
	remaining.Store(int64(b.N))
	var failed atomic.Pointer[error]
	var wg sync.WaitGroup
	b.ReportAllocs()
	cpuStart := processCPUTime()
	b.ResetTimer()
	for _, fn := range run {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for remaining.Add(-1) >= 0 {
				if err := fn(ctx); err != nil {
					failed.CompareAndSwap(nil, &err)
					return
				}
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	b.ReportMetric(float64(processCPUTime()-cpuStart)/float64(b.N), "cpu-ns/op")
	if err := failed.Load(); err != nil {
		b.Fatal(*err)
	}
}

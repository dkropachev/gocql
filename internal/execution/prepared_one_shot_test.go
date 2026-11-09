// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/retry"
)

func TestPreparedOneShot(t *testing.T) {
	t.Run("Cancel", func(t *testing.T) {
		// TestPreparedOneShot/Cancel/CallReuse checks that a query whose call was
		// cancelled before its answer serves the next call, and that the late answer
		// does not reach that call.
		t.Run("CallReuse", func(t *testing.T) {
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
		})
	})

	// TestPreparedOneShot/Concurrent runs Exec, One and Each from many goroutines
	// over a real socket.
	t.Run("Concurrent", func(t *testing.T) {
		for _, workload := range []string{"pwrite", "pone"} {
			t.Run(workload, func(t *testing.T) {
				s := loopbackTestSession(t, workload)
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				var wg sync.WaitGroup
				errs := make(chan error, 32)
				if workload == "pwrite" {
					statement := binding.Statement[loopbackRow, codec.EmptyRecord]{Kind: cqltype.OrdinaryStatement, CQL: "INSERT INTO ks.tbl(k, value) VALUES(?, ?)", Params: codec.Params(codec.ParamAt(0, codec.Int32, func(r loopbackRow) int32 { return r.k }), codec.ParamAt(1, codec.BytesEncoder, func(r loopbackRow) []byte { return r.value })), Rows: codec.EmptyRowDecoder}
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
	})

	t.Run("Options", func(t *testing.T) {
		// TestPreparedOneShot/Options/PerCall checks that each call's options reach the
		// wire while calls reuse one pooled query.
		t.Run("PerCall", func(t *testing.T) {
			s, _, peers := runtimeTestSession(t, 1)
			defer s.Close()
			defer peers[0].Close()
			requests := make(chan oneShotRequest, 1)
			p := oneShotPrepare(t, s, peers[0], requests)
			quorum := testOptions(OptionValues{Consistency: cqltype.Option[cqltype.Consistency]{Set: true, Value: cqltype.Quorum}, PageSize: cqltype.Option[int32]{Set: true, Value: 7}})
			two := testOptions(OptionValues{Consistency: cqltype.Option[cqltype.Consistency]{Set: true, Value: cqltype.Two}})
			defaults := settings.InternalDefaultProfile()
			for i, tc := range []struct {
				options     Options
				consistency cqltype.Consistency
				pageSize    int32
			}{
				{quorum, cqltype.Quorum, 7},
				{two, cqltype.Two, defaults.PageSize},
				{Options{}, defaults.Consistency, defaults.PageSize},
				{quorum, cqltype.Quorum, 7},
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
		})
	})

	t.Run("Pool", func(t *testing.T) {
		// TestPreparedOneShot/Pool/Collected checks that the session does not keep an
		// unused statement's pool alive.
		t.Run("Collected", func(t *testing.T) {
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
				if n := InternalQueryPools(s); n != 1 {
					t.Fatalf("pools %d, want 1", n)
				}
			}
			use()
			deadline := time.Now().Add(5 * time.Second)
			for InternalQueryPools(s) != 0 {
				if time.Now().After(deadline) {
					t.Fatal("unused pool still registered")
				}
				runtime.GC()
				time.Sleep(time.Millisecond)
			}
		})

		// TestPreparedOneShot/Pool/IdleCap checks that after a burst the pool keeps at most
		// its idle cap of queries and closes the rest.
		t.Run("IdleCap", func(t *testing.T) {
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
			if n := InternalPooledBusy(s); n != burst {
				t.Fatalf("%d busy pooled queries during the burst, want %d", n, burst)
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
			if n := InternalPooledBusy(s); n != 0 {
				t.Fatalf("%d busy pooled queries after the burst, want 0", n)
			}
		})
	})

	// TestPreparedOneShot/SessionClose checks that closing the session stops an
	// unanswered call and fails later ones.
	t.Run("SessionClose", func(t *testing.T) {
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
		if _, err := p.Exec(context.Background(), 2, Options{}); !errors.Is(err, runtimeerrors.ErrClosed) {
			t.Fatalf("call after close: %v", err)
		}
	})

	// TestPreparedOneShot/SessionCloseDuringBackoff checks that closing the
	// session ends a call waiting out a retry delay, on both execution paths,
	// and that Close returns only after the call's query is idle.
	t.Run("SessionCloseDuringBackoff", func(t *testing.T) {
		for _, general := range []bool{false, true} {
			t.Run(map[bool]string{false: "Direct", true: "General"}[general], func(t *testing.T) {
				s, _, peers := runtimeTestSession(t, 1)
				defer peers[0].Close()
				requests := make(chan oneShotRequest, 1)
				p := oneShotPrepare(t, s, peers[0], requests)
				decided := make(chan struct{}, 1)
				profile := settings.InternalDefaultProfile()
				profile.Retry = retry.PolicyFunc(func(retry.Attempt) retry.Decision {
					decided <- struct{}{}
					return retry.Decision{Action: retry.Same, Delay: time.Hour}
				})
				if general {
					profile.Speculation = neverSpeculate{}
				}
				call := make(chan error, 1)
				go func() {
					_, err := p.Exec(context.Background(), 1, testOptions(OptionValues{Profile: &profile}))
					call <- err
				}()
				r := <-requests
				if _, err := peers[0].Write(serverError(t, r.stream, protocol.ErrCodeUnavailable, unavailableBody())); err != nil {
					t.Fatal(err)
				}
				<-decided
				closed := make(chan struct{})
				go func() { s.Close(); close(closed) }()
				select {
				case <-closed:
				case <-time.After(5 * time.Second):
					t.Fatal("close waited out the retry delay")
				}
				if n := InternalPooledBusy(s); n != 0 {
					t.Fatalf("%d busy pooled queries after close", n)
				}
				if err := <-call; !errors.Is(err, runtimeerrors.ErrClosed) {
					t.Fatalf("call in backoff ended with %v, want ErrClosed", err)
				}
			})
		}
	})

	// TestPreparedOneShot/SessionCloseWaitsForEach checks that Close returns
	// only after an Each callback that is running returns.
	t.Run("SessionCloseWaitsForEach", func(t *testing.T) {
		s, _, peers := runtimeTestSession(t, 1)
		defer peers[0].Close()
		requests := make(chan oneShotRequest, 1)
		p := oneShotPrepare(t, s, peers[0], requests)
		entered, release := make(chan struct{}), make(chan struct{})
		var returned atomic.Bool
		call := make(chan error, 1)
		go func() {
			var dst []byte
			call <- p.Each(context.Background(), 1, &dst, func(*[]byte) error {
				close(entered)
				<-release
				returned.Store(true)
				return nil
			}, Options{})
		}()
		r := <-requests
		if _, err := peers[0].Write(typedTestResponse(t, 4, r.stream, runtimeRowsBody(nil, "row"))); err != nil {
			t.Fatal(err)
		}
		<-entered
		closed := make(chan struct{})
		go func() { s.Close(); close(closed) }()
		select {
		case <-closed:
			t.Fatal("close returned while an Each callback ran")
		case <-time.After(100 * time.Millisecond):
		}
		close(release)
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("close did not return after the callback")
		}
		if !returned.Load() {
			t.Fatal("close returned before the callback")
		}
		if err := <-call; err != nil {
			t.Fatal(err)
		}
	})
}

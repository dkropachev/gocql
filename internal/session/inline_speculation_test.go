// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

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
	"github.com/scylladb/gocql/v2/internal/cluster"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/retry"
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
		trace := cql.UUID{15: 7}
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
		page, err := RunPage(ctx, s, bound, cql.StartPage(), 0, testOptions(Options{Profile: &profile, Idempotent: cql.Some(true)}))
		if err != nil {
			t.Fatal(err)
		}
		var row cql.Bytes
		if !page.NextInto(&row) || string(ownedBytesForTest(row)) != "winner" {
			t.Fatal("page is not the speculative winner's", page.Err())
		}
		traces := page.Info().CQLTraces()
		page.Close()
		host := implEndpoint.InternalHostInfoHostUUID(conns[<-winner].InternalHost)
		if len(traces) != 1 || traces[0] != (cql.CQLTraceRef{ID: trace, HostID: host, Page: 1, Attempt: 2, Speculative: true}) {
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
			page, err := RunPage(ctx, s, bound, cql.StartPage(), 0, testOptions(Options{Profile: &profile, Idempotent: cql.Some(true)}))
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
			_, _ = peers[first.peer].Write(errorResponse(t, first.stream, cqlerr.CodeServer, nil))
			second := next()
			if second.peer < 0 {
				return
			}
			_, _ = peers[second.peer].Write(typedTestResponse(t, 4, second.stream, runtimeRowsBody(nil, "row")))
			served <- second.at.Sub(first.at)
		}()
		bound := runtimeQuery(t)
		page, err := RunPage(ctx, s, bound, cql.StartPage(), 0, testOptions(Options{Profile: &profile, Idempotent: cql.Some(true)}))
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

func (scheduleSpeculation) Delay(s retry.Speculation) (time.Duration, bool) {
	return 50 * time.Millisecond, s.Started == 1
}

// zeroSpeculation starts up to two speculative attempts at once.
type zeroSpeculation struct{}

func (zeroSpeculation) Delay(s retry.Speculation) (time.Duration, bool) { return 0, s.Started <= 2 }

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

// recordSpeculation keeps what each Delay call was given.
type recordSpeculation struct{ seen *[]retry.Speculation }

func (r recordSpeculation) Delay(s retry.Speculation) (time.Duration, bool) {
	*r.seen = append(*r.seen, s)
	return time.Millisecond, true
}

// TestSpeculationInput checks what a speculation policy is given: the
// execution's context with the caller's values and deadline, the attempts
// started, the time since the first and the request's table and
// consistency.
func TestSpeculationInput(t *testing.T) {
	type key struct{}
	var seen []retry.Speculation
	options := &InternalResolvedOptions{}
	options.InternalProfile.Speculation = recordSpeculation{&seen}
	request := &InternalExecutionRequest{idempotent: true, options: options}
	request.routing.Kind = cql.OrdinaryStatement
	request.routing.Keyspace, request.routing.Table = "ks", "tbl"
	request.routing.Consistency = cql.LocalQuorum
	deadline := time.Now().Add(time.Hour)
	ctx, cancel := context.WithDeadline(context.WithValue(context.Background(), key{}, "tier-1"), deadline)
	defer cancel()
	var rt InternalSessionRuntime
	var first time.Time
	for started := 1; started <= 2; started++ {
		if _, ok, err := rt.speculationDelay(ctx, request, started, &first); !ok || err != nil {
			t.Fatalf("speculation declined: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	if len(seen) != 2 {
		t.Fatalf("Delay called %d times", len(seen))
	}
	for i, s := range seen {
		if got, _ := s.Context.Deadline(); s.Context.Value(key{}) != "tier-1" || !got.Equal(deadline) || s.Started != i+1 || s.Keyspace != "ks" || s.Table != "tbl" || s.Consistency != cql.LocalQuorum {
			t.Errorf("call %d: %+v", i+1, s)
		}
	}
	if seen[0].Elapsed != 0 || seen[1].Elapsed < time.Millisecond {
		t.Errorf("Elapsed %v then %v, want zero then at least 1ms", seen[0].Elapsed, seen[1].Elapsed)
	}
}

// Options.Speculation replaces the profile's policy, nil keeps it, and
// retry.NoSpeculation removes it so no scheduler is allocated.
func TestSpeculationOverride(t *testing.T) {
	var seen []retry.Speculation
	profilePolicy, override := recordSpeculation{&seen}, retry.SpeculationPolicyFunc(func(retry.Speculation) (time.Duration, bool) { return 0, true })
	base := &settings.Profile{Speculation: profilePolicy}
	for _, c := range []struct {
		name   string
		set    retry.SpeculationPolicy
		isNil  bool
		isProf bool
	}{
		{"inherit", nil, false, true},
		{"replace", override, false, false},
		{"off", retry.NoSpeculation{}, true, false},
		{"off pointer", &retry.NoSpeculation{}, true, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			resolved, err := resolveOptionValues(base, Options{Speculation: c.set})
			if err != nil {
				t.Fatal(err)
			}
			got := resolved.InternalProfile.Speculation
			switch {
			case c.isNil:
				if got != nil {
					t.Fatalf("Speculation = %T, want nil", got)
				}
			case c.isProf:
				if _, ok := got.(recordSpeculation); !ok {
					t.Fatalf("Speculation = %T, want the profile's", got)
				}
			default:
				if _, ok := got.(retry.SpeculationPolicyFunc); !ok {
					t.Fatalf("Speculation = %T, want the override", got)
				}
			}
		})
	}
	if d, ok := (retry.NoSpeculation{}).Delay(retry.Speculation{}); ok || d != 0 {
		t.Fatalf("NoSpeculation.Delay = %v, %v", d, ok)
	}
}

// TestNegativeSpeculationDelayFails checks that a speculation policy that
// returns a negative delay fails the execution with a cql.ValidationError,
// as a negative retry delay does, instead of silently not speculating:
// before anything is sent when it is the page's first answer, and with the
// attempts in flight abandoned when it is a later one.
func TestNegativeSpeculationDelayFails(t *testing.T) {
	negativeFrom := func(started int) retry.SpeculationPolicy {
		return retry.SpeculationPolicyFunc(func(s retry.Speculation) (time.Duration, bool) {
			if s.Started >= started {
				return -time.Millisecond, true
			}
			return time.Millisecond, true
		})
	}
	run := func(t *testing.T, s *cluster.Core, profile settings.Profile) error {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		bound := runtimeQuery(t)
		defer bound.Close()
		page, err := RunPage(ctx, s, bound, cql.StartPage(), 0, testOptions(Options{Profile: &profile, Idempotent: cql.Some(true)}))
		if page != nil {
			page.Close()
		}
		return err
	}
	check := func(t *testing.T, err error, status cqlerr.ExecutionStatus) {
		t.Helper()
		if _, ok := errors.AsType[*cql.ValidationError](err); !ok || cqlerr.ExecutionStatusOf(err) != status {
			t.Fatalf("err = %v (%v); want a cql.ValidationError reporting %v", err, cqlerr.ExecutionStatusOf(err), status)
		}
	}

	t.Run("InlineFirst", func(t *testing.T) {
		s, _, peers := runtimeTestSession(t, 1)
		quiet := silent(t, peers[0])
		profile := settings.InternalDefaultProfile()
		profile.Speculation = negativeFrom(1)
		check(t, run(t, s, profile), cqlerr.NotExecuted)
		peers[0].Close()
		if !<-quiet {
			t.Fatal("the request was sent")
		}
	})
	t.Run("InlineLater", func(t *testing.T) {
		s, _, peers := runtimeTestSession(t, 2)
		for _, peer := range peers {
			go func() { _, _ = io.Copy(io.Discard, peer) }()
		}
		profile := settings.InternalDefaultProfile()
		profile.Speculation = negativeFrom(2)
		check(t, run(t, s, profile), cqlerr.MaybeExecuted)
	})
	t.Run("General", func(t *testing.T) {
		s, routes, _, _, peers := hostsSession(t, 2)
		for _, peer := range peers {
			go func() { _, _ = io.Copy(io.Discard, peer) }()
		}
		profile := settings.InternalDefaultProfile()
		profile.Routing = routes
		profile.Speculation = negativeFrom(1)
		if speculationOnly(&InternalExecutionRequest{InternalLogical: &logicalExecution{}, options: &InternalResolvedOptions{InternalProfile: profile}}) {
			t.Fatal("a custom routing policy must keep the general path")
		}
		check(t, run(t, s, profile), cqlerr.MaybeExecuted)
	})
}

// TestSpeculationSkip pins the reason an execution with a speculation
// policy reports for not speculating.
func TestSpeculationSkip(t *testing.T) {
	for _, c := range []struct {
		kind       cql.StatementKind
		idempotent bool
		want       cql.SpeculationSkip
	}{
		{cql.OrdinaryStatement, true, cql.SpeculationNotSkipped},
		{cql.OrdinaryStatement, false, cql.SpeculationSkippedNotIdempotent},
		{cql.ConditionalStatement, true, cql.SpeculationSkippedConditional},
		{cql.UnknownStatement, false, cql.SpeculationSkippedConditional},
	} {
		if got := speculationSkip(c.kind, c.idempotent); got != c.want {
			t.Errorf("speculationSkip(%v, %t) = %v, want %v", c.kind, c.idempotent, got, c.want)
		}
	}
}

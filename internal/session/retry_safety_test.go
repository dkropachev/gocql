// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/cluster"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/retry"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

// recordingRoutes routes to its targets in order and records every attempt
// event it observes.
type recordingRoutes struct {
	targets []metadata.Target
	mu      sync.Mutex
	events  []routing2.AttemptEvent
}

func (p *recordingRoutes) Validate(*routing.MetadataSnapshot) error { return nil }
func (p *recordingRoutes) Pick(routing2.Info, *routing.MetadataSnapshot) (routing.Route, bool) {
	return routing.Route{}, false
}
func (p *recordingRoutes) Fallback(routing2.Info, *routing.MetadataSnapshot) routing.QueryPlan {
	return &routing.InternalTargetPlan{InternalTargets: append([]metadata.Target(nil), p.targets...)}
}
func (p *recordingRoutes) Observe(e routing2.AttemptEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, e)
}
func (p *recordingRoutes) observed() []routing2.AttemptEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]routing2.AttemptEvent(nil), p.events...)
}

// twoHostSession is a session of two hosts whose plan tries the first, then
// the second.
func twoHostSession(t *testing.T) (*cluster.Core, *recordingRoutes, func(*Options) error, []*transport.Conn, []net.Conn) {
	t.Helper()
	s, conns, peers := runtimeTestSession(t, 2)
	routes := &recordingRoutes{targets: []metadata.Target{{HostID: implEndpoint.InternalHostInfoHostUUID(conns[0].InternalHost)}, {HostID: implEndpoint.InternalHostInfoHostUUID(conns[1].InternalHost)}}}
	run := func(o *Options) error {
		bound := runtimeQuery(t)
		defer bound.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		page, err := RunPage(ctx, s, bound, cql.StartPage, 0, testOptions(*o))
		if page != nil {
			page.Close()
		}
		return err
	}
	return s, routes, run, conns, peers
}

// silent reports, on the returned channel, whether peer received any byte
// before the test ends.
func silent(t *testing.T, peer net.Conn) <-chan bool {
	t.Helper()
	got := make(chan bool, 1)
	go func() {
		var b [1]byte
		n, _ := peer.Read(b[:])
		got <- n == 0
	}()
	return got
}

// TestRetrySafety pins which failed attempts retry.Safe replays, through the
// direct path (no speculation) and the general one (a speculation policy that
// never fires).
func TestRetrySafety(t *testing.T) {
	for _, general := range []bool{false, true} {
		name := map[bool]string{false: "Direct", true: "General"}[general]
		t.Run(name, func(t *testing.T) {
			profileFor := func(routes *recordingRoutes) *settings.Profile {
				profile := settings.InternalDefaultProfile()
				profile.Routing = routes
				profile.Retry = retry.Safe{}
				if general {
					profile.Speculation = neverSpeculate{}
				}
				return new(profile)
			}

			// A non-idempotent request whose connection broke after it was
			// written may have been applied: Safe must not replay it.
			t.Run("NonIdempotentNotReplayedAfterSend", func(t *testing.T) {
				_, routes, run, conns, peers := twoHostSession(t)
				second := silent(t, peers[1])
				go func() {
					typedTestReadRequest(t, conns[0], peers[0])
					peers[0].Close()
				}()
				err := run(&Options{Profile: profileFor(routes)})
				if err == nil {
					t.Fatal("request succeeded")
				}
				if got := cqlerr.ExecutionStatusOf(err); got != cqlerr.MaybeExecuted {
					t.Fatalf("status %v: %v", got, err)
				}
				failure, ok := errors.AsType[*cqlerr.ExecutionError](err)
				if !ok || len(failure.Attempts) != 1 || failure.Result.Attempts != 1 {
					t.Fatalf("want one attempt: %v", err)
				}
				peers[1].Close()
				if !<-second {
					t.Fatal("the request was replayed on the second host")
				}
			})

			// The same failure of an idempotent request is replayed on the
			// next host.
			t.Run("IdempotentReplayedAfterSend", func(t *testing.T) {
				_, routes, run, conns, peers := twoHostSession(t)
				go func() {
					typedTestReadRequest(t, conns[0], peers[0])
					peers[0].Close()
				}()
				go func() {
					stream := typedTestReadRequest(t, conns[1], peers[1])
					_, _ = peers[1].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "row")))
				}()
				if err := run(&Options{Profile: profileFor(routes), Idempotent: true}); err != nil {
					t.Fatal(err)
				}
			})

			// A server rejection before execution is replayed even for a
			// non-idempotent request.
			t.Run("NonIdempotentReplayedWhenNotExecuted", func(t *testing.T) {
				_, routes, run, conns, peers := twoHostSession(t)
				go func() {
					stream := typedTestReadRequest(t, conns[0], peers[0])
					_, _ = peers[0].Write(serverError(t, stream, cqlerr.CodeUnavailable, unavailableBody()))
				}()
				go func() {
					stream := typedTestReadRequest(t, conns[1], peers[1])
					_, _ = peers[1].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "row")))
				}()
				if err := run(&Options{Profile: profileFor(routes)}); err != nil {
					t.Fatal(err)
				}
				if n := len(routes.observed()); n != 2 {
					t.Fatalf("%d attempts, want 2", n)
				}
			})

			// A server error after which the write may have been applied
			// stops a non-idempotent request.
			t.Run("NonIdempotentWriteTimeoutStops", func(t *testing.T) {
				_, routes, run, conns, peers := twoHostSession(t)
				second := silent(t, peers[1])
				go func() {
					stream := typedTestReadRequest(t, conns[0], peers[0])
					_, _ = peers[0].Write(serverError(t, stream, cqlerr.CodeWriteTimeout, writeTimeoutBody(0, "SIMPLE")))
				}()
				err := run(&Options{Profile: profileFor(routes)})
				if _, ok := errors.AsType[*cqlerr.WriteTimeoutError](err); !ok {
					t.Fatalf("want the write timeout: %v", err)
				}
				peers[1].Close()
				if !<-second {
					t.Fatal("the request was replayed on the second host")
				}
			})
		})
	}
}

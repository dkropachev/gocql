// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/retry"
)

// TestRequestTimeoutCause checks that a request the driver's own timeout
// ended reports cqlerr.ErrRequestTimeout and KindTimeout, and one the
// caller's deadline ended reports neither but KindCanceled; both match
// context.DeadlineExceeded.
func TestRequestTimeoutCause(t *testing.T) {
	for _, general := range []bool{false, true} {
		for _, driver := range []bool{false, true} {
			name := "Direct"
			if general {
				name = "General"
			}
			if driver {
				name += "/Driver"
			} else {
				name += "/Caller"
			}
			t.Run(name, func(t *testing.T) {
				s, conns, peers := runtimeTestSession(t, 1)
				profile := settings.InternalDefaultProfile()
				profile.Retry = nil
				profile.Timeout = 0
				if general {
					profile.Speculation = neverSpeculate{}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if driver {
					profile.Timeout = 20 * time.Millisecond
				} else {
					var cancelShort context.CancelFunc
					ctx, cancelShort = context.WithTimeout(ctx, 20*time.Millisecond)
					defer cancelShort()
				}
				go typedTestReadRequest(t, conns[0], peers[0]) // never answered
				bound := runtimeQuery(t)
				defer bound.Close()
				page, err := RunPage(ctx, s, bound, cql.StartPage(), 0, testOptions(Options{Profile: new(profile)}))
				if page != nil {
					page.Close()
				}
				want := cqlerr.KindCanceled
				if driver {
					want = cqlerr.KindTimeout
				}
				if !errors.Is(err, context.DeadlineExceeded) || cqlerr.KindOf(err) != want {
					t.Fatalf("RunPage = %v (%v); want a deadline of kind %v", err, cqlerr.KindOf(err), want)
				}
				if errors.Is(err, cqlerr.ErrRequestTimeout) != driver {
					t.Fatalf("RunPage = %v; ErrRequestTimeout %t, want %t", err, errors.Is(err, cqlerr.ErrRequestTimeout), driver)
				}
			})
		}
	}
}

// TestWorkerRequestTimeoutCause checks the reusable query's execution
// context: its own timeout reports cqlerr.ErrRequestTimeout and KindTimeout,
// the caller's deadline KindCanceled.
func TestWorkerRequestTimeoutCause(t *testing.T) {
	for _, driver := range []bool{false, true} {
		t.Run(map[bool]string{false: "Caller", true: "Driver"}[driver], func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			values := Options{Retry: retry.Never{}}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if driver {
				values.Timeout = cql.Some(20 * time.Millisecond)
			} else {
				var cancelShort context.CancelFunc
				ctx, cancelShort = context.WithTimeout(ctx, 20*time.Millisecond)
				defer cancelShort()
			}
			q, err := unpreparedQuery(s, binding.Statement[cql.EmptyRow, cql.EmptyRow]{Kind: cql.OrdinaryStatement, CQL: "INSERT INTO ks.t(v) VALUES(1)", Params: cql.NoParams, Rows: execRowsDecoder{}, BindMarkers: cql.NewColumns()}, values)
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()
			if q.state.execCtx == nil {
				t.Fatal("the query has no reusable execution context")
			}
			go typedTestReadRequest(t, conns[0], peers[0]) // never answered
			_, err = q.Exec(ctx, cql.EmptyRow{})
			want := cqlerr.KindCanceled
			if driver {
				want = cqlerr.KindTimeout
			}
			if !errors.Is(err, context.DeadlineExceeded) || cqlerr.KindOf(err) != want || errors.Is(err, cqlerr.ErrRequestTimeout) != driver {
				t.Fatalf("Exec = %v (%v); want a deadline of kind %v", err, cqlerr.KindOf(err), want)
			}
		})
	}
}

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
	"github.com/scylladb/gocql/v2/internal/settings"
)

// TestRequestTimeoutCause checks that a request the driver's own timeout
// ended reports cqlerr.ErrRequestTimeout, and one the caller's deadline
// ended does not; both match context.DeadlineExceeded.
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
				if !errors.Is(err, context.DeadlineExceeded) || cqlerr.KindOf(err) != cqlerr.KindTimeout {
					t.Fatalf("RunPage = %v; want a deadline", err)
				}
				if errors.Is(err, cqlerr.ErrRequestTimeout) != driver {
					t.Fatalf("RunPage = %v; ErrRequestTimeout %t, want %t", err, errors.Is(err, cqlerr.ErrRequestTimeout), driver)
				}
			})
		}
	}
}

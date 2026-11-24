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
	"github.com/scylladb/gocql/v2/retry"
)

// TestUnknownRetryActionFails checks that an Action the driver does not
// define fails the request instead of turning the failure into a success:
// the removed IgnoreWrite's value made a failed read an empty page.
func TestUnknownRetryActionFails(t *testing.T) {
	for _, general := range []bool{false, true} {
		name := "Direct"
		if general {
			name = "General"
		}
		t.Run(name, func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			profile := settings.InternalDefaultProfile()
			profile.Retry = retry.PolicyFunc(func(retry.Attempt) retry.Decision { return retry.Decision{Action: retry.Action(3)} })
			if general {
				profile.Speculation = neverSpeculate{}
			}
			go func() {
				stream := typedTestReadRequest(t, conns[0], peers[0])
				_, _ = peers[0].Write(serverError(t, stream, cqlerr.CodeUnavailable, unavailableBody()))
			}()
			bound := runtimeQuery(t)
			defer bound.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			page, err := RunPage(ctx, s, bound, cql.StartPage(), 0, testOptions(Options{Profile: new(profile)}))
			if page != nil {
				page.Close()
			}
			if _, ok := errors.AsType[*cql.ValidationError](err); !ok {
				t.Fatalf("RunPage = %v; want a validation error for the unknown action", err)
			}
		})
	}
}

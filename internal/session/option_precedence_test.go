// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/settings"
)

// TestOptionPrecedence pins the order the config and exec package docs
// describe: the session's default profile, then Options.Profile replacing it
// whole, then each set Options field.
func TestOptionPrecedence(t *testing.T) {
	session := settings.InternalDefaultProfile()
	session.Consistency = cql.One
	session.PageTimeout = 5 * time.Second
	session.Timeout = time.Second

	r, err := resolveOptionValues(&session, Options{})
	if err != nil || r.InternalProfile.Consistency != cql.One || r.InternalProfile.PageTimeout != 5*time.Second {
		t.Fatalf("session profile not applied: %+v, %v", r.InternalProfile, err)
	}

	replacement := settings.InternalDefaultProfile()
	replacement.Consistency = cql.Two
	r, err = resolveOptionValues(&session, Options{Profile: &replacement})
	if err != nil {
		t.Fatal(err)
	}
	if r.InternalProfile.Consistency != cql.Two {
		t.Errorf("Options.Profile consistency = %v, want TWO", r.InternalProfile.Consistency)
	}
	if r.InternalProfile.PageTimeout != replacement.PageTimeout || r.InternalProfile.Timeout != replacement.Timeout {
		t.Errorf("Options.Profile merged with the session profile instead of replacing it: %+v", r.InternalProfile)
	}

	r, err = resolveOptionValues(&session, Options{
		Profile:     &replacement,
		Consistency: cql.Some(cql.Three),
		Timeout:     cql.Some(2 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if r.InternalProfile.Consistency != cql.Three || r.InternalProfile.Timeout != 2*time.Second {
		t.Errorf("Options fields did not override the profile: %+v", r.InternalProfile)
	}
}

// TestOptionTimeoutsTighten checks that Options.Timeout and
// Options.PageTimeout add a limit to the profile's rather than replace it: the
// earlier one applies, and a longer or zero option leaves the profile's.
func TestOptionTimeoutsTighten(t *testing.T) {
	for _, tc := range []struct {
		name            string
		profile, option time.Duration
		want            time.Duration
	}{
		{name: "OptionEarlier", profile: time.Minute, option: time.Second, want: time.Second},
		{name: "ProfileEarlier", profile: time.Second, option: time.Minute, want: time.Second},
		{name: "ZeroOptionKeepsProfile", profile: time.Second, option: 0, want: time.Second},
		{name: "ZeroProfileTakesOption", profile: 0, option: time.Second, want: time.Second},
		{name: "BothZero", profile: 0, option: 0, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			profile := settings.InternalDefaultProfile()
			profile.Timeout, profile.PageTimeout = tc.profile, tc.profile
			for _, base := range []struct {
				name    string
				session *settings.Profile
				options Options
			}{
				{name: "SessionProfile", session: &profile},
				{name: "OptionsProfile", options: Options{Profile: &profile}},
			} {
				options := base.options
				options.Timeout, options.PageTimeout = cql.Some(tc.option), cql.Some(tc.option)
				r, err := resolveOptionValues(base.session, options)
				if err != nil {
					t.Fatalf("%s: %v", base.name, err)
				}
				if r.InternalProfile.Timeout != tc.want || r.InternalProfile.PageTimeout != tc.want {
					t.Errorf("%s: timeout %v, page timeout %v, want %v", base.name, r.InternalProfile.Timeout, r.InternalProfile.PageTimeout, tc.want)
				}
			}
		})
	}
	for _, options := range []Options{{Timeout: cql.Some(-time.Second)}, {PageTimeout: cql.Some(-time.Second)}} {
		if _, err := resolveOptionValues(nil, options); err == nil {
			t.Errorf("negative option %+v accepted", options)
		}
	}
}

// TestExecutionDeadline checks that the call's context deadline and the
// resolved timeout both bound an execution, and the earlier one wins.
func TestExecutionDeadline(t *testing.T) {
	for _, tc := range []struct {
		name             string
		parent, timeout  time.Duration
		want             time.Duration
		parentHasNoLimit bool
	}{
		{name: "ContextEarlier", parent: time.Minute, timeout: time.Hour, want: time.Minute},
		{name: "TimeoutEarlier", parent: time.Hour, timeout: time.Minute, want: time.Minute},
		{name: "NoTimeout", parent: time.Minute, timeout: 0, want: time.Minute},
		{name: "NoContextDeadline", timeout: time.Minute, want: time.Minute, parentHasNoLimit: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := context.Background()
			if !tc.parentHasNoLimit {
				var cancel context.CancelFunc
				parent, cancel = context.WithTimeout(parent, tc.parent)
				defer cancel()
			}
			start := time.Now()
			ctx, cancel := InternalExecutionContext(parent, tc.timeout)
			defer cancel()
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Fatal("execution has no deadline")
			}
			if got := deadline.Sub(start); got > tc.want+time.Second || got < tc.want-time.Second {
				t.Fatalf("deadline in %v, want about %v", got, tc.want)
			}
		})
	}
}

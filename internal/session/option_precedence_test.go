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

	// A zero PageSize keeps the profile's; any other replaces it.
	session.PageSize = 7
	if r, err = resolveOptionValues(&session, Options{}); err != nil || r.InternalProfile.PageSize != 7 {
		t.Errorf("zero PageSize: page size %d, %v, want the profile's 7", r.InternalProfile.PageSize, err)
	}
	if r, err = resolveOptionValues(&session, Options{PageSize: 3}); err != nil || r.InternalProfile.PageSize != 3 {
		t.Errorf("PageSize 3: page size %d, %v, want 3", r.InternalProfile.PageSize, err)
	}
	if _, err = resolveOptionValues(&session, Options{PageSize: -2}); err == nil {
		t.Error("negative PageSize accepted")
	}
}

// TestOptionTimeoutsReplace checks that a set Options.Timeout or
// Options.PageTimeout replaces the profile's limit, longer or shorter, and
// that a set zero removes it.
func TestOptionTimeoutsReplace(t *testing.T) {
	for _, tc := range []struct {
		name            string
		profile, option time.Duration
		want            time.Duration
	}{
		{name: "OptionShorter", profile: time.Minute, option: time.Second, want: time.Second},
		{name: "OptionLonger", profile: time.Second, option: 10 * time.Minute, want: 10 * time.Minute},
		{name: "ZeroOptionRemovesLimit", profile: time.Second, option: 0, want: 0},
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
	// An unset option keeps the profile's limit.
	profile := settings.InternalDefaultProfile()
	profile.Timeout, profile.PageTimeout = time.Second, 2*time.Second
	if r, err := resolveOptionValues(&profile, Options{Consistency: cql.Some(cql.One)}); err != nil || r.InternalProfile.Timeout != time.Second || r.InternalProfile.PageTimeout != 2*time.Second {
		t.Errorf("unset options changed the profile's limits: %+v, %v", r.InternalProfile, err)
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

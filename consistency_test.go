// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
)

func TestConsistency(t *testing.T) {
	t.Parallel()

	t.Run("MarshalText", func(t *testing.T) {
		// Every level survives a text round trip, which is what config files and flags
		// rely on.
		t.Run("RoundTrip", func(t *testing.T) {
			t.Parallel()

			for _, tc := range allConsistencies {
				text, err := tc.c.MarshalText()
				if err != nil || string(text) != tc.name {
					t.Fatalf("%v.MarshalText() = %q, %v; want %q", uint16(tc.c), text, err, tc.name)
				}
				var got cql.Consistency
				if err := got.UnmarshalText(text); err != nil || got != tc.c {
					t.Fatalf("UnmarshalText(%q) = %v, %v; want %v", text, got, err, tc.c)
				}
			}
		})
	})

	t.Run("ParseConsistency", func(t *testing.T) {
		// ParseConsistency panics on input it cannot parse; ParseConsistencyWrapper is
		// the non-panicking form and returns the same error instead.
		t.Run("Invalid", func(t *testing.T) {
			t.Parallel()

			for _, in := range []string{"", "bogus", " one", "LOCAL QUORUM"} {
				if _, err := cql.ParseConsistencyWrapper(in); err == nil {
					t.Fatalf("ParseConsistencyWrapper(%q) returned no error", in)
				}

				func() {
					defer func() {
						r := recover()
						err, ok := r.(error)
						if !ok || !strings.Contains(err.Error(), "invalid consistency") {
							t.Fatalf("ParseConsistency(%q) panicked with %v, want an invalid consistency error", in, r)
						}
					}()
					cql.ParseConsistency(in)
				}()
			}
		})

		t.Run("Valid", func(t *testing.T) {
			t.Parallel()

			for _, tc := range allConsistencies {
				for _, in := range []string{tc.name, strings.ToLower(tc.name)} {
					if got := cql.ParseConsistency(in); got != tc.c {
						t.Fatalf("ParseConsistency(%q) = %v, want %v", in, got, tc.c)
					}
					got, err := cql.ParseConsistencyWrapper(in)
					if err != nil || got != tc.c {
						t.Fatalf("ParseConsistencyWrapper(%q) = %v, %v; want %v", in, got, err, tc.c)
					}
				}
			}

			if got := cql.ParseConsistency("Local_Quorum"); got != cql.LocalQuorum {
				t.Fatalf("ParseConsistency is case-insensitive: got %v for Local_Quorum", got)
			}
		})
	})

	t.Run("String", func(t *testing.T) {
		t.Run("Unknown", func(t *testing.T) {
			t.Parallel()

			if got := cql.Consistency(0x42).String(); got != "UNKNOWN_CONS_0x42" {
				t.Fatalf("String() = %q, want UNKNOWN_CONS_0x42", got)
			}
		})
	})

	t.Run("UnmarshalText", func(t *testing.T) {
		// UnmarshalText is exact: it neither upper-cases nor trims, unlike the Parse
		// functions below, and it leaves the receiver untouched on error.
		t.Run("Rejects", func(t *testing.T) {
			t.Parallel()

			for _, in := range []string{"", "quorum", " QUORUM", "UNKNOWN_CONS_0x42", "LOCALQUORUM"} {
				got := cql.LocalOne
				err := got.UnmarshalText([]byte(in))
				if err == nil || !strings.Contains(err.Error(), "invalid consistency") {
					t.Fatalf("UnmarshalText(%q) error = %v, want invalid consistency", in, err)
				}
				if got != cql.LocalOne {
					t.Fatalf("UnmarshalText(%q) changed the receiver to %v on error", in, got)
				}
			}
		})
	})
}

var allConsistencies = []struct {
	c    cql.Consistency
	name string
}{
	{cql.Any, "ANY"},
	{cql.One, "ONE"},
	{cql.Two, "TWO"},
	{cql.Three, "THREE"},
	{cql.Quorum, "QUORUM"},
	{cql.All, "ALL"},
	{cql.LocalQuorum, "LOCAL_QUORUM"},
	{cql.EachQuorum, "EACH_QUORUM"},
	{cql.Serial, "SERIAL"},
	{cql.LocalSerial, "LOCAL_SERIAL"},
	{cql.LocalOne, "LOCAL_ONE"},
}

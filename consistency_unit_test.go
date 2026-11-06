package gocql

import (
	"strings"
	"testing"
)

var allConsistencies = []struct {
	c    Consistency
	name string
}{
	{Any, "ANY"},
	{One, "ONE"},
	{Two, "TWO"},
	{Three, "THREE"},
	{Quorum, "QUORUM"},
	{All, "ALL"},
	{LocalQuorum, "LOCAL_QUORUM"},
	{EachQuorum, "EACH_QUORUM"},
	{Serial, "SERIAL"},
	{LocalSerial, "LOCAL_SERIAL"},
	{LocalOne, "LOCAL_ONE"},
}

// Every level survives a text round trip, which is what config files and flags
// rely on.
func TestConsistencyTextRoundTrip(t *testing.T) {
	t.Parallel()

	for _, tc := range allConsistencies {
		text, err := tc.c.MarshalText()
		if err != nil || string(text) != tc.name {
			t.Fatalf("%v.MarshalText() = %q, %v; want %q", uint16(tc.c), text, err, tc.name)
		}
		var got Consistency
		if err := got.UnmarshalText(text); err != nil || got != tc.c {
			t.Fatalf("UnmarshalText(%q) = %v, %v; want %v", text, got, err, tc.c)
		}
	}
}

func TestConsistencyStringUnknown(t *testing.T) {
	t.Parallel()

	if got := Consistency(0x42).String(); got != "UNKNOWN_CONS_0x42" {
		t.Fatalf("String() = %q, want UNKNOWN_CONS_0x42", got)
	}
}

// UnmarshalText is exact: it neither upper-cases nor trims, unlike the Parse
// functions below, and it leaves the receiver untouched on error.
func TestConsistencyUnmarshalTextRejects(t *testing.T) {
	t.Parallel()

	for _, in := range []string{"", "quorum", " QUORUM", "UNKNOWN_CONS_0x42", "LOCALQUORUM"} {
		got := LocalOne
		err := got.UnmarshalText([]byte(in))
		if err == nil || !strings.Contains(err.Error(), "invalid consistency") {
			t.Fatalf("UnmarshalText(%q) error = %v, want invalid consistency", in, err)
		}
		if got != LocalOne {
			t.Fatalf("UnmarshalText(%q) changed the receiver to %v on error", in, got)
		}
	}
}

func TestParseConsistency(t *testing.T) {
	t.Parallel()

	for _, tc := range allConsistencies {
		for _, in := range []string{tc.name, strings.ToLower(tc.name)} {
			if got := ParseConsistency(in); got != tc.c {
				t.Fatalf("ParseConsistency(%q) = %v, want %v", in, got, tc.c)
			}
			got, err := ParseConsistencyWrapper(in)
			if err != nil || got != tc.c {
				t.Fatalf("ParseConsistencyWrapper(%q) = %v, %v; want %v", in, got, err, tc.c)
			}
		}
	}

	if got := ParseConsistency("Local_Quorum"); got != LocalQuorum {
		t.Fatalf("ParseConsistency is case-insensitive: got %v for Local_Quorum", got)
	}
}

// ParseConsistency panics on input it cannot parse; ParseConsistencyWrapper is
// the non-panicking form and returns the same error instead.
func TestParseConsistencyInvalid(t *testing.T) {
	t.Parallel()

	for _, in := range []string{"", "bogus", " one", "LOCAL QUORUM"} {
		if _, err := ParseConsistencyWrapper(in); err == nil {
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
			ParseConsistency(in)
		}()
	}
}

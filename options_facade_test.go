// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cql/stream"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/exec"
	implSession "github.com/scylladb/gocql/v2/internal/session"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/retry"
)

func TestPublicOptionsSnapshot(t *testing.T) {
	profile := codecTestMust(config.NewProfile().WithPageSize(11).Build())
	payload := []byte("original")
	want := exec.Options{
		Profile: &profile, Retry: retry.Safe{MaxAttempts: 7}, Keyspace: "ks",
		CustomPayload: []cql.PayloadEntry{{Key: "key", Value: cql.Nullable[cql.Bytes]{Valid: true, Value: cql.BorrowBytes(payload)}}},
		Timeout:       cql.Some(time.Second), PageTimeout: cql.Some(2 * time.Second),
		Prefetch: cql.Some(0), Timestamp: cql.Some[int64](123), PageSize: cql.Some[int32](5),
		NowInSeconds: cql.Some[int32](456), ShardID: cql.Some[uint32](2),
		SerialConsistency: exec.SerialSerial, Consistency: cql.Some(cql.One),
		HostID:          cql.Some(cql.MustParseUUID("00112233-4455-4677-8899-aabbccddeeff")),
		IncludeMetadata: true, Idempotent: true, Trace: true,
	}
	if got := roundTripOptions(t, want); !reflect.DeepEqual(got, want) {
		t.Fatalf("public options lost settings:\n got %+v\nwant %+v", got, want)
	}
	if got := roundTripOptions(t, exec.Options{SerialConsistency: exec.SerialOmit}); got.SerialConsistency != exec.SerialOmit {
		t.Fatal("explicitly omitted serial consistency was lost")
	}
	raw := codecTestMust(implSession.NewCompiledOptions(nil, internalOptions(want)))
	bound := exec.CompiledOptions(raw)
	profile.PageSize = 99
	payload[0] = 'X'
	got := bound.Values()
	if got.Profile.PageSize != 11 || string(got.CustomPayload[0].Value.Value.OwnedBytes()) != "original" {
		t.Fatal("bound public options borrowed caller profile or payload")
	}
	got.Profile.PageSize = 100
	got.CustomPayload[0].Key = "changed"
	if again := bound.Values(); again.Profile.PageSize != 11 || again.CustomPayload[0].Key != "key" {
		t.Fatal("Values exposed mutable bound storage")
	}
	if zero := (exec.CompiledOptions{}).Values(); !reflect.DeepEqual(zero, exec.Options{}) {
		t.Fatalf("zero bound options do not return zero overrides: %+v", zero)
	}
}

// roundTripOptions converts o to the internal options, compiles them and
// reads them back through exec.CompiledOptions.Values.
func roundTripOptions(t *testing.T, o exec.Options) exec.Options {
	t.Helper()
	compiled, err := implSession.NewCompiledOptions(nil, internalOptions(o))
	if err != nil {
		t.Fatal(err)
	}
	return exec.CompiledOptions(compiled).Values()
}

func TestOptionsProfileMustBeBuilt(t *testing.T) {
	literal := config.Profile{PageSize: 1000}
	_, err := implSession.NewCompiledOptions(nil, internalOptions(exec.Options{Profile: &literal}))
	if v, ok := errors.AsType[*cqlerr.ValidationError](err); !ok || v.Field != "profile" || !strings.Contains(err.Error(), "NewProfile") {
		t.Fatalf("compiling a Profile literal = %v, want a profile ValidationError naming NewProfile", err)
	}
	built := codecTestMust(config.NewProfile().Build())
	built.PageSize = 1000
	compiled := codecTestMust(implSession.NewCompiledOptions(nil, internalOptions(exec.Options{Profile: &built})))
	if got := exec.CompiledOptions(compiled).Values().Profile; got == nil || got.PageSize != 1000 || !builtProfile(*got) {
		t.Fatalf("edited built profile = %+v", got)
	}
}

// Keep public records and their implementation counterparts synchronized as
// settings are added. Private execution hooks deliberately stay internal.
func TestPublicOptionFields(t *testing.T) {
	for _, pair := range [][2]reflect.Type{
		{reflect.TypeFor[config.Profile](), reflect.TypeFor[implSettings.Profile]()},
		{reflect.TypeFor[exec.Options](), reflect.TypeFor[implSession.Options]()},
		{reflect.TypeFor[exec.PrepareOptions](), reflect.TypeFor[implSession.PrepareOptions]()},
		{reflect.TypeFor[exec.StreamOptions](), reflect.TypeFor[implSession.StreamOptions]()},
	} {
		public, internal := pair[0], pair[1]
		for i := range internal.NumField() {
			f := internal.Field(i)
			if f.IsExported() {
				if _, ok := public.FieldByName(f.Name); !ok {
					t.Errorf("%s is missing %s", public, f.Name)
				}
			}
		}
		for i := range public.NumField() {
			f := public.Field(i)
			if !f.IsExported() {
				continue
			}
			if _, ok := internal.FieldByName(f.Name); !ok {
				t.Errorf("%s is missing %s", internal, f.Name)
			}
		}
	}
}

func builtProfile(p config.Profile) bool {
	v := implSettings.InternalProfileFromPublic(p)
	return v.InternalBuilt()
}

// A second option fails every call before the handle is touched, with the same
// validation error.
func TestTooManyOptions(t *testing.T) {
	ctx := context.Background()
	var p *Prepared[int32, []byte]
	var s *testSession
	var dst []byte
	two := []exec.CompiledOptions{{}, {}}
	check := func(name string, err error) {
		t.Helper()
		if ve, ok := errors.AsType[*cqlerr.ValidationError](err); !ok || ve.Field != "options" || ve.Err.Error() != "at most one CompiledOptions" {
			t.Errorf("%s: error %v", name, err)
		}
	}
	_, err := p.Exec(ctx, 1, two...)
	check("Exec", err)
	_, _, err = p.One(ctx, 1, &dst, two...)
	check("One", err)
	_, err = p.Each(ctx, 1, &dst, func(*[]byte) error { return nil }, two...)
	check("Each", err)
	yields := 0
	for row, err := range p.All(ctx, 1, &dst, two...) {
		yields++
		if row != nil {
			t.Error("All yielded a row")
		}
		check("All", err)
	}
	if yields != 1 {
		t.Errorf("All: %d yields, want 1", yields)
	}
	_, err = p.Run(ctx, 1, two...)
	check("Run", err)
	_, err = p.RunPage(ctx, 1, cql.PageState{}, 0, two...)
	check("RunPage", err)
	_, err = s.ExecBatch(ctx, nil, two...)
	check("ExecBatch", err)
	_, err = s.RunBatch[[]byte](ctx, nil, nil, two...)
	check("RunBatch", err)
	checkPrepare := func(name string, err error) {
		t.Helper()
		if ve, ok := errors.AsType[*cqlerr.ValidationError](err); !ok || ve.Field != "options" || ve.Err.Error() != "at most one PrepareOptions" {
			t.Errorf("%s: error %v", name, err)
		}
	}
	_, err = s.Prepare(ctx, cql.Statement[int32, []byte]{}, exec.PrepareOptions{}, exec.PrepareOptions{})
	checkPrepare("Prepare", err)
	_, err = s.PrepareStream(ctx, stream.Statement[int32, []byte]{}, exec.PrepareOptions{}, exec.PrepareOptions{})
	checkPrepare("PrepareStream", err)
}

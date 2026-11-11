// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	implSession "github.com/scylladb/gocql/v2/internal/session"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/retry"
)

func TestPublicOptionsSnapshot(t *testing.T) {
	profile := codecTestMust(NewProfile().WithPageSize(11).Build())
	payload := []byte("original")
	want := Options{
		Profile: &profile, Retry: retry.Safe{MaxAttempts: 7}, Keyspace: "ks",
		CustomPayload: []cql.PayloadEntry{{Key: "key", Value: cql.Nullable[cql.Bytes]{Valid: true, Value: cql.BorrowBytes(payload)}}},
		Timeout:       cql.Some(time.Second), PageTimeout: cql.Some(2 * time.Second),
		Prefetch: cql.Some(0), Timestamp: cql.Some[int64](123), PageSize: cql.Some[int32](5),
		NowInSeconds: cql.Some[int32](456), ShardID: cql.Some[uint32](2),
		SerialConsistency: cql.Some(cql.Serial), Consistency: cql.Some(cql.One),
		HostID:          cql.Some(cql.MustParseUUID("00112233-4455-4677-8899-aabbccddeeff")),
		IncludeMetadata: true, Idempotent: true, Trace: true,
	}
	if got := publicOptions(internalOptions(want)); !reflect.DeepEqual(got, want) {
		t.Fatalf("public options lost settings:\n got %+v\nwant %+v", got, want)
	}
	if got := publicOptions(internalOptions(Options{NoSerialConsistency: true})); !got.NoSerialConsistency {
		t.Fatal("explicitly disabled serial consistency was lost")
	}
	raw := codecTestMust(implSession.NewCompiledOptions(nil, internalOptions(want)))
	bound := CompiledOptions(raw)
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
	if zero := (CompiledOptions{}).Values(); !reflect.DeepEqual(zero, Options{}) {
		t.Fatalf("zero bound options do not return zero overrides: %+v", zero)
	}
}

func TestOptionsProfileMustBeBuilt(t *testing.T) {
	literal := Profile{PageSize: 1000}
	_, err := implSession.NewCompiledOptions(nil, internalOptions(Options{Profile: &literal}))
	if v, ok := errors.AsType[*cqlerr.ValidationError](err); !ok || v.Field != "profile" || !strings.Contains(err.Error(), "NewProfile") {
		t.Fatalf("compiling a Profile literal = %v, want a profile ValidationError naming NewProfile", err)
	}
	built := codecTestMust(NewProfile().Build())
	built.PageSize = 1000
	compiled := codecTestMust(implSession.NewCompiledOptions(nil, internalOptions(Options{Profile: &built})))
	if got := CompiledOptions(compiled).Values().Profile; got == nil || got.PageSize != 1000 || !got.built {
		t.Fatalf("edited built profile = %+v", got)
	}
}

// Keep public records and their implementation counterparts synchronized as
// settings are added. Private execution hooks deliberately stay internal.
func TestPublicOptionFields(t *testing.T) {
	for _, pair := range [][2]reflect.Type{
		{reflect.TypeFor[Profile](), reflect.TypeFor[implSettings.Profile]()},
		{reflect.TypeFor[Options](), reflect.TypeFor[implSession.Options]()},
		{reflect.TypeFor[PrepareOptions](), reflect.TypeFor[implSession.PrepareOptions]()},
		{reflect.TypeFor[StreamOptions](), reflect.TypeFor[implSession.StreamOptions]()},
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

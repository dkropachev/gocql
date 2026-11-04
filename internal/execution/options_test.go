// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"testing"
	"time"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

func TestNewOptionsRejectsInvalidValues(t *testing.T) {
	invalid := map[string]OptionValues{
		"consistency":  {Consistency: Option[Consistency]{Set: true, Value: Consistency(0xff)}},
		"serial":       {SerialConsistency: Option[Option[Consistency]]{Set: true, Value: Option[Consistency]{Set: true, Value: One}}},
		"timeout":      {Timeout: Option[time.Duration]{Set: true, Value: -1}},
		"page size":    {PageSize: Option[int32]{Set: true}},
		"prefetch":     {Prefetch: Option[int]{Set: true, Value: 2}},
		"host":         {HostID: Option[UUID]{Set: true}},
		"shard":        {ShardID: Option[uint32]{Set: true}},
		"payload keys": {CustomPayload: []PayloadEntry{{Key: "k"}, {Key: "k"}}},
		"profile":      {Profile: &Profile{}},
	}
	for name, values := range invalid {
		if _, err := NewOptions(values); err == nil {
			t.Errorf("%s: invalid values accepted", name)
		}
	}
}

func TestNewOptionsFreezesValues(t *testing.T) {
	payload := []byte("value")
	profile := defaultProfile()
	profile.PageSize = 10
	values := OptionValues{Profile: &profile, CustomPayload: []PayloadEntry{{Key: "k", Value: cqltype.Nullable[cqltype.Bytes]{Valid: true, Value: cqltype.BorrowBytes(payload)}}}}
	o, err := NewOptions(values)
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	profile.PageSize = 20
	values.CustomPayload[0].Key = "changed"
	got := o.Values()
	if got.Profile.PageSize != 10 || got.CustomPayload[0].Key != "k" || string(got.CustomPayload[0].Value.Value.OwnedBytes()) != "value" {
		t.Fatalf("frozen options changed: page size %d, payload %q=%q", got.Profile.PageSize, got.CustomPayload[0].Key, got.CustomPayload[0].Value.Value.OwnedBytes())
	}
	got.Profile.PageSize = 30
	got.CustomPayload[0].Key = "other"
	if again := o.Values(); again.Profile.PageSize != 10 || again.CustomPayload[0].Key != "k" {
		t.Fatal("Values exposed frozen storage")
	}
}

func TestOptionsResolveOncePerSession(t *testing.T) {
	_, rt := attachmentSession()
	sessionProfile := defaultProfile()
	sessionProfile.PageSize = 7
	rt.InternalSetProfile(&sessionProfile)

	var zero Options
	if r := zero.resolve(rt); r != &rt.defaultOptions || r.InternalProfile.PageSize != 7 {
		t.Fatal("zero options do not use the session default", r.InternalProfile.PageSize)
	}

	own := defaultProfile()
	own.PageSize = 9
	fixed := testOptions(OptionValues{Profile: &own})
	if r := fixed.resolve(rt); r != fixed.p.fixed || r.InternalProfile.PageSize != 9 {
		t.Fatal("options with a profile not resolved at creation")
	}

	overrides := testOptions(OptionValues{Consistency: Option[Consistency]{Set: true, Value: One}})
	first := overrides.resolve(rt)
	if first.InternalProfile.PageSize != 7 || first.InternalProfile.Consistency != One {
		t.Fatal("overrides not merged over the session profile", first.InternalProfile)
	}
	if overrides.resolve(rt) != first {
		t.Fatal("resolution not reused for the same session")
	}
	_, other := attachmentSession()
	if r := overrides.resolve(other); r == first || r.InternalProfile.PageSize != defaultProfile().PageSize {
		t.Fatal("resolution reused across sessions")
	}
}

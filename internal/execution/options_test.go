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
		if _, err := NewOptions(nil, values); err == nil {
			t.Errorf("%s: invalid values accepted", name)
		}
	}
}

func TestNewOptionsFreezesValues(t *testing.T) {
	payload := []byte("value")
	profile := defaultProfile()
	profile.PageSize = 10
	values := OptionValues{Profile: &profile, CustomPayload: []PayloadEntry{{Key: "k", Value: cqltype.Nullable[cqltype.Bytes]{Valid: true, Value: cqltype.BorrowBytes(payload)}}}}
	o, err := NewOptions(nil, values)
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

func TestOptionsResolvedForTheirSession(t *testing.T) {
	s, rt := attachmentSession()
	sessionProfile := defaultProfile()
	sessionProfile.PageSize = 7
	rt.InternalSetProfile(&sessionProfile)
	var scratch InternalResolvedOptions

	var zero Options
	if r := zero.resolve(rt, &scratch); r != &rt.defaultOptions || r.InternalProfile.PageSize != 7 {
		t.Fatal("zero options do not use the session default", r.InternalProfile.PageSize)
	}

	own := defaultProfile()
	own.PageSize = 9
	fixed := testOptions(OptionValues{Profile: &own})
	if r := fixed.resolve(rt, &scratch); r != &fixed.p.resolved || r.InternalProfile.PageSize != 9 {
		t.Fatal("options with a profile not resolved at creation")
	}

	overrides := OptionValues{Consistency: Option[Consistency]{Set: true, Value: One}}
	bound, err := NewOptions(s, overrides)
	if err != nil {
		t.Fatal(err)
	}
	r := bound.resolve(rt, &scratch)
	if r != &bound.p.resolved || r.InternalProfile.PageSize != 7 || r.InternalProfile.Consistency != One {
		t.Fatal("options not resolved for their session at creation", r.InternalProfile)
	}

	// Options used without their session resolve on each execution.
	_, other := attachmentSession()
	if r := bound.resolve(other, &scratch); r != &scratch || r.InternalProfile.PageSize != defaultProfile().PageSize || r.InternalProfile.Consistency != One {
		t.Fatal("options of another session not resolved for this one", r.InternalProfile)
	}
	unbound := testOptions(overrides)
	scratch = InternalResolvedOptions{}
	if r := unbound.resolve(rt, &scratch); r != &scratch || r.InternalProfile.PageSize != 7 {
		t.Fatal("options without a session not resolved for this one", r.InternalProfile)
	}
}

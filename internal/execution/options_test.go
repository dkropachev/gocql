// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/model"
)

func TestOptions(t *testing.T) {
	t.Run("BindOptions", func(t *testing.T) {
		t.Run("KeepsEmptyOnSessionDefault", func(t *testing.T) {
			s, _ := attachmentSession()
			zero, err := InternalBindOptions(s, OptionValues{})
			if err != nil || zero.p != nil {
				t.Fatalf("empty values bound to %+v, %v; want the zero Options", zero.p, err)
			}
			set, err := InternalBindOptions(s, OptionValues{Idempotent: true})
			if err != nil || set.p == nil || !set.p.bound {
				t.Fatalf("values not bound to the session: %+v, %v", set.p, err)
			}
			if _, err := InternalBindOptions(nil, OptionValues{Idempotent: true}); err == nil {
				t.Fatal("values bound without a session")
			}
		})
	})

	t.Run("NewOptions", func(t *testing.T) {
		t.Run("FreezesValues", func(t *testing.T) {
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
		})

		t.Run("RejectsInvalidValues", func(t *testing.T) {
			invalid := map[string]OptionValues{
				"consistency":  {Consistency: Option[Consistency]{Set: true, Value: Consistency(0xff)}},
				"serial":       {SerialConsistency: Option[Consistency]{Set: true, Value: One}},
				"timeout":      {Timeout: Option[time.Duration]{Set: true, Value: -1}},
				"page size":    {PageSize: Option[int32]{Set: true}},
				"prefetch":     {Prefetch: Option[int]{Set: true, Value: 2}},
				"host":         {HostID: Option[UUID]{Set: true}},
				"shard":        {ShardID: Option[uint32]{Set: true}},
				"payload keys": {CustomPayload: []PayloadEntry{{Key: "k"}, {Key: "k"}}},
				"profile":      {Profile: &Profile{}},
				"serial both":  {SerialConsistency: Option[Consistency]{Set: true, Value: LocalSerial}, NoSerialConsistency: true},
			}
			for name, values := range invalid {
				if _, err := NewOptions(nil, values); err == nil {
					t.Errorf("%s: invalid values accepted", name)
				}
			}
		})

		t.Run("SerialConsistencyOverrides", func(t *testing.T) {
			set, err := resolveOptionValues(nil, OptionValues{SerialConsistency: cqltype.Some(Serial)})
			if err != nil {
				t.Fatal(err)
			}
			if !set.InternalProfile.SerialConsistency.Set || set.InternalProfile.SerialConsistency.Value != Serial {
				t.Fatalf("SerialConsistency = %+v, want SERIAL", set.InternalProfile.SerialConsistency)
			}
			none, err := resolveOptionValues(nil, OptionValues{NoSerialConsistency: true})
			if err != nil {
				t.Fatal(err)
			}
			if none.InternalProfile.SerialConsistency.Set {
				t.Fatalf("SerialConsistency = %+v, want omitted", none.InternalProfile.SerialConsistency)
			}
			inherited, err := resolveOptionValues(nil, OptionValues{})
			if err != nil {
				t.Fatal(err)
			}
			if inherited.InternalProfile.SerialConsistency != defaultProfile().SerialConsistency {
				t.Fatalf("SerialConsistency = %+v, want the profile's", inherited.InternalProfile.SerialConsistency)
			}
		})
	})

	t.Run("Resolve", func(t *testing.T) {
		t.Run("BoundSession", func(t *testing.T) {
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

			// Bound options are used as they are on any session, with the defaults
			// of the session they were bound to.
			_, other := attachmentSession()
			if r := bound.resolve(other, &scratch); r != &bound.p.resolved || r.InternalProfile.PageSize != 7 {
				t.Fatal("bound options resolved again on another session", r.InternalProfile)
			}
			unbound := testOptions(overrides)
			scratch = InternalResolvedOptions{}
			if r := unbound.resolve(rt, &scratch); r != &scratch || r.InternalProfile.PageSize != 7 {
				t.Fatal("options without a session not resolved for this one", r.InternalProfile)
			}
		})
	})

	t.Run("Templates", func(t *testing.T) {
		t.Run("EncodedAtResolution", func(t *testing.T) {
			own := defaultProfile()
			own.PageSize = 10
			o := testOptions(OptionValues{Profile: &own, Keyspace: "ks"})
			resolved := o.p.resolved
			if resolved.templates == nil {
				t.Fatal("options resolved without templates")
			}
			for version := byte(4); version <= 5; version++ {
				for _, ts := range []bool{false, true} {
					w := &attemptRequest{PageSize: 10, DefaultTimestamp: ts, SerialConsistency: own.SerialConsistency.Value}
					if version == 5 {
						w.Keyspace = "ks"
					}
					want := &resolved.templates[version-4][0]
					if ts {
						want = &resolved.templates[version-4][1]
					}
					if got := paramsTemplate(&resolved, w, version); got == nil || got != want {
						t.Fatalf("v%d timestamp %v: template not the resolved one", version, ts)
					}
				}
			}
			// A v5 EXECUTE sends no keyspace, so it takes the keyspace-less template
			// whatever keyspace its statement was prepared in.
			for _, ts := range []bool{false, true} {
				w := &attemptRequest{PageSize: 10, DefaultTimestamp: ts, SerialConsistency: own.SerialConsistency.Value, Keyspace: "prepared_ks"}
				setPrepared(w, &InternalHostPrepared{InternalId: []byte{1}}, &Conn{}, true)
				if w.Keyspace != "" {
					t.Fatal("EXECUTE kept the keyspace")
				}
				want := &resolved.templates[2][0]
				if ts {
					want = &resolved.templates[2][1]
				}
				got := paramsTemplate(&resolved, w, 5)
				if got == nil || got != want || got.Flags&frm.FlagWithKeyspace != 0 {
					t.Fatalf("EXECUTE timestamp %v: not the keyspace-less resolved template", ts)
				}
			}
			// Another keyspace, an unpaged request and protocol v3 fall back.
			if got := paramsTemplate(&resolved, &attemptRequest{PageSize: 10, SerialConsistency: own.SerialConsistency.Value, Keyspace: "other"}, 5); got == &resolved.templates[1][0] {
				t.Fatal("template used for another keyspace")
			}
			if got := paramsTemplate(&resolved, &attemptRequest{}, 4); got == &resolved.templates[0][0] {
				t.Fatal("template used for an unpaged request")
			}
			if resolved.templates.find(&attemptRequest{PageSize: 10, SerialConsistency: own.SerialConsistency.Value}, 3) != nil {
				t.Fatal("template found for protocol v3")
			}
		})

		// Requests the resolved options have no template for encode field by field:
		// a batch, and options resolved for one execution.
		t.Run("ParamsFallbacks", func(t *testing.T) {
			_, rt := attachmentSession()
			profile := &rt.defaultOptions.InternalProfile
			w := &attemptRequest{PageSize: int(profile.PageSize), SerialConsistency: profile.SerialConsistency.Value}
			if paramsTemplate(&rt.defaultOptions, w, 4) == nil {
				t.Fatal("no template for the session's default options")
			}
			w.Batch = &model.TypedBatchRequest{}
			if paramsTemplate(&rt.defaultOptions, w, 4) != nil {
				t.Fatal("a batch took a template")
			}
			var scratch InternalResolvedOptions
			if paramsTemplate(&scratch, &attemptRequest{}, 4) != nil {
				t.Fatal("options resolved for one execution took a template")
			}
		})

		// Options with a timestamp send one with every request, so they encode no
		// template without it.
		t.Run("WithTimestamp", func(t *testing.T) {
			profile := defaultProfile()
			templates := newOptionTemplates(&profile, &OptionValues{Timestamp: Option[int64]{Value: 1, Set: true}}, "ks")
			for shape := range templates {
				if templates[shape][0].Protocol != 0 || templates[shape][1].Protocol == 0 {
					t.Fatalf("shape %d: want only the timestamp template", shape)
				}
			}
			if templates.find(&attemptRequest{PageSize: int(profile.PageSize), SerialConsistency: profile.SerialConsistency.Value}, 4) != nil {
				t.Fatal("an empty slot matched a request")
			}
		})
	})
}

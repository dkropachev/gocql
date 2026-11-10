// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func TestOptions(t *testing.T) {
	t.Run("BindOptions", func(t *testing.T) {
		t.Run("KeepsEmptyOnSessionDefault", func(t *testing.T) {
			s, _ := attachmentSession()
			zero, err := InternalBindOptions(s, Options{})
			if err != nil || zero.p != nil {
				t.Fatalf("empty values bound to %+v, %v; want the zero BoundOptions", zero.p, err)
			}
			set, err := InternalBindOptions(s, Options{Idempotent: true})
			if err != nil || set.p == nil || !set.p.bound {
				t.Fatalf("values not bound to the session: %+v, %v", set.p, err)
			}
			if _, err := InternalBindOptions(nil, Options{Idempotent: true}); err == nil {
				t.Fatal("values bound without a session")
			}
		})
	})

	t.Run("NewOptions", func(t *testing.T) {
		t.Run("FreezesValues", func(t *testing.T) {
			payload := []byte("value")
			profile := settings.InternalDefaultProfile()
			profile.PageSize = 10
			values := Options{Profile: &profile, CustomPayload: []model.PayloadEntry{{Key: "k", Value: cqltype.Nullable[cqltype.Bytes]{Valid: true, Value: cqltype.BorrowBytes(payload)}}}}
			o, err := NewBoundOptions(nil, values)
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
			invalid := map[string]Options{
				"consistency":  {Consistency: cqltype.Option[cqltype.Consistency]{Set: true, Value: cqltype.Consistency(0xff)}},
				"serial":       {SerialConsistency: cqltype.Option[cqltype.Consistency]{Set: true, Value: cqltype.One}},
				"timeout":      {Timeout: cqltype.Option[time.Duration]{Set: true, Value: -1}},
				"page size":    {PageSize: cqltype.Option[int32]{Set: true}},
				"prefetch":     {Prefetch: cqltype.Option[int]{Set: true, Value: 2}},
				"host":         {HostID: cqltype.Option[cqltype.UUID]{Set: true}},
				"shard":        {ShardID: cqltype.Option[uint32]{Set: true}},
				"payload keys": {CustomPayload: []model.PayloadEntry{{Key: "k"}, {Key: "k"}}},
				"profile":      {Profile: &settings.Profile{}},
				"serial both":  {SerialConsistency: cqltype.Option[cqltype.Consistency]{Set: true, Value: cqltype.LocalSerial}, NoSerialConsistency: true},
			}
			for name, values := range invalid {
				if _, err := NewBoundOptions(nil, values); err == nil {
					t.Errorf("%s: invalid values accepted", name)
				}
			}
		})

		t.Run("SerialConsistencyOverrides", func(t *testing.T) {
			set, err := resolveOptionValues(nil, Options{SerialConsistency: cqltype.Some(cqltype.Serial)})
			if err != nil {
				t.Fatal(err)
			}
			if !set.InternalProfile.SerialConsistency.Set || set.InternalProfile.SerialConsistency.Value != cqltype.Serial {
				t.Fatalf("SerialConsistency = %+v, want SERIAL", set.InternalProfile.SerialConsistency)
			}
			none, err := resolveOptionValues(nil, Options{NoSerialConsistency: true})
			if err != nil {
				t.Fatal(err)
			}
			if none.InternalProfile.SerialConsistency.Set {
				t.Fatalf("SerialConsistency = %+v, want omitted", none.InternalProfile.SerialConsistency)
			}
			inherited, err := resolveOptionValues(nil, Options{})
			if err != nil {
				t.Fatal(err)
			}
			if inherited.InternalProfile.SerialConsistency != settings.InternalDefaultProfile().SerialConsistency {
				t.Fatalf("SerialConsistency = %+v, want the profile's", inherited.InternalProfile.SerialConsistency)
			}
		})
	})

	t.Run("Resolve", func(t *testing.T) {
		t.Run("BoundSession", func(t *testing.T) {
			s, rt := attachmentSession()
			sessionProfile := settings.InternalDefaultProfile()
			sessionProfile.PageSize = 7
			rt.InternalSetProfile(&sessionProfile)
			var scratch InternalResolvedOptions

			var zero BoundOptions
			if r := zero.resolve(rt, &scratch); r != &rt.defaultOptions || r.InternalProfile.PageSize != 7 {
				t.Fatal("zero options do not use the session default", r.InternalProfile.PageSize)
			}

			own := settings.InternalDefaultProfile()
			own.PageSize = 9
			fixed := testOptions(Options{Profile: &own})
			if r := fixed.resolve(rt, &scratch); r != &fixed.p.resolved || r.InternalProfile.PageSize != 9 {
				t.Fatal("options with a profile not resolved at creation")
			}

			overrides := Options{Consistency: cqltype.Option[cqltype.Consistency]{Set: true, Value: cqltype.One}}
			bound, err := NewBoundOptions(s, overrides)
			if err != nil {
				t.Fatal(err)
			}
			r := bound.resolve(rt, &scratch)
			if r != &bound.p.resolved || r.InternalProfile.PageSize != 7 || r.InternalProfile.Consistency != cqltype.One {
				t.Fatal("options not resolved for their session at creation", r.InternalProfile)
			}

			// On another session, bound options are resolved again for that
			// session: its defaults apply, not those of the binding session.
			_, other := attachmentSession()
			otherProfile := settings.InternalDefaultProfile()
			otherProfile.PageSize = 11
			other.InternalSetProfile(&otherProfile)
			scratch = InternalResolvedOptions{}
			if r := bound.resolve(other, &scratch); r != &scratch || r.InternalProfile.PageSize != 11 || r.InternalProfile.Consistency != cqltype.One {
				t.Fatal("bound options not resolved for another session", r.InternalProfile)
			}
			if r := bound.resolveFixed(other); r == &bound.p.resolved || r.InternalProfile.PageSize != 11 || r.templates == nil {
				t.Fatal("bound options not fixed for another session", r.InternalProfile)
			}
			// Options that name their own profile and were built without a
			// session hold on every session.
			if r := fixed.resolve(other, &scratch); r != &fixed.p.resolved {
				t.Fatal("profile options resolved again on another session")
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
			own := settings.InternalDefaultProfile()
			own.PageSize = 10
			o := testOptions(Options{Profile: &own, Keyspace: "ks"})
			resolved := o.p.resolved
			if resolved.templates == nil {
				t.Fatal("options resolved without templates")
			}
			for version := byte(4); version <= 5; version++ {
				for _, ts := range []bool{false, true} {
					w := &model.AttemptRequest{PageSize: 10, DefaultTimestamp: ts, SerialConsistency: own.SerialConsistency.Value}
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
				w := &model.AttemptRequest{PageSize: 10, DefaultTimestamp: ts, SerialConsistency: own.SerialConsistency.Value, Keyspace: "prepared_ks"}
				setPrepared(w, &InternalHostPrepared{InternalId: []byte{1}}, &transport.Conn{}, true)
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
			if got := paramsTemplate(&resolved, &model.AttemptRequest{PageSize: 10, SerialConsistency: own.SerialConsistency.Value, Keyspace: "other"}, 5); got == &resolved.templates[1][0] {
				t.Fatal("template used for another keyspace")
			}
			if got := paramsTemplate(&resolved, &model.AttemptRequest{}, 4); got == &resolved.templates[0][0] {
				t.Fatal("template used for an unpaged request")
			}
			if resolved.templates.find(&model.AttemptRequest{PageSize: 10, SerialConsistency: own.SerialConsistency.Value}, 3) != nil {
				t.Fatal("template found for protocol v3")
			}
		})

		// Requests the resolved options have no template for encode field by field:
		// a batch, and options resolved for one execution.
		t.Run("ParamsFallbacks", func(t *testing.T) {
			_, rt := attachmentSession()
			profile := &rt.defaultOptions.InternalProfile
			w := &model.AttemptRequest{PageSize: int(profile.PageSize), SerialConsistency: profile.SerialConsistency.Value}
			if paramsTemplate(&rt.defaultOptions, w, 4) == nil {
				t.Fatal("no template for the session's default options")
			}
			w.Batch = &model.TypedBatchRequest{}
			if paramsTemplate(&rt.defaultOptions, w, 4) != nil {
				t.Fatal("a batch took a template")
			}
			var scratch InternalResolvedOptions
			if paramsTemplate(&scratch, &model.AttemptRequest{}, 4) != nil {
				t.Fatal("options resolved for one execution took a template")
			}
		})

		// BoundOptions with a timestamp send one with every request, so they encode no
		// template without it.
		t.Run("WithTimestamp", func(t *testing.T) {
			profile := settings.InternalDefaultProfile()
			templates := newOptionTemplates(&profile, &Options{Timestamp: cqltype.Option[int64]{Value: 1, Set: true}}, "ks")
			for shape := range templates {
				if templates[shape][0].Protocol != 0 || templates[shape][1].Protocol == 0 {
					t.Fatalf("shape %d: want only the timestamp template", shape)
				}
			}
			if templates.find(&model.AttemptRequest{PageSize: int(profile.PageSize), SerialConsistency: profile.SerialConsistency.Value}, 4) != nil {
				t.Fatal("an empty slot matched a request")
			}
		})
	})
}

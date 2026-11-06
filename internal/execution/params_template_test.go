// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"testing"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	model "github.com/scylladb/gocql/v2/internal/model"
)

// Requests the resolved options have no template for encode field by field:
// a batch, and options resolved for one execution.
func TestParamsTemplateFallbacks(t *testing.T) {
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
}

func TestOptionTemplatesEncodedAtResolution(t *testing.T) {
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
}

// Options with a timestamp send one with every request, so they encode no
// template without it.
func TestOptionTemplatesWithTimestamp(t *testing.T) {
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
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/settings"
)

// TestUnpagedPageSize checks that cqltype.Unpaged, set on options or on a
// session profile, resolves to a request without a page size, and that it
// rejects an explicit prefetch.
func TestUnpagedPageSize(t *testing.T) {
	unpaged := Options{PageSize: cqltype.Some(cqltype.Unpaged)}
	r, err := resolveOptionValues(nil, unpaged)
	if err != nil || r.InternalProfile.PageSize != 0 {
		t.Fatal("options", r.InternalProfile.PageSize, err)
	}
	profile := settings.InternalDefaultProfile()
	profile.PageSize = cqltype.Unpaged
	if err := profile.ValidateProfile(); err != nil {
		t.Fatal("profile", err)
	}
	if r, err = resolveOptionValues(&profile, Options{}); err != nil || r.InternalProfile.PageSize != 0 {
		t.Fatal("session profile", r.InternalProfile.PageSize, err)
	}
	if r, err = resolveOptionValues(&profile, Options{PageSize: cqltype.Some[int32](100)}); err != nil || r.InternalProfile.PageSize != 100 {
		t.Fatal("override", r.InternalProfile.PageSize, err)
	}
	if r, err = resolveOptionValues(nil, Options{Profile: &profile}); err != nil || r.InternalProfile.PageSize != 0 {
		t.Fatal("options profile", r.InternalProfile.PageSize, err)
	}
	unpaged.Prefetch = cqltype.Some(1)
	if _, err := resolveOptionValues(nil, unpaged); !hasErrorType[*settings.UnsupportedSettingError](err) {
		t.Fatal("prefetch", err)
	}
	unpaged.Prefetch = cqltype.Some(0)
	if _, err := resolveOptionValues(nil, unpaged); err != nil {
		t.Fatal("zero prefetch", err)
	}
	// A request without a page size carries no page-size flag.
	for _, size := range []int{0, 100} {
		tmpl, err := protocol.NewParamsTemplate(&model.AttemptRequest{PageSize: size}, 4)
		if err != nil {
			t.Fatal(err)
		}
		if got := tmpl.Flags&frm.FlagPageSize != 0; got != (size > 0) {
			t.Fatal("page size flag", size, got)
		}
	}
}

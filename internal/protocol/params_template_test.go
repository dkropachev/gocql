// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"bytes"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// A request encoded from its parameters template is byte-identical to one
// encoded field by field, for every combination of options, and a template
// for other options is ignored.
func TestParamsTemplateMatchesFieldEncoding(t *testing.T) {
	value := []byte{0, 0, 0, 5, 'v', 'a', 'l', 'u', 'e'}
	values, _ := wire.NewChain(wire.Borrow(value))
	now := int32(7)
	cases := 0
	for _, version := range []byte{3, 4, 5} {
		settings := protocolSettings(version)
		for mask := 0; mask < 1<<9; mask++ {
			bit := func(i int) bool { return mask&(1<<i) != 0 }
			if version < 5 && (bit(5) || bit(6)) {
				continue // keyspace and now_in_seconds require v5
			}
			r := &model.AttemptRequest{Statement: "SELECT v FROM ks.t WHERE k=?", Consistency: cqltype.LocalQuorum, Timestamp: 0x0102030405060708}
			if bit(0) {
				r.Statement, r.PreparedID, r.ResultMetadataID = "", []byte("prepared-id"), []byte("md")
			}
			if bit(1) {
				r.Values, r.ValueCount = values, 1
			}
			if bit(2) {
				r.PageSize = 100
			}
			if bit(3) {
				r.SerialConsistency = cqltype.LocalSerial
			}
			if bit(4) {
				r.DefaultTimestamp = true
			}
			if bit(5) {
				r.Keyspace = "ks"
			}
			if bit(6) {
				r.HasNowInSeconds, r.NowInSeconds = true, now
			}
			if bit(7) {
				r.PagingState = []byte("state")
			}
			if bit(8) {
				r.SkipMetadata = true
			}
			plain, err := settings.BuildTypedRequest(r, 9)
			if err != nil {
				t.Fatal(version, mask, err)
			}
			want := typedTestBytes(t, plain)

			template, err := NewParamsTemplate(r, version)
			if err != nil {
				t.Fatal(version, mask, err)
			}
			if !template.Matches(r, version) {
				t.Fatalf("v%d mask %#x: template does not match its own request", version, mask)
			}
			r.Params = template
			got, err := settings.BuildTypedRequest(r, 9)
			if err != nil {
				t.Fatal(version, mask, err)
			}
			if !bytes.Equal(typedTestBytes(t, got), want) {
				t.Fatalf("v%d mask %#x: templated frame differs\n got %x\nwant %x", version, mask, typedTestBytes(t, got), want)
			}

			// A template for another page size, or another protocol, does
			// not match, so the caller does not attach it.
			other := *r
			other.PageSize = r.PageSize + 7
			if stale, _ := NewParamsTemplate(&other, version); stale.Matches(r, version) {
				t.Fatalf("v%d mask %#x: template for another page size matches", version, mask)
			}
			if template.Matches(r, version^1) {
				t.Fatalf("v%d mask %#x: template matches another protocol", version, mask)
			}
			cases++
		}
	}
	if cases == 0 {
		t.Fatal("no cases ran")
	}
}

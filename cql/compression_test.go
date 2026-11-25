// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"encoding/json"
	"testing"
)

// TestCompressionText parses every compression's name in any case,
// round-trips it through JSON, and rejects other names.
func TestCompressionText(t *testing.T) {
	for _, tc := range []struct {
		text string
		want Compression
	}{
		{"none", NoCompression}, {"lz4", LZ4Compression}, {"LZ4", LZ4Compression}, {"Snappy", SnappyCompression},
	} {
		if got, err := ParseCompression(tc.text); err != nil || got != tc.want {
			t.Fatalf("ParseCompression(%q) = %v, %v; want %v", tc.text, got, err, tc.want)
		}
		encoded, err := json.Marshal(tc.want)
		if err != nil {
			t.Fatalf("json.Marshal(%v): %v", tc.want, err)
		}
		var decoded Compression
		if err := json.Unmarshal(encoded, &decoded); err != nil || decoded != tc.want {
			t.Fatalf("JSON round trip of %v through %s = %v, %v", tc.want, encoded, decoded, err)
		}
	}
	for _, s := range []string{"", "zstd", "Compression(3)"} {
		if c, err := ParseCompression(s); err == nil {
			t.Fatalf("ParseCompression(%q) = %v, want an error", s, c)
		}
	}
	if _, err := Compression(3).MarshalText(); err == nil {
		t.Fatal("an undefined Compression marshaled")
	}
}

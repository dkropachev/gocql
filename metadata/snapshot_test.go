// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata_test

import (
	"testing"

	"github.com/scylladb/gocql/v2/metadata"
)

func TestEmptySnapshot(t *testing.T) {
	for name, s := range map[string]*metadata.Snapshot{"nil": nil, "zero": {}} {
		if s.Generation() != 0 || s.Hosts() != nil || s.Diagnostics() != nil {
			t.Errorf("%s: reports a cluster", name)
		}
		if _, ok := s.Keyspace("ks"); ok {
			t.Errorf("%s: Keyspace found a keyspace", name)
		}
		if _, err := s.Replicas("ks", "t", 0); err == nil {
			t.Errorf("%s: Replicas succeeded", name)
		}
		if _, err := s.Token("ks", "t"); err == nil {
			t.Errorf("%s: Token succeeded", name)
		}
		if _, err := s.TokenRanges("ks", "t"); err == nil {
			t.Errorf("%s: TokenRanges succeeded", name)
		}
	}
}

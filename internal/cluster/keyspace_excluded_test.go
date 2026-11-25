// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"testing"

	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/metadata"
)

// TestKeyspaceExcluded checks when Connect warns that the session keyspace
// is left out of the metadata keyspaces.
func TestKeyspaceExcluded(t *testing.T) {
	for _, tc := range []struct {
		keyspace  string
		keyspaces []string
		mode      metadata.Mode
		want      bool
	}{
		{"app", []string{"other"}, metadata.ModeFull, true},
		{"app", []string{"other"}, metadata.ModeKeyspaces, true},
		{"app", []string{"other", "app"}, metadata.ModeFull, false},
		{"app", nil, metadata.ModeFull, false},
		{"", []string{"other"}, metadata.ModeFull, false},
		{"app", []string{"other"}, metadata.ModeNone, false},
	} {
		config := settings.DefaultConfig("h")
		config.Keyspace, config.Metadata.Keyspaces, config.Metadata.Mode = tc.keyspace, tc.keyspaces, tc.mode
		if got := keyspaceExcluded(config); got != tc.want {
			t.Errorf("keyspace %q, metadata keyspaces %v, %v: %t", tc.keyspace, tc.keyspaces, tc.mode, got)
		}
	}
}

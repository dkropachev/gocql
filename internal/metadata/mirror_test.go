// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"testing"

	"github.com/scylladb/gocql/v2/internal/link"
	"github.com/scylladb/gocql/v2/metadata"
)

func TestMirrorLayouts(t *testing.T) {
	if err := link.CheckLayout[metadata.Keyspace, keyspaceRep](); err != nil {
		t.Error(err)
	}
	if err := link.CheckLayout[metadata.Aggregate, aggregateRep](); err != nil {
		t.Error(err)
	}
}

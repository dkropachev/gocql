// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"testing"

	"github.com/scylladb/gocql/v2/internal/link"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/routing"
)

func TestMirrorLayouts(t *testing.T) {
	if err := link.CheckLayout[metadata.Route, Route](); err != nil {
		t.Error(err)
	}
	if err := link.CheckLayout[routing.PartitionKey, partitionKeyRep](); err != nil {
		t.Error(err)
	}
	if metadata.AnyShard != AnyShard {
		t.Errorf("AnyShard differs: metadata %d, engine %d", metadata.AnyShard, AnyShard)
	}
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"github.com/scylladb/gocql/v2/internal/link"
	"github.com/scylladb/gocql/v2/metadata"
)

var (
	newPublicSnapshot = link.Get[func(any) *metadata.Snapshot](link.NewSnapshot)
	publicSource      = link.Get[func(*metadata.Snapshot) any](link.SnapshotSource)
)

func SnapshotData(v *MetadataSnapshot) *SnapshotState { return &v.state }

func NewMetadataSnapshot(v SnapshotState) *MetadataSnapshot {
	m := &MetadataSnapshot{state: v, id: snapshotIDs.Add(1)}
	m.public = newPublicSnapshot(m)
	return m
}

// Public returns the metadata.Snapshot handle of m, nil for a nil m.
func (m *MetadataSnapshot) Public() *metadata.Snapshot {
	if m == nil {
		return nil
	}
	return m.public
}

// SnapshotOf returns the snapshot behind s, nil for a nil s.
func SnapshotOf(s *metadata.Snapshot) *MetadataSnapshot {
	m, _ := publicSource(s).(*MetadataSnapshot)
	return m
}

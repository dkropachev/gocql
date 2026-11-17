// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package model

import (
	"slices"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/metadata"
)

// Placement is a set of distinct host/shard destinations. Duplicate entries
// cannot satisfy an exact authoritative-set assertion in either direction.
func sameReplicaSet(a, b []metadata.Target) bool {
	if len(a) != len(b) {
		return false
	}
	for i, target := range a {
		if slices.Contains(a[:i], target) || !slices.Contains(b, target) {
			return false
		}
	}
	for i, target := range b {
		if slices.Contains(b[:i], target) {
			return false
		}
	}
	return true
}

func TestTabletOracle(t *testing.T) {
	t.Run("SameReplicaSet", func(t *testing.T) {
		a := metadata.Target{HostID: cql.UUID{15: 1}, ShardID: cql.Option[uint32]{Set: true}}
		b := metadata.Target{HostID: cql.UUID{15: 2}, ShardID: cql.Option[uint32]{Set: true, Value: 1}}
		otherShard := a
		otherShard.ShardID.Value = 1
		for _, test := range []struct {
			name        string
			left, right []metadata.Target
			want        bool
		}{
			{"empty", nil, nil, true},
			{"same", []metadata.Target{a, b}, []metadata.Target{a, b}, true},
			{"reordered", []metadata.Target{a, b}, []metadata.Target{b, a}, true},
			{"driver-duplicate", []metadata.Target{a, a}, []metadata.Target{a, b}, false},
			{"authority-duplicate", []metadata.Target{a, b}, []metadata.Target{a, a}, false},
			{"both-duplicate", []metadata.Target{a, a}, []metadata.Target{a, a}, false},
			{"different-length", []metadata.Target{a}, []metadata.Target{a, b}, false},
			{"different-shard", []metadata.Target{a, b}, []metadata.Target{otherShard, b}, false},
		} {
			t.Run(test.name, func(t *testing.T) {
				if got := sameReplicaSet(test.left, test.right); got != test.want {
					t.Fatal(got, test.want)
				}
			})
		}
	})
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"github.com/scylladb/gocql/v2/internal/link"
	"github.com/scylladb/gocql/v2/internal/wire"
	"github.com/scylladb/gocql/v2/routing"
)

// PartitionKeyState mirrors the bytes of a routing.PartitionKey.
type PartitionKeyState struct {
	chain wire.Chain
}

// partitionKeyRep mirrors routing.PartitionKey.
type partitionKeyRep struct{ state PartitionKeyState }

// PartitionKeyData returns the bytes of v.
func PartitionKeyData(v *routing.PartitionKey) *PartitionKeyState {
	return &link.Mirror[partitionKeyRep](v).state
}

// NewPartitionKey returns a partition key of v's bytes.
func NewPartitionKey(v PartitionKeyState) *routing.PartitionKey {
	k := new(routing.PartitionKey)
	link.Mirror[partitionKeyRep](k).state = v
	return k
}

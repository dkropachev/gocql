// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"github.com/scylladb/gocql/v2/cql"
)

// Target names a host, and optionally a shard of it, a request goes to.
//
// The zero Target names no host; a request that routing.SingleTarget sends to
// it fails with a cql.ValidationError.
type Target struct {
	// StrictShard restricts the request to ShardID. Without it, a request
	// for a shard without a connection may use another shard of the host.
	// It requires ShardID.
	StrictShard bool
	// HostID names the host.
	HostID cql.UUID
	// ShardID names the shard, if set.
	ShardID cql.Option[uint32]
}

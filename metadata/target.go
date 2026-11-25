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
	// HostID names the host.
	HostID cql.UUID
	// ShardID names the shard, if set. A request for it goes to that shard
	// only, unless AnyShardFallback is set.
	ShardID cql.Option[uint32]
	// AnyShardFallback lets a request for ShardID use another shard of the
	// host when ShardID has no connection or the host has no such shard. It
	// requires ShardID. The zero value keeps the request on ShardID, as
	// exec.Options.AnyShardFallback does for a pinned request.
	AnyShardFallback bool
}

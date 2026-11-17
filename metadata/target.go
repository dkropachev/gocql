// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"github.com/scylladb/gocql/v2/cql"
)

// Target names a host, and optionally a shard of it, a request goes to.
type Target struct {
	StrictShard bool
	HostID      cql.UUID
	ShardID     cql.Option[uint32]
}

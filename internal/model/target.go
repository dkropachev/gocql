// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package model

import (
	"github.com/scylladb/gocql/v2/cql"
)

type Target struct {
	StrictShard bool
	HostID      cql.UUID
	ShardID     cql.Option[uint32]
}

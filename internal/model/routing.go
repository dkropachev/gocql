// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package model

import (
	"github.com/scylladb/gocql/v2/cql"
)

type RoutingInfo struct {
	Keyspace, Table string
	Token           cql.Option[int64]
	Kind            cql.StatementKind
	Conditional     bool
	Consistency     cql.Consistency
	HostID          cql.Option[cql.UUID]
	ShardID         cql.Option[uint32]
}

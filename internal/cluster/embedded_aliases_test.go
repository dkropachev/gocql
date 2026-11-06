package cluster

import (
	implPool "github.com/scylladb/gocql/v2/internal/pool"
)

// Aliases for types embedded by moved test fixtures.

type nopConnPicker = implPool.InternalNopConnPicker
type roundRobinHostPolicy = InternalRoundRobinHostPolicy

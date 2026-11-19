package session

import (
	"github.com/scylladb/gocql/v2/internal/pool"
	implPool "github.com/scylladb/gocql/v2/internal/pool"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
)

// Aliases for types embedded by moved test fixtures.

type ReconnectionPolicy = pool.ReconnectionPolicy
type RoutingPolicy = implRouting.RoutingPolicy
type nopConnPicker = implPool.InternalNopConnPicker

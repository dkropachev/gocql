package session

import (
	cluster "github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/pool"
	implPool "github.com/scylladb/gocql/v2/internal/pool"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
)

// Aliases for types embedded by moved test fixtures.

type HostSelectionPolicy = implCluster.HostSelectionPolicy
type ReconnectionPolicy = pool.ReconnectionPolicy
type RoutingPolicy = implRouting.RoutingPolicy
type nopConnPicker = implPool.InternalNopConnPicker
type roundRobinHostPolicy = cluster.InternalRoundRobinHostPolicy

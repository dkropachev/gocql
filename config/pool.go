// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package config

import (
	"errors"
	"fmt"

	"github.com/scylladb/gocql/v2/cql"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

// Pool is how many connections a session keeps to each node, as
// ConfigBuilder.WithPool sets: PerShard or PerHost. No other type
// implements it.
type Pool interface {
	pool()
}

// PerShard keeps connections to every shard of each node, so each request
// goes out on a connection of the shard that owns its data. A node that
// reports no shards, such as Cassandra, counts as one shard. The zero
// PerShard is the default pool.
type PerShard struct {
	// Connections is the number of connections to each shard. Zero means
	// one.
	Connections int
	// MaxExcess caps the connections opened beyond Connections per shard,
	// which a connection the node assigned to a full shard becomes. A
	// Cassandra node counts as one shard, so there it caps the excess per
	// node. Unset, it is 2; cql.Some(0) keeps none.
	MaxExcess cql.Option[int]
	// SourcePorts are the local source ports the shard-aware port is dialed
	// from. The zero PortRange is the default, 49152-65535. A range must
	// not be empty or start below 1024, and has no effect, which Build
	// rejects, where the shard-aware port is not dialed: with ShardAwarePort
	// false, under WithClientRoutes without ShardAwarePort, and with a
	// Cassandra dialect, as a cqlerr.UnsupportedSettingError.
	SourcePorts PortRange
	// ShardAwarePort chooses whether each new connection picks its shard by
	// dialing the node's ScyllaDB shard-aware port from a local source
	// port of that shard, taken from SourcePorts (true), or connects
	// through the regular port and lets the node assign the shard, the pool
	// dialing until every shard is covered (false). Unset, it is true,
	// except under WithClientRoutes, where it is false. Set it true under
	// WithClientRoutes only when every route endpoint forwards to
	// ScyllaDB's Proxy Protocol v2 shard-aware listener and passes the
	// client's source port, since NAT rewrites the port and loses the shard.
	// Cassandra has no shards, so there it has no effect.
	ShardAwarePort cql.Option[bool]
}

// PerHost keeps a number of connections to each node regardless of its
// shard count.
//
// On ScyllaDB it turns shard-aware connection management off: connections
// are dialed through the regular port only, never the shard-aware port, so
// the node assigns each connection's shard and some shards may get none. A
// request whose shard has no connection goes out on another shard's
// connection and costs the node a cross-shard hop. The zero PerHost keeps
// one connection to each node.
type PerHost struct {
	// Connections is the number of connections to each node. Zero means
	// one.
	Connections int
}

// PortRange is the local ports First to Last, inclusive. The zero
// PortRange means the default range, 49152-65535.
type PortRange struct {
	// First is the lowest port of the range.
	First uint16
	// Last is the highest port of the range.
	Last uint16
}

func (PerShard) pool() {}
func (PerHost) pool()  {}

// Default pool settings.
const (
	defaultMaxExcessPerShard = 2
	defaultSourcePortFirst   = 49152
	defaultSourcePortLast    = 65535
)

// applyPool sets the pool layout of settings from pool, which nil leaves at
// the default PerShard. A cql.ValidationError names WithPool and the field.
func applyPool(settings *implSettings.Config, pool Pool) error {
	options := &settings.Pool
	invalid := func(format string, args ...any) error {
		return &cql.ValidationError{Field: "WithPool", Err: fmt.Errorf(format, args...)}
	}
	switch p := pool.(type) {
	case nil:
		return applyPool(settings, PerShard{})
	case *PerShard:
		if p == nil {
			return applyPool(settings, nil)
		}
		return applyPool(settings, *p)
	case *PerHost:
		if p == nil {
			return applyPool(settings, nil)
		}
		return applyPool(settings, *p)
	case PerHost:
		if p.Connections < 0 {
			return invalid("PerHost.Connections must not be negative, got %d", p.Connections)
		}
		options.ConnectionsPerHost, options.ConnectionsPerShard = max(p.Connections, 1), 0
		options.MaxExcessPerShard = 0
		options.AdvancedShardAwareness.Enabled = false
		return nil
	case PerShard:
		if p.Connections < 0 {
			return invalid("PerShard.Connections must not be negative, got %d", p.Connections)
		}
		if p.MaxExcess.Set && p.MaxExcess.Value < 0 {
			return invalid("PerShard.MaxExcess must not be negative, got %d", p.MaxExcess.Value)
		}
		options.ConnectionsPerShard, options.ConnectionsPerHost = max(p.Connections, 1), 0
		options.MaxExcessPerShard = defaultMaxExcessPerShard
		if p.MaxExcess.Set {
			options.MaxExcessPerShard = p.MaxExcess.Value
		}
		on := settings.ClientRoutes == nil
		if p.ShardAwarePort.Set {
			on = p.ShardAwarePort.Value
		}
		ports := p.SourcePorts
		if ports != (PortRange{}) {
			if ports.First < 1024 || ports.First > ports.Last {
				return invalid("PerShard.SourcePorts %d-%d is not a range within 1024-65535", ports.First, ports.Last)
			}
			if err := implSettings.ScyllaOnly(settings, "WithPool"); err != nil {
				return err
			}
			if !on {
				reason := "PerShard.ShardAwarePort is false"
				if !p.ShardAwarePort.Set {
					reason = "WithClientRoutes dials no shard-aware port unless PerShard.ShardAwarePort is set"
				}
				return invalid("PerShard.SourcePorts has no effect: %s", reason)
			}
		} else {
			ports = PortRange{First: defaultSourcePortFirst, Last: defaultSourcePortLast}
		}
		options.AdvancedShardAwareness.Enabled = on
		options.AdvancedShardAwareness.SourcePortStart, options.AdvancedShardAwareness.SourcePortEnd = ports.First, ports.Last
		return nil
	}
	return &cql.ValidationError{Field: "WithPool", Err: errors.New("unknown pool")}
}

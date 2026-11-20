// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

import (
	"crypto/tls"
	"fmt"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/metadata"
)

// ReconnectPolicy decides how long to wait before reconnecting to a node.
type ReconnectPolicy interface {
	// Delay returns the wait before the attempt-th reconnection, or false to
	// give up until the node is reported up again.
	Delay(attempt int) (time.Duration, bool)
}

// TLSSource supplies the TLS configuration of each new connection, so the
// configuration can change while sessions run. The driver calls TLSConfig on
// every dial, from many goroutines, and copies the result; it must be fast and
// must not return nil. Connections already open keep the configuration they
// were dialed with. Pass a source to config.ConfigBuilder.WithTLSSource;
// config.TLSFileSource is a ready-made one. Use config.ConfigBuilder.WithTLS
// for a fixed configuration.
type TLSSource interface {
	// TLSConfig returns the configuration for the next connection. It is
	// called on every dial, from many goroutines; the result is copied and
	// must not be nil.
	TLSConfig() *tls.Config
}

// ConvictionSource names what failed to connect: a pool connection or the
// control connection. The zero ConvictionSource is ConvictionPool. Later
// releases may add values, so a switch over them needs a default case.
type ConvictionSource uint8

const (
	// ConvictionPool is a host pool connection. An event with Shard set is
	// one shard's connection; an event without it means the pool lost its
	// last connection to the host.
	ConvictionPool ConvictionSource = iota
	// ConvictionControl is the control connection's dial to the host.
	ConvictionControl
)

// String returns the conviction source name, or ConvictionSource(n) for a
// value it does not define.
func (s ConvictionSource) String() string {
	switch s {
	case ConvictionPool:
		return "pool"
	case ConvictionControl:
		return "control"
	default:
		return fmt.Sprintf("ConvictionSource(%d)", uint8(s))
	}
}

// ConvictionEvent describes a connection failure, or a recovery, that a
// ConvictionPolicy receives.
//
// The driver fills it; its zero value is empty.
type ConvictionEvent struct {
	// Err is the connection error; nil in Reset.
	Err error
	// Host is detached, read-only host metadata that may be retained.
	Host metadata.HostSnapshot
	// Shard is set when the event concerns a connection the pool opened to
	// one shard: a failed attempt to open it or, in Reset, the connection
	// to it opened again. It is the shard the pool asked for; a dialer that
	// cannot target shards connects wherever the server places it.
	Shard cql.Option[uint32]
	// Source names what failed to connect.
	Source ConvictionSource
}

// ConvictionPolicy decides whether connection failures mark a host down.
// Callbacks run concurrently, some on the goroutine that opens connections,
// so they must not block.
//
// When the pool asks for a specific shard, which takes a shard-aware port and
// a known shard count, it reports each failed attempt to open that connection
// with Shard set, once per attempt after reconnection retries, before the
// event without Shard that follows when the host has no connection left. A
// new pool does not know the shard count, so the failure of its first
// connection has no Shard.
type ConvictionPolicy interface {
	// AddFailure reports a failure and returns whether to mark the host
	// down. A true verdict on a shard event marks the whole host down once
	// the pool stops filling, even though the pool still has connections
	// to other shards; there is no per-shard down state. A request for a
	// shard without a connection already uses another shard of the host,
	// unless it names a shard or is conditional.
	AddFailure(ConvictionEvent) bool
	// Reset reports that connections opened again: with Shard set, the
	// connection to a shard whose failure was reported; without it, the
	// first connection of a pool that had none, such as a new pool at
	// startup or after the host was marked down, or a pool that lost every
	// connection. Implementations should clear the matching state.
	Reset(ConvictionEvent)
}

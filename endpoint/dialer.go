// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

import (
	"context"
	"errors"
	"net"
	"net/netip"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/metadata"
)

// DialTarget is a resolved raw connection target. Hooks must honor the selected
// endpoint and source, or return an error. TLS and protocol setup follow dialing.
//
// The driver fills it; its zero value names no address.
type DialTarget struct {
	// LocalAddress is the source address to dial from, or the zero AddrPort
	// for any. Its port is set when a shard-aware port connection needs a
	// source port of the shard, and zero otherwise. When that port is taken,
	// return an error that matches syscall.EADDRINUSE or
	// cqlerr.ErrScyllaSourcePortAlreadyInUse, and the driver tries the next
	// port of the shard.
	LocalAddress netip.AddrPort
	// Network and Address are the network and address to dial, as for
	// net.Dialer.DialContext.
	Network, Address string
	// Host is the host the connection is for.
	Host metadata.HostSnapshot
	// ShardID is the shard a shard-aware port connection targets, and is
	// unset otherwise.
	ShardID cql.Option[uint32]
}

// Dialer opens the raw connections of a session; TLS is applied afterward.
type Dialer interface {
	// DialContext opens a raw connection to the target. It must honor the
	// target's address and source, and return promptly once the context is
	// done.
	DialContext(context.Context, DialTarget) (net.Conn, error)
}

// DialerFunc adapts a context-aware raw connection hook to Dialer.
type DialerFunc func(context.Context, DialTarget) (net.Conn, error)

// DialContext calls f.
func (f DialerFunc) DialContext(ctx context.Context, target DialTarget) (net.Conn, error) {
	return f(ctx, target)
}

// DefaultDialer opens the selected target with Go's network dialer, with TCP
// keepalive off. It is the dialer a session uses without
// config.ConfigBuilder.WithDialer, and a custom dialer may wrap it. The dial
// is bounded by its context, which the session gives the connect timeout; the
// session sets TCP keepalive and the other socket options on the connection
// afterwards. It also supports Unix targets; TCP source selection is never
// applied to Unix sockets.
//
// The zero DefaultDialer, its only value, is ready to use.
type DefaultDialer struct{}

// DialContext dials the target with a net.Dialer. A source address requires
// a TCP network; any other network with one fails with a ValidationError.
func (DefaultDialer) DialContext(ctx context.Context, target DialTarget) (net.Conn, error) {
	n := net.Dialer{KeepAlive: -1}
	if target.LocalAddress.IsValid() {
		if target.Network != "tcp" && target.Network != "tcp4" && target.Network != "tcp6" {
			return nil, &cql.ValidationError{Field: "local address", Err: errors.New("source selection requires TCP")}
		}
		n.LocalAddr = net.TCPAddrFromAddrPort(target.LocalAddress)
	}
	return n.DialContext(ctx, target.Network, target.Address)
}

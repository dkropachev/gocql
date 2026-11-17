// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/metadata"
)

// DialTarget is a resolved raw connection target. Hooks must honor the selected
// endpoint and source, or return an error. TLS and protocol setup follow dialing.
type DialTarget struct {
	LocalAddress     netip.AddrPort
	Network, Address string
	Host             metadata.HostSnapshot
	ShardID          cql.Option[uint32]
}

// Dialer opens the raw connections of a session; TLS is applied afterward.
type Dialer interface {
	DialContext(context.Context, DialTarget) (net.Conn, error)
}

// DialerFunc adapts a context-aware raw connection hook to Dialer.
type DialerFunc func(context.Context, DialTarget) (net.Conn, error)

func (f DialerFunc) DialContext(ctx context.Context, target DialTarget) (net.Conn, error) {
	return f(ctx, target)
}

// DefaultDialer opens the selected target with Go's network dialer. It also
// supports Unix targets; TCP source selection is never applied to Unix sockets.
type DefaultDialer struct{ ConnectTimeout, SocketKeepalive time.Duration }

func (d DefaultDialer) DialContext(ctx context.Context, target DialTarget) (net.Conn, error) {
	keepalive := d.SocketKeepalive
	if keepalive == 0 {
		keepalive = -1
	}
	n := net.Dialer{Timeout: d.ConnectTimeout, KeepAlive: keepalive}
	if target.LocalAddress.IsValid() {
		if target.Network != "tcp" && target.Network != "tcp4" && target.Network != "tcp6" {
			return nil, &cql.ValidationError{Field: "local address", Err: errors.New("source selection requires TCP")}
		}
		n.LocalAddr = net.TCPAddrFromAddrPort(target.LocalAddress)
	}
	return n.DialContext(ctx, target.Network, target.Address)
}

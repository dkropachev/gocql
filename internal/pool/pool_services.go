// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package pool

import (
	"context"
	"net"

	"log/slog"

	"github.com/scylladb/gocql/v2/internal/connectivity"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/transport"
)

type InternalPoolServices struct {
	Context      context.Context
	Logger       *slog.Logger
	Config       *connectivity.Config
	Conviction   connectivity.ConvictionPolicy
	Reconnection ReconnectionPolicy
	Worker       func(func()) bool
	Dial         func(context.Context, *endpoint.HostInfo, transport.ConnErrorHandler, int, int) (*transport.Conn, error)
	Connected    func(*InternalHostConnPool)
	Down         func(net.IP, int)
}

func (p *InternalHostConnPool) typedConfig() (*connectivity.Config, bool) {
	return p.InternalServices.Config, p.InternalServices.Config != nil
}

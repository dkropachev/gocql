// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

// Facade aliases referenced only by tests.

import (
	"net"

	"github.com/scylladb/gocql/v2/internal/settings"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
	implTransport "github.com/scylladb/gocql/v2/internal/transport"
)

func applyV2SocketOptions(a0 net.Conn, a1 settings.Config, a2 string) error {
	return implTransport.InternalApplyV2SocketOptions(a0, implSettings.InternalConnectivityConfig(a1), a2)
}

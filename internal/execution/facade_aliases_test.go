// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

// Facade aliases referenced only by tests.

import (
	"net"

	"github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/settings"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
	implTransport "github.com/scylladb/gocql/v2/internal/transport"
)

func resolveOptions(a0 *cluster.Session, a1 Options) (InternalResolvedOptions, error) {
	return InternalResolveFor(a0, a1), nil
}

func applyV2SocketOptions(a0 net.Conn, a1 settings.Config, a2 string) error {
	return implTransport.InternalApplyV2SocketOptions(a0, implSettings.InternalConnectivityConfig(a1), a2)
}

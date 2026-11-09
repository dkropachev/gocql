// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
)

func OpenSingleHost(ctx context.Context, config Config, hostID UUID) (*implCluster.Core, error) {
	if hostID.IsEmpty() {
		return nil, typeValidation("host ID", "empty UUID")
	}
	previous := config.HostFilter
	config.HostFilter = func(host HostSnapshot) bool { return host.ID == hostID && (previous == nil || previous(host)) }
	return Connect(ctx, config)
}

func Connect(ctx context.Context, config Config) (*implCluster.Core, error) {
	return implCluster.InternalConnectCluster(ctx, config, func(s *implCluster.Core, c Config) *Config {
		rt := InternalRuntimeFor(s)
		rt.InternalConfig = c
		rt.InternalSetProfile(c.DefaultProfile)
		return &rt.InternalConfig
	})
}

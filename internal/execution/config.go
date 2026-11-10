// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/settings"
)

func OpenSingleHost(ctx context.Context, config settings.Config, hostID cqltype.UUID) (*Session, error) {
	if hostID.IsEmpty() {
		return nil, cqltype.TypeValidation("host ID", "empty UUID")
	}
	previous := config.HostFilter
	config.HostFilter = func(host model.HostSnapshot) bool { return host.ID == hostID && (previous == nil || previous(host)) }
	return Connect(ctx, config)
}

func Connect(ctx context.Context, config settings.Config) (*Session, error) {
	core, err := implCluster.InternalConnectCluster(ctx, config, func(s *implCluster.Core, c settings.Config) *settings.Config {
		rt := InternalRuntimeFor(s)
		rt.InternalConfig = c
		rt.InternalSetProfile(c.DefaultProfile)
		return &rt.InternalConfig
	})
	if err != nil {
		return nil, err
	}
	return InternalWrapSession(core), nil
}

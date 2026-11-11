// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/settings"
)

func Connect(ctx context.Context, config settings.Config) (*Session, error) {
	core, err := implCluster.InternalConnectCluster(ctx, config, func(s *implCluster.Core, c settings.Config) *settings.Config {
		rt := InternalRuntimeFor(s)
		rt.config = c
		rt.InternalSetProfile(c.DefaultProfile)
		return &rt.config
	})
	if err != nil {
		return nil, err
	}
	return InternalWrapSession(core), nil
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/observe"
)

func Connect(ctx context.Context, config settings.Config) (*Session, error) {
	core, err := implCluster.InternalConnectCluster(ctx, config, func(s *implCluster.Core, c settings.Config) *settings.Config {
		rt := InternalRuntimeFor(s)
		rt.setConfig(c)
		rt.InternalSetProfile(c.DefaultProfile)
		return &rt.config
	})
	if err != nil {
		return nil, err
	}
	return InternalWrapSession(core), nil
}

// setConfig installs c and keeps its observer's hooks, nil without one.
func (rt *InternalSessionRuntime) setConfig(c settings.Config) {
	rt.config = c
	rt.observerHooks = nil
	if !c.Observer.IsZero() {
		rt.observerHooks = &rt.config.Observer
	}
}

// overridesObserver reports whether values change which hooks see an
// execution, so that it does not call the session's hooks alone.
func overridesObserver(values *Options) bool {
	return !values.Observer.IsZero() || values.NoSessionObserver
}

// executionHooks returns the hooks of an execution with values on rt's
// session, when overridesObserver(values): the session's hooks, then
// values.Observer, as observe.Join orders them, or values.Observer alone
// with NoSessionObserver. It never returns nil.
func executionHooks(rt *InternalSessionRuntime, values *Options) *observe.Hooks {
	hooks := values.Observer
	if !values.NoSessionObserver {
		hooks = observe.Join(rt.config.Observer, values.Observer)
	}
	return &hooks
}

// Observer returns the hooks the session was configured with, the zero
// Hooks without them.
func (s *Session) Observer() observe.Hooks {
	if s.Core() == nil {
		return observe.Hooks{}
	}
	return InternalRuntimeFor(s.Core()).config.Observer
}

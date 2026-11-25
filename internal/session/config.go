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

// setConfig installs c and finds its observer's hooks. Config.Build has
// validated the observer.
func (rt *InternalSessionRuntime) setConfig(c settings.Config) {
	rt.config = c
	rt.observerHooks, _ = settings.NewObserverHooks(c.Observer)
}

// noObserverHooks are the hooks of an execution nothing observes.
var noObserverHooks = &settings.ObserverHooks{}

// overridesObserver reports whether values change which observer sees an
// execution, so that it does not call the session's hooks alone.
func overridesObserver(values *Options) bool {
	return values.Observer != nil || values.NoSessionObserver
}

// executionHooks returns the hooks of an execution with values on rt's
// session, when overridesObserver(values): the session observer's hooks, then
// values.Observer's, as observe.Join orders them; values.Observer's alone
// with NoSessionObserver; and noObserverHooks when that leaves none.
func executionHooks(rt *InternalSessionRuntime, values *Options) (*settings.ObserverHooks, error) {
	observer := values.Observer
	if !values.NoSessionObserver && rt.config.Observer != nil {
		observer = observe.Join(rt.config.Observer, values.Observer)
	}
	hooks, err := settings.NewObserverHooks(observer)
	if hooks == nil && err == nil {
		hooks = noObserverHooks
	}
	return hooks, err
}

// Observer returns the observer the session was configured with, nil
// without one.
func (s *Session) Observer() observe.Observer {
	if s.Core() == nil {
		return nil
	}
	return InternalRuntimeFor(s.Core()).config.Observer
}

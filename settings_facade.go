// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	logging "github.com/scylladb/gocql/v2/internal/logging"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

type ClientRoutesOptions = implSettings.ClientRoutesOptions
type Config = implSettings.Config
type ConvictionPolicy = implSettings.ConvictionPolicy

func DefaultConfig(a0 ...string) Config { return implSettings.DefaultConfig(a0...) }

type IdentityOptions = implSettings.IdentityOptions

type LifecycleEvent = implSettings.LifecycleEvent
type LifecycleObserver = implSettings.LifecycleObserver

type Observer = implSettings.Observer

type AttemptEvent = implRouting.AttemptEvent

type PoolOptions = implSettings.PoolOptions
type Profile = implSettings.Profile
type ProtocolLimits = implSettings.ProtocolLimits
type ReconnectPolicy = implSettings.ReconnectPolicy
type RequestEvent = implSettings.RequestEvent

type SimpleConvictionPolicy = implSettings.SimpleConvictionPolicy

type SpeculationPolicy = implSettings.SpeculationPolicy

type StdLogger = logging.StdLogger

// Deprecated: use cqlerr.UnsupportedSettingError.
type UnsupportedSettingError = implSettings.UnsupportedSettingError

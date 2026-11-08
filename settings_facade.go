// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	logging "github.com/scylladb/gocql/v2/internal/logging"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

type ConvictionPolicy = implSettings.ConvictionPolicy

type LifecycleEvent = implSettings.LifecycleEvent
type LifecycleObserver = implSettings.LifecycleObserver

type Observer = implSettings.Observer

type AttemptEvent = implRouting.AttemptEvent

type Profile = implSettings.Profile
type ReconnectPolicy = implSettings.ReconnectPolicy
type RequestEvent = implSettings.RequestEvent

type SimpleConvictionPolicy = implSettings.SimpleConvictionPolicy

type SpeculationPolicy = implSettings.SpeculationPolicy

type StdLogger = logging.StdLogger

// Deprecated: use cqlerr.UnsupportedSettingError.
type UnsupportedSettingError = implSettings.UnsupportedSettingError

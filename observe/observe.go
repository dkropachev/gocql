// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package observe lets an application watch what a session does: every
// request and attempt (Observer), and the session's and its connections'
// lifecycle (LifecycleObserver). config.ConfigBuilder.WithObserver and
// WithLifecycleObserver install them.
package observe

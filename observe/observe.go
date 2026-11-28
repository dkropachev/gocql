// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package observe lets an application watch every request and attempt a
// session runs, from the client side, through the functions of a Hooks.
// config.ConfigBuilder.WithObserver installs Hooks for the session, and
// execopt.Options.Observer adds Hooks to an execution, joined after the
// session's; Join combines several. Changes of the cluster and of the
// session's connection pools, such as a host going down, are events instead:
// see gocql.Session.Subscribe and the event package.
package observe

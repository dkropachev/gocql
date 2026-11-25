// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package observe lets an application watch every request and attempt a
// session runs, from the client side. config.ConfigBuilder.WithObserver
// installs an Observer for the session, and exec.Options.Observer adds one to
// an execution, joined after the session's; Join combines several. Changes of the cluster and of the
// session's connection pools, such as a host going down, are events instead:
// see gocql.Session.Subscribe and the event package.
package observe

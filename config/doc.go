// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package config builds the configuration of a gocql session and the
// execution profiles of its requests.
//
// New starts a ConfigBuilder for a set of contact points; its With methods
// choose settings and Build validates them into an immutable Config, which
// gocql.Connect opens a session with:
//
//	cfg, err := config.New("10.0.0.1", "10.0.0.2").
//		WithKeyspace("app").
//		WithConnectionsPerShard(2).
//		Build()
//	session, err := gocql.Connect[gocql.ScyllaV4](ctx, cfg)
//
// NewProfile starts a ProfileBuilder for execution defaults: the session's
// default profile (ConfigBuilder.WithDefaultProfile) or a per-request one
// (exec.Options.Profile). WithTLS encrypts connections with a fixed
// tls.Config; WithTLSSource takes a TLSSource such as NewTLSFiles, which
// reloads certificates while the session runs. Observer and LifecycleObserver
// receive requests and session events.
package config

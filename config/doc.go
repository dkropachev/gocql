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
//	session, err := gocql.Connect(ctx, cfg, gocql.ScyllaV4)
//
// NewProfile starts a ProfileBuilder for execution defaults: the session's
// default profile (ConfigBuilder.WithDefaultProfile) or a per-request one
// (exec.Options.Profile). WithTLS encrypts connections with a fixed
// tls.Config; WithTLSSource takes an endpoint.TLSSource such as NewTLSFiles,
// which reloads certificates while the session runs. WithObserver and
// WithLifecycleObserver install an observe.Observer and an
// observe.LifecycleObserver.
//
// # Which setting wins
//
// A request's execution settings resolve in three steps, each overriding the
// one before:
//
//  1. The session's default profile: the one ConfigBuilder.WithDefaultProfile
//     or ConfigureDefaultProfile chose, or the driver's defaults.
//  2. exec.Options.Profile, when set, replaces that profile whole. A field the
//     profile did not choose keeps the driver's default, not the session's.
//  3. Each exec.Options field that is set overrides the matching field of the
//     profile: a cql.Option whose Set is true, a non-nil Retry, and a
//     SerialConsistency other than exec.SerialInherit.
//
// Time limits combine rather than replace each other. The context passed to
// the call, the profile's Timeout and exec.Options.Timeout all bound the whole
// execution, retries and every page Rows fetches included, and whichever
// expires first ends it. An option therefore only tightens the profile's
// limit: a longer exec.Options.Timeout, or a zero one, leaves the profile's in
// force, and a zero profile Timeout leaves the option's and the context's.
// PageTimeout combines the same way, the earliest of the profile's and
// exec.Options.PageTimeout bounding each page fetch on its own, within the
// execution's deadline.
//
// Preparation takes no exec.Options. Session.Prepare and
// Session.PrepareStream are bounded by their context and the session's
// default profile Timeout, whichever expires first. A statement the driver
// prepares again during an execution, after an UNPREPARED error or on a host
// that has not seen it, is bounded by that execution's deadline.
package config

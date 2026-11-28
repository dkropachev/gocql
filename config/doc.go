// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package config builds the configuration of a gocql session and the
// execution profiles of its requests.
//
// New starts a ConfigBuilder for a Dialect and a set of contact points; its
// With methods choose settings and Build validates them, against the dialect
// too, into an immutable Config, which gocql.Connect opens a session with:
//
//	cfg, err := config.New(config.ScyllaV4, "10.0.0.1", "10.0.0.2").
//		WithKeyspace("app").
//		WithConnectionsPerShard(2).
//		Build()
//	session, err := gocql.Connect(ctx, cfg)
//
// NewProfile starts a ProfileBuilder for execution defaults: the session's
// default profile (ConfigBuilder.WithDefaultProfile) or a per-request one
// (execopt.Options.Profile). WithTLS encrypts connections with a fixed
// tls.Config; WithTLSSource takes an endpoint.TLSSource such as NewTLSFileSource,
// which reloads certificates while the session runs. WithObserver installs
// observe.Hooks; observe.Join combines several.
//
// # Which setting wins
//
// A request's execution settings resolve in three steps, each overriding the
// one before:
//
//  1. The session's default profile: the one ConfigBuilder.WithDefaultProfile
//     chose, or the driver's defaults; Config.DefaultProfile returns it.
//  2. execopt.Options.Profile, when set, replaces that profile whole. A
//     field the profile did not choose keeps the driver's default, not the
//     session's.
//  3. Each execopt.Options field that is set overrides the matching field of
//     the profile: a cql.Option whose Set is true, such as SerialConsistency,
//     a non-nil Retry, and a non-zero PageSize. The options a call passes to
//     a view made by gocql.Prepared.With are laid over the view's own, field
//     by field, first; see package execopt.
//
// A set execopt.Options.Timeout replaces the profile's Timeout, longer or
// shorter, and a set zero removes it. The resulting limit and the context
// passed to the call both bound the whole execution, retries and every page
// Rows fetches included, and whichever expires first ends it.
// execopt.Options.PageTimeout replaces the profile's PageTimeout the same way;
// it bounds each page fetch on its own, within the execution's deadline.
//
// Preparation takes no execopt.Options. Session.Prepare and
// Session.PrepareStream are bounded by their context and the session's
// default profile Timeout, whichever expires first. A statement the driver
// prepares again during an execution, after an UNPREPARED error or on a host
// that has not seen it, is bounded by that execution's deadline.
package config

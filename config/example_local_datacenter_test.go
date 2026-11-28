// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package config_test

import (
	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/routing"
)

// A session in dc1 routes token-aware to replicas there. A profile for
// analytics reads routes to dc2 instead; its policy names its own
// datacenter, so the session's does not apply to it.
func ExampleConfigBuilder_WithLocalDatacenter() {
	cfg, err := config.New(config.ScyllaV4, "10.0.0.1").
		WithLocalDatacenter("dc1").
		Build()
	if err != nil {
		panic(err)
	}
	analytics := routing.NewPolicy(routing.Options{Datacenter: "dc2"})
	// Derive the profile from the session's, so only its routing differs;
	// with a session, start from session.DefaultProfile().Builder().
	profile, err := cfg.DefaultProfile().Builder().WithRouting(analytics).Build()
	if err != nil {
		panic(err)
	}
	// Connect with cfg; run analytics reads with execopt.Options{Profile: &profile}.
	_, _ = cfg, profile
}

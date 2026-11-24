// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package config

import (
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

// ClientRoute is one client connection of ConfigBuilder.WithClientRoutes. It
// is a plain record; its zero value names no connection, which Build rejects.
type ClientRoute struct {
	// ConnectionID is the client connection ID the cluster publishes routes
	// for. Build rejects an empty or repeated ID.
	ConnectionID string
	// Hostname, when not empty, is dialed for this connection instead of the
	// address the cluster publishes.
	Hostname string
}

// clientRoutesOptions converts routes to the internal settings, or returns
// nil for none. Settings.Validate rejects empty and repeated IDs.
func clientRoutesOptions(routes []ClientRoute) *implSettings.ClientRoutesOptions {
	if len(routes) == 0 {
		return nil
	}
	o := &implSettings.ClientRoutesOptions{ConnectionIDs: make([]string, len(routes))}
	for i, route := range routes {
		o.ConnectionIDs[i] = route.ConnectionID
		if route.Hostname != "" {
			if o.HostnameOverrides == nil {
				o.HostnameOverrides = make(map[string]string)
			}
			o.HostnameOverrides[route.ConnectionID] = route.Hostname
		}
	}
	return o
}

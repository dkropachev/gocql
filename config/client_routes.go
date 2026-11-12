// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package config

import (
	"errors"
	"slices"

	"github.com/scylladb/gocql/v2/cqlerr"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

// ClientRoute is one client connection of a ClientRoutes.
type ClientRoute struct {
	// ConnectionID is the client connection ID the cluster publishes routes
	// for.
	ConnectionID string
	// Hostname, when not empty, is dialed for this connection instead of the
	// address the cluster publishes.
	Hostname string
}

// ClientRoutes is a validated, immutable ScyllaDB client routes setting for
// ConfigBuilder.WithClientRoutes. Build one with NewClientRoutes and
// ClientRoutesBuilder.Build. The zero ClientRoutes has no routes and removes
// client routes from a configuration.
type ClientRoutes struct {
	routes []ClientRoute
}

// Routes returns a copy of the routes, in the order they were added.
func (r ClientRoutes) Routes() []ClientRoute { return slices.Clone(r.routes) }

// options converts r to the internal settings.
func (r ClientRoutes) options() *implSettings.ClientRoutesOptions {
	o := &implSettings.ClientRoutesOptions{ConnectionIDs: make([]string, len(r.routes))}
	for i, route := range r.routes {
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

// ClientRoutesBuilder assembles a ClientRoutes. The last call for a setting
// wins. A ClientRoutesBuilder is not safe for concurrent use.
type ClientRoutesBuilder struct {
	routes []ClientRoute
}

// NewClientRoutes starts a client routes setting with no routes.
func NewClientRoutes() *ClientRoutesBuilder {
	return &ClientRoutesBuilder{}
}

// WithRoute routes through the client connection connectionID, dialing
// hostname instead of the published address when hostname is not empty.
// Adding a connectionID again replaces its hostname and keeps its position.
func (b *ClientRoutesBuilder) WithRoute(connectionID, hostname string) *ClientRoutesBuilder {
	route := ClientRoute{ConnectionID: connectionID, Hostname: hostname}
	if i := slices.IndexFunc(b.routes, func(r ClientRoute) bool { return r.ConnectionID == connectionID }); i >= 0 {
		b.routes[i] = route
	} else {
		b.routes = append(b.routes, route)
	}
	return b
}

// Build validates the routes: at least one, none with an empty connection
// ID. Later calls on the builder do not affect the returned ClientRoutes.
func (b *ClientRoutesBuilder) Build() (ClientRoutes, error) {
	invalid := func(reason string) error {
		return &cqlerr.ValidationError{Field: "client routes", Err: errors.New(reason)}
	}
	if len(b.routes) == 0 {
		return ClientRoutes{}, invalid("no routes; add one with WithRoute")
	}
	for _, r := range b.routes {
		if r.ConnectionID == "" {
			return ClientRoutes{}, invalid("empty connection ID")
		}
	}
	return ClientRoutes{routes: slices.Clone(b.routes)}, nil
}

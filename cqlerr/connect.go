// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import (
	"fmt"
	"strings"
)

// HostError is the failure to connect to one host.
//
// The driver returns it filled in; its zero value has no detail.
type HostError struct {
	// Err is the reason the connection failed.
	Err error
	// Address is the host:port that was dialed, as configured or resolved.
	// It is text, not a netip.AddrPort, because a contact point that failed
	// to resolve has no IP address and a Unix socket has a path.
	Address string
}

// ConnectError reports that no host was reachable while a session was
// connecting; it matches ErrNoConnectionsStarted.
//
// The driver returns it filled in; its zero value has no detail.
type ConnectError struct {
	// Stage names the step that failed, such as "protocol discovery".
	Stage string
	// Hosts lists every host tried, in the order they were tried.
	Hosts []HostError
}

// Error names the failed stage and each host's error, listing at most the
// last three hosts tried.
func (e *ConnectError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "no host reachable during %s", e.Stage)
	hosts := e.Hosts
	if skipped := len(hosts) - maxRenderedAttempts; skipped > 0 {
		fmt.Fprintf(&b, " (%d of %d hosts shown)", maxRenderedAttempts, len(hosts))
		hosts = hosts[skipped:]
	}
	for i, h := range hosts {
		if i == 0 {
			b.WriteString(": ")
		} else {
			b.WriteString("; ")
		}
		fmt.Fprintf(&b, "%s: %v", h.Address, h.Err)
	}
	return b.String()
}

// Kind returns KindConnection. KindOf reports a host's server error instead,
// such as KindUnauthorized for rejected credentials.
func (e *ConnectError) Kind() Kind { return KindConnection }

// Unwrap returns ErrNoConnectionsStarted followed by each host's error.
func (e *ConnectError) Unwrap() []error {
	out := make([]error, 0, 1+len(e.Hosts))
	out = append(out, ErrNoConnectionsStarted)
	for _, h := range e.Hosts {
		out = append(out, h.Err)
	}
	return out
}

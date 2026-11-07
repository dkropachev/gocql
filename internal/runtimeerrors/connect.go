// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package runtimeerrors

import (
	"fmt"
	"strings"
)

// HostError is the failure to connect to one host.
type HostError struct {
	Err error
	// Address is the host:port that was dialed.
	Address string
}

// ConnectError reports that no host could be reached while a session was
// connecting. errors.Is matches ErrNoConnectionsStarted, and errors.As finds
// each host's cause, such as a *net.OpError.
type ConnectError struct {
	// Stage names the step that failed, such as "protocol discovery".
	Stage string
	// Hosts lists every host tried, in the order they were tried.
	Hosts []HostError
}

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

// Unwrap returns ErrNoConnectionsStarted followed by each host's error.
func (e *ConnectError) Unwrap() []error {
	out := make([]error, 0, 1+len(e.Hosts))
	out = append(out, ErrNoConnectionsStarted)
	for _, h := range e.Hosts {
		out = append(out, h.Err)
	}
	return out
}

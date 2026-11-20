// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import (
	"fmt"
	"sort"
	"strings"
)

// PartialRefreshError reports keyspaces whose previous valid schema was kept.
//
// The driver returns it filled in; its zero value is an error without detail.
type PartialRefreshError struct {
	Topology     error
	ClientRoutes error
	Keyspaces    map[string]error
	Generation   uint64
}

// Error names the metadata generation and the error of each keyspace, the
// topology and the client routes that failed to refresh.
func (e *PartialRefreshError) Error() string {
	names := make([]string, 0, len(e.Keyspaces))
	for name := range e.Keyspaces {
		names = append(names, name)
	}
	sort.Strings(names)
	var details []string
	for _, name := range names {
		details = append(details, fmt.Sprintf("%s: %v", name, e.Keyspaces[name]))
	}
	if e.Topology != nil {
		details = append(details, "topology: "+e.Topology.Error())
	}
	if e.ClientRoutes != nil {
		details = append(details, "client routes: "+e.ClientRoutes.Error())
	}
	return fmt.Sprintf("gocql: metadata generation %d retained stale data (%s)", e.Generation, strings.Join(details, "; "))
}

// Unwrap returns the keyspace errors in keyspace name order, followed by
// Topology and ClientRoutes when they are set.
func (e *PartialRefreshError) Unwrap() []error {
	names := make([]string, 0, len(e.Keyspaces))
	for name := range e.Keyspaces {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]error, 0, len(names))
	for _, name := range names {
		out = append(out, e.Keyspaces[name])
	}
	if e.Topology != nil {
		out = append(out, e.Topology)
	}
	if e.ClientRoutes != nil {
		out = append(out, e.ClientRoutes)
	}
	return out
}

// NoHostsError distinguishes an empty eligible peer scope from agreement.
//
// The driver returns it filled in; its zero value is an error without detail.
type NoHostsError struct {
	Operation string
	Causes    []error
}

// Error names the operation that had no eligible host.
func (e *NoHostsError) Error() string { return "gocql: no eligible hosts for " + e.Operation }

// Unwrap returns a copy of Causes.
func (e *NoHostsError) Unwrap() []error { return append([]error(nil), e.Causes...) }

// Kind returns KindConnection: no host could serve the operation. A cause
// of a more specific kind, such as a server error, decides KindOf.
func (e *NoHostsError) Kind() Kind { return KindConnection }

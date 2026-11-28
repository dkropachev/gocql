// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import (
	"fmt"
	"sort"
	"strings"

	"github.com/scylladb/gocql/v2/cql"
)

// PartialRefreshError reports keyspaces whose previous valid schema was kept.
//
// The driver returns it filled in; its zero value is an error without detail.
type PartialRefreshError struct {
	// Topology is the topology refresh's error, or nil.
	Topology error
	// ClientRoutes is the client routes refresh's error, or nil.
	ClientRoutes error
	// Keyspaces holds the error of each keyspace whose schema was kept, by
	// name.
	Keyspaces map[string]error
	// Generation is the metadata generation the refresh left in place.
	Generation uint64
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

// Kind returns KindConnection: the refresh did not complete, and refreshing
// again can. A cause of a more specific kind, such as a server error or a
// timeout, decides KindOf.
func (e *PartialRefreshError) Kind() Kind { return KindConnection }

// NoHostsError reports that an operation found no host to run on. Schema
// agreement returns it, inside a SchemaAgreementError, when no host the
// host filter accepts is known, so that an empty set of hosts is not
// mistaken for hosts that agree.
//
// The driver returns it filled in; its zero value is an error without detail.
type NoHostsError struct {
	// Operation names what needed a host, such as "schema agreement".
	Operation string
	// Causes holds the errors that left no host eligible.
	Causes []error
}

// Error names the operation that had no eligible host.
func (e *NoHostsError) Error() string { return "gocql: no eligible hosts for " + e.Operation }

// Unwrap returns a copy of Causes.
func (e *NoHostsError) Unwrap() []error { return append([]error(nil), e.Causes...) }

// Kind returns KindConnection: no host could serve the operation. A cause
// of a more specific kind, such as a server error, decides KindOf.
func (e *NoHostsError) Kind() Kind { return KindConnection }

// SchemaAgreementError reports that waiting for the nodes' schema versions
// to agree, or the metadata refresh that follows a schema change, failed.
// Session.AwaitSchemaAgreement returns it, and so does a schema change
// statement whose automatic agreement or refresh failed after the server
// applied it; that request's ExecutionStatus is PartiallyExecuted.
//
// The driver returns it filled in; its zero value is an error without detail.
type SchemaAgreementError struct {
	// Err is why it failed: ErrRequestTimeout when the agreement timeout
	// ran out with the versions apart or hosts unreachable, the caller's
	// context error, a NoHostsError, or the error of a check or of the
	// refresh.
	Err error
	// Versions holds the schema version each host reported in the last
	// check, by host ID. It is empty when the failure came before a check
	// completed, or after agreement, in the refresh.
	Versions map[cql.UUID]cql.UUID
	// Unreachable holds the hosts, by ID, that the last check could not
	// read: a host the host filter accepts that is down or has no open
	// connection blocks agreement until the agreement timeout ends.
	Unreachable []cql.UUID
	// Applied reports that a schema change statement applied before the
	// agreement or refresh failed, so it must not be sent again as if it
	// had not.
	Applied bool
}

// Error says what failed, how many versions and unreachable hosts the last
// check saw, and why.
func (e *SchemaAgreementError) Error() string {
	var b strings.Builder
	if e.Applied {
		b.WriteString("gocql: schema change applied, but schema agreement or metadata refresh failed")
	} else {
		b.WriteString("gocql: schema agreement failed")
	}
	if len(e.Versions) > 0 || len(e.Unreachable) > 0 {
		distinct := make(map[cql.UUID]struct{}, len(e.Versions))
		for _, v := range e.Versions {
			distinct[v] = struct{}{}
		}
		fmt.Fprintf(&b, " (%d schema versions, %d unreachable hosts)", len(distinct), len(e.Unreachable))
	}
	if e.Err != nil {
		b.WriteString(": ")
		b.WriteString(e.Err.Error())
	}
	return b.String()
}

// Unwrap returns Err; KindOf and ExecutionStatusOf classify it.
func (e *SchemaAgreementError) Unwrap() error { return e.Err }

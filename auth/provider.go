// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package auth

import (
	"context"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/metadata"
)

// Authenticator classes that servers name when they ask a connection to
// authenticate: the authenticator argument of Provider.Start, and the
// classes Password may allow.
const (
	// PasswordAuthenticator is Cassandra's and ScyllaDB's password
	// authenticator.
	PasswordAuthenticator = "org.apache.cassandra.auth.PasswordAuthenticator"
	// ScyllaTransitionalAuthenticator is ScyllaDB's authenticator for
	// turning authentication on in a running cluster: it checks the
	// credentials a connection sends, and lets in one without valid ones.
	ScyllaTransitionalAuthenticator = "com.scylladb.auth.TransitionalAuthenticator"
	// ScyllaSaslauthdAuthenticator is ScyllaDB's authenticator that checks
	// passwords through saslauthd, such as against LDAP.
	ScyllaSaslauthdAuthenticator = "com.scylladb.auth.SaslauthdAuthenticator"
)

// Provider authenticates connections, starting an Exchange for each.
// Connections open concurrently, so Start must be safe for concurrent use.
type Provider interface {
	// Start begins the exchange for a connection to host, whose server named
	// its authenticator class in authenticator, and returns the exchange and
	// the initial response, null when there is none.
	Start(ctx context.Context, host metadata.HostSnapshot, authenticator string) (Exchange, cql.Nullable[[]byte], error)
}

// Exchange is one connection's authentication. The driver calls it from one
// goroutine at a time, in the order of the server's messages.
//
// Bytes cross it borrowed, both ways. A server message passed to Challenge
// or Success is valid only until the method returns: copy it to keep it. A
// response returned by Provider.Start or Challenge is sent before the driver
// calls the exchange again, and the driver neither changes it nor keeps it
// after that: an exchange may reuse one buffer for its responses, but must
// not change a response before its next method call.
type Exchange interface {
	// Challenge answers one server challenge, which may be null, and returns
	// the response to send.
	Challenge(context.Context, cql.Nullable[cql.Bytes]) (cql.Nullable[[]byte], error)
	// Success receives the server's final message, which may be null. An
	// error fails the connection.
	Success(context.Context, cql.Nullable[cql.Bytes]) error
	// Close releases the exchange. The driver calls it once, however the
	// exchange ended.
	Close()
}

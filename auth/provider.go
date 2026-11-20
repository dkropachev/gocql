// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package auth

import (
	"context"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/metadata"
)

// Provider authenticates a connection. Start begins an exchange for the
// connection to host, whose server named authenticator, and returns the
// initial response to send, if any.
type Provider interface {
	// Start begins the exchange for a connection to the host, whose server
	// named the authenticator class in the string, and returns the exchange and
	// the initial response, null when there is none.
	Start(context.Context, metadata.HostSnapshot, string) (Exchange, cql.Nullable[[]byte], error)
}

// Exchange is one connection's authentication: Challenge answers each
// server challenge, Success receives the server's final message, and Close
// releases the exchange however it ended.
type Exchange interface {
	// Challenge answers one server challenge, which may be null, and returns
	// the response to send. The challenge borrows the server's frame: copy it
	// to keep it after Challenge returns.
	Challenge(context.Context, cql.Nullable[cql.Bytes]) (cql.Nullable[[]byte], error)

	// Success receives the server's final message, which may be null. An
	// error fails the connection.
	Success(context.Context, cql.Nullable[cql.Bytes]) error

	// Close releases the exchange. The driver calls it once, however the
	// exchange ended.
	Close()
}

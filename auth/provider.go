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
	Start(context.Context, metadata.HostSnapshot, string) (Exchange, cql.Nullable[[]byte], error)
}

// Exchange is one connection's authentication: Challenge answers each
// server challenge, Success receives the server's final message, and Close
// releases the exchange however it ended.
type Exchange interface {
	Challenge(context.Context, cql.Nullable[cql.Bytes]) (cql.Nullable[[]byte], error)
	Success(context.Context, cql.Nullable[cql.Bytes]) error
	Close()
}

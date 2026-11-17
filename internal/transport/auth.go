// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"context"
	"sync"

	"github.com/scylladb/gocql/v2/auth"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/metadata"
)

// The bridge only adapts the handshake's state machine; no value marshaler is
// involved. One bridge is created for each connection and closed on every exit.
type v2Authenticator struct {
	ctx      context.Context
	provider auth.Provider
	exchange auth.Exchange
	host     metadata.HostSnapshot
	once     sync.Once
	started  bool
}

func InternalNewV2Authenticator(ctx context.Context, host metadata.HostSnapshot, provider auth.Provider) *v2Authenticator {
	return &v2Authenticator{ctx: ctx, host: host, provider: provider}
}
func (a *v2Authenticator) Challenge(req []byte) ([]byte, Authenticator, error) {
	if err := a.ctx.Err(); err != nil {
		return nil, nil, err
	}
	var response cql.Nullable[[]byte]
	var err error
	if !a.started {
		a.started = true
		a.exchange, response, err = a.provider.Start(a.ctx, a.host, string(req))
		if err == nil && a.exchange == nil {
			err = cqltype.TypeValidation("authentication", "provider returned a nil exchange")
		}
	} else {
		response, err = a.exchange.Challenge(a.ctx, nullableAuthBytes(req))
	}
	if err != nil {
		return nil, nil, err
	}
	if !response.Valid {
		return nil, a, nil
	}
	if response.Value == nil {
		return []byte{}, a, nil
	}
	return response.Value, a, nil
}
func (a *v2Authenticator) Success(data []byte) error {
	if err := a.ctx.Err(); err != nil {
		return err
	}
	if a.exchange == nil {
		return cqltype.TypeValidation("authentication", "success before exchange")
	}
	return a.exchange.Success(a.ctx, nullableAuthBytes(data))
}
func (a *v2Authenticator) Close() {
	a.once.Do(func() {
		if a.exchange != nil {
			a.exchange.Close()
		}
	})
}
func nullableAuthBytes(data []byte) cql.Nullable[cql.Bytes] {
	if data == nil {
		return cql.Nullable[cql.Bytes]{}
	}
	return cql.Nullable[cql.Bytes]{Value: cql.BorrowBytes(data), Valid: true}
}

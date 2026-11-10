// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import (
	"context"
	"errors"
	"strings"

	connectivity "github.com/scylladb/gocql/v2/internal/connectivity"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
)

// AuthProvider creates independent exchanges and must be concurrency safe.
type AuthProvider = connectivity.AuthProvider
type AuthExchange = connectivity.AuthExchange
type InternalPasswordAuth struct{ username, password string }

func PasswordAuth(username, password string) AuthProvider {
	return InternalPasswordAuth{username: username, password: password}
}
func (p InternalPasswordAuth) Start(ctx context.Context, _ model.HostSnapshot, _ string) (AuthExchange, cqltype.Nullable[[]byte], error) {
	if err := ctx.Err(); err != nil {
		return nil, cqltype.Nullable[[]byte]{}, err
	}
	if strings.ContainsRune(p.username, 0) || strings.ContainsRune(p.password, 0) {
		return nil, cqltype.Nullable[[]byte]{}, cqltype.TypeValidation("password authentication", "credentials contain NUL")
	}
	response := make([]byte, len(p.username)+len(p.password)+2)
	copy(response[1:], p.username)
	copy(response[len(p.username)+2:], p.password)
	return &InternalPasswordExchange{}, cqltype.Nullable[[]byte]{Value: response, Valid: true}, nil
}

type InternalPasswordExchange struct{}

func (*InternalPasswordExchange) Challenge(ctx context.Context, _ cqltype.Nullable[cqltype.Bytes]) (cqltype.Nullable[[]byte], error) {
	if err := ctx.Err(); err != nil {
		return cqltype.Nullable[[]byte]{}, err
	}
	return cqltype.Nullable[[]byte]{}, errors.New("gocql: password authenticator received an unexpected challenge")
}
func (*InternalPasswordExchange) Success(ctx context.Context, _ cqltype.Nullable[cqltype.Bytes]) error {
	return ctx.Err()
}
func (*InternalPasswordExchange) Close() {}

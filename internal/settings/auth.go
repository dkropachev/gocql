// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import (
	"context"
	"errors"
	"strings"

	"github.com/scylladb/gocql/v2/cql"
	connectivity "github.com/scylladb/gocql/v2/internal/connectivity"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
)

// AuthProvider creates independent exchanges and must be concurrency safe.
type AuthProvider = connectivity.AuthProvider
type AuthExchange = connectivity.AuthExchange
type passwordAuth struct{ username, password string }

func PasswordAuth(username, password string) AuthProvider {
	return passwordAuth{username: username, password: password}
}
func (p passwordAuth) Start(ctx context.Context, _ model.HostSnapshot, _ string) (AuthExchange, cql.Nullable[[]byte], error) {
	if err := ctx.Err(); err != nil {
		return nil, cql.Nullable[[]byte]{}, err
	}
	if strings.ContainsRune(p.username, 0) || strings.ContainsRune(p.password, 0) {
		return nil, cql.Nullable[[]byte]{}, cqltype.TypeValidation("password authentication", "credentials contain NUL")
	}
	response := make([]byte, len(p.username)+len(p.password)+2)
	copy(response[1:], p.username)
	copy(response[len(p.username)+2:], p.password)
	return &passwordExchange{}, cql.Nullable[[]byte]{Value: response, Valid: true}, nil
}

type passwordExchange struct{}

func (*passwordExchange) Challenge(ctx context.Context, _ cql.Nullable[cql.Bytes]) (cql.Nullable[[]byte], error) {
	if err := ctx.Err(); err != nil {
		return cql.Nullable[[]byte]{}, err
	}
	return cql.Nullable[[]byte]{}, errors.New("gocql: password authenticator received an unexpected challenge")
}
func (*passwordExchange) Success(ctx context.Context, _ cql.Nullable[cql.Bytes]) error {
	return ctx.Err()
}
func (*passwordExchange) Close() {}

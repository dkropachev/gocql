// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package auth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/metadata"
)

type passwordAuth struct {
	username, password string
	allowed            []string
}

// Password authenticates with PasswordAuthenticator credentials. When
// allowed names authenticator classes, such as
// "org.apache.cassandra.auth.PasswordAuthenticator", a connection whose
// server asks for any other class fails before the credentials are sent, so
// they reach only servers that are expected to check them; no allowed class
// sends them to any server that asks, as v1's PasswordAuthenticator did with
// no AllowedAuthenticators. Classes are compared exactly.
func Password(username, password string, allowed ...string) Provider {
	return passwordAuth{username: username, password: password, allowed: slices.Clone(allowed)}
}

// String names the username and leaves the password out, so that printing a
// configuration does not print the password.
func (p passwordAuth) String() string {
	return "auth.Password(" + strconv.Quote(p.username) + ", <redacted>)"
}

// GoString is String, for %#v.
func (p passwordAuth) GoString() string { return p.String() }

// LogValue is String, for log/slog.
func (p passwordAuth) LogValue() slog.Value { return slog.StringValue(p.String()) }

func (p passwordAuth) Start(ctx context.Context, _ metadata.HostSnapshot, authenticator string) (Exchange, cql.Nullable[[]byte], error) {
	if err := ctx.Err(); err != nil {
		return nil, cql.Nullable[[]byte]{}, err
	}
	if len(p.allowed) > 0 && !slices.Contains(p.allowed, authenticator) {
		return nil, cql.Nullable[[]byte]{}, fmt.Errorf("gocql: the server's authenticator %q is not one auth.Password allows, %q", authenticator, p.allowed)
	}
	if strings.ContainsRune(p.username, 0) || strings.ContainsRune(p.password, 0) {
		return nil, cql.Nullable[[]byte]{}, &cql.ValidationError{Field: "password authentication", Err: errors.New("credentials contain NUL")}
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

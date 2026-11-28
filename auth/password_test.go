// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package auth

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/metadata"
)

// TestPasswordIsNotPrinted prints a Password provider every way a
// configuration might be printed or logged, alone and inside a struct.
func TestPasswordIsNotPrinted(t *testing.T) {
	const secret = "s3cr3t-P@ssw0rd-1188"
	p := Password("cassandra", secret)
	holder := struct{ Auth Provider }{p}
	var out bytes.Buffer
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
		fmt.Fprintf(&out, format+"\n", p)
		fmt.Fprintf(&out, format+"\n", holder)
	}
	for _, h := range []slog.Handler{slog.NewTextHandler(&out, nil), slog.NewJSONHandler(&out, nil)} {
		slog.New(h).Info("config", "auth", p, "holder", holder)
	}
	if strings.Contains(out.String(), secret) {
		t.Fatalf("the password was printed:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "cassandra") {
		t.Fatalf("the username was not printed:\n%s", out.String())
	}
}

// TestPasswordExchange runs a whole PasswordAuthenticator exchange: the
// initial response carries the credentials, a challenge fails, Success
// accepts the server's final message, and Close releases the exchange.
func TestPasswordExchange(t *testing.T) {
	ctx := context.Background()
	exchange, response, err := Password("user", "pass").Start(ctx, metadata.HostSnapshot{}, PasswordAuthenticator)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if want := []byte("\x00user\x00pass"); !response.Valid || !bytes.Equal(response.Value, want) {
		t.Fatalf("initial response = %q (valid %v), want %q", response.Value, response.Valid, want)
	}
	if got, err := exchange.Challenge(ctx, cql.Nullable[cql.Bytes]{}); err == nil || got.Valid {
		t.Fatalf("Challenge = %v, %v; want an unexpected-challenge error", got, err)
	}
	if err := exchange.Success(ctx, cql.Nullable[cql.Bytes]{}); err != nil {
		t.Fatalf("Success: %v", err)
	}
	exchange.Close()

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := exchange.Success(cancelled, cql.Nullable[cql.Bytes]{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Success after cancel = %v, want context.Canceled", err)
	}
	if _, _, err := Password("us\x00er", "pass").Start(ctx, metadata.HostSnapshot{}, ""); err == nil {
		t.Fatal("Start accepted credentials containing NUL")
	}
}

// TestPasswordAllowedAuthenticators sends the credentials only to an allowed
// authenticator class, and to any class when none is allowed.
func TestPasswordAllowedAuthenticators(t *testing.T) {
	ctx := context.Background()
	const allowed = PasswordAuthenticator
	classes := []string{allowed, ScyllaTransitionalAuthenticator}
	p := Password("user", "pass", classes...)
	classes[0] = "changed"
	if _, response, err := p.Start(ctx, metadata.HostSnapshot{}, allowed); err != nil || !response.Valid {
		t.Fatalf("Start with an allowed class = %v, %v", response, err)
	}
	for _, class := range []string{"com.example.Other", "", "org.apache.cassandra.auth.passwordauthenticator"} {
		if exchange, response, err := p.Start(ctx, metadata.HostSnapshot{}, class); err == nil || exchange != nil || response.Valid {
			t.Fatalf("Start with %q = %v, %v, %v; want an error and no credentials", class, exchange, response, err)
		} else if !strings.Contains(err.Error(), strconv.Quote(class)) {
			t.Fatalf("error %q does not name %q", err, class)
		}
	}
	if _, _, err := Password("user", "pass").Start(ctx, metadata.HostSnapshot{}, "com.example.Other"); err != nil {
		t.Fatalf("Start without allowed classes = %v, want any class accepted", err)
	}
}

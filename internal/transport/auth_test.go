// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/scylladb/gocql/v2/auth"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/routing"
)

type v2TestAuthProvider struct{ exchanges []*v2TestAuthExchange }
type v2TestAuthExchange struct {
	challengeNull, success bool
	closed                 int
}

func (p *v2TestAuthProvider) Start(context.Context, routing.HostSnapshot, string) (auth.Exchange, cql.Nullable[[]byte], error) {
	e := &v2TestAuthExchange{}
	p.exchanges = append(p.exchanges, e)
	return e, cql.Nullable[[]byte]{Valid: true}, nil
}
func (e *v2TestAuthExchange) Challenge(_ context.Context, b cql.Nullable[cql.Bytes]) (cql.Nullable[[]byte], error) {
	e.challengeNull = !b.Valid
	return cql.Nullable[[]byte]{}, nil
}
func (e *v2TestAuthExchange) Success(context.Context, cql.Nullable[cql.Bytes]) error {
	e.success = true
	return nil
}
func (e *v2TestAuthExchange) Close() { e.closed++ }
func TestV2AuthExchangeIsolationAndNull(t *testing.T) {
	p := &v2TestAuthProvider{}
	a := newV2Authenticator(context.Background(), routing.HostSnapshot{}, p)
	b := newV2Authenticator(context.Background(), routing.HostSnapshot{}, p)
	response, next, err := a.Challenge([]byte("test"))
	if err != nil || response == nil || next != a {
		t.Fatal("present empty response lost", response, next, err)
	}
	_, _, _ = b.Challenge([]byte("test"))
	response, _, err = a.Challenge(nil)
	if err != nil || response != nil || !p.exchanges[0].challengeNull {
		t.Fatal("null challenge/response lost")
	}
	if err := a.Success(nil); err != nil {
		t.Fatal(err)
	}
	a.Close()
	a.Close()
	b.Close()
	if !p.exchanges[0].success || p.exchanges[1].success || p.exchanges[0].closed != 1 || p.exchanges[1].closed != 1 {
		t.Fatal("exchange sharing/lifetime failure")
	}
}
func TestV2PasswordAuthAndCancellation(t *testing.T) {
	provider := auth.Password("alice", "secret")
	exchange, token, err := provider.Start(context.Background(), routing.HostSnapshot{}, "org.apache.cassandra.auth.PasswordAuthenticator")
	if err != nil || !token.Valid || !bytes.Equal(token.Value, []byte("\x00alice\x00secret")) {
		t.Fatal(token, err)
	}
	if _, err := exchange.Challenge(context.Background(), cql.Nullable[cql.Bytes]{}); err == nil {
		t.Fatal("unexpected challenge accepted")
	}
	exchange.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := provider.Start(ctx, routing.HostSnapshot{}, ""); err != context.Canceled {
		t.Fatal(err)
	}
}

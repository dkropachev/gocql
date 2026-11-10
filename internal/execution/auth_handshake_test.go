/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package execution

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/scylladb/gocql/v2/internal/endpoint"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func TestAuthHandshake(t *testing.T) {
	t.Run("Approve", func(t *testing.T) {
		tests := map[bool]bool{
			transport.InternalApprove("org.apache.cassandra.auth.PasswordAuthenticator", []string{}):                                             true,
			transport.InternalApprove("org.apache.cassandra.auth.MutualTlsWithPasswordFallbackAuthenticator", []string{}):                        true,
			transport.InternalApprove("org.apache.cassandra.auth.MutualTlsAuthenticator", []string{}):                                            true,
			transport.InternalApprove("com.instaclustr.cassandra.auth.SharedSecretAuthenticator", []string{}):                                    true,
			transport.InternalApprove("com.datastax.bdp.cassandra.auth.DseAuthenticator", []string{}):                                            true,
			transport.InternalApprove("io.aiven.cassandra.auth.AivenAuthenticator", []string{}):                                                  true,
			transport.InternalApprove("com.amazon.helenus.auth.HelenusAuthenticator", []string{}):                                                true,
			transport.InternalApprove("com.ericsson.bss.cassandra.ecaudit.auth.AuditAuthenticator", []string{}):                                  true,
			transport.InternalApprove("com.scylladb.auth.SaslauthdAuthenticator", []string{}):                                                    true,
			transport.InternalApprove("com.scylladb.auth.TransitionalAuthenticator", []string{}):                                                 true,
			transport.InternalApprove("com.instaclustr.cassandra.auth.InstaclustrPasswordAuthenticator", []string{}):                             true,
			transport.InternalApprove("com.apache.cassandra.auth.FakeAuthenticator", []string{}):                                                 true,
			transport.InternalApprove("com.apache.cassandra.auth.FakeAuthenticator", nil):                                                        true,
			transport.InternalApprove("com.apache.cassandra.auth.FakeAuthenticator", []string{"com.apache.cassandra.auth.FakeAuthenticator"}):    true,
			transport.InternalApprove("com.apache.cassandra.auth.FakeAuthenticator", []string{"com.apache.cassandra.auth.NotFakeAuthenticator"}): false,
		}
		for k, v := range tests {
			if k != v {
				t.Fatalf("expected '%v', got '%v'", k, v)
			}
		}
	})

	// AuthProvider picks the authenticator per host.
	t.Run("AuthProvider", func(t *testing.T) {
		t.Parallel()

		want := passwordToken("per-host", "pw")
		srv := newAuthTestServer(t, func(token []byte) authReply {
			if string(token) != want {
				return authReply{op: frm.OpError, errCode: protocol.ErrCodeCredentials, errMsg: "bad credentials"}
			}
			return authReply{op: frm.OpAuthSuccess}
		})

		var mu sync.Mutex
		var hosts []string
		cfg, _ := srv.cluster(nil)
		cfg.AuthProvider = func(h *endpoint.HostInfo) (transport.Authenticator, error) {
			mu.Lock()
			hosts = append(hosts, h.ConnectAddress().String())
			mu.Unlock()
			return transport.PasswordAuthenticator{Username: "per-host", Password: "pw"}, nil
		}
		s, err := cfg.Open()
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		s.Close()

		mu.Lock()
		defer mu.Unlock()
		if len(hosts) == 0 || hosts[0] != "127.0.0.1" {
			t.Fatalf("AuthProvider called for %v, want 127.0.0.1", hosts)
		}

		errProvider := errors.New("no credentials for host")
		cfg, log := srv.cluster(nil)
		cfg.AuthProvider = func(*endpoint.HostInfo) (transport.Authenticator, error) { return nil, errProvider }
		requireHandshakeError(t, cfg, log, errProvider.Error())
	})

	t.Run("Errors", func(t *testing.T) {
		t.Run("AuthenticatorErrors", func(t *testing.T) {
			t.Parallel()

			errChallenge := errors.New("challenge rejected")
			errSuccess := errors.New("success rejected")

			for _, tc := range []struct {
				name string
				auth stepAuthenticator
				want error
			}{
				{"ChallengeFails", stepAuthenticator{challengeErr: errChallenge}, errChallenge},
				{"SuccessFails", stepAuthenticator{successErr: errSuccess}, errSuccess},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					srv := newAuthTestServer(t, multiStepReply)
					tc.auth.log = &authLog{}
					cfg, log := srv.cluster(tc.auth)
					requireHandshakeError(t, cfg, log, tc.want.Error())
				})
			}
		})

		// PasswordAuthenticator is single-step: it returns no follow-up
		// authenticator. A server that answers its credentials with a challenge
		// anyway must fail the connection, not dereference the missing one.
		t.Run("ChallengeWithoutChallenger", func(t *testing.T) {
			t.Parallel()

			srv := newAuthTestServer(t, func([]byte) authReply {
				return authReply{op: frm.OpAuthChallenge, data: []byte("more?")}
			})
			cfg, log := srv.cluster(transport.PasswordAuthenticator{Username: "u", Password: "p"})
			requireHandshakeError(t, cfg, log, "unexpected authentication challenge")
		})

		t.Run("Failures", func(t *testing.T) {
			t.Parallel()

			success := func([]byte) authReply { return authReply{op: frm.OpAuthSuccess} }

			tests := []struct {
				name    string
				auth    transport.Authenticator
				reply   func([]byte) authReply
				wantErr string
			}{
				{
					name:    "NoAuthenticatorConfigured",
					auth:    nil,
					reply:   success,
					wantErr: fmt.Sprintf("authentication required (using %q)", testAuthClass),
				},
				{
					name:    "AuthenticatorClassNotAllowed",
					auth:    transport.PasswordAuthenticator{Username: "u", Password: "p", AllowedAuthenticators: []string{"com.example.Other"}},
					reply:   success,
					wantErr: "unexpected authenticator",
				},
				{
					name: "BadCredentials",
					auth: transport.PasswordAuthenticator{Username: "u", Password: "wrong"},
					reply: func([]byte) authReply {
						return authReply{op: frm.OpError, errCode: protocol.ErrCodeCredentials, errMsg: "Provided username u and/or password are incorrect"}
					},
					wantErr: "password are incorrect",
				},
				{
					name:    "UnexpectedFrame",
					auth:    transport.PasswordAuthenticator{Username: "u", Password: "p"},
					reply:   func([]byte) authReply { return authReply{op: frm.OpReady} },
					wantErr: "unknown frame response during authentication",
				},
			}

			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					srv := newAuthTestServer(t, tc.reply)
					cfg, log := srv.cluster(tc.auth)
					requireHandshakeError(t, cfg, log, tc.wantErr)
				})
			}
		})
	})

	t.Run("MultiStep", func(t *testing.T) {
		t.Parallel()

		srv := newAuthTestServer(t, multiStepReply)
		log := &authLog{}
		cfg, _ := srv.cluster(stepAuthenticator{log: log})
		s, err := cfg.Open()
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer s.Close()

		want := []string{
			fmt.Sprintf("challenge 0 %q", testAuthClass),
			`challenge 1 "c1"`,
			`success 2 "done"`,
		}
		got := log.get()
		if len(got) < len(want) {
			t.Fatalf("authenticator calls = %q, want %q", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("authenticator calls = %q, want them to start with %q", got, want)
			}
		}
	})

	t.Run("Password", func(t *testing.T) {
		t.Parallel()

		want := passwordToken("cassandra", "secret")
		srv := newAuthTestServer(t, func(token []byte) authReply {
			if string(token) != want {
				return authReply{op: frm.OpError, errCode: protocol.ErrCodeCredentials, errMsg: "bad credentials"}
			}
			return authReply{op: frm.OpAuthSuccess}
		})

		cfg, _ := srv.cluster(transport.PasswordAuthenticator{Username: "cassandra", Password: "secret"})
		s, err := cfg.Open()
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer s.Close()
		if err := execTestStatement(context.Background(), s, "void", CompiledOptions{}); err != nil {
			t.Fatalf("query after authenticating: %v", err)
		}
		if got := srv.received(); len(got) == 0 || got[0] != want {
			t.Fatalf("server received tokens %q, want %q", got, want)
		}
	})
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/auth"
	"github.com/scylladb/gocql/v2/cqlerr"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/metadata"
)

// TestLogsDoNotLeakCredentials connects with a password, and with a TLS
// client key, through handshakes that succeed and that fail, logging at Debug,
// and checks that neither the password nor the key reaches a log record, the
// error, or the printed configuration.
func TestLogsDoNotLeakCredentials(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "client"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keySecret := key.D.String()

	for _, tc := range []struct {
		name  string
		reply func([]byte) authReply
		tls   bool
	}{
		{"BadCredentials", func([]byte) authReply {
			return authReply{op: frm.OpError, errCode: cqlerr.CodeBadCredentials, errMsg: "Username and/or password are incorrect"}
		}, false},
		{"UnexpectedChallenge", func([]byte) authReply { return authReply{op: frm.OpAuthChallenge, data: []byte("challenge")} }, false},
		{"Success", func([]byte) authReply { return authReply{op: frm.OpAuthSuccess} }, false},
		// A listener that closes every connection fails the handshake.
		{"TLSHandshakeFails", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var srv *authTestServer
			address := ""
			if tc.reply != nil {
				srv = newAuthTestServer(t, tc.reply)
				address = srv.Address
			} else {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = listener.Close() })
				go func() {
					for {
						conn, err := listener.Accept()
						if err != nil {
							return
						}
						_ = conn.Close()
					}
				}()
				address = listener.Addr().String()
			}
			logger, logs := logging.NewCapture()
			config := settings.DefaultConfig(address)
			config.Protocol = 4
			config.Metadata.Mode = metadata.ModeNone
			config.Metadata.InitialDiscovery = false
			config.Metadata.AutomaticAgreement = false
			config.Pool.AdvancedShardAwareness.Enabled = false
			config.Pool.ConnectionsPerHost = 1
			config.Logger = logger
			config.Auth = auth.Password("cassandra", redactionSecret)
			if tc.tls {
				config.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, InsecureSkipVerify: true}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			s, err := Connect(ctx, config)
			if s != nil {
				_ = s.Close()
			}
			if srv != nil && !strings.Contains(strings.Join(srv.received(), "\n"), redactionSecret) {
				t.Fatal("the password did not reach the server")
			}
			printed := fmt.Sprintf("%v\n%+v\n%#v\n%v\n%+v\n", config.Auth, config.Auth, config.Auth, config, config)
			if err != nil {
				printed += errorText(err)
			}
			logged := logs.String()
			for _, secret := range []string{redactionSecret, keySecret} {
				if strings.Contains(printed, secret) {
					t.Errorf("the error or configuration leaks a secret: %s", printed)
				}
				if strings.Contains(logged, secret) || logs.Contains(secret) {
					t.Errorf("a log record leaks a secret: %s", logged)
				}
			}
			if logged == "" {
				t.Error("nothing was logged")
			}
		})
	}
}

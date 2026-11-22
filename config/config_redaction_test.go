// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package config

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/auth"
)

// TestConfigDoesNotPrintCredentials prints and logs a Config holding a
// password and a TLS client key and checks that neither appears.
func TestConfigDoesNotPrintCredentials(t *testing.T) {
	const password = "s3cr3t-P@ssw0rd-1188"
	const key = "PRIVATE-KEY-MATERIAL-1188"
	tlsConfig := &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{[]byte("cert")}, PrivateKey: key}}}
	c, err := New(ScyllaV4, "127.0.0.1").WithAuth(auth.Password("cassandra", password)).WithTLS(tlsConfig).Build()
	if err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	fmt.Fprintf(&out, "%v\n%+v\n%#v\n%s\n", c, c, c, c)
	slog.New(slog.NewTextHandler(&out, nil)).Info("config", "config", c)
	slog.New(slog.NewJSONHandler(&out, nil)).Info("config", "config", c)
	for _, secret := range []string{password, key} {
		if strings.Contains(out.String(), secret) {
			t.Fatalf("a secret was printed:\n%s", out.String())
		}
	}
	if !strings.Contains(out.String(), "auth=true") || !strings.Contains(out.String(), "tls=true") {
		t.Fatalf("the config did not print:\n%s", out.String())
	}
}

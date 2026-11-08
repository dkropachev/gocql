// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"log"
	"os"

	gocql "github.com/scylladb/gocql/v2"
)

// Reload the client certificate and CA from files while the session runs.
func ExampleConfigBuilder_TLSFiles() {
	config, err := gocql.NewConfig("192.168.1.1", "192.168.1.2").
		TLS(&tls.Config{MinVersion: tls.VersionTLS13}).
		TLSFiles(gocql.TLSFiles{
			CertFile: "/etc/scylla-client/tls.crt",
			KeyFile:  "/etc/scylla-client/tls.key",
			CAFile:   "/etc/scylla-client/ca.crt",
		}).
		Build()
	if err != nil {
		log.Fatal(err)
	}
	session, err := gocql.Connect(context.Background(), config)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	// Connections dialed after the files change use the new certificate.
}

// Replace the whole TLS configuration of a running session.
func ExampleSession_UpdateTLS() {
	var session *gocql.Session // from gocql.Connect with TLS
	pem, err := os.ReadFile("/etc/scylla-client/new-ca.crt")
	if err != nil {
		log.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(pem)
	pair, err := tls.LoadX509KeyPair("/etc/scylla-client/new.crt", "/etc/scylla-client/new.key")
	if err != nil {
		log.Fatal(err)
	}
	err = session.UpdateTLS(&tls.Config{
		MinVersion:         tls.VersionTLS13,
		RootCAs:            roots,
		Certificates:       []tls.Certificate{pair},
		ClientSessionCache: tls.NewLRUClientSessionCache(0), // new identity, new tickets
	})
	if err != nil {
		log.Fatal(err)
	}
	// Open connections keep their TLS session; new dials use this config.
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package config_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"log"
	"os"
	"sync/atomic"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/config"
)

// Reload the client certificate and CA from files while sessions run.
func ExampleNewTLSFiles() {
	files, err := config.NewTLSFiles(&tls.Config{MinVersion: tls.VersionTLS13}, config.TLSFiles{
		CertFile: "/etc/scylla-client/tls.crt",
		KeyFile:  "/etc/scylla-client/tls.key",
		CAFile:   "/etc/scylla-client/ca.crt",
	})
	if err != nil {
		log.Fatal(err)
	}
	cfg, err := config.New("192.168.1.1", "192.168.1.2").WithTLSSource(files).Build()
	if err != nil {
		log.Fatal(err)
	}
	session, err := gocql.Connect[gocql.ScyllaV4](context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	// The first connection dialed after the files change uses them.
	// files.Err reports a change that could not be loaded.
}

// rotatingTLS is a TLSSource the application updates, for example from a
// secrets manager.
type rotatingTLS struct{ current atomic.Pointer[tls.Config] }

func (r *rotatingTLS) TLSConfig() *tls.Config { return r.current.Load() }

// Replace the whole TLS configuration of running sessions.
func ExampleTLSSource() {
	source := &rotatingTLS{}
	source.current.Store(&tls.Config{MinVersion: tls.VersionTLS13})
	cfg, err := config.New("192.168.1.1").WithTLSSource(source).Build()
	if err != nil {
		log.Fatal(err)
	}
	session, err := gocql.Connect[gocql.ScyllaV4](context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

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
	source.current.Store(&tls.Config{
		MinVersion:         tls.VersionTLS13,
		RootCAs:            roots,
		Certificates:       []tls.Certificate{pair},
		ClientSessionCache: tls.NewLRUClientSessionCache(0), // new identity and CA, new tickets
	})
	// Open connections keep their TLS session; new dials use this config.
}

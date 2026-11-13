// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"context"
	"testing"
	"time"

	implProtocol "github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/transport"
)

// BenchmarkDial measures opening one pool connection: dial, OPTIONS and
// STARTUP against the in-process test server.
func BenchmarkDial(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := NewTestServer(b, implProtocol.ProtoVersion4, ctx)
	defer srv.Stop()
	cluster := testCluster(implProtocol.ProtoVersion4, srv.Address)
	cluster.NumConns = 1
	session, err := cluster.Open()
	if err != nil {
		b.Fatal(err)
	}
	defer session.Close()
	if err := waitForPoolSize(session, 1, 10*time.Second); err != nil {
		b.Fatal(err)
	}
	hosts := sessionGetHosts(session)
	cfg := SessionData(session).InternalConnCfg
	handler := transport.InternalConnErrorHandlerFn(func(*transport.Conn, error, bool) {})
	b.ReportAllocs()
	for b.Loop() {
		conn, err := sessionDial(session, SessionData(session).InternalCtx, hosts[0], cfg, handler)
		if err != nil {
			b.Fatal(err)
		}
		conn.Close()
	}
}

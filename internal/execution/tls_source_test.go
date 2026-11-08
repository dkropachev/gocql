// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"testing"
	"time"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func TestTLSSourceSession(t *testing.T) {
	srv := NewSSLTestServer(t, protocol.ProtoVersion4, context.Background())
	defer srv.Stop()
	srv.setQueryHook(func(query string, stream int, response *protocol.Framer) bool {
		if !strings.Contains(query, "system.local") {
			return false
		}
		writeRows(response, stream, systemLocalCols, [][][]byte{systemLocalRow(net.IPv4(127, 0, 0, 1))})
		return true
	})
	srv.setRegisterHook(func(_ []string, stream int, response *protocol.Framer) {
		response.WriteHeader(0, frm.OpReady, stream)
	})
	// The fixture certificate names no IP address, so verification is off.
	source := &testTLSSource{}
	source.Store(&tls.Config{InsecureSkipVerify: true})
	config := settings.DefaultConfig(srv.Address)
	config.Protocol = 4
	config.Metadata.InitialDiscovery = false
	config.Metadata.Mode = settings.MetadataDisabled
	config.Metadata.PeriodicRefresh = 0
	config.ReportConfiguration = false
	config.Pool.ConnectionsPerHost = 1
	config.Pool.ShardAwarePort = false
	config.TLS = source.TLSConfig()
	config.TLSSource = source
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session, err := Connect(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	dialer, ok := implCluster.SessionData(session).InternalConnCfg.HostDialer.(*transport.InternalV2TLSHostDialer)
	if !ok || dialer.InternalConfig.TLSSource != source {
		t.Fatal("the session dialer does not read the TLS source")
	}
	if err := execTestStatement(ctx, session, "void", Options{}); err != nil {
		t.Fatal(err)
	}
}

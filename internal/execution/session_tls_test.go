// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"crypto/tls"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func TestSessionTLSRuntime(t *testing.T) {
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
	ca, err := os.ReadFile("../../testdata/pki/ca.crt")
	if err != nil {
		t.Fatal(err)
	}
	caPath := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(caPath, ca, 0o600); err != nil {
		t.Fatal(err)
	}
	files := settings.TLSFiles{CertFile: "../../testdata/pki/gocql.crt", KeyFile: "../../testdata/pki/gocql.key", CAFile: caPath, Interval: 5 * time.Millisecond}
	content, err := settings.ReadTLSFiles(files)
	if err != nil {
		t.Fatal(err)
	}
	// The fixture certificate names no IP address, so the test skips
	// verification; RootCAs still shows which CA file was applied.
	base := &tls.Config{InsecureSkipVerify: true}
	config := settings.DefaultConfig(srv.Address)
	config.Protocol = 4
	config.Metadata.InitialDiscovery = false
	config.Metadata.Mode = settings.MetadataDisabled
	config.Metadata.PeriodicRefresh = 0
	config.ReportConfiguration = false
	config.Pool.ConnectionsPerHost = 1
	config.Pool.ShardAwarePort = false
	config.TLS, err = settings.BuildTLSFromFiles(base, content)
	if err != nil {
		t.Fatal(err)
	}
	config.TLSFiles, config.TLSFilesBase = &files, base
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session, err := Connect(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	dialer, ok := implCluster.SessionData(session).InternalConnCfg.HostDialer.(*transport.InternalV2TLSHostDialer)
	if !ok || dialer.InternalTLS == nil {
		t.Fatal("session dialer does not read the live TLS config")
	}
	initial := implCluster.InternalSessionCurrentTLS(session)
	if dialer.InternalTLS.Load() != initial {
		t.Fatal("dialer and session hold different TLS configs")
	}

	if err := os.WriteFile(caPath, append(append([]byte(nil), ca...), ca...), 0o600); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for implCluster.InternalSessionCurrentTLS(session) == initial {
		if time.Now().After(deadline) {
			t.Fatal("the session did not reload the changed CA file")
		}
		time.Sleep(time.Millisecond)
	}
	if dialer.InternalTLS.Load() != implCluster.InternalSessionCurrentTLS(session) {
		t.Fatal("dialer does not see the reloaded config")
	}

	if err := session.UpdateTLS(&tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13}); err != nil {
		t.Fatal(err)
	}
	current := dialer.InternalTLS.Load()
	if current.MinVersion != tls.VersionTLS13 || len(current.Certificates) != 1 || current.RootCAs == nil {
		t.Fatal("UpdateTLS did not keep the file certificate and CA over the new base")
	}
	if err := execTestStatement(ctx, session, "void", Options{}); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if err := session.UpdateTLS(&tls.Config{}); err == nil {
		t.Fatal("UpdateTLS on a closed session succeeded")
	}
}

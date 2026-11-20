//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/metadata"
)

// TestKeyspaceToCQLTablets checks that a keyspace recreated from the ToCQL
// a session builds itself, as it does when it loads keyspaces without their
// DESCRIBE output, keeps its tablets option (scylladb/gocql#1055).
func TestKeyspaceToCQLTablets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cfg := createV2Cluster(func(b *config.ConfigBuilder) { b.WithMetadataMode(metadata.SchemaKeyspaces) })
	s, err := gocql.Connect(ctx, mustBuild(t, cfg), testDialect)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	supported, err := v2TabletsSupported(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	if !supported {
		t.Skip("server does not provide tablets")
	}
	for _, tc := range []struct {
		name, option, clause string
		tablets              bool
	}{
		{"Initial", "{'initial': 4}", "AND tablets = {'initial': 4}", true},
		{"Vnodes", "{'enabled': false}", "AND tablets = {'enabled': false}", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keyspace := testKeyspaceName(t, fmt.Sprintf("tocql_tablets_%x", time.Now().UnixNano()))
			create := "CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = " + tc.option
			if err := v2CreateTable(ctx, s, create); err != nil {
				t.Fatal(err)
			}
			defer func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if err := v2CreateTable(cleanup, s, "DROP KEYSPACE IF EXISTS "+keyspace); err != nil {
					t.Error(err)
				}
			}()
			check := func(stage string) string {
				t.Helper()
				if err := s.RefreshMetadata(ctx); err != nil {
					t.Fatal(stage, err)
				}
				ks, ok := s.Metadata().Keyspace(keyspace)
				if !ok {
					t.Fatal(stage, "keyspace not in metadata")
				}
				if ks.TabletBased != tc.tablets {
					t.Fatalf("%s: TabletBased %v, want %v", stage, ks.TabletBased, tc.tablets)
				}
				stmt, err := ks.ToCQL()
				if err != nil {
					t.Fatal(stage, err)
				}
				if !strings.Contains(stmt, tc.clause) {
					t.Fatalf("%s: ToCQL lacks %q:\n%s", stage, tc.clause, stmt)
				}
				return stmt
			}
			stmt := check("created")
			if err := v2CreateTable(ctx, s, "DROP KEYSPACE "+keyspace); err != nil {
				t.Fatal(err)
			}
			if err := v2CreateTable(ctx, s, strings.TrimSpace(stmt)); err != nil {
				t.Fatalf("recreate from %q: %v", stmt, err)
			}
			check("recreated")
		})
	}
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

//go:build integration

package integration

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	gocql "github.com/scylladb/gocql/v2"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
)

// dropLeftoverKeyspaces removes what an earlier run against the same cluster
// left behind, so that its fixed table and keyspace names do not collide with
// this run's CREATE statements. It runs before any test, while no session can
// still be using these keyspaces.
//
// It only touches the shared default namespace: the gocql_test fixture
// keyspaces (including gocql_test_fail, whose writes Cassandra is told to
// fail) and the keyspaces testKeyspaceName derives from test paths, which
// all start with "test". An owned fixture run (GOCQL_TEST_RUN) has a fresh
// namespace and its own cleanup, so it is left alone, as is a run that selects
// no tests.
func dropLeftoverKeyspaces() error {
	if os.Getenv("GOCQL_TEST_RUN") != "" || os.Getenv("GOCQL_FIXTURE_CLEANUP") != "" {
		return nil
	}
	if f := flag.Lookup("test.run"); f != nil && f.Value.String() == "^$" {
		return nil
	}
	if f := flag.Lookup("test.list"); f != nil && f.Value.String() != "" {
		return nil
	}
	fixture := map[string]bool{fixtureKeyspace(""): true, fixtureKeyspace("_td"): true, fixtureKeyspace("_fail"): true}
	leftover := func(name string) bool { return fixture[name] || strings.HasPrefix(name, "test") }

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	config, err := createV2Cluster(func(c *gocql.ConfigBuilder) {
		c.WithKeyspace("system").
			WithIgnoreEvents(gocql.SchemaEvent, gocql.TopologyEvent, gocql.StatusEvent).
			WithManualSchemaAgreement().
			NoRefreshAfterAgreement()
	}).Build()
	if err != nil {
		return fmt.Errorf("build config: %w", err)
	}
	s, err := connectOnceCredentialsWork(ctx, config)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer s.Close()
	rows, err := v2Query(ctx, s, "SELECT keyspace_name FROM system_schema.keyspaces")
	if err != nil {
		return fmt.Errorf("list keyspaces: %w", err)
	}
	var names []string
	var row cql.DynamicRow
	for rows.NextInto(&row) {
		value, err := row.At(0)
		if err != nil {
			rows.Close()
			return err
		}
		name, err := value.Text()
		if err != nil {
			rows.Close()
			return err
		}
		if leftover(name.String()) {
			names = append(names, name.String())
		}
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("list keyspaces: %w", err)
	}
	for _, name := range names {
		stmt := "DROP KEYSPACE IF EXISTS " + `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
		if err := v2CreateTable(ctx, s, stmt); err != nil {
			return fmt.Errorf("drop keyspace %s: %w", name, err)
		}
	}
	if len(names) > 0 {
		fmt.Fprintf(os.Stderr, "dropped %d keyspaces left by an earlier run\n", len(names))
	}
	return nil
}

// connectOnceCredentialsWork connects, retrying while the server rejects the
// credentials. With PasswordAuthenticator, Cassandra creates its default
// superuser a few seconds after the native transport starts accepting
// connections, so a suite started right after make cassandra-start would see
// every session refused. Any other error is returned at once.
func connectOnceCredentialsWork(ctx context.Context, config gocql.Config) (*gocql.Session[testDialect], error) {
	for {
		s, err := gocql.Connect[testDialect](ctx, config)
		if request, ok := errors.AsType[cqlerr.ServerError](err); err == nil || !ok || request.Code() != cqlerr.CodeBadCredentials {
			return s, err
		}
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(time.Second):
		}
	}
}

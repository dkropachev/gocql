//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package integration

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/config"
	implSession "github.com/scylladb/gocql/v2/internal/session"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql/dynamic"
	"github.com/scylladb/gocql/v2/cqlerr"
)

func TestFixture(t *testing.T) {
	// The runner always invokes cleanup after its test subprocess, even on failure.
	// A random run prefix is required; default/manual suites cannot trigger it.
	t.Run("Cleanup", func(t *testing.T) {
		if os.Getenv("GOCQL_FIXTURE_CLEANUP") != "1" {
			t.Skip("cleanup runs only in the native runner's explicit post-package subprocess")
		}
		cleanupNamespaces(t, nil)
	})

	t.Run("ClientCertificate", func(t *testing.T) {
		if !*flagRunSslTest || *flagDistribution != "scylla" {
			t.Skip("requires Scylla mutual-TLS fixture")
		}
		accepted, err := gocql.Connect[testDialect](context.Background(), mustBuild(t, createV2Cluster()))
		if err != nil {
			t.Fatal("valid client certificate rejected", err)
		}
		accepted.Close()
		cfg := createV2ClusterWithout(fixtureTLS, func(c *config.ConfigBuilder) {
			noCertificate := fixtureClientTLS()
			noCertificate.Certificates = nil
			c.WithTLS(fixtureTLSConfig(noCertificate))
		})
		rejected, err := gocql.Connect[testDialect](context.Background(), mustBuild(t, cfg))
		if err == nil {
			rejected.Close()
			t.Fatal("mutual-TLS server accepted missing client certificate")
		}
		t.Log("MISSING_CLIENT_CERT_REJECTED", err)
	})

	// Explicit lifecycle probe used only by the harness's failure/cancellation gate.
	// It provisions a real owned namespace before announcing the injection point.
	t.Run("HarnessLifecycle", func(t *testing.T) {
		action := os.Getenv("GOCQL_FIXTURE_ACTION")
		if action == "" {
			t.Skip("explicit harness failure/cancellation probe")
		}
		s := createV2Session(t)
		defer s.Close()
		if marker := os.Getenv("GOCQL_FIXTURE_MARKER"); marker != "" {
			if err := os.WriteFile(marker, []byte((*implSession.Session)(s).Data().InternalCfg.Keyspace), 0600); err != nil {
				t.Fatal(err)
			}
		}
		switch action {
		case "fail":
			t.Fatal("intentional harness failure after provisioning")
		case "cancel":
			select {}
		default:
			t.Fatal("unknown lifecycle injection")
		}
	})

	// This verifies the helpers' ownership and cardinality contract against a real
	// server, including the second page fetched after the helper returns its cursor.
	t.Run("Helpers", func(t *testing.T) {
		session := createV2Session(t, func(cfg *config.ConfigBuilder) {
			cfg.ConfigureDefaultProfile(func(p *config.ProfileBuilder) { p.WithPageSize(1) })
		})
		defer session.Close()
		ctx := context.Background()
		table := (fixtureKeyspace("") + ".") + testTableName(t, "ownership")
		if err := v2CreateTable(ctx, session, "CREATE TABLE IF NOT EXISTS "+table+" (bucket int, id int, payload blob, PRIMARY KEY (bucket, id))"); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := v2CreateTable(ctx, session, "DROP TABLE IF EXISTS "+table); err != nil {
				t.Error(err)
			}
		}()
		payload := []byte("owned helper result")
		for id := int32(1); id <= 2; id++ {
			if _, err := v2Exec(ctx, session, "INSERT INTO "+table+" (bucket, id, payload) VALUES (?, ?, ?)", dynamic.Int(1), dynamic.Int(id), dynamic.Blob(payload)); err != nil {
				t.Fatal(err)
			}
		}
		row, err := v2ReadOne(ctx, session, "SELECT payload FROM "+table+" WHERE bucket = ? AND id = ?", dynamic.Int(1), dynamic.Int(1))
		if err != nil {
			t.Fatal(err)
		}
		value, err := row.Get("payload")
		if err != nil {
			t.Fatal(err)
		}
		got, err := value.Bytes()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.OwnedBytes(), payload) {
			t.Fatal("single-row helper did not retain an owned payload")
		}
		if _, err := v2ReadOne(ctx, session, "SELECT payload FROM "+table+" WHERE bucket = ?", dynamic.Int(2)); !errors.Is(err, cqlerr.ErrNoRows) {
			t.Fatalf("empty result: got %v, want ErrNoRows", err)
		}
		if _, err := v2ReadOne(ctx, session, "SELECT payload FROM "+table+" WHERE bucket = ?", dynamic.Int(1)); !errors.Is(err, errMultipleRows) {
			t.Fatalf("multiple result pages: got %v, want errMultipleRows", err)
		}
		rows, err := v2Query(ctx, session, "SELECT id FROM "+table+" WHERE bucket = ?", dynamic.Int(1))
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		count := 0
		var cur dynamic.Row
		for rows.NextInto(&cur) {
			count++
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if count != 2 {
			t.Fatalf("paged result has %d rows, want 2", count)
		}
	})

	t.Run("Identity", func(t *testing.T) {
		s, err := gocql.Connect[testDialect](context.Background(), mustBuild(t, createV2Cluster()))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		query := "SELECT release_version FROM system.local"
		if *flagDistribution == "scylla" {
			query = "SELECT version FROM system.versions WHERE key='local'"
		}
		row, err := v2ReadOne(context.Background(), s, query)
		if err != nil {
			t.Fatal(err)
		}
		value, err := row.At(0)
		if err != nil {
			t.Fatal(err)
		}
		version, err := value.Text()
		if err != nil {
			t.Fatal(err)
		}
		t.Log("ACTUAL_SERVER_VERSION", version.String(), "PROTOCOL", (*implSession.Session)(s).Data().InternalCfg.ProtoVersion)
		if !strings.HasPrefix(version.String(), strings.TrimPrefix(flagCassVersion.String(), "v")) {
			t.Fatalf("unexpected server version %q", version.String())
		}
	})

	// Verifies the retry logic deterministically (no
	// cluster required): topology-busy errors are retried until success, other
	// errors are returned immediately, and retries are bounded.
	t.Run("RetryOnTopologyBusy", func(t *testing.T) {
		t.Parallel()

		busy := errors.New("Error during truncate: exceptions::invalid_request_exception (Another global topology request is ongoing, please retry.)")
		busyTruncate := errors.New("Error during truncate: std::runtime_error (Another TRUNCATE TABLE is ongoing, please retry.)")
		other := errors.New("some other error")

		// noSleep avoids real backoff so these subtests stay fast and deterministic.
		noSleep := func(time.Duration) {}

		t.Run("retries busy then succeeds", func(t *testing.T) {
			calls := 0
			err := retryOnTopologyBusyWithSleep(func() error {
				calls++
				if calls < 3 {
					return busy
				}
				return nil
			}, noSleep)
			if err != nil {
				t.Fatalf("expected success, got %v", err)
			}
			if calls != 3 {
				t.Fatalf("expected 3 calls, got %d", calls)
			}
		})

		t.Run("does not retry non-busy errors", func(t *testing.T) {
			calls := 0
			err := retryOnTopologyBusyWithSleep(func() error {
				calls++
				return other
			}, noSleep)
			if err != other {
				t.Fatalf("expected the original error, got %v", err)
			}
			if calls != 1 {
				t.Fatalf("expected 1 call (no retry), got %d", calls)
			}
		})

		t.Run("gives up after max attempts", func(t *testing.T) {
			calls := 0
			err := retryOnTopologyBusyWithSleep(func() error {
				calls++
				return busy
			}, noSleep)
			if !isTopologyBusyErr(err) {
				t.Fatalf("expected the busy error after exhausting retries, got %v", err)
			}
			if calls != 10 {
				t.Fatalf("expected 10 attempts, got %d", calls)
			}
		})

		t.Run("isTopologyBusyErr classification", func(t *testing.T) {
			if !isTopologyBusyErr(busy) {
				t.Error("busy error should be classified as topology-busy")
			}
			if !isTopologyBusyErr(busyTruncate) {
				t.Error("busyTruncate (runtime_error TRUNCATE variant) should be classified as topology-busy")
			}
			if isTopologyBusyErr(other) {
				t.Error("other error should not be classified as topology-busy")
			}
			if isTopologyBusyErr(nil) {
				t.Error("nil should not be classified as topology-busy")
			}
		})

		t.Run("retries the TRUNCATE-variant busy error", func(t *testing.T) {
			calls := 0
			err := retryOnTopologyBusyWithSleep(func() error {
				calls++
				if calls < 2 {
					return busyTruncate
				}
				return nil
			}, noSleep)
			if err != nil {
				t.Fatalf("expected success, got %v", err)
			}
			if calls != 2 {
				t.Fatalf("expected 2 calls, got %d", calls)
			}
		})
	})
}

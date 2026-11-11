//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package integration

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestExamples(t *testing.T) {
	// Executes the actual compiled children. A killed child is
	// joined before the existing runner prefix cleanup is allowed to remove its data.
	t.Run("Ownership", func(t *testing.T) {
		if os.Getenv("GOCQL_TEST_RUN") == "" {
			t.Skip("requires locked native run")
		}
		cfg := mustBuild(t, createV2Cluster())
		if *flagProto != 4 || *flagRunSslTest {
			t.Skip("raw v4 ownership lane")
		}
		dir := t.TempDir()
		example := filepath.Join(dir, "example")
		benchmark := filepath.Join(dir, "benchmark.test")
		for _, command := range [][]string{{"build", "-o", example, "./examples/streaming"}, {"test", "-c", "-o", benchmark, "./tests/bench/e2e"}} {
			cmd := exec.Command("go", command...)
			cmd.Dir = "../.." // module root
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("build: %v\n%s", err, output)
			}
		}
		host := net.JoinHostPort(getClusterHosts()[0], "9042")
		exampleArgs := []string{"-hosts", host, "-protocol", "4"}
		if *flagRunAuthTest {
			exampleArgs = append(exampleArgs, "-username", "cassandra", "-password", "cassandra")
		}
		var killed []string
		for _, workload := range []string{"example", "benchmark"} {
			for _, failure := range []string{"error", "cancel", "pause"} {
				t.Run(workload+"/"+failure, func(t *testing.T) {
					binary, args := example, exampleArgs
					env := append(os.Environ(), "GOCQL_EXAMPLE_FAILURE="+failure)
					if workload == "benchmark" {
						binary = benchmark
						args = []string{"-update-golden", "-test.run=^$"}
						env = append(os.Environ(), "GOCQL_BENCH_FAILURE="+failure, "GOCQL_BENCH_ADDRESS="+host, "GOCQL_BENCH_CORPUS_DIR="+t.TempDir())
					}
					namespace, output, err := ownedChild(t, binary, args, env, failure == "pause", cfg)
					t.Logf("owned %s %s namespace=%s\n%s", workload, failure, namespace, output)
					if err == nil {
						t.Fatal("injected failure unexpectedly succeeded")
					}
					if failure == "pause" {
						if strings.Contains(output, "CLEANED ") {
							t.Fatal("killed child unexpectedly cleaned")
						}
						killed = append(killed, namespace)
						return
					}
					if !strings.Contains(output, "CLEANED "+namespace) {
						t.Fatal("cleanup did not complete")
					}
					if failure == "error" && !strings.Contains(output, "injected ") {
						t.Fatal("missing injected error")
					}
					if failure == "cancel" && !strings.Contains(output, "context canceled") {
						t.Fatal("missing cancellation error")
					}
					testNamespace(t, cfg, namespace, false)
				})
			}
		}
		// Both actual binaries have exited and been joined above. Exercise the same
		// cleanup implementation as the native runner, scoped to these exact children.
		cleanupNamespaces(t, killed)
		for _, namespace := range killed {
			testNamespace(t, cfg, namespace, false)
			t.Log("KILLED CHILD PREFIX CLEANUP VERIFIED", namespace)
		}
	})

	t.Run("RegenerateReplay", func(t *testing.T) {
		if os.Getenv("GOCQL_REGENERATE_BENCH") != "1" {
			t.Skip("explicit recording mode only")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, "go", "test", "-race", "-count=1", "-run", "^TestSingleConnection$/^Replay$", "./tests/bench/e2e", "-args", "-update-golden")
		cmd.Dir = "../.." // module root
		cmd.Env = append(os.Environ(), "GOCQL_BENCH_ADDRESS="+net.JoinHostPort(getClusterHosts()[0], "9042"))
		output, err := cmd.CombinedOutput()
		t.Logf("generation command: %s\n%s", strings.Join(cmd.Args, " "), output)
		if err != nil {
			t.Fatal(err)
		}
		var corpus struct {
			Namespace string `json:"namespace"`
		}
		data, err := os.ReadFile("../../tests/bench/e2e/corpus.json")
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &corpus); err != nil {
			t.Fatal(err)
		}
		testNamespace(t, mustBuild(t, createV2Cluster()), corpus.Namespace, false)
		t.Log("GENERATED CORPUS NAMESPACE ABSENT", corpus.Namespace)
	})

	// This subprocess is a normal consumer build: root _test.go aliases cannot make
	// the standalone example compile or run.
	t.Run("Streaming", func(t *testing.T) {
		cfg := mustBuild(t, createV2Cluster())
		host := getClusterHosts()[0]
		args := []string{"run", "./examples/streaming", "-hosts", net.JoinHostPort(host, strconv.Itoa(int(fixturePort()))), "-dialect", testDialectDistribution + "-v" + strconv.Itoa(testDialectProtocol)}
		switch *flagCompressTest {
		case "lz4":
			args = append(args, "-compression", "lz4")
		case "snappy":
			args = append(args, "-compression", "snappy")
		}
		if *flagRunAuthTest {
			args = append(args, "-username", "cassandra", "-password", "cassandra")
		}
		if *flagRunSslTest {
			args = append(args, "-ca", "testdata/pki/ca.crt", "-cert", "testdata/pki/gocql.crt", "-key", "testdata/pki/gocql.key", "-tls-uri", "spiffe://test.cassandra.apache.org/cassandra-gocql-driver/integrationTest/cassandra")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, "go", args...)
		cmd.Dir = "../.." // the example and its paths are relative to the module root
		output, err := cmd.CombinedOutput()
		t.Logf("command: go %s\n%s", strings.Join(args, " "), output)
		if err != nil {
			t.Fatal(err)
		}
		// Without -incremental the example buffers whole pages on multiplexed
		// connections, which carry frame compression, so it always streams.
		want := "3 rows; seeded nested storage reused; query admission and drain verified"
		for _, line := range strings.Split(string(output), "\n") {
			if strings.HasPrefix(line, "CLEANED ") {
				testNamespace(t, cfg, strings.TrimPrefix(line, "CLEANED "), false)
			}
		}
		if !strings.Contains(string(output), "CLEANED ") {
			t.Fatal("example did not confirm cleanup")
		}
		if !strings.Contains(string(output), want) {
			t.Fatalf("example did not report verified behavior: %s", output)
		}
	})
}

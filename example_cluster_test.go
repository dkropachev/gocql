//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql_test

import (
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
)

// The examples connect to localhost:9042, as a user's program would, and
// carry no Output comment, so go test only compiles them. With the
// integration tag and -examples.cluster, TestClusterExamples runs them
// against that ScyllaDB node: it forwards 127.0.0.1:9042 to the node,
// creates the tables the examples assume, runs each example and checks what
// it printed.
var flagExamplesCluster = flag.String("examples.cluster", os.Getenv("GOCQL_EXAMPLES_CLUSTER"),
	"address of a ScyllaDB node to run the cluster-backed examples against; also $GOCQL_EXAMPLES_CLUSTER")

// clusterExample is one example and what it must print.
type clusterExample struct {
	name string
	run  func()
	// setup runs before the example.
	setup []string
	// want matches the whole output, line by line.
	want []string
}

func TestClusterExamples(t *testing.T) {
	if *flagExamplesCluster == "" {
		t.Skip("-examples.cluster is not set")
	}
	node := *flagExamplesCluster
	if _, _, err := net.SplitHostPort(node); err != nil {
		node = net.JoinHostPort(node, "9042")
	}
	forwardLocalhost(t, node)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cfg, err := config.New(node).Build()
	if err != nil {
		t.Fatal(err)
	}
	s, err := gocql.Connect(ctx, cfg, gocql.ScyllaV4)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	exec := func(stmt string) {
		t.Helper()
		if _, err := s.UnpreparedExec(ctx, cql.Command(stmt), cql.EmptyParams{}); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	for _, stmt := range []string{
		"DROP KEYSPACE IF EXISTS example",
		"DROP KEYSPACE IF EXISTS app",
		// Without tablets, so the LWT examples run on every ScyllaDB version
		// in the matrix, including those without LWT on tablets.
		"CREATE KEYSPACE example WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}",
		"CREATE TABLE example.itoa (id int PRIMARY KEY, description text)",
		"CREATE TABLE example.table1 (id int PRIMARY KEY, name text, score double)",
		"CREATE TABLE example.batches (pk int, ck int, description text, PRIMARY KEY (pk, ck))",
		"CREATE TABLE example.log (line text PRIMARY KEY)",
		"CREATE TABLE example.my_lwt_batch_table (pk text, ck text, version int, value text, PRIMARY KEY (pk, ck))",
		"CREATE TABLE example.tweet (timeline text, id uuid, text text, PRIMARY KEY (timeline, id))",
	} {
		exec(stmt)
	}
	for i := range 5 {
		exec("INSERT INTO example.itoa (id, description) VALUES (" + string(rune('1'+i)) + ", 'row')")
	}
	exec("INSERT INTO example.table1 (id, name, score) VALUES (1, 'first', 1.5)")
	defer func() {
		for _, stmt := range []string{"DROP KEYSPACE IF EXISTS example", "DROP KEYSPACE IF EXISTS app"} {
			if _, err := s.UnpreparedExec(ctx, cql.Command(stmt), cql.EmptyParams{}); err != nil {
				t.Errorf("%s: %v", stmt, err)
			}
		}
	}()

	uuid := `[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`
	for _, ex := range []clusterExample{
		{name: "Paging", run: Example_paging, want: []string{"5", "1", "2", "4", "3"}},
		{name: "DynamicColumns", run: Example_dynamicColumns, want: []string{"id int", "name (text|varchar)", "score double", "1 first 1.5 "}},
		{name: "SessionExecBatch", run: ExampleSession_ExecBatch, want: []string{"1 2 1.2", "1 3 1.3", "1 4 1.4", "1 5 1.5"}},
		{name: "Errors", run: Example_errors},
		{name: "ConnectErrors", run: Example_connectErrors},
		{
			name: "SessionRunBatchConditional", run: ExampleSession_RunBatch_conditional,
			setup: []string{
				"INSERT INTO example.my_lwt_batch_table (pk, ck, version, value) VALUES ('pk1', 'ck1', 1, 'a')",
				"INSERT INTO example.my_lwt_batch_table (pk, ck, version, value) VALUES ('pk1', 'ck2', 1, 'A')",
			},
			want: []string{"applied: false", "ck1 1", "applied: false", "ck2 1"},
		},
		{
			name: "SessionRunConditionalBatch", run: ExampleSession_RunConditionalBatch,
			setup: []string{
				"UPDATE example.my_lwt_batch_table SET version = 2 WHERE pk = 'pk1' AND ck = 'ck1'",
				"UPDATE example.my_lwt_batch_table SET version = 2 WHERE pk = 'pk1' AND ck = 'ck2'",
			},
			want: []string{"applied: true"},
		},
		{name: "ParamsOf", run: ExampleParamsOf, want: []string{uuid + " hello world"}},
		{name: "PreparedQuery", run: ExamplePrepared_Query},
		{name: "SessionUnpreparedExec", run: ExampleSession_UnpreparedExec},
	} {
		t.Run(ex.name, func(t *testing.T) {
			for _, stmt := range ex.setup {
				exec(stmt)
			}
			lines := captureStdout(t, ex.run)
			if len(lines) != len(ex.want) {
				t.Fatalf("printed %d lines, want %d:\n%s", len(lines), len(ex.want), strings.Join(lines, "\n"))
			}
			for i, want := range ex.want {
				if !regexp.MustCompile("^" + want + "$").MatchString(lines[i]) {
					t.Errorf("line %d: %q does not match %q", i+1, lines[i], want)
				}
			}
		})
	}
	// ExampleSession_UnpreparedExec creates keyspace app.
	if err := s.RefreshMetadata(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Metadata().Keyspace("app"); !ok {
		t.Error("no keyspace app after ExampleSession_UnpreparedExec")
	}
}

// captureStdout runs f with os.Stdout redirected and returns the lines it
// printed. An example that fails calls log.Fatal, which ends the test
// binary with the error on stderr.
func captureStdout(t *testing.T, f func()) []string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout := os.Stdout
	os.Stdout = w
	done := make(chan string)
	go func() {
		out, _ := io.ReadAll(r)
		done <- string(out)
	}()
	defer func() { os.Stdout = stdout }()
	f()
	os.Stdout = stdout
	w.Close()
	out := strings.TrimRight(<-done, "\n")
	if out == "" {
		return nil
	}
	return strings.Split(out, "\n")
}

// forwardLocalhost forwards connections to 127.0.0.1:9042, the address the
// examples use, to node until the test ends. The driver reaches the node
// itself through the address the node advertises.
func forwardLocalhost(t *testing.T, node string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:9042")
	if err != nil {
		t.Skipf("cannot listen on the examples' address: %v", err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var conns []net.Conn
	track := func(c net.Conn) bool {
		mu.Lock()
		defer mu.Unlock()
		if conns == nil {
			return false
		}
		conns = append(conns, c)
		return true
	}
	conns = []net.Conn{}
	t.Cleanup(func() {
		l.Close()
		mu.Lock()
		for _, c := range conns {
			c.Close()
		}
		conns = nil
		mu.Unlock()
		wg.Wait()
	})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			client, err := l.Accept()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					t.Errorf("forwarder: %v", err)
				}
				return
			}
			server, err := net.Dial("tcp", node)
			if err != nil {
				client.Close()
				continue
			}
			if !track(client) || !track(server) {
				client.Close()
				server.Close()
				return
			}
			wg.Add(2)
			go func() { defer wg.Done(); io.Copy(server, client); server.Close() }()
			go func() { defer wg.Done(); io.Copy(client, server); client.Close() }()
		}
	}()
}

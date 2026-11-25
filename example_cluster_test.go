//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
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
// it printed to stdout and to the log, and what it wrote.
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
	// logs matches the lines the example itself logged, joined by newlines;
	// empty, it must log nothing. The driver's own records, which share the
	// log package's output, are left out.
	logs string
	// refuse makes connections to localhost:9042 fail while it runs.
	refuse bool
	// check runs after the example.
	check func(t *testing.T)
}

func TestClusterExamples(t *testing.T) {
	if *flagExamplesCluster == "" {
		t.Skip("-examples.cluster is not set")
	}
	node := *flagExamplesCluster
	if _, _, err := net.SplitHostPort(node); err != nil {
		node = net.JoinHostPort(node, "9042")
	}
	refuse := forwardLocalhost(t, node)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cfg, err := config.New(config.ScyllaV4, node).Build()
	if err != nil {
		t.Fatal(err)
	}
	s, err := gocql.Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	exec := func(t *testing.T, stmt string) {
		t.Helper()
		if _, err := s.UnpreparedExec(ctx, cql.Command(stmt), cql.EmptyParams{}); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	for _, stmt := range []string{
		"DROP KEYSPACE IF EXISTS example",
		"DROP KEYSPACE IF EXISTS app",
	} {
		exec(t, stmt)
	}
	// Without tablets where the version has them, so the LWT examples run
	// on versions without LWT on tablets too.
	keyspace := "CREATE KEYSPACE example WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
	if _, err := s.UnpreparedExec(ctx, cql.Command(keyspace+" AND tablets = {'enabled': false}"), cql.EmptyParams{}); err != nil {
		t.Logf("creating keyspace example without tablets: %v; creating it with the defaults", err)
		exec(t, keyspace)
	}
	for _, stmt := range []string{
		"CREATE TABLE example.itoa (id int PRIMARY KEY, description text)",
		"CREATE TABLE example.table1 (id int PRIMARY KEY, name text, score double)",
		"CREATE TABLE example.batches (pk int, ck int, description text, PRIMARY KEY (pk, ck))",
		"CREATE TABLE example.log (line text PRIMARY KEY)",
		"CREATE TABLE example.my_lwt_batch_table (pk text, ck text, version int, value text, PRIMARY KEY (pk, ck))",
		"CREATE TABLE example.tweet (timeline text, id uuid, text text, PRIMARY KEY (timeline, id))",
	} {
		exec(t, stmt)
	}
	for i := range 5 {
		exec(t, fmt.Sprintf("INSERT INTO example.itoa (id, description) VALUES (%d, 'row')", i+1))
	}
	exec(t, "INSERT INTO example.table1 (id, name, score) VALUES (1, 'first', 1.5)")
	defer func() {
		for _, stmt := range []string{"DROP KEYSPACE IF EXISTS example", "DROP KEYSPACE IF EXISTS app"} {
			if _, err := s.UnpreparedExec(ctx, cql.Command(stmt), cql.EmptyParams{}); err != nil {
				t.Errorf("%s: %v", stmt, err)
			}
		}
	}()

	uuid := `[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`
	count := func(t *testing.T, query string) int {
		t.Helper()
		var n int64
		_, err := s.UnpreparedEach(ctx, cql.Statement[cql.EmptyParams, int64]{CQL: query, Rows: cql.OneColumn(cql.Int64Codec)},
			cql.EmptyParams{}, &n, func(*int64) error { return nil })
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return int(n)
	}
	for _, ex := range []clusterExample{
		{name: "Example", run: Example, want: []string{uuid + " hello world"}},
		{name: "Paging", run: Example_paging, want: []string{"5", "1", "2", "4", "3"}},
		{name: "DynamicColumns", run: Example_dynamicColumns, want: []string{"id int", "name (text|varchar)", "score double", "1 first 1.5 "}},
		{name: "SessionExecBatch", run: ExampleSession_ExecBatch, want: []string{"1 2 1.2", "1 3 1.3", "1 4 1.4", "1 5 1.5"}},
		{
			// The insert succeeds, so the example inspects a nil error.
			name: "Errors", run: Example_errors,
			check: func(t *testing.T) {
				if n := count(t, "SELECT count(*) FROM example.log WHERE line = 'hello'"); n != 1 {
					t.Errorf("example.log holds %d hello lines", n)
				}
			},
		},
		{
			// Every connection to localhost:9042 fails, so Connect reports
			// each host.
			name: "ConnectErrors", run: Example_connectErrors, refuse: true,
			logs: `(?s).*127\.0\.0\.1:9042: .*`,
		},
		{
			name: "CASBatchRows", run: ExampleCASBatchRows,
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
		// The tweet Example wrote, then this one's.
		{name: "ParamsOf", run: ExampleParamsOf, want: []string{uuid + " hello world", uuid + " hello world"}},
		{
			name: "PreparedWorker", run: ExamplePrepared_Worker,
			check: func(t *testing.T) {
				for _, text := range []string{"first", "second", "third"} {
					if n := count(t, "SELECT count(*) FROM example.tweet WHERE timeline = 'me' AND text = '"+text+"' ALLOW FILTERING"); n != 1 {
						t.Errorf("%d tweets with text %s", n, text)
					}
				}
			},
		},
		{
			name: "SessionUnpreparedExec", run: ExampleSession_UnpreparedExec,
			check: func(t *testing.T) {
				if err := s.RefreshMetadata(ctx); err != nil {
					t.Fatal(err)
				}
				if _, ok := s.Metadata().Keyspace("app"); !ok {
					t.Error("no keyspace app")
				}
			},
		},
		{name: "SessionSubscribe", run: ExampleSession_Subscribe, want: []string{"app.events 2 columns"}},
	} {
		t.Run(ex.name, func(t *testing.T) {
			for _, stmt := range ex.setup {
				exec(t, stmt)
			}
			refuse.Store(ex.refuse)
			var logs strings.Builder
			log.SetOutput(&logs)
			lines := captureStdout(t, ex.run)
			log.SetOutput(os.Stderr)
			refuse.Store(false)
			if len(lines) != len(ex.want) {
				t.Errorf("printed %d lines, want %d:\n%s", len(lines), len(ex.want), strings.Join(lines, "\n"))
			} else {
				for i, want := range ex.want {
					if !regexp.MustCompile("^" + want + "$").MatchString(lines[i]) {
						t.Errorf("line %d: %q does not match %q", i+1, lines[i], want)
					}
				}
			}
			// The log package prefixes each record with its file
			// (common_test.go sets log.Lshortfile).
			var own []string
			for _, line := range strings.Split(logs.String(), "\n") {
				if exampleLogLine.MatchString(line) {
					own = append(own, line)
				}
			}
			if got := strings.Join(own, "\n"); ex.logs == "" && got != "" || ex.logs != "" && !regexp.MustCompile("^"+ex.logs+"$").MatchString(got) {
				t.Errorf("logged:\n%s", got)
			}
			if ex.check != nil {
				ex.check(t)
			}
		})
	}
}

// exampleLogLine matches a log record written by an example file.
var exampleLogLine = regexp.MustCompile(` example(_[a-z_]+)?_test\.go:[0-9]+: `)

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
// itself through the address the node advertises. While the returned flag
// is set, it closes each connection at once instead.
func forwardLocalhost(t *testing.T, node string) *atomic.Bool {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:9042")
	if err != nil {
		t.Fatalf("cannot listen on the examples' address: %v", err)
	}
	refuse := new(atomic.Bool)
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
			if refuse.Load() {
				client.Close()
				continue
			}
			server, err := net.DialTimeout("tcp", node, 10*time.Second)
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
	return refuse
}

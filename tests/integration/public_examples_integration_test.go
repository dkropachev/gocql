//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package integration

import (
	"bufio"
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql/dynamic"
	"github.com/scylladb/gocql/v2/routing"
)

func testNamespace(t *testing.T, cfg config.Config, namespace string, wantPresent bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := gocql.Connect[testDialect](ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.AwaitSchemaAgreement(ctx); err != nil {
		t.Fatal("namespace observation schema agreement", err)
	}
	value, err := dynamic.Text(namespace)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := v2Query(ctx, s, "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?", value)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var row dynamic.Row
	present := rows.NextInto(&row)
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if present != wantPresent {
		t.Fatalf("namespace %s present=%t want=%t", namespace, present, wantPresent)
	}
}
func ownedChild(t *testing.T, binary string, args, env []string, kill bool, cfg config.Config) (string, string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = env
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan string, 1)
	output := make(chan string, 1)
	go func() {
		var text strings.Builder
		scanner := bufio.NewScanner(pipe)
		for scanner.Scan() {
			line := scanner.Text()
			text.WriteString(line + "\n")
			if strings.HasPrefix(line, "READY ") {
				ready <- strings.TrimPrefix(line, "READY ")
			}
		}
		if err := scanner.Err(); err != nil {
			text.WriteString(err.Error())
		}
		output <- text.String()
	}()
	joined := false
	t.Cleanup(func() {
		if !joined {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	var namespace string
	select {
	case namespace = <-ready:
	case text := <-output:
		_ = cmd.Wait()
		joined = true
		t.Fatalf("child ended before creating namespace: %s", text)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if !strings.HasPrefix(namespace, os.Getenv("GOCQL_TEST_RUN")+"_") {
		t.Fatal("child escaped run prefix", namespace)
	}
	if kill {
		testNamespace(t, cfg, namespace, true)
		if err := cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
	}
	text := <-output
	err = cmd.Wait()
	joined = true
	return namespace, text, err
}

type testConviction struct {
	mu      sync.Mutex
	cause   error
	hosts   []routing.HostSnapshot
	targets []routing.HostSnapshot
	verdict bool
	resets  int
}

func (p *testConviction) AddFailure(e config.ConvictionEvent) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	host := e.Host
	if e.Shard.Set || e.Source != config.ConvictionPool {
		panic("conviction event is not a whole-host pool failure")
	}
	if !errors.Is(e.Err, p.cause) {
		panic("conviction lost original failure")
	}
	host.Tokens = append([]int64(nil), host.Tokens...)
	p.hosts = append(p.hosts, host)
	return p.verdict
}
func (p *testConviction) Reset(e config.ConvictionEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e.Host.ID.IsEmpty() || len(p.hosts) > 0 && p.hosts[0].ID != e.Host.ID {
		panic("reset lost host identity")
	}
	p.resets++
}

type noReconnect struct{}

func (noReconnect) Delay(int) (time.Duration, bool) { return 0, false }

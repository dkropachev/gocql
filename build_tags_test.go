// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"
	"time"
)

// TestBuildTagsCost builds a program that uses the driver with each of the
// measurement build tags and without them, and reads the symbols each binary
// links. A production build, without tags, must link none of the gocql_perf
// counters and none of the gocql_debug assertions, so they cost it nothing;
// each tagged build must link its own, which shows the check can fail.
func TestBuildTagsCost(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the streaming example three times")
	}
	const module = `github\.com/scylladb/gocql/v2/internal/`
	markers := map[string]*regexp.Regexp{
		"gocql_perf":  regexp.MustCompile(module + `(cqlperf\.compiles|(wire|session|routing|paging)\.perfCounts|wirecompress\.perfCalls)$`),
		"gocql_debug": regexp.MustCompile(module + `wire\.\(\*Cursor\)\.assertHead$`),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	symbols := func(tags string) []string {
		t.Helper()
		bin := filepath.Join(t.TempDir(), "streaming")
		build := exec.CommandContext(ctx, "go", "build", "-tags="+tags, "-o", bin, "./examples/streaming")
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("go build -tags=%q: %v\n%s", tags, err, out)
		}
		out, err := exec.CommandContext(ctx, "go", "tool", "nm", bin).Output()
		if err != nil {
			t.Fatalf("go tool nm: %v", err)
		}
		// Each line is "address kind name"; the name is the last field.
		return regexp.MustCompile(`(?m)\S+$`).FindAllString(string(out), -1)
	}
	linked := func(syms []string, marker *regexp.Regexp) []string {
		var found []string
		for _, s := range syms {
			if marker.MatchString(s) {
				found = append(found, s)
			}
		}
		return found
	}

	production := symbols("")
	for tag, marker := range markers {
		if found := linked(production, marker); len(found) != 0 {
			t.Errorf("a build without tags links the %s code: %q", tag, found)
		}
	}
	for tag, marker := range markers {
		t.Run(tag, func(t *testing.T) {
			if len(linked(symbols(tag), marker)) == 0 {
				t.Errorf("-tags %s links none of %s", tag, marker)
			}
		})
	}
}

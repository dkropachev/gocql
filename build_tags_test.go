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
	// Each tag's markers are one symbol per package the tag changes.
	const module = `github\.com/scylladb/gocql/v2/internal/`
	markers := map[string][]*regexp.Regexp{}
	for _, pkg := range []string{`cqlperf\.compiles`, `wire\.perfCounts`, `session\.perfCounts`, `routing\.perfCounts`, `paging\.perfCounts`, `wirecompress\.perfCalls`} {
		markers["gocql_perf"] = append(markers["gocql_perf"], regexp.MustCompile(module+pkg+`$`))
	}
	markers["gocql_debug"] = []*regexp.Regexp{regexp.MustCompile(module + `wire\.\(\*Cursor\)\.assertHead$`)}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	symbols := func(t *testing.T, tags string) []string {
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
	linked := func(syms []string, marker *regexp.Regexp) bool {
		for _, s := range syms {
			if marker.MatchString(s) {
				return true
			}
		}
		return false
	}

	production := symbols(t, "")
	for tag, list := range markers {
		for _, marker := range list {
			if linked(production, marker) {
				t.Errorf("a build without tags links the %s code %s", tag, marker)
			}
		}
	}
	for tag, list := range markers {
		t.Run(tag, func(t *testing.T) {
			syms := symbols(t, tag)
			for _, marker := range list {
				if !linked(syms, marker) {
					t.Errorf("-tags %s does not link %s", tag, marker)
				}
			}
		})
	}
}

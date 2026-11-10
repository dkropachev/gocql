// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql_test

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// These are consumer documentation queries: type aliases previously compiled
// correctly while hiding the methods from go doc.
func TestPublicMethodDocumentation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, symbol := range []string{"Session.Prepare", "Prepared.One", "Query.Close", "StreamQuery.Run", "Rows.NextInto", "Page.Retain", "RowStream.Close", "Batch.Reset"} {
		t.Run(symbol, func(t *testing.T) {
			out, err := exec.CommandContext(ctx, "go", "doc", "github.com/scylladb/gocql/v2."+symbol).CombinedOutput()
			if err != nil {
				t.Fatalf("go doc: %v\n%s", err, out)
			}
			if !strings.Contains(string(out), "func (") {
				t.Fatalf("method is not discoverable: %s", out)
			}
		})
	}
	out, err := exec.CommandContext(ctx, "go", "doc", "github.com/scylladb/gocql/v2/cql.Column").CombinedOutput()
	if err != nil || strings.Contains(string(out), "a0") || !strings.Contains(string(out), "name string") || !strings.Contains(string(out), "Column maps") {
		t.Fatalf("public field mapping documentation: %v\n%s", err, out)
	}
}

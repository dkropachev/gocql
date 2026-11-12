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
	for _, symbol := range []string{"Session.Prepare", "Session.PrepareStream", "Session.UnpreparedEach", "PreparedStream.Query", "Prepared.One", "Query.Close", "StreamQuery.Run", "Rows.NextInto", "Page.Retain", "RowStream.Close", "Batch.Reset", "CompiledOptions.Values", "config.ConfigBuilder.WithDefaultProfile", "config.ConfigBuilder.ConfigureDefaultProfile", "cql.PageState.Token"} {
		t.Run(symbol, func(t *testing.T) {
			path := "github.com/scylladb/gocql/v2." + symbol
			if pkg, rest, ok := strings.Cut(symbol, "."); ok && pkg == strings.ToLower(pkg) {
				path = "github.com/scylladb/gocql/v2/" + pkg + "." + rest
			}
			out, err := exec.CommandContext(ctx, "go", "doc", path).CombinedOutput()
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
	for _, check := range []struct{ symbol, want string }{
		{"github.com/scylladb/gocql/v2.Options.PageSize", "PageSize"},
		{"github.com/scylladb/gocql/v2/config.Profile.Consistency", "Consistency"},
		{"github.com/scylladb/gocql/v2.PrepareOptions.HostID", "HostID"},
		{"github.com/scylladb/gocql/v2.StreamOptions.Incremental", "Incremental"},
		{"github.com/scylladb/gocql/v2/config.Observer", "ObserveRequest"},
		{"github.com/scylladb/gocql/v2/metrics.Snapshot", "Requests"},
		{"github.com/scylladb/gocql/v2/cql.Statement.Rows", "Rows"},
		{"github.com/scylladb/gocql/v2/cql/stream.Statement.Rows", "Rows"},
		{"github.com/scylladb/gocql/v2/cql.Statement.BindMarkers", "BindMarkers"},
		{"github.com/scylladb/gocql/v2/cql.StructFields.Columns", "Columns"},
		{"github.com/scylladb/gocql/v2/cql.Encoder", "CheckType"},
		{"github.com/scylladb/gocql/v2/cql.Decoder", "Clone"},
		{"github.com/scylladb/gocql/v2/cql.ParamEncoder", "Compile"},
		{"github.com/scylladb/gocql/v2/cql.RowDecoder", "Compile"},
		{"github.com/scylladb/gocql/v2/cql/stream.Decoder", "Compile"},
		{"github.com/scylladb/gocql/v2/cql/stream.RowDecoder", "Compile"},
		{"github.com/scylladb/gocql/v2/cql/dynamic.Value", "Elements"},
		{"github.com/scylladb/gocql/v2/cql/dynamic.Row", "Get"},
	} {
		t.Run(check.symbol, func(t *testing.T) {
			out, err := exec.CommandContext(ctx, "go", "doc", check.symbol).CombinedOutput()
			if err != nil || !strings.Contains(string(out), check.want) {
				t.Fatalf("public contract is not discoverable: %v\n%s", err, out)
			}
		})
	}
}

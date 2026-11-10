// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerator(t *testing.T) {
	// The committed fixture output must be what cqlgen generates now; regenerate
	// with go generate ./cmd/cqlgen/internal/fixture after changing either.
	t.Run("Generate", func(t *testing.T) {
		dir := filepath.Join("internal", "fixture")
		name, got, err := generate(dir, []string{"Fixture", "Point", "StreamFixture"})
		if err != nil {
			t.Fatal(err)
		}
		if name != "fixture_cql.go" {
			t.Fatalf("output name %q", name)
		}
		want, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Fatalf("%s is stale; run go generate ./cmd/cqlgen/internal/fixture\n%s", name, got)
		}
	})

	t.Run("GenerateSource", func(t *testing.T) {
		t.Run("Vector", func(t *testing.T) {
			_, out, err := generateSource(t, "x.go", "package x\n\ntype T struct { Values []int32 `cql:\"values,codec=VectorCodec,stream=StreamVector\"` }\n", "T")
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{
				`cql.Param("values", cql.VectorCodec(cql.Int32),`,
				`cql.Column("values", cql.VectorCodec(cql.Int32),`,
				`cql.StreamColumn("values", cql.StreamVector(cql.StreamInt32),`,
			} {
				if !strings.Contains(out, want) {
					t.Errorf("missing %s in:\n%s", want, out)
				}
			}
		})

		t.Run("AliasedImportTestFile", func(t *testing.T) {
			name, out, err := generateSource(t, "model_test.go", `package model_test

import q "github.com/scylladb/gocql/v2/cql"

type Row struct {
	ID   q.UUID
	Name q.Nullable[string] `+"`cql:\"full_name\"`"+`
}
`, "Row")
			if err != nil {
				t.Fatal(err)
			}
			if name != "model_cql_test.go" {
				t.Fatalf("output name %q", name)
			}
			for _, want := range []string{
				`q "github.com/scylladb/gocql/v2/cql"`,
				`q.Param("id", q.UUIDCodec, func(v Row) q.UUID { return v.ID })`,
				`q.Column("full_name", q.NullableDecoder(q.StringCodec), func(v *Row) *q.Nullable[string] { return &v.Name })`,
			} {
				if !strings.Contains(out, want) {
					t.Errorf("missing %s in:\n%s", want, out)
				}
			}
		})

		// Embedded structs flatten like reflection flattens them: an outer field
		// hides an embedded one, and structs from other packages work.
		t.Run("Embedded", func(t *testing.T) {
			_, out, err := generateSource(t, "x.go", `package x

import "github.com/scylladb/gocql/v2/cql"

type audit struct {
	CreatedAt cql.Timestamp
	ID        int64
}

type T struct {
	ID int64
	audit
	cql.Duration
	Skipped audit `+"`cql:\"-\"`"+`
}
`, "T")
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{
				`cql.Param("id", cql.Int64, func(v T) int64 { return v.ID })`,
				`cql.Column("createdat", cql.TimestampCodec, func(v *T) *cql.Timestamp { return &v.audit.CreatedAt })`,
				`cql.Column("months", cql.Int32, func(v *T) *int32 { return &v.Duration.Months })`,
				`cql.StreamColumn("nanoseconds", cql.StreamInt64, func(v *T) *int64 { return &v.Duration.Nanoseconds })`,
			} {
				if !strings.Contains(out, want) {
					t.Errorf("missing %s in:\n%s", want, out)
				}
			}
			if strings.Contains(out, "v.audit.ID") {
				t.Errorf("embedded ID not hidden by T.ID:\n%s", out)
			}
		})

		t.Run("Errors", func(t *testing.T) {
			for want, src := range map[string]string{
				"no codec for type int":        "type T struct{ A int }",
				"is not a built-in for string": "type T struct{ A string `cql:\"a,codec=Int32\"` }",
				"no codec for type Defined":    "import \"github.com/scylladb/gocql/v2/cql\"\ntype Defined cql.UUID\ntype T struct{ A Defined }",
				"embedded pointer field E":     "type E struct{ A int32 }\ntype T struct{ *E }",
				"only the tag":                 "type E struct{ A int32 }\ntype T struct{ E `cql:\"e\"` }",
				"both map column \"a\"":        "type E struct{ A int32 }\ntype F struct{ A int32 }\ntype T struct{ E; F }",
				"both map column":              "type T struct{ A int32; B int32 `cql:\"a\"` }",
				"unknown cql tag option":       "type T struct{ A int32 `cql:\"a,omitempty\"` }",
				"generic structs":              "type T[X any] struct{ A X }",
				"is not a struct":              "type T int",
				"type T not found":             "type U struct{}",
			} {
				_, _, err := generateSource(t, "x.go", "package x\n\n"+src+"\n", "T")
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Errorf("%q: got %v", want, err)
				}
			}
		})

		// An alias maps like the type it names, as reflection sees it; a type defined
		// from it is a different type.
		t.Run("ResolvesAliases", func(t *testing.T) {
			_, out, err := generateSource(t, "x.go", `package x

import "github.com/scylladb/gocql/v2/cql"

type ID = cql.UUID
type Blobs = [][]byte

type T struct {
	A ID
	B Blobs
}
`, "T")
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{
				`cql.StreamColumn("a", cql.StreamUUID, func(v *T) *ID { return &v.A })`,
				`cql.StreamColumn("b", cql.StreamList(cql.StreamBlob), func(v *T) *Blobs { return &v.B })`,
			} {
				if !strings.Contains(out, want) {
					t.Errorf("missing %s in:\n%s", want, out)
				}
			}
			if want := `cql.Column("b", cql.ListCodec(cql.OwnedBlobCodec), func(v *T) *Blobs { return &v.B })`; !strings.Contains(out, want) {
				t.Errorf("missing %s in:\n%s", want, out)
			}
		})

		// A type only one table covers gets only that mapping.
		t.Run("StreamOnly", func(t *testing.T) {
			_, out, err := generateSource(t, "x.go", `package x

import "github.com/scylladb/gocql/v2/cql"

type T struct{ D cql.StreamDecimalValue }
`, "T")
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(out, "Params:") || !strings.Contains(out, "cql.StreamDecimal,") {
				t.Errorf("expected only a stream mapping:\n%s", out)
			}
		})
	})
}

func generateSource(t *testing.T, file, src string, types ...string) (string, string, error) {
	t.Helper()
	// Inside the module, so go list resolves the cql import; the leading
	// underscore keeps ./... patterns away from it.
	dir, err := os.MkdirTemp(".", "_gentest")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	if err := os.WriteFile(filepath.Join(dir, file), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	name, out, err := generate(dir, types)
	return name, string(out), err
}

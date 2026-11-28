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
	// with go generate ./cmd/cqlgen/internal/fixture or go test ./cmd/cqlgen
	// -update after changing either.
	t.Run("Generate", func(t *testing.T) {
		dir := filepath.Join("internal", "fixture")
		name, got, err := generateUDT(dir, []string{"Fixture", "Point", "StreamFixture", "Customer"}, []string{"Address"})
		if err != nil {
			t.Fatal(err)
		}
		if name != "fixture_cql.go" {
			t.Fatalf("output name %q", name)
		}
		if *update {
			if err := os.WriteFile(filepath.Join(dir, name), got, 0o644); err != nil {
				t.Fatal(err)
			}
			return
		}
		want, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Fatalf("%s is stale; run go generate ./cmd/cqlgen/internal/fixture or go test ./cmd/cqlgen -update\n%s", name, got)
		}
	})

	t.Run("GenerateSource", func(t *testing.T) {
		t.Run("Vector", func(t *testing.T) {
			_, out, err := generateSource(t, "x.go", "package x\n\ntype T struct { Values []int32 `cql:\"values,codec=VectorCodec,stream=Vector\"` }\n", "T")
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{
				`cql.Param("values", cql.VectorCodec(cql.Int32Codec),`,
				`cql.Column("values", cql.VectorCodec(cql.Int32Codec),`,
				`cql.StreamColumn("values", cql.StreamVector(cql.StreamInt32),`,
			} {
				if !strings.Contains(out, want) {
					t.Errorf("missing %s in:\n%s", want, out)
				}
			}
		})

		// -udt gives a type a CQLCodec method, which the other mappings
		// of the run use for fields of the type and of types derived from it.
		t.Run("UDT", func(t *testing.T) {
			dir, err := os.MkdirTemp(".", "_gentest")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.RemoveAll(dir) })
			src := "package x\n\ntype Addr struct{ Street string }\n\ntype T struct {\n\tHome Addr\n\tAll []Addr\n\tByName map[string]*Addr\n}\n"
			if err := os.WriteFile(filepath.Join(dir, "x.go"), []byte(src), 0o644); err != nil {
				t.Fatal(err)
			}
			_, got, err := generateUDT(dir, []string{"T"}, []string{"Addr"})
			if err != nil {
				t.Fatal(err)
			}
			out := string(got)
			for _, want := range []string{
				`func (Addr) CQLCodec() cql.Codec[Addr] { return cqlgenUDTAddr() }`,
				`var cqlgenUDTAddr = sync.OnceValue(func() cql.Codec[Addr] { return cql.MustUDTCodecOf[Addr]() })`,
				`func (Addr) CQLFields() cql.StructFields[Addr]`,
				`cql.Param("home", new(Addr).CQLCodec(), func(v T) Addr { return v.Home })`,
				`cql.Column("all", cql.SliceCodec(new(Addr).CQLCodec()), func(v *T) *[]Addr { return &v.All })`,
				`cql.Column("by_name", cql.MapCodec(cql.StringCodec, cql.PointerCodec(new(Addr).CQLCodec())), func(v *T) *map[string]*Addr { return &v.ByName })`,
			} {
				if !strings.Contains(out, want) {
					t.Errorf("missing %s in:\n%s", want, out)
				}
			}
		})

		t.Run("GoMap", func(t *testing.T) {
			_, out, err := generateSource(t, "x.go", "package x\n\nimport \"github.com/scylladb/gocql/v2/cql\"\n\ntype T struct { Attrs map[string]int64; Blobs map[cql.UUID][]byte }\n", "T")
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{
				`cql.Param("attrs", cql.MapCodec(cql.StringCodec, cql.Int64Codec), func(v T) map[string]int64 { return v.Attrs }),`,
				`cql.Column("blobs", cql.MapCodec(cql.UUIDCodec, cql.ByteSliceCodec), func(v *T) *map[cql.UUID][]byte { return &v.Blobs }),`,
			} {
				if !strings.Contains(out, want) {
					t.Errorf("missing %s in:\n%s", want, out)
				}
			}
			// Go maps have no streaming decoder, so only the buffered
			// mapping is generated.
			if strings.Contains(out, "StreamColumns") {
				t.Errorf("stream mapping generated for a map field:\n%s", out)
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
				`cql.Param("id", cql.Int64Codec, func(v T) int64 { return v.ID })`,
				`cql.Column("created_at", cql.TimestampCodec, func(v *T) *cql.Timestamp { return &v.audit.CreatedAt })`,
				`cql.Column("months", cql.Int32Codec, func(v *T) *int32 { return &v.Duration.Months })`,
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

		// A shallower field hides a same-depth pair declared before it, as
		// reflection does.
		t.Run("LateShallowerField", func(t *testing.T) {
			_, out, err := generateSource(t, "x.go", `package x

type A struct{ ID int64 }
type B struct{ ID int64 }

type T struct {
	A
	B
	ID int64
}
`, "T")
			if err != nil {
				t.Fatal(err)
			}
			if want := `cql.Param("id", cql.Int64Codec, func(v T) int64 { return v.ID })`; !strings.Contains(out, want) {
				t.Errorf("missing %s in:\n%s", want, out)
			}
			if strings.Contains(out, "v.A.ID") || strings.Contains(out, "v.B.ID") {
				t.Errorf("embedded ID not hidden by T.ID:\n%s", out)
			}
		})

		t.Run("Errors", func(t *testing.T) {
			for want, src := range map[string]string{
				"no codec for type uint":       "type T struct{ A uint }",
				"is not a built-in for string": "type T struct{ A string `cql:\"a,codec=Int32Codec\"` }",
				"no codec for type Defined":    "import \"github.com/scylladb/gocql/v2/cql\"\ntype Defined cql.Duration\ntype T struct{ A Defined }",
				"embedded pointer field E":     "type E struct{ A int32 }\ntype T struct{ *E }",
				"only the tag":                 "type E struct{ A int32 }\ntype T struct{ E `cql:\"e\"` }",
				"tagged unexported field":      "type T struct{ a int32 `cql:\"a\"` }",
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
				`cql.StreamColumn("b", cql.StreamSlice(cql.StreamByteSlice), func(v *T) *Blobs { return &v.B })`,
			} {
				if !strings.Contains(out, want) {
					t.Errorf("missing %s in:\n%s", want, out)
				}
			}
			if want := `cql.Column("b", cql.SliceCodec(cql.ByteSliceCodec), func(v *T) *Blobs { return &v.B })`; !strings.Contains(out, want) {
				t.Errorf("missing %s in:\n%s", want, out)
			}
		})

		// A named type without an entry of its own maps like its underlying
		// type, as reflection maps it, converting the field.
		t.Run("NamedTypes", func(t *testing.T) {
			_, out, err := generateSource(t, "x.go", `package x

import "github.com/scylladb/gocql/v2/cql"

type Status string
type Tags []string
type Count int
type Blob []byte

type T struct {
	S Status
	G Tags
	C Count
	L []Status
	P *Status
	N cql.Nullable[Status]
	M map[Status]Count
	B Blob
}
`, "T")
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{
				`cql.Param("s", cql.StringCodec, func(v T) string { return string(v.S) })`,
				`cql.Column("s", cql.StringCodec, func(v *T) *string { return (*string)(&v.S) })`,
				`cql.Param("g", cql.SliceCodec(cql.StringCodec), func(v T) []string { return []string(v.G) })`,
				`cql.Column("c", cql.IntCodec, func(v *T) *int { return (*int)(&v.C) })`,
				// Go does not convert these, so the accessors convert the pointer.
				`"unsafe"`,
				`cql.Param("l", cql.SliceCodec(cql.StringCodec), func(v T) []string { return *(*[]string)(unsafe.Pointer(&v.L)) })`,
				`cql.Column("l", cql.SliceCodec(cql.StringCodec), func(v *T) *[]string { return (*[]string)(unsafe.Pointer(&v.L)) })`,
				`cql.Column("p", cql.PointerCodec(cql.StringCodec), func(v *T) **string { return (**string)(unsafe.Pointer(&v.P)) })`,
				`cql.Column("n", cql.NullableDecoder(cql.StringCodec), func(v *T) *cql.Nullable[string] { return (*cql.Nullable[string])(unsafe.Pointer(&v.N)) })`,
				`cql.Column("m", cql.MapCodec(cql.StringCodec, cql.IntCodec), func(v *T) *map[string]int { return (*map[string]int)(unsafe.Pointer(&v.M)) })`,
				`cql.Column("b", cql.ByteSliceCodec, func(v *T) *[]byte { return (*[]byte)(&v.B) })`,
			} {
				if !strings.Contains(out, want) {
					t.Errorf("missing %s in:\n%s", want, out)
				}
			}
		})

		// A type only one table covers gets only that mapping.
		t.Run("StreamOnly", func(t *testing.T) {
			_, out, err := generateSource(t, "x.go", `package x

import "github.com/scylladb/gocql/v2/cql"

type T struct{ D cql.DecimalParts }
`, "T")
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(out, "CQLFields") || !strings.Contains(out, "cql.StreamDecimalParts,") {
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

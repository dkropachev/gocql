// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Command cqlgen generates the mappings of Go structs, so cql.ParamsOf,
// cql.RowOf and cql.StreamRowOf use generated code instead of reflecting the
// struct at startup. The mappings match what reflection builds: the same
// `cql:"name,codec=X,stream=Y"` tags, the same Go type to codec tables, and
// the same type identity, so an alias maps like the type it names. A
// derived field type, such as [][]int32 or map[string]*Address, maps with
// nested codec expressions, such as cql.SliceCodec(cql.SliceCodec(
// cql.Int32Codec)), and a type with a CQLCodec method with its codec. A
// field type with no built-in or derived codec needs a tag naming a Go
// expression, such as `cql:"location,codec=geo.PointCodec"`. The buffered mapping (ParamsOf,
// RowOf) and the streaming one (cql.StreamRowOf) are each generated when every
// field has a codec for it. Structs embedded by value are flattened as
// reflection flattens them.
//
// Usage, in the file that declares the struct:
//
//	//go:generate go run github.com/scylladb/gocql/v2/cmd/cqlgen -type=Tweet
//
// The output lands next to that file, as <file>_cql.go (or <file>_cql_test.go
// for a struct declared in a test file), as methods of each type: CQLFields
// for the buffered mapping, which cql.ParamsOf and cql.RowOf find through the
// method set, and CQLStreamColumns for the streaming one, which cql.StreamRowOf
// finds. Nothing is registered, so a package-level variable such as
// `var rows = cql.MustRowOf[Tweet]()` uses the generated mapping too.
//
// -udt=Address also gives each named struct a CQLCodec method that maps it
// as a UDT with cql.MustUDTCodecOf, built once, so every struct mapping maps
// fields of Address, []Address, *Address and map values of Address without
// a cql.WithCodec option; the other types of the run use it too:
//
//	//go:generate go run github.com/scylladb/gocql/v2/cmd/cqlgen -type=Customer -udt=Address
//
// cqlgen type-checks the package against export data from go list, so the
// package must compile.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	types := flag.String("type", "", "comma-separated struct type names")
	udts := flag.String("udt", "", "comma-separated struct type names that also map as UDTs (a CQLCodec method)")
	output := flag.String("output", "", "output file name; default <file>_cql.go next to the first type")
	flag.Parse()
	if *types == "" && *udts == "" {
		fmt.Fprintln(os.Stderr, "cqlgen: -type or -udt is required")
		flag.Usage()
		os.Exit(2)
	}
	dir := "."
	if flag.NArg() > 0 {
		dir = flag.Arg(0)
	}
	name, src, err := generateUDT(dir, splitList(*types), splitList(*udts))
	if err != nil {
		fmt.Fprintln(os.Stderr, "cqlgen:", err)
		os.Exit(1)
	}
	if *output != "" {
		name = *output
	}
	if err := os.WriteFile(filepath.Join(dir, name), src, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "cqlgen:", err)
		os.Exit(1)
	}
}

// splitList splits a comma-separated flag value, dropping empty names.
func splitList(s string) []string {
	var out []string
	for _, v := range strings.Split(s, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

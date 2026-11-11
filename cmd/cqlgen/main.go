// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Command cqlgen generates the mappings of Go structs, so cql.ParamsOf,
// cql.RowOf and stream.RowOf use generated code instead of reflecting the
// struct at startup. The mappings match what reflection builds: the same
// `cql:"name,codec=X,stream=Y"` tags, the same Go type to codec tables, and
// the same type identity, so an alias maps like the type it names. A field
// type with no built-in codec needs a tag naming a Go expression, such as
// `cql:"location,codec=geo.PointCodec"`. The buffered mapping (ParamsOf,
// RowOf) and the streaming one (stream.RowOf) are each generated when every
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
// method set, and CQLStreamColumns for the streaming one, which stream.RowOf
// finds. Nothing is registered, so a package-level variable such as
// `var rows = cql.MustRowOf[Tweet]()` uses the generated mapping too.
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
	types := flag.String("type", "", "comma-separated struct type names (required)")
	output := flag.String("output", "", "output file name; default <file>_cql.go next to the first type")
	flag.Parse()
	if *types == "" {
		fmt.Fprintln(os.Stderr, "cqlgen: -type is required")
		flag.Usage()
		os.Exit(2)
	}
	dir := "."
	if flag.NArg() > 0 {
		dir = flag.Arg(0)
	}
	name, src, err := generate(dir, strings.Split(*types, ","))
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

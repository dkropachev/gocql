// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql_test

import (
	"context"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"
)

// The package layers of docs/package-layout.md. A package may import only
// packages of its own layer or of a lower one, with two refinements: layer 1
// may import layer 0 but no other internal package, and layer 0 imports no
// public package. Code outside the library (commands, tests) may import
// anything; examples are user code and import only the public API.
const (
	layerPrimitive  = 0 // internal primitives
	layerVocabulary = 1 // public types, interfaces and thin logic
	layerEngine     = 2 // internal engine
	layerSurface    = 3 // public entry points that wire layer 1 to layer 2
	layerTool       = 4 // commands and tests outside the library: unchecked
	layerExample    = 5 // examples: user code, public packages only
)

const modulePath = "github.com/scylladb/gocql/v2"

// surfacePackages are the public layer-3 packages; every other public
// package is layer 1.
var surfacePackages = map[string]bool{
	"":         true, // the root package, gocql
	"config":   true,
	"execopt":  true,
	"cqltrace": true,
	// routingtest builds the engine's snapshots for policy tests.
	"routing/routingtest": true,
}

// primitivePackages are the internal layer-0 packages; every other internal
// package is layer 2.
var primitivePackages = map[string]bool{
	"internal/cqlperf":      true,
	"internal/crc":          true,
	"internal/cqlproto":     true,
	"internal/debounce":     true,
	"internal/eventbus":     true,
	"internal/frame":        true,
	"internal/link":         true,
	"internal/logging":      true,
	"internal/murmur":       true,
	"internal/segment":      true,
	"internal/streams":      true,
	"internal/structmap":    true,
	"internal/syncx":        true,
	"internal/wire":         true,
	"internal/wirecompress": true,
}

// layerOf returns the layer of the module package at rel, its path inside the
// module. Every module package has one, so a new package lands in a layer
// without being listed: a public one in layer 1, an internal one in layer 2.
func layerOf(rel string) int {
	switch {
	case strings.HasPrefix(rel, "cmd/"), strings.HasPrefix(rel, "tests/"), strings.HasPrefix(rel, "ci/"),
		strings.HasPrefix(rel, "docs/"), strings.HasPrefix(rel, "internal/testkit"),
		strings.HasPrefix(rel, "internal/tests"):
		return layerTool
	case strings.HasPrefix(rel, "examples/"):
		return layerExample
	case primitivePackages[rel]:
		return layerPrimitive
	case strings.HasPrefix(rel, "internal/"):
		return layerEngine
	case surfacePackages[rel]:
		return layerSurface
	}
	return layerVocabulary
}

func allowedImport(fromLayer, toLayer int) bool {
	switch fromLayer {
	case layerPrimitive:
		return toLayer == layerPrimitive
	case layerVocabulary:
		return toLayer == layerVocabulary || toLayer == layerPrimitive
	case layerEngine:
		return toLayer != layerSurface && toLayer != layerTool && toLayer != layerExample
	case layerSurface:
		return toLayer != layerTool && toLayer != layerExample
	case layerExample:
		return toLayer == layerVocabulary || toLayer == layerSurface
	}
	return true
}

// TestPackageLayers checks the imports of every package of the module
// against the layers. A package's non-test imports must follow them; its
// tests may also use test helpers. It checks the default build and the build
// under each of tagSets.
func TestPackageLayers(t *testing.T) {
	found := map[string]bool{}
	for _, tags := range append([]string{""}, tagSets...) {
		checkPackageLayers(t, tags, found)
	}
	violations := make([]string, 0, len(found))
	for v := range found {
		violations = append(violations, v)
	}
	sort.Strings(violations)
	for _, v := range violations {
		t.Errorf("layer violation: %s (see docs/package-layout.md)", v)
	}
}

func checkPackageLayers(t *testing.T, tags string, found map[string]bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, "go", "list", "-tags="+tags, "-f", "{{.ImportPath}} {{join .Imports \" \"}}", "./...").Output()
	if err != nil {
		t.Fatalf("go list -tags=%s: %v", tags, err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(line)
		from, ok := strings.CutPrefix(fields[0], modulePath)
		if !ok {
			continue
		}
		from = strings.TrimPrefix(from, "/")
		fromLayer := layerOf(from)
		for _, imp := range fields[1:] {
			to, ok := strings.CutPrefix(imp, modulePath)
			if !ok {
				continue
			}
			to = strings.TrimPrefix(to, "/")
			if !allowedImport(fromLayer, layerOf(to)) {
				found[displayPackage(from)+" -> "+displayPackage(to)] = true
			}
		}
	}
}

func displayPackage(rel string) string {
	if rel == "" {
		return "gocql"
	}
	return rel
}

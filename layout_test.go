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
// may import layer 0 and internal/link but no other internal package, and
// layer 0 imports no public package.
const (
	layerPrimitive  = 0 // internal primitives
	layerVocabulary = 1 // public types, interfaces and thin logic
	layerEngine     = 2 // internal engine
	layerSurface    = 3 // public entry points that wire layer 1 to layer 2
)

const modulePath = "github.com/scylladb/gocql/v2"

var vocabularyPackages = map[string]bool{
	"auth":        true,
	"cql":         true,
	"cql/dynamic": true,
	"cql/stream":  true,
	"cqlerr":      true,
	"endpoint":    true,
	"event":       true,
	"metadata":    true,
	"metrics":     true,
	"observe":     true,
	"retry":       true,
	"routing":     true,
}

var primitivePackages = map[string]bool{
	"internal/crc":          true,
	"internal/cqlproto":     true,
	"internal/debounce":     true,
	"internal/eventbus":     true,
	"internal/frame":        true,
	"internal/link":         true,
	"internal/logging":      true,
	"internal/lru":          true,
	"internal/murmur":       true,
	"internal/segment":      true,
	"internal/streams":      true,
	"internal/structmap":    true,
	"internal/syncx":        true,
	"internal/wire":         true,
	"internal/wirecompress": true,
}

// layerViolations lists the imports that break the layering today. Each
// entry is "importer -> imported". Remove entries as the code is fixed; the
// test fails on any violation missing here and on any entry that is no longer
// a violation.
var layerViolations = map[string]bool{
	"auth -> internal/settings":          true,
	"cqlerr -> internal/cqltype":         true,
	"cqlerr -> internal/metadata":        true,
	"cqlerr -> internal/protocol":        true,
	"cqlerr -> internal/runtimeerrors":   true,
	"cqlerr -> internal/transport":       true,
	"endpoint -> internal/endpoint":      true,
	"event -> internal/cluster":          true,
	"internal/tablets -> internal/tests": true,
	"metadata -> internal/metadata":      true,
	"metadata -> internal/routing":       true,
	"metadata -> internal/settings":      true,
	"routing -> internal/model":          true,
	"routing -> internal/routing":        true,
}

func layerOf(rel string) (int, bool) {
	switch {
	case primitivePackages[rel]:
		return layerPrimitive, true
	case vocabularyPackages[rel]:
		return layerVocabulary, true
	case strings.HasPrefix(rel, "internal/testkit"), strings.HasPrefix(rel, "internal/tests"):
		return 0, false
	case strings.HasPrefix(rel, "internal/"):
		return layerEngine, true
	case rel == "", rel == "config", rel == "exec", rel == "scylla", rel == "cqltrace":
		return layerSurface, true
	}
	return 0, false
}

func allowedImport(fromLayer, toLayer int) bool {
	switch fromLayer {
	case layerPrimitive:
		return toLayer == layerPrimitive
	case layerVocabulary:
		return toLayer == layerVocabulary || toLayer == layerPrimitive
	case layerEngine:
		return toLayer != layerSurface
	}
	return true
}

func TestPackageLayers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, "go", "list", "-f", "{{.ImportPath}} {{join .Imports \" \"}}", "./...").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	found := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(line)
		from, ok := strings.CutPrefix(fields[0], modulePath)
		if !ok {
			continue
		}
		from = strings.TrimPrefix(from, "/")
		fromLayer, ok := layerOf(from)
		if !ok {
			continue
		}
		for _, imp := range fields[1:] {
			to, ok := strings.CutPrefix(imp, modulePath)
			if !ok {
				continue
			}
			to = strings.TrimPrefix(to, "/")
			toLayer, ok := layerOf(to)
			if !ok || !allowedImport(fromLayer, toLayer) {
				found[displayPackage(from)+" -> "+displayPackage(to)] = true
			}
		}
	}
	var unexpected, stale []string
	for v := range found {
		if !layerViolations[v] {
			unexpected = append(unexpected, v)
		}
	}
	for v := range layerViolations {
		if !found[v] {
			stale = append(stale, v)
		}
	}
	sort.Strings(unexpected)
	sort.Strings(stale)
	for _, v := range unexpected {
		t.Errorf("layer violation: %s (see docs/package-layout.md)", v)
	}
	for _, v := range stale {
		t.Errorf("layerViolations entry is fixed, remove it: %s", v)
	}
}

func displayPackage(rel string) string {
	if rel == "" {
		return "gocql"
	}
	return rel
}

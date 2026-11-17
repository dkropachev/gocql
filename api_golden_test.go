// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql_test

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"go/importer"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"
)

var updateAPI = flag.Bool("update-api", false, "rewrite testdata/api.txt and testdata/internal_aliases.txt")

const (
	apiGoldenPath      = "testdata/api.txt"
	internalAliasPath  = "testdata/internal_aliases.txt"
	internalPathPrefix = modulePath + "/internal/"
)

// TestPublicAPI records every exported name of the public packages, with the
// fields and method sets of their types, in testdata/api.txt. A type alias is
// listed with what it resolves to, so a change to an internal type that a
// public alias exposes shows up here too. Any difference fails the test: the
// API changes only through a reviewed diff of that file.
//
// It also keeps testdata/internal_aliases.txt, the public names that are still
// aliases of internal types, which docs/package-layout.md forbids. The list
// may only shrink.
func TestPublicAPI(t *testing.T) {
	api, aliases := describePublicAPI(t)
	if *updateAPI {
		writeGolden(t, apiGoldenPath, api)
		writeGolden(t, internalAliasPath, aliases)
		return
	}
	compareGolden(t, apiGoldenPath, api)
	compareGolden(t, internalAliasPath, aliases)
}

func writeGolden(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func compareGolden(t *testing.T, path, got string) {
	t.Helper()
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v; run go test -run TestPublicAPI -update-api", err)
	}
	if string(want) == got {
		return
	}
	wantLines := strings.Split(string(want), "\n")
	gotLines := strings.Split(got, "\n")
	wantSet := make(map[string]bool, len(wantLines))
	for _, l := range wantLines {
		wantSet[l] = true
	}
	gotSet := make(map[string]bool, len(gotLines))
	for _, l := range gotLines {
		gotSet[l] = true
	}
	var diff strings.Builder
	for _, l := range wantLines {
		if !gotSet[l] {
			fmt.Fprintf(&diff, "- %s\n", l)
		}
	}
	for _, l := range gotLines {
		if !wantSet[l] {
			fmt.Fprintf(&diff, "+ %s\n", l)
		}
	}
	t.Fatalf("%s is out of date; review the change and run go test -run TestPublicAPI -update-api\n%s", path, diff.String())
}

type listedPackage struct {
	ImportPath string
	Export     string
}

func describePublicAPI(t *testing.T) (api, aliases string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, "go", "list", "-export", "-deps", "-json", "./...").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	exports := map[string]string{}
	var public []string
	dec := json.NewDecoder(bytes.NewReader(out))
	for {
		var p listedPackage
		if err := dec.Decode(&p); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		exports[p.ImportPath] = p.Export
		if isPublicPackage(p.ImportPath) {
			public = append(public, p.ImportPath)
		}
	}
	sort.Strings(public)
	fset := token.NewFileSet()
	imp := importer.ForCompiler(fset, "gc", func(path string) (io.ReadCloser, error) {
		return os.Open(exports[path])
	})
	var apiOut, aliasOut strings.Builder
	for _, path := range public {
		pkg, err := imp.Import(path)
		if err != nil {
			t.Fatalf("import %s: %v", path, err)
		}
		describePackage(&apiOut, &aliasOut, pkg)
	}
	return apiOut.String(), aliasOut.String()
}

func isPublicPackage(path string) bool {
	rel, ok := strings.CutPrefix(path, modulePath)
	if !ok {
		return false
	}
	rel = strings.TrimPrefix(rel, "/")
	_, layer := layerOf(rel)
	if !layer || strings.HasPrefix(rel, "internal/") {
		return false
	}
	return true
}

// apiQualifier prints module packages by their path inside the module, so an
// internal type in a public signature reads "internal/x.T".
func apiQualifier(p *types.Package) string {
	if rel, ok := strings.CutPrefix(p.Path(), modulePath); ok {
		return displayPackage(strings.TrimPrefix(rel, "/"))
	}
	return p.Path()
}

func describePackage(api, aliases *strings.Builder, pkg *types.Package) {
	name := apiQualifier(pkg)
	fmt.Fprintf(api, "package %s\n", name)
	scope := pkg.Scope()
	for _, n := range scope.Names() {
		obj := scope.Lookup(n)
		if !obj.Exported() {
			continue
		}
		switch obj := obj.(type) {
		case *types.TypeName:
			describeType(api, aliases, name, obj)
		default:
			fmt.Fprintf(api, "\t%s\n", types.ObjectString(obj, apiQualifier))
		}
	}
	api.WriteString("\n")
}

func describeType(api, aliases *strings.Builder, pkgName string, obj *types.TypeName) {
	fmt.Fprintf(api, "\t%s\n", typeHeader(obj))
	t := obj.Type()
	if obj.IsAlias() {
		target := types.Unalias(t)
		if named, ok := target.(*types.Named); ok && named.Obj().Pkg() != nil &&
			strings.HasPrefix(named.Obj().Pkg().Path(), internalPathPrefix) {
			fmt.Fprintf(aliases, "%s.%s = %s\n", pkgName, obj.Name(), types.TypeString(target, apiQualifier))
		}
		t = target
	}
	switch u := t.Underlying().(type) {
	case *types.Struct:
		for i := 0; i < u.NumFields(); i++ {
			if f := u.Field(i); f.Exported() {
				fmt.Fprintf(api, "\t\tfield %s %s\n", f.Name(), types.TypeString(f.Type(), apiQualifier))
			}
		}
	case *types.Interface:
		for i := 0; i < u.NumMethods(); i++ {
			if m := u.Method(i); m.Exported() {
				fmt.Fprintf(api, "\t\tmethod %s%s\n", m.Name(), strings.TrimPrefix(types.TypeString(m.Type(), apiQualifier), "func"))
			}
		}
		return
	}
	named, ok := t.(*types.Named)
	if !ok {
		return
	}
	ms := types.NewMethodSet(types.NewPointer(named))
	for i := 0; i < ms.Len(); i++ {
		m := ms.At(i).Obj()
		if !m.Exported() {
			continue
		}
		sig := m.Type().(*types.Signature)
		recv := "T"
		if _, ptr := sig.Recv().Type().(*types.Pointer); ptr {
			recv = "*T"
		}
		fmt.Fprintf(api, "\t\tmethod (%s) %s%s\n", recv, m.Name(), strings.TrimPrefix(types.TypeString(sig, apiQualifier), "func"))
	}
}

// typeHeader prints a type declaration without the unexported parts of its
// underlying type, so internal fields do not churn the golden file.
func typeHeader(obj *types.TypeName) string {
	name := apiQualifier(obj.Pkg()) + "." + obj.Name()
	if obj.IsAlias() {
		return "type " + name + typeParams(obj.Type()) + " = " + types.TypeString(types.Unalias(obj.Type()), apiQualifier)
	}
	var kind string
	switch u := obj.Type().Underlying().(type) {
	case *types.Struct:
		kind = "struct"
	case *types.Interface:
		kind = "interface"
		for i := 0; i < u.NumMethods(); i++ {
			if !u.Method(i).Exported() {
				kind = "interface (sealed)"
				break
			}
		}
	default:
		kind = types.TypeString(u, apiQualifier)
	}
	return "type " + name + typeParams(obj.Type()) + " " + kind
}

func typeParams(t types.Type) string {
	var params *types.TypeParamList
	switch t := t.(type) {
	case *types.Named:
		params = t.TypeParams()
	case *types.Alias:
		params = t.TypeParams()
	}
	if params.Len() == 0 {
		return ""
	}
	parts := make([]string, params.Len())
	for i := range parts {
		p := params.At(i)
		parts[i] = p.Obj().Name() + " " + types.TypeString(p.Constraint(), apiQualifier)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

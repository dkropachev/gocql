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

var updateAPI = flag.Bool("update-api", false, "rewrite testdata/api.txt")

const apiGoldenPath = "testdata/api.txt"

// TestPublicAPI records every exported name of the public packages in
// testdata/api.txt: the constants with their values, variables, functions,
// and types with their exported fields (promoted ones included) and method
// sets. A type alias is listed with what it resolves to. Any difference fails
// the test, so the API changes only through a reviewed diff of that file; run
// go test -run TestPublicAPI -update-api . after a deliberate change.
//
// It also fails, whatever the golden file says, when an internal type appears
// anywhere in the public API: in a signature, a field, or as the target of an
// alias. docs/package-layout.md forbids both.
//
// Build tags must not change the public API: the build under each of
// tagSets describes exactly the default API.
func TestPublicAPI(t *testing.T) {
	api := describePublicAPI(t, "")
	for _, line := range strings.Split(api, "\n") {
		if strings.Contains(line, "internal/") {
			t.Errorf("internal type in the public API: %s", strings.TrimSpace(line))
		}
	}
	if *updateAPI {
		if err := os.WriteFile(apiGoldenPath, []byte(api), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(apiGoldenPath)
	if err != nil {
		t.Fatalf("%v; run go test -run TestPublicAPI -update-api .", err)
	}
	if string(want) != api {
		t.Fatalf("%s is out of date; review the change and run go test -run TestPublicAPI -update-api .\n%s",
			apiGoldenPath, lineDiff(string(want), api))
	}
	for _, tags := range tagSets {
		t.Run(tags, func(t *testing.T) {
			if tagged := describePublicAPI(t, tags); tagged != api {
				t.Errorf("-tags %s changes the public API:\n%s", tags, lineDiff(api, tagged))
			}
		})
	}
}

// tagSets are the build tags the library's own files use; each must leave the
// public API and the package layers unchanged.
var tagSets = []string{"gocql_perf", "gocql_debug", "integration"}

// lineDiff lists the lines only in want (-) and only in got (+). Every line of
// the golden file is unique, as each member line names its type.
func lineDiff(want, got string) string {
	wantSet, gotSet := map[string]bool{}, map[string]bool{}
	for _, l := range strings.Split(want, "\n") {
		wantSet[l] = true
	}
	for _, l := range strings.Split(got, "\n") {
		gotSet[l] = true
	}
	var diff strings.Builder
	for _, l := range strings.Split(want, "\n") {
		if !gotSet[l] {
			fmt.Fprintf(&diff, "- %s\n", l)
		}
	}
	for _, l := range strings.Split(got, "\n") {
		if !wantSet[l] {
			fmt.Fprintf(&diff, "+ %s\n", l)
		}
	}
	return diff.String()
}

type listedPackage struct {
	ImportPath string
	Export     string
}

func describePublicAPI(t *testing.T, tags string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, "go", "list", "-tags="+tags, "-export", "-deps", "-json", "./...").Output()
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
	var api strings.Builder
	for _, path := range public {
		pkg, err := imp.Import(path)
		if err != nil {
			t.Fatalf("import %s: %v", path, err)
		}
		describePackage(&api, pkg)
	}
	return api.String()
}

// isPublicPackage reports whether path is a package of the driver's API: a
// module package of layer 1 or 3.
func isPublicPackage(path string) bool {
	rel, ok := strings.CutPrefix(path, modulePath)
	if !ok || rel != "" && !strings.HasPrefix(rel, "/") {
		return false
	}
	layer := layerOf(strings.TrimPrefix(rel, "/"))
	return layer == layerVocabulary || layer == layerSurface
}

// apiQualifier prints module packages by their path inside the module, so an
// internal type in a public signature reads "internal/x.T".
func apiQualifier(p *types.Package) string {
	if rel, ok := strings.CutPrefix(p.Path(), modulePath); ok {
		return displayPackage(strings.TrimPrefix(rel, "/"))
	}
	return p.Path()
}

func describePackage(api *strings.Builder, pkg *types.Package) {
	fmt.Fprintf(api, "package %s\n", apiQualifier(pkg))
	scope := pkg.Scope()
	for _, n := range scope.Names() {
		obj := scope.Lookup(n)
		if !obj.Exported() {
			continue
		}
		switch obj := obj.(type) {
		case *types.TypeName:
			describeType(api, obj)
		case *types.Const:
			fmt.Fprintf(api, "\t%s = %s\n", types.ObjectString(obj, apiQualifier), obj.Val().ExactString())
		default:
			fmt.Fprintf(api, "\t%s\n", types.ObjectString(obj, apiQualifier))
		}
	}
	api.WriteString("\n")
}

func describeType(api *strings.Builder, obj *types.TypeName) {
	fmt.Fprintf(api, "\t%s\n", typeHeader(obj))
	member := "\t\t" + apiQualifier(obj.Pkg()) + "." + obj.Name() + " "
	t := types.Unalias(obj.Type())
	switch u := t.Underlying().(type) {
	case *types.Struct:
		for _, f := range exportedFields(t, obj.Pkg()) {
			fmt.Fprintf(api, "%sfield %s %s\n", member, f.Name(), types.TypeString(f.Type(), apiQualifier))
		}
	case *types.Interface:
		for i := 0; i < u.NumMethods(); i++ {
			if m := u.Method(i); m.Exported() {
				fmt.Fprintf(api, "%smethod %s%s\n", member, m.Name(), strings.TrimPrefix(types.TypeString(m.Type(), apiQualifier), "func"))
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
		fmt.Fprintf(api, "%smethod (%s) %s%s\n", member, recv, m.Name(), strings.TrimPrefix(types.TypeString(sig, apiQualifier), "func"))
	}
}

// exportedFields returns the exported fields a value of t can select,
// promoted fields of embedded structs included, in declaration order.
func exportedFields(t types.Type, pkg *types.Package) []*types.Var {
	var names []string
	seen := map[string]bool{}
	var walk func(types.Type, int)
	walk = func(t types.Type, depth int) {
		if p, ok := t.(*types.Pointer); ok {
			t = p.Elem()
		}
		s, ok := t.Underlying().(*types.Struct)
		if !ok || depth > 8 {
			return
		}
		for i := 0; i < s.NumFields(); i++ {
			f := s.Field(i)
			if f.Exported() && !seen[f.Name()] {
				seen[f.Name()] = true
				names = append(names, f.Name())
			}
			if f.Embedded() {
				walk(f.Type(), depth+1)
			}
		}
	}
	walk(t, 0)
	var fields []*types.Var
	for _, n := range names {
		if v, ok := lookupField(t, pkg, n); ok {
			fields = append(fields, v)
		}
	}
	return fields
}

// lookupField resolves the field a selector name picks on t, if unambiguous.
func lookupField(t types.Type, pkg *types.Package, name string) (*types.Var, bool) {
	obj, _, _ := types.LookupFieldOrMethod(t, true, pkg, name)
	v, ok := obj.(*types.Var)
	return v, ok && v.IsField()
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

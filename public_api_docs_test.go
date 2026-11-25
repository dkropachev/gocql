// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql_test

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// These are consumer documentation queries: type aliases previously compiled
// correctly while hiding the methods from go doc.
func TestPublicMethodDocumentation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, symbol := range []string{"Session.Prepare", "Session.PrepareStream", "Session.UnpreparedEach", "PreparedStream.Worker", "Prepared.One", "Worker.Close", "StreamWorker.Run", "Rows.NextInto", "Page.Retain", "RowStream.Close", "Batch.Reset", "exec.CompiledOptions.Values", "config.ConfigBuilder.WithDefaultProfile", "config.ConfigBuilder.ConfigureDefaultProfile", "cql.PageState.Token"} {
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
		{"github.com/scylladb/gocql/v2/exec.Options.PageSize", "PageSize"},
		{"github.com/scylladb/gocql/v2/config.Profile.Consistency", "Consistency"},
		{"github.com/scylladb/gocql/v2/exec.PrepareOptions.HostID", "HostID"},
		{"github.com/scylladb/gocql/v2/exec.StreamOptions.Incremental", "Incremental"},
		{"github.com/scylladb/gocql/v2/observe.Observer", "RequestObserver"},
		{"github.com/scylladb/gocql/v2/observe.RequestObserver", "StartRequest"},
		{"github.com/scylladb/gocql/v2/observe.AttemptObserver", "StartAttempt"},
		{"github.com/scylladb/gocql/v2/cqltrace.Fetcher", "Settle"},
		{"github.com/scylladb/gocql/v2/exec.Options.CQLTrace", "server-side"},
		{"github.com/scylladb/gocql/v2/metrics.Snapshot", "Requests"},
		{"github.com/scylladb/gocql/v2/cql.Statement.Rows", "Rows"},
		{"github.com/scylladb/gocql/v2/cql.StreamStatement.Rows", "Rows"},
		{"github.com/scylladb/gocql/v2/cql.Statement.BindMarkers", "BindMarkers"},
		{"github.com/scylladb/gocql/v2/cql.StructFields.Columns", "Columns"},
		{"github.com/scylladb/gocql/v2/cql.Encoder", "CheckType"},
		{"github.com/scylladb/gocql/v2/cql.Decoder", "Clone"},
		{"github.com/scylladb/gocql/v2/cql.ParamEncoder", "Compile"},
		{"github.com/scylladb/gocql/v2/cql.RowDecoder", "Compile"},
		{"github.com/scylladb/gocql/v2/cql.StreamDecoder", "Compile"},
		{"github.com/scylladb/gocql/v2/cql.StreamRowDecoder", "Compile"},
		{"github.com/scylladb/gocql/v2/cql.Value", "Elements"},
		{"github.com/scylladb/gocql/v2/cql.DynamicRow", "Get"},
	} {
		t.Run(check.symbol, func(t *testing.T) {
			out, err := exec.CommandContext(ctx, "go", "doc", check.symbol).CombinedOutput()
			if err != nil || !strings.Contains(string(out), check.want) {
				t.Fatalf("public contract is not discoverable: %v\n%s", err, out)
			}
		})
	}
}

// TestExportedSymbolsDocumented fails for a public package without a package
// doc comment, and for each exported declaration of one without
// documentation: a top-level function, type, constant or variable, a method
// of an exported type, a method of an exported interface, and a field of an
// exported struct type or of a struct type nested in one. Embedded fields are
// exempt. A doc comment on a grouped const, var or type declaration covers
// its members. A trailing comment on the same line documents a constant, a
// variable, an interface method or a struct field, but not a function or a
// type. A comment holding only directives, such as //go:noinline or
// //nolint:errcheck, is not documentation. Test files and nested modules are not
// checked.
func TestExportedSymbolsDocumented(t *testing.T) {
	root := moduleRoot(t)
	var missing []string
	checked := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == "." {
			rel = ""
		}
		if rel != "" {
			name := d.Name()
			if name == "testdata" || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") {
				return filepath.SkipDir
			}
			if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
				return filepath.SkipDir // a nested module
			}
		}
		if layer := layerOf(rel); layer != layerVocabulary && layer != layerSurface {
			return nil
		}
		found, n := undocumentedInDir(t, root, path)
		missing = append(missing, found...)
		checked += n
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	if checked == 0 {
		t.Fatalf("no public packages found under %s", root)
	}
	sort.Strings(missing)
	for _, m := range missing {
		t.Errorf("missing doc comment: %s", m)
	}
}

// moduleRoot returns the directory holding go.mod, starting from the test's
// working directory.
func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("working directory: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the working directory")
		}
		dir = parent
	}
}

// undocumentedInDir parses the non-test Go files of dir and returns where its
// undocumented exported declarations are, and how many packages it checked.
func undocumentedInDir(t *testing.T, root, dir string) ([]string, int) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read directory %s: %v", dir, err)
	}
	fset := token.NewFileSet()
	files := map[string][]*ast.File{} // by package name
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(dir, name)
		f, err := parser.ParseFile(fset, path, nil, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		files[f.Name.Name] = append(files[f.Name.Name], f)
	}
	var missing []string
	report := func(pos token.Pos, what string) {
		p := fset.Position(pos)
		rel, err := filepath.Rel(root, p.Filename)
		if err != nil {
			rel = p.Filename
		}
		missing = append(missing, filepath.ToSlash(rel)+":"+strconv.Itoa(p.Line)+": "+what)
	}
	for pkg, pkgFiles := range files {
		hasDoc := false
		for _, f := range pkgFiles {
			hasDoc = hasDoc || documented(f.Doc)
			for _, decl := range f.Decls {
				checkDecl(decl, report)
			}
		}
		if !hasDoc {
			rel, _ := filepath.Rel(root, dir)
			missing = append(missing, filepath.ToSlash(rel)+": package "+pkg)
		}
	}
	return missing, len(files)
}

// documented reports whether a comment group holds text other than
// directives.
func documented(groups ...*ast.CommentGroup) bool {
	for _, cg := range groups {
		if cg != nil && strings.TrimSpace(cg.Text()) != "" {
			return true
		}
	}
	return false
}

// checkDecl reports each exported name decl declares without documentation,
// as TestExportedSymbolsDocumented defines it.
func checkDecl(decl ast.Decl, report func(token.Pos, string)) {
	switch d := decl.(type) {
	case *ast.FuncDecl:
		if !d.Name.IsExported() || documented(d.Doc) {
			return
		}
		if d.Recv == nil {
			report(d.Pos(), "func "+d.Name.Name)
			return
		}
		if recv := receiverName(d.Recv.List[0].Type); ast.IsExported(recv) {
			report(d.Pos(), "method "+recv+"."+d.Name.Name)
		}
	case *ast.GenDecl:
		for _, spec := range d.Specs {
			switch s := spec.(type) {
			case *ast.TypeSpec:
				if !s.Name.IsExported() {
					continue
				}
				if !documented(s.Doc, d.Doc) {
					report(s.Pos(), "type "+s.Name.Name)
				}
				switch typ := s.Type.(type) {
				case *ast.InterfaceType:
					for _, m := range typ.Methods.List {
						if _, isFunc := m.Type.(*ast.FuncType); !isFunc || documented(m.Doc, m.Comment) {
							continue
						}
						for _, n := range m.Names {
							if n.IsExported() {
								report(n.Pos(), "interface method "+s.Name.Name+"."+n.Name)
							}
						}
					}
				case *ast.StructType:
					checkFields(s.Name.Name, typ, report)
				}
			case *ast.ValueSpec:
				if documented(s.Doc, d.Doc, s.Comment) {
					continue
				}
				for _, n := range s.Names {
					if n.IsExported() {
						report(n.Pos(), d.Tok.String()+" "+n.Name)
					}
				}
			}
		}
	}
}

// checkFields reports the undocumented exported fields of the struct type
// named owner, and of the struct types nested in its fields.
func checkFields(owner string, st *ast.StructType, report func(token.Pos, string)) {
	for _, f := range st.Fields.List {
		if len(f.Names) == 0 {
			continue // embedded
		}
		for _, n := range f.Names {
			if !n.IsExported() {
				continue
			}
			if !documented(f.Doc, f.Comment) {
				report(n.Pos(), "field "+owner+"."+n.Name)
			}
			if nested := nestedStruct(f.Type); nested != nil {
				checkFields(owner+"."+n.Name, nested, report)
			}
		}
	}
}

// nestedStruct returns the struct type a field's type spells out, through
// pointers, slices, arrays and map values, or nil.
func nestedStruct(expr ast.Expr) *ast.StructType {
	for {
		switch e := expr.(type) {
		case *ast.StructType:
			return e
		case *ast.StarExpr:
			expr = e.X
		case *ast.ArrayType:
			expr = e.Elt
		case *ast.MapType:
			expr = e.Value
		default:
			return nil
		}
	}
}

// receiverName returns the name of a method receiver's type.
func receiverName(expr ast.Expr) string {
	for {
		switch e := expr.(type) {
		case *ast.StarExpr:
			expr = e.X
		case *ast.ParenExpr:
			expr = e.X
		case *ast.IndexExpr:
			expr = e.X
		case *ast.IndexListExpr:
			expr = e.X
		case *ast.Ident:
			return e.Name
		default:
			return ""
		}
	}
}

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
	for _, symbol := range []string{"Session.Prepare", "Session.PrepareStream", "Session.UnpreparedEach", "PreparedStream.Query", "Prepared.One", "Query.Close", "StreamQuery.Run", "Rows.NextInto", "Page.Retain", "RowStream.Close", "Batch.Reset", "exec.CompiledOptions.Values", "config.ConfigBuilder.WithDefaultProfile", "config.ConfigBuilder.ConfigureDefaultProfile", "cql.PageState.Token"} {
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
// doc comment, and for each exported declaration of one without a doc
// comment: a top-level function, type, constant or variable, a method of an
// exported type, and a method of an exported interface. A doc comment on a
// grouped const, var or type declaration covers its members, and a trailing
// comment documents a constant or variable. Struct fields are not checked.
// Test files are not checked.
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
		name := d.Name()
		if rel != "" && (name == "testdata" || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")) {
			return filepath.SkipDir
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
		t.Fatal(err)
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
		t.Fatal(err)
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
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	files := map[string][]*ast.File{} // by package name
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
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
			hasDoc = hasDoc || f.Doc != nil
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

func checkDecl(decl ast.Decl, report func(token.Pos, string)) {
	switch d := decl.(type) {
	case *ast.FuncDecl:
		if !d.Name.IsExported() || d.Doc != nil {
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
				if s.Doc == nil && d.Doc == nil {
					report(s.Pos(), "type "+s.Name.Name)
				}
				if it, ok := s.Type.(*ast.InterfaceType); ok {
					for _, m := range it.Methods.List {
						if _, isFunc := m.Type.(*ast.FuncType); !isFunc || m.Doc != nil || m.Comment != nil {
							continue
						}
						for _, n := range m.Names {
							if n.IsExported() {
								report(n.Pos(), "interface method "+s.Name.Name+"."+n.Name)
							}
						}
					}
				}
			case *ast.ValueSpec:
				if s.Doc != nil || d.Doc != nil || s.Comment != nil {
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

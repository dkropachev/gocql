// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestDocSnippets compiles the Go snippets of docs/source, as
// TestClusterExamples runs the examples, so that a renamed or removed symbol
// fails here instead of in a reader's editor.
//
// A snippet is usually a fragment. Its top-level declarations, the lines
// from one that starts a type or a named function at the first column to
// its closing brace, are placed at package level, and its other lines in the
// body of a function returning an error, after the imports and variables of
// snippetPrelude. Names the prose assumes go in an HTML comment directly
// above the fence, which the rendered page does not show:
//
//	<!-- snippet
//	var rows []int32
//	-->
//
// and <!-- snippet: skip --> leaves out a block that is not this driver's
// Go, such as v1 code in the migration guide. Unused variables and imports
// are not errors here: a fragment often declares a value it does not use.
func TestDocSnippets(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a package per snippet")
	}
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	docs, err := filepath.Glob(filepath.Join(root, "docs", "source", "*.md"))
	if err != nil {
		t.Fatal(err)
	}
	nested, err := filepath.Glob(filepath.Join(root, "docs", "source", "*", "*.md"))
	if err != nil {
		t.Fatal(err)
	}
	docs = append(docs, nested...)

	if err := os.MkdirAll(filepath.Join(root, "testdata"), 0o755); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(filepath.Join(root, "testdata"), "snippets-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	var packages []string
	for _, doc := range docs {
		rel, err := filepath.Rel(root, doc)
		if err != nil {
			t.Fatal(err)
		}
		text, err := os.ReadFile(doc)
		if err != nil {
			t.Fatal(err)
		}
		for i, s := range docSnippets(string(text)) {
			if s.skip {
				continue
			}
			pkg := fmt.Sprintf("s%03d", len(packages))
			if err := os.Mkdir(filepath.Join(dir, pkg), 0o755); err != nil {
				t.Fatal(err)
			}
			src := snippetSource(pkg, doc, rel, i, s)
			if err := os.WriteFile(filepath.Join(dir, pkg, "snippet.go"), []byte(src), 0o644); err != nil {
				t.Fatal(err)
			}
			packages = append(packages, "./"+filepath.ToSlash(filepath.Join(must(filepath.Rel(root, dir)), pkg)))
		}
	}
	if len(packages) < 20 {
		t.Fatalf("found only %d snippets", len(packages))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", append([]string{"build", "-o", os.DevNull, "-gcflags=-e"}, packages...)...)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err == nil {
		return
	}
	var failures []string
	for _, line := range strings.Split(string(out), "\n") {
		switch {
		case line == "", strings.HasPrefix(line, "#"),
			strings.Contains(line, "declared and not used"), strings.Contains(line, "imported and not used"):
		default:
			failures = append(failures, strings.TrimPrefix(strings.TrimPrefix(line, root+string(filepath.Separator)), "./"))
		}
	}
	if len(failures) > 0 {
		t.Fatalf("doc snippets do not compile:\n%s", strings.Join(failures, "\n"))
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// docSnippet is one Go block of a document.
type docSnippet struct {
	prelude string // declarations from the comment above the fence
	code    string
	line    int // the 1-based line of the block's first code line
	skip    bool
}

var (
	snippetFence   = regexp.MustCompile("(?ms)^```go\n(.*?)^```")
	snippetComment = regexp.MustCompile(`(?s)^<!-- snippet(: skip)?\n?(.*)-->\s*$`)
)

// docSnippets returns the Go blocks of a document with what the comment
// above each says.
func docSnippets(text string) []docSnippet {
	var out []docSnippet
	for _, m := range snippetFence.FindAllStringSubmatchIndex(text, -1) {
		s := docSnippet{code: text[m[2]:m[3]], line: strings.Count(text[:m[2]], "\n") + 1}
		before := text[:m[0]]
		if i := strings.LastIndex(before, "<!-- snippet"); i >= 0 && !strings.Contains(before[i:], "```") {
			before = before[i:]
		}
		if c := snippetComment.FindStringSubmatch(before); c != nil && strings.Count(c[2], "-->") == 0 {
			s.skip = c[1] != ""
			s.prelude = c[2]
		}
		out = append(out, s)
	}
	return out
}

// snippetTopLevel matches the first line of a top-level declaration.
var snippetTopLevel = regexp.MustCompile(`^(type |func [A-Za-z(])`)

// snippetSource returns a Go file of package pkg that holds snippet s of
// document doc, named rel in the repository, with line directives that point
// compiler errors at doc.
func snippetSource(pkg, doc, rel string, index int, s docSnippet) string {
	var decls, body strings.Builder
	lines := strings.Split(strings.TrimSuffix(s.code, "\n"), "\n")
	for i := 0; i < len(lines); i++ {
		if !snippetTopLevel.MatchString(lines[i]) {
			fmt.Fprintf(&body, "//line %s:%d\n%s\n", doc, s.line+i, lines[i])
			continue
		}
		fmt.Fprintf(&decls, "//line %s:%d\n", doc, s.line+i)
		for ; i < len(lines); i++ {
			decls.WriteString(lines[i] + "\n")
			// The declaration ends with a closing brace at the first
			// column, or on its own line when it opens no block there.
			if lines[i] == "}" || (strings.HasPrefix(lines[i], "type ") || strings.HasPrefix(lines[i], "func ")) && !strings.HasSuffix(lines[i], "{") {
				break
			}
		}
	}
	return fmt.Sprintf("// Snippet %d of %s.\n\npackage %s\n\n%s\n%s\n%s\nfunc snippet() error {\n%s\treturn nil\n}\n",
		index, rel, pkg, snippetPrelude, s.prelude, decls.String(), body.String())
}

// snippetPrelude imports the packages the snippets use and declares the
// names every document assumes.
const snippetPrelude = `import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/auth"
	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/cqltrace"
	"github.com/scylladb/gocql/v2/endpoint"
	"github.com/scylladb/gocql/v2/exec"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/observe"
	"github.com/scylladb/gocql/v2/retry"
	"github.com/scylladb/gocql/v2/routing"
)

var (
	ctx     context.Context
	session *gocql.Session
	cfg     config.Config
	err     error
)
`

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql_test

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// gridRow is a statement kind of the execution grid in docs/api-surface.md:
// the type that holds its methods, the prefix they carry and the options
// parameter each of them ends in ("" for none).
type gridRow struct {
	typ, prefix, options string
}

var gridRows = map[string]gridRow{
	"Throwaway":    {typ: "Session", prefix: "Unprepared", options: "options ...exec.Options"},
	"Worker-owned": {typ: "Query"},
	"Shared":       {typ: "Prepared", options: "options ...exec.CompiledOptions"},
}

// gridVerbs are the verbs each output shape allows, in every row.
var gridVerbs = map[string][]string{
	"None":      {"Exec"},
	"One row":   {"One"},
	"Many rows": {"Each", "All", "Run"},
	"Pages":     {"RunPage"},
}

// gridNonExecution lists the exported methods of the grid's types that are
// not grid cells. Batches follow their own rules, and the rest execute
// nothing.
var gridNonExecution = map[string][]string{
	"Session": {
		"AwaitSchemaAgreement", "Close", "Closed", "CompileOptions", "Dialect",
		"ExecBatch", "ID", "Metadata", "Metrics", "Prepare", "PrepareStream",
		"RefreshMetadata", "RunBatch", "RunConditionalBatch", "Subscribe",
		"UnpreparedAddToBatch",
	},
	"Query":    {"AddToBatch", "Close", "Routing", "Wait"},
	"Prepared": {"AddToBatch", "Columns", "Info", "Parameters", "Query"},
}

// apiMethod matches a method line of testdata/api.txt, which TestPublicAPI
// keeps equal to the compiled API.
var apiMethod = regexp.MustCompile(`^\t\tgocql\.(\w+) method \(\*?T\) (\w+)(?:\[[^\]]*\])?\((.*)$`)

// TestExecutionGrid checks the grid of docs/api-surface.md against the public
// API: every documented cell method exists, uses its column's verb and its
// row's prefix, and ends in its row's options form; every other exported
// method of Session, Query and Prepared is either in the grid or listed in
// gridNonExecution, so a new execution method fails until the grid documents
// it. The throwaway pages cell is empty by design, and the throwaway many-rows
// cell lacks Run because Run returns a handle.
func TestExecutionGrid(t *testing.T) {
	cells := parseGrid(t)
	params := map[string]string{} // "Type.Method" → parameter list
	raw, err := os.ReadFile(apiGoldenPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if m := apiMethod.FindStringSubmatch(line); m != nil {
			params[m[1]+"."+m[2]] = parameterList(m[3])
		}
	}

	documented := map[string]bool{}
	for row, cols := range cells {
		spec, ok := gridRows[row]
		if !ok {
			t.Errorf("grid row %q is not a known statement kind", row)
			continue
		}
		for col, methods := range cols {
			verbs, ok := gridVerbs[col]
			if !ok {
				t.Errorf("grid column %q is not a known output shape", col)
				continue
			}
			for _, method := range methods {
				documented[method] = true
				typ, name, _ := strings.Cut(method, ".")
				if typ != spec.typ {
					t.Errorf("%s row: %s is not a method of %s", row, method, spec.typ)
				}
				verb, ok := strings.CutPrefix(name, spec.prefix)
				if !ok || !slices.Contains(verbs, verb) {
					t.Errorf("%s/%s: %s is not one of the verbs %v with the prefix %q", row, col, method, verbs, spec.prefix)
				}
				p, ok := params[method]
				if !ok {
					t.Errorf("%s/%s: %s is documented but not in the public API", row, col, method)
					continue
				}
				if !optionsForm(p, spec.options) {
					t.Errorf("%s/%s: %s(%s) does not end in %q", row, col, method, p, spec.options)
				}
			}
		}
	}
	// The pages column of the throwaway row is empty by design, and Run is
	// absent from it because it returns a handle.
	if got := cells["Throwaway"]["Pages"]; len(got) != 0 {
		t.Errorf("throwaway pages cell holds %v; it is empty by design", got)
	}
	if slices.Contains(cells["Throwaway"]["Many rows"], "Session.UnpreparedRun") {
		t.Error("throwaway many-rows cell holds Session.UnpreparedRun; Run is absent from it by design")
	}
	// The reusable rows hold one method per consumption mode.
	for _, row := range []string{"Worker-owned", "Shared"} {
		for col, verbs := range gridVerbs {
			if len(cells[row][col]) != len(verbs) {
				t.Errorf("%s/%s holds %v, want one method for each of %v", row, col, cells[row][col], verbs)
			}
		}
	}

	for method, p := range params {
		typ, name, _ := strings.Cut(method, ".")
		nonExec, ok := gridNonExecution[typ]
		if !ok || documented[method] {
			continue
		}
		if !slices.Contains(nonExec, name) {
			t.Errorf("%s(%s) is neither a cell of the grid in docs/api-surface.md nor a known non-execution method", method, p)
		}
	}
	for typ, names := range gridNonExecution {
		for _, name := range names {
			if _, ok := params[typ+"."+name]; !ok {
				t.Errorf("gridNonExecution lists %s.%s, which the public API lacks", typ, name)
			}
		}
	}

	// Batches send with the shared form's options.
	for _, method := range []string{"Session.ExecBatch", "Session.RunBatch", "Session.RunConditionalBatch"} {
		if p := params[method]; !optionsForm(p, gridRows["Shared"].options) {
			t.Errorf("%s(%s) does not end in %q", method, p, gridRows["Shared"].options)
		}
	}
}

// parameterList returns the parameters of a signature from rest, the text
// after its opening parenthesis.
func parameterList(rest string) string {
	depth := 0
	for i, r := range rest {
		switch r {
		case '(':
			depth++
		case ')':
			if depth == 0 {
				return rest[:i]
			}
			depth--
		}
	}
	return rest
}

// optionsForm reports whether the parameter list params ends in options, or,
// with options "", takes no options at all.
func optionsForm(params, options string) bool {
	if options == "" {
		return !strings.Contains(params, "options ")
	}
	return strings.HasSuffix(params, ", "+options)
}

// parseGrid returns the cells of the table under "## The grid" in
// docs/api-surface.md: row → column → methods.
func parseGrid(t *testing.T) map[string]map[string][]string {
	t.Helper()
	raw, err := os.ReadFile("docs/api-surface.md")
	if err != nil {
		t.Fatal(err)
	}
	_, section, ok := strings.Cut(string(raw), "\n## The grid\n")
	if !ok {
		t.Fatal(`docs/api-surface.md has no "## The grid" section`)
	}
	section, _, _ = strings.Cut(section, "\n## ")
	var header []string
	cells := map[string]map[string][]string{}
	for _, line := range strings.Split(section, "\n") {
		if !strings.HasPrefix(line, "|") {
			continue
		}
		fields := strings.Split(strings.Trim(line, "|"), "|")
		for i := range fields {
			fields[i] = strings.TrimSpace(fields[i])
		}
		switch {
		case header == nil:
			header = fields
		case strings.HasPrefix(fields[0], "---"):
		default:
			if len(fields) != len(header) {
				t.Fatalf("grid row %q has %d cells, want %d", fields[0], len(fields), len(header))
			}
			row := map[string][]string{}
			for i, cell := range fields[1:] {
				var methods []string
				for _, m := range strings.Split(cell, ",") {
					if m = strings.TrimSpace(m); strings.HasPrefix(m, "`") {
						methods = append(methods, strings.Trim(m, "`"))
					}
				}
				row[header[i+1]] = methods
			}
			cells[fields[0]] = row
		}
	}
	if len(cells) != len(gridRows) {
		t.Fatalf("grid has rows %v, want %d", cells, len(gridRows))
	}
	return cells
}

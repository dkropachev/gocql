// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// runtimeModules are the only modules besides this one that the library's
// packages may import. Everything else in go.mod is for tests and tools.
var runtimeModules = []string{
	"github.com/klauspost/compress",
	"github.com/pierrec/lz4/v4",
	"golang.org/x/sync",
}

// libraryPackages returns the import paths of the module's library packages
// under tags: the public and internal packages, not tests or examples, and
// with commands the commands users run (cmd/cqlgen) too.
func libraryPackages(ctx context.Context, t *testing.T, tags string, commands bool) []string {
	t.Helper()
	out, err := exec.CommandContext(ctx, "go", "list", "-tags="+tags, "./...").Output()
	if err != nil {
		t.Fatalf("go list -tags=%s: %v", tags, err)
	}
	var pkgs []string
	for _, pkg := range strings.Fields(string(out)) {
		rel, ok := strings.CutPrefix(pkg, modulePath)
		if !ok {
			continue
		}
		rel = strings.TrimPrefix(rel, "/")
		switch layerOf(rel) {
		case layerTool:
			if !commands || !strings.HasPrefix(rel, "cmd/") {
				continue
			}
		case layerExample:
			continue
		}
		pkgs = append(pkgs, pkg)
	}
	return pkgs
}

// TestRuntimeDependencies checks that the library's packages and the
// commands, under the default build and each of tagSets, import packages
// only from the standard library, this module and runtimeModules.
func TestRuntimeDependencies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, tags := range append([]string{""}, tagSets...) {
		args := append([]string{"list", "-tags=" + tags, "-deps", "-f", "{{with .Module}}{{.Path}} {{end}}{{.ImportPath}}"}, libraryPackages(ctx, t, tags, true)...)
		out, err := exec.CommandContext(ctx, "go", args...).Output()
		if err != nil {
			t.Fatalf("go list -deps -tags=%s: %v", tags, err)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			mod, pkg, ok := strings.Cut(line, " ")
			if !ok || mod == modulePath || slices.Contains(runtimeModules, mod) {
				continue
			}
			t.Errorf("-tags=%q: library imports %s from module %s, which is not a runtime dependency", tags, pkg, mod)
		}
	}
}

// TestConsumerModuleRequirements builds a module that imports every public
// package, as a user would. go mod tidy must give it only this module and
// runtimeModules as requirements, and the consumer must build with go.sum
// holding source hashes for those modules alone, so no test-only module of
// this module's go.mod is built for it. Tidy itself still fetches the
// test-only modules and records their hashes in the consumer's go.sum, so
// that go test of this module's packages works from there; the build never
// reads them. It runs offline, against the module cache the module's own
// tests filled.
func TestConsumerModuleRequirements(t *testing.T) {
	if testing.Short() {
		t.Skip("runs go mod tidy and go build in a temporary module")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	var imports []string
	for _, pkg := range libraryPackages(ctx, t, "", false) {
		if rel := strings.TrimPrefix(strings.TrimPrefix(pkg, modulePath), "/"); !strings.HasPrefix(rel, "internal/") && rel != "internal" {
			imports = append(imports, pkg)
		}
	}
	if len(imports) < 2 {
		t.Fatalf("found only public packages %v", imports)
	}

	dir := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	goVersion, err := exec.CommandContext(ctx, "go", "list", "-m", "-f", "{{.GoVersion}}").Output()
	if err != nil {
		t.Fatal(err)
	}
	write("go.mod", fmt.Sprintf("module example.com/consumer\n\ngo %s\n\nrequire %s v2.0.0\n\nreplace %s => %s\n",
		strings.TrimSpace(string(goVersion)), modulePath, modulePath, root))
	var src strings.Builder
	src.WriteString("package main\n\nimport (\n")
	for _, pkg := range imports {
		fmt.Fprintf(&src, "\t_ %q\n", pkg)
	}
	src.WriteString(")\n\nfunc main() {}\n")
	write("main.go", src.String())

	run := func(mod string, args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, "go", args...)
		cmd.Dir = dir
		// Offline: whatever the consumer needs must already be in the module
		// cache, so nothing is fetched to make the check pass.
		cmd.Env = append(os.Environ(), "GOPROXY=off", "GOFLAGS=-mod="+mod, "GOWORK=off", "GOTOOLCHAIN=local")
		out, err := cmd.Output()
		if err != nil {
			stderr := ""
			if ee, ok := errors.AsType[*exec.ExitError](err); ok {
				stderr = string(ee.Stderr)
			}
			t.Fatalf("go %s: %v\n%s%s", strings.Join(args, " "), err, out, stderr)
		}
		return string(out)
	}
	run("mod", "mod", "tidy")

	allowed := append([]string{modulePath}, runtimeModules...)
	required := modFileRequirements(t, filepath.Join(dir, "go.mod"))
	for _, mod := range required {
		if !slices.Contains(allowed, mod) {
			t.Errorf("consumer go.mod requires %s", mod)
		}
	}
	if !slices.Contains(required, modulePath) {
		t.Errorf("consumer go.mod does not require %s: %v", modulePath, required)
	}

	// Keep only the go.mod hashes of the other modules: a build that needed
	// one of them would fail on its missing go.sum entry.
	sumPath := filepath.Join(dir, "go.sum")
	sum, err := os.ReadFile(sumPath)
	if err != nil {
		t.Fatal(err)
	}
	var kept strings.Builder
	for _, line := range strings.SplitAfter(string(sum), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && !strings.HasSuffix(fields[1], "/go.mod") && !slices.Contains(allowed, fields[0]) {
			continue
		}
		kept.WriteString(line)
	}
	if err := os.WriteFile(sumPath, []byte(kept.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	run("readonly", "build", "-o", os.DevNull, ".")
	for _, mod := range strings.Fields(run("readonly", "list", "-deps", "-f", "{{with .Module}}{{.Path}}{{end}}", ".")) {
		if mod != "example.com/consumer" && !slices.Contains(allowed, mod) {
			t.Errorf("consumer build uses module %s", mod)
		}
	}
}

// modFileRequirements returns the module paths a go.mod file requires.
func modFileRequirements(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var mods []string
	inBlock := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		switch {
		case line == "require (":
			inBlock = true
		case inBlock && line == ")":
			inBlock = false
		case inBlock && line != "" && !strings.HasPrefix(line, "//"):
			mods = append(mods, strings.Fields(line)[0])
		case strings.HasPrefix(line, "require ") && !strings.HasSuffix(line, "("):
			mods = append(mods, strings.Fields(line)[1])
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return mods
}

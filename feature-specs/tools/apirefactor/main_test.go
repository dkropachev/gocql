package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func copyFixture(t *testing.T) string {
	t.Helper()
	dst := t.TempDir()
	if err := os.CopyFS(dst, os.DirFS("testdata/fx")); err != nil {
		t.Fatal(err)
	}
	return dst
}

func goBuild(t *testing.T, dir string) {
	t.Helper()
	cmd := exec.Command("go", "vet", "./...")
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go vet: %v\n%s", err, out)
	}
}

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestUnfacade(t *testing.T) {
	dir := copyFixture(t)
	if err := run(dir, "unfacade", "example.com/fx/outer"); err != nil {
		t.Fatal(err)
	}
	goBuild(t, dir)
	if f := read(t, filepath.Join(dir, "outer/aliases_inner_facade.go")); !strings.Contains(f, "type PageData = inner.Data") || strings.Contains(f, "Codec") {
		t.Errorf("facade should keep only the embedded alias:\n%s", f)
	}
	use := read(t, filepath.Join(dir, "outer/use.go"))
	for _, want := range []string{`inner1 "example.com/fx/inner"`, "inner1.NewCodec[int](inner)", "inner1.Plain(inner1.Default", "func Local() *State { return newState() }", "type Wrap struct{ inner1.Plainish }"} {
		if !strings.Contains(use, want) {
			t.Errorf("use.go missing %q:\n%s", want, use)
		}
	}
	if c := read(t, filepath.Join(dir, "consumer/consumer.go")); !strings.Contains(c, "inner.Plain(1)") {
		t.Errorf("consumer not rewritten:\n%s", c)
	}
	// Fixed point.
	if err := run(dir, "unfacade", "example.com/fx/outer"); err != nil {
		t.Fatal(err)
	}
}

func TestUnexport(t *testing.T) {
	dir := copyFixture(t)
	if err := run(dir, "unexport", "example.com/fx/outer"); err != nil {
		t.Fatal(err)
	}
	goBuild(t, dir)
	s := read(t, filepath.Join(dir, "outer/state.go"))
	for _, want := range []string{"s.count++", "InternalTagged int", "func helper(", "func (s *State) bump()", "func InternalExported()", "func InternalSolo()"} {
		if !strings.Contains(s, want) {
			t.Errorf("state.go missing %q:\n%s", want, s)
		}
	}
}

func TestMethodize(t *testing.T) {
	dir := copyFixture(t)
	if err := run(dir, "methodize", "example.com/fx/outer"); err != nil {
		t.Fatal(err)
	}
	goBuild(t, dir)
	s := read(t, filepath.Join(dir, "outer/state.go"))
	for _, want := range []string{"func (s *State) helper() int", "s.helper()", "func InternalReport(s *State) int"} {
		if !strings.Contains(s, want) {
			t.Errorf("state.go missing %q:\n%s", want, s)
		}
	}
}

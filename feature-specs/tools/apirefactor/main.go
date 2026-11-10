// Command apirefactor rewrites internal package boundaries of the gocql module.
//
//	go run . -root ../../.. unfacade github.com/scylladb/gocql/v2/internal/cluster
//	go run . -root ../../.. unexport github.com/scylladb/gocql/v2/internal/cluster
//	go run . -root ../../.. methodize github.com/scylladb/gocql/v2/internal/cluster
//
// Every mode loads the whole module (with tests) under each tag set, refuses
// any rename it cannot prove safe (printing why on stderr), applies the rest
// in place, and is a fixed point when rerun.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"golang.org/x/tools/go/ast/astutil"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/imports"
)

// tagSets are the build configurations whose files must all stay correct.
var tagSets = []string{"", "integration gocql_debug gocql_perf", "old", "race"}

var facadeFile = regexp.MustCompile(`^aliases_.*_facade\.go$`)

func main() {
	root := flag.String("root", ".", "module root")
	flag.Parse()
	if flag.NArg() != 2 {
		fmt.Fprintln(os.Stderr, "usage: apirefactor -root DIR unfacade|unexport|methodize IMPORTPATH")
		os.Exit(2)
	}
	if err := run(*root, flag.Arg(0), flag.Arg(1)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type world struct {
	fset *token.FileSet
	pkgs []*packages.Package // module packages, every variant, every tag set
}

func run(root, mode, target string) error {
	w, err := load(root)
	if err != nil {
		return err
	}
	e := newEdits(w.fset)
	switch mode {
	case "unfacade":
		err = unfacade(w, e, target)
	case "unexport":
		err = unexport(w, e, target)
	case "methodize":
		err = methodize(w, e, target)
	default:
		err = fmt.Errorf("unknown mode %q", mode)
	}
	if err != nil {
		return err
	}
	n, err := e.apply()
	fmt.Printf("changed %d files\n", n)
	return err
}

func load(root string) (*world, error) {
	w := &world{fset: token.NewFileSet()}
	for _, tags := range tagSets {
		cfg := &packages.Config{
			Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes |
				packages.NeedTypesInfo | packages.NeedImports | packages.NeedDeps | packages.NeedModule,
			Dir: root, Tests: true, Fset: w.fset,
			BuildFlags: []string{"-tags=" + tags},
		}
		pkgs, err := packages.Load(cfg, "./...")
		if err != nil {
			return nil, err
		}
		if packages.PrintErrors(pkgs) > 0 {
			return nil, fmt.Errorf("type errors with tags %q", tags)
		}
		for _, p := range pkgs {
			if strings.HasSuffix(p.ID, ".test") || p.Types == nil {
				continue // generated test main
			}
			w.pkgs = append(w.pkgs, p)
		}
	}
	return w, nil
}

// key names an object by its declaring position, which is stable across the
// package variants and tag sets that each type-check the same file.
func (w *world) key(obj types.Object) string {
	if obj == nil || obj.Pkg() == nil || !obj.Pos().IsValid() {
		return ""
	}
	switch o := obj.(type) {
	case *types.Func:
		obj = o.Origin()
	case *types.Var:
		obj = o.Origin()
	}
	return obj.Pkg().Path() + "|" + w.fset.Position(obj.Pos()).String()
}

func (w *world) filename(f *ast.File) string { return w.fset.Position(f.Pos()).Filename }

func (w *world) variants(path string) []*packages.Package {
	var out []*packages.Package
	for _, p := range w.pkgs {
		if p.Types.Path() == path {
			out = append(out, p)
		}
	}
	return out
}

// lookup reports what name resolves to at pos in p.
func lookup(p *packages.Package, pos token.Pos, name string) types.Object {
	s := p.Types.Scope().Innermost(pos)
	if s == nil {
		return nil
	}
	_, obj := s.LookupParent(name, pos)
	return obj
}

// ---- edits ----

type span struct{ start, end int }

type edits struct {
	fset    *token.FileSet
	files   map[string]map[span]string
	remove  map[string][]span            // deleted ranges; edits inside them are dropped
	imports map[string]map[string]string // file -> path -> name
}

func newEdits(fset *token.FileSet) *edits {
	return &edits{fset: fset, files: map[string]map[span]string{}, remove: map[string][]span{}, imports: map[string]map[string]string{}}
}

func (e *edits) replace(from, to token.Pos, text string) error {
	p, q := e.fset.Position(from), e.fset.Position(to)
	m := e.files[p.Filename]
	if m == nil {
		m = map[span]string{}
		e.files[p.Filename] = m
	}
	s := span{p.Offset, q.Offset}
	if old, ok := m[s]; ok && old != text {
		return fmt.Errorf("%s: conflicting edits %q and %q", p, old, text)
	}
	m[s] = text
	return nil
}

func (e *edits) delete(from, to token.Pos) error {
	p, q := e.fset.Position(from), e.fset.Position(to)
	s := span{p.Offset, q.Offset}
	if !slices.Contains(e.remove[p.Filename], s) {
		e.remove[p.Filename] = append(e.remove[p.Filename], s)
	}
	return e.replace(from, to, "")
}

func (e *edits) addImport(file, path, name string) {
	m := e.imports[file]
	if m == nil {
		m = map[string]string{}
		e.imports[file] = m
	}
	m[path] = name
}

func (e *edits) apply() (int, error) {
	changed := 0
	for file, m := range e.files {
		src, err := os.ReadFile(file)
		if err != nil {
			return changed, err
		}
		var spans []span
		for s := range m {
			inside := false
			for _, r := range e.remove[file] {
				if s != r && s.start >= r.start && s.end <= r.end {
					inside = true
				}
			}
			if !inside {
				spans = append(spans, s)
			}
		}
		slices.SortFunc(spans, func(a, b span) int { return b.start - a.start })
		for i := 1; i < len(spans); i++ {
			if spans[i].end > spans[i-1].start {
				return changed, fmt.Errorf("%s: overlapping edits at %d", file, spans[i].start)
			}
		}
		out := slices.Clone(src)
		for _, s := range spans {
			out = slices.Concat(out[:s.start], []byte(m[s]), out[s.end:])
		}
		if out, err = fixImports(file, out, e.imports[file]); err != nil {
			return changed, err
		}
		if bytes.Equal(out, src) {
			continue
		}
		if empty, err := onlyImports(out); err != nil {
			return changed, err
		} else if empty && facadeFile.MatchString(filepath.Base(file)) {
			if err := os.Remove(file); err != nil {
				return changed, err
			}
		} else if err := os.WriteFile(file, out, 0o644); err != nil {
			return changed, err
		}
		changed++
	}
	return changed, nil
}

func fixImports(file string, src []byte, add map[string]string) ([]byte, error) {
	if len(add) > 0 {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, file, src, parser.ParseComments)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", file, err)
		}
		for path, name := range add {
			if name == filepath.Base(path) {
				name = ""
			}
			astutil.AddNamedImport(fset, f, name, path)
		}
		var buf bytes.Buffer
		if err := format.Node(&buf, fset, f); err != nil {
			return nil, err
		}
		src = buf.Bytes()
	}
	// Drops imports the edits left unused; nothing is left unresolved, so it adds none.
	return imports.Process(file, src, &imports.Options{Comments: true, TabIndent: true, TabWidth: 8})
}

func onlyImports(src []byte) (bool, error) {
	f, err := parser.ParseFile(token.NewFileSet(), "", src, parser.SkipObjectResolution)
	if err != nil {
		return false, err
	}
	for _, d := range f.Decls {
		if g, ok := d.(*ast.GenDecl); !ok || g.Tok != token.IMPORT {
			return false, nil
		}
	}
	return true, nil
}

// ---- unfacade ----

// fwd is what a facade name forwards to. local marks a target in the facade's
// own package, which uses inside that package name without a qualifier.
type fwd struct {
	path, pkgName, name string
	local               bool
}

func unfacade(w *world, e *edits, target string) error {
	fwds := map[string]fwd{}
	assigned := assignedVars(w)
	embedded := embeddedNames(w)
	for _, p := range w.variants(target) {
		for _, f := range p.Syntax {
			if !facadeFile.MatchString(filepath.Base(w.filename(f))) {
				continue
			}
			for _, d := range f.Decls {
				switch d := d.(type) {
				case *ast.GenDecl:
					if d.Tok == token.IMPORT {
						continue
					}
					for _, spec := range d.Specs {
						var name *ast.Ident
						var t *fwd
						switch s := spec.(type) {
						case *ast.TypeSpec:
							if s.Assign.IsValid() {
								name, t = s.Name, ref(p.TypesInfo, s.Type, tparams(s.TypeParams))
							}
						case *ast.ValueSpec:
							if len(s.Names) == 1 && len(s.Values) == 1 && s.Type == nil {
								name, t = s.Names[0], ref(p.TypesInfo, s.Values[0], nil)
							}
						}
						if t == nil {
							continue
						}
						k := w.key(p.TypesInfo.Defs[name])
						if assigned[k] {
							fmt.Fprintf(os.Stderr, "blocked %s: assigned or addressed\n", name.Name)
							continue
						}
						if embedded[k] && name.Name != t.name {
							fmt.Fprintf(os.Stderr, "blocked %s: embedded, and renaming it renames the field\n", name.Name)
							continue
						}
						fwds[k] = *t
						if len(d.Specs) == 1 {
							if err := e.delete(declStart(d.Doc, d.Pos()), d.End()); err != nil {
								return err
							}
						} else if err := e.delete(spec.Pos(), spec.End()); err != nil {
							return err
						}
					}
				case *ast.FuncDecl:
					t := forwardingFunc(p.TypesInfo, d)
					if t == nil {
						continue
					}
					fwds[w.key(p.TypesInfo.Defs[d.Name])] = *t
					if err := e.delete(declStart(d.Doc, d.Pos()), d.End()); err != nil {
						return err
					}
				}
			}
		}
	}
	// Collect uses per file, then choose one alias per (file, path) that
	// resolves to the package at every use.
	type use struct {
		p        *packages.Package
		from, to token.Pos
		f        fwd
	}
	uses := map[*ast.File][]use{}
	for _, p := range w.pkgs {
		for _, f := range p.Syntax {
			ast.Inspect(f, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.SelectorExpr:
					x, ok := n.X.(*ast.Ident)
					if !ok {
						return true
					}
					if _, ok := p.TypesInfo.Uses[x].(*types.PkgName); !ok {
						return true
					}
					if t, ok := fwds[w.key(p.TypesInfo.Uses[n.Sel])]; ok {
						uses[f] = append(uses[f], use{p, n.Pos(), n.End(), t})
					}
					return false
				case *ast.Ident:
					if t, ok := fwds[w.key(p.TypesInfo.Uses[n])]; ok {
						uses[f] = append(uses[f], use{p, n.Pos(), n.End(), t})
					}
				}
				return true
			})
		}
	}
	for f, us := range uses {
		names := map[string]string{}
		for _, u := range us {
			if _, ok := names[u.f.path]; ok || (u.f.local && u.p.Types.Path() == u.f.path) {
				continue
			}
			var positions []token.Pos
			for _, v := range us {
				if v.f.path == u.f.path {
					positions = append(positions, v.from)
				}
			}
			name, isNew := alias(u.p, f, u.f, positions)
			names[u.f.path] = name
			if isNew {
				e.addImport(w.filename(f), u.f.path, name)
			}
		}
		for _, u := range us {
			text := names[u.f.path] + "." + u.f.name
			if u.f.local && u.p.Types.Path() == u.f.path {
				target := u.p.Types.Scope().Lookup(u.f.name)
				if lookup(u.p, u.from, u.f.name) != target {
					return fmt.Errorf("%s: %s is shadowed here", w.fset.Position(u.from), u.f.name)
				}
				text = u.f.name
			}
			if err := e.replace(u.from, u.to, text); err != nil {
				return err
			}
		}
	}
	return nil
}

func declStart(doc *ast.CommentGroup, pos token.Pos) token.Pos {
	if doc != nil {
		return doc.Pos()
	}
	return pos
}

func tparams(l *ast.FieldList) []string {
	var out []string
	if l != nil {
		for _, f := range l.List {
			for _, n := range f.Names {
				out = append(out, n.Name)
			}
		}
	}
	return out
}

// ref recognises pkg.Name or pkg.Name[T1, …] with exactly the given type parameters.
func ref(info *types.Info, x ast.Expr, tps []string) *fwd {
	var idx []ast.Expr
	switch v := x.(type) {
	case *ast.IndexExpr:
		x, idx = v.X, []ast.Expr{v.Index}
	case *ast.IndexListExpr:
		x, idx = v.X, v.Indices
	}
	if len(idx) != len(tps) {
		return nil
	}
	for i, ix := range idx {
		if id, ok := ix.(*ast.Ident); !ok || id.Name != tps[i] {
			return nil
		}
	}
	if id, ok := x.(*ast.Ident); ok {
		obj := info.Uses[id]
		if obj == nil || obj.Pkg() == nil || obj.Parent() != obj.Pkg().Scope() {
			return nil
		}
		return &fwd{path: obj.Pkg().Path(), pkgName: obj.Pkg().Name(), name: id.Name, local: true}
	}
	sel, ok := x.(*ast.SelectorExpr)
	if !ok {
		return nil
	}
	id, ok := sel.X.(*ast.Ident)
	if !ok {
		return nil
	}
	pn, ok := info.Uses[id].(*types.PkgName)
	if !ok {
		return nil
	}
	return &fwd{path: pn.Imported().Path(), pkgName: pn.Imported().Name(), name: sel.Sel.Name}
}

// forwardingFunc recognises a wrapper whose body only calls pkg.F with its own
// parameters in order, and whose parameter types match F's (so untyped nil and
// interface conversions behave the same at every call site).
func forwardingFunc(info *types.Info, d *ast.FuncDecl) *fwd {
	if d.Recv != nil || d.Body == nil || len(d.Body.List) != 1 {
		return nil
	}
	var call *ast.CallExpr
	switch s := d.Body.List[0].(type) {
	case *ast.ReturnStmt:
		if len(s.Results) == 1 {
			call, _ = s.Results[0].(*ast.CallExpr)
		}
	case *ast.ExprStmt:
		call, _ = s.X.(*ast.CallExpr)
	}
	if call == nil {
		return nil
	}
	params := tparams(d.Type.Params)
	if len(params) != len(call.Args) {
		return nil
	}
	for i, a := range call.Args {
		if id, ok := a.(*ast.Ident); !ok || id.Name != params[i] {
			return nil
		}
	}
	wsig := info.Defs[d.Name].Type().(*types.Signature)
	if call.Ellipsis.IsValid() != wsig.Variadic() {
		return nil
	}
	t := ref(info, call.Fun, tparams(d.Type.TypeParams))
	if t == nil {
		return nil
	}
	fun := call.Fun
	if ix, ok := fun.(*ast.IndexExpr); ok {
		fun = ix.X
	} else if ix, ok := fun.(*ast.IndexListExpr); ok {
		fun = ix.X
	}
	var fid *ast.Ident
	if sel, ok := fun.(*ast.SelectorExpr); ok {
		fid = sel.Sel
	} else {
		fid = fun.(*ast.Ident)
	}
	tobj, ok := info.Uses[fid].(*types.Func)
	if !ok {
		return nil
	}
	tsig := tobj.Type().(*types.Signature)
	if tsig.Params().Len() != wsig.Params().Len() {
		return nil
	}
	for i := range wsig.Params().Len() {
		wt, tt := wsig.Params().At(i).Type(), tsig.Params().At(i).Type()
		if wsig.TypeParams().Len() == 0 {
			if !types.Identical(wt, tt) {
				return nil
			}
		} else if types.IsInterface(wt) != types.IsInterface(tt) {
			return nil
		}
	}
	return t
}

// embeddedNames returns the keys of type names used as embedded fields: the
// field takes the name of the alias, so qualifying it would rename the field.
func embeddedNames(w *world) map[string]bool {
	out := map[string]bool{}
	for _, p := range w.pkgs {
		for _, f := range p.Syntax {
			ast.Inspect(f, func(n ast.Node) bool {
				st, ok := n.(*ast.StructType)
				if !ok {
					return true
				}
				for _, fld := range st.Fields.List {
					if len(fld.Names) != 0 {
						continue
					}
					x := fld.Type
					if star, ok := x.(*ast.StarExpr); ok {
						x = star.X
					}
					switch v := x.(type) {
					case *ast.IndexExpr:
						x = v.X
					case *ast.IndexListExpr:
						x = v.X
					}
					switch v := x.(type) {
					case *ast.Ident:
						out[w.key(p.TypesInfo.Uses[v])] = true
					case *ast.SelectorExpr:
						out[w.key(p.TypesInfo.Uses[v.Sel])] = true
					}
				}
				return true
			})
		}
	}
	return out
}

// assignedVars returns the keys of package-level vars that are assigned or
// have their address taken anywhere, which a facade var must not be.
func assignedVars(w *world) map[string]bool {
	out := map[string]bool{}
	mark := func(p *packages.Package, x ast.Expr) {
		switch v := x.(type) {
		case *ast.Ident:
			out[w.key(p.TypesInfo.Uses[v])] = true
		case *ast.SelectorExpr:
			out[w.key(p.TypesInfo.Uses[v.Sel])] = true
		}
	}
	for _, p := range w.pkgs {
		for _, f := range p.Syntax {
			ast.Inspect(f, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.AssignStmt:
					for _, l := range n.Lhs {
						mark(p, l)
					}
				case *ast.IncDecStmt:
					mark(p, n.X)
				case *ast.UnaryExpr:
					if n.Op == token.AND {
						mark(p, n.X)
					}
				}
				return true
			})
		}
	}
	return out
}

// alias picks the name file f uses for path: an existing import that resolves
// at every position, or a new name that resolves to nothing at any of them.
func alias(p *packages.Package, f *ast.File, t fwd, positions []token.Pos) (string, bool) {
	ok := func(name string, want types.Object) bool {
		for _, pos := range positions {
			if got := lookup(p, pos, name); got != want {
				return false
			}
		}
		return true
	}
	for _, imp := range f.Imports {
		path, _ := strconv.Unquote(imp.Path.Value)
		if path != t.path {
			continue
		}
		if obj := p.TypesInfo.Implicits[imp]; obj == nil && imp.Name != nil {
			obj = p.TypesInfo.Defs[imp.Name]
			if obj != nil && ok(obj.Name(), obj) {
				return obj.Name(), false
			}
		} else if obj != nil && ok(obj.Name(), obj) {
			return obj.Name(), false
		}
	}
	for i := 0; ; i++ {
		name := t.pkgName
		if i > 0 {
			name += strconv.Itoa(i)
		}
		if ok(name, nil) {
			return name, true
		}
	}
}

// ---- unexport ----

type cand struct {
	obj     types.Object
	newName string
	owner   string // scope for duplicate detection: package or struct/receiver
	blocked []string
}

func unexport(w *world, e *edits, target string) error {
	ifaceMethods, embedded, tagged, fieldOwner := map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]string{}
	for _, p := range w.pkgs {
		for _, tv := range p.TypesInfo.Types {
			if it, ok := tv.Type.Underlying().(*types.Interface); ok {
				for i := range it.NumMethods() {
					ifaceMethods[it.Method(i).Name()] = true
				}
			}
		}
		for _, obj := range p.TypesInfo.Defs {
			if tn, ok := obj.(*types.TypeName); ok {
				if it, ok := tn.Type().Underlying().(*types.Interface); ok {
					for i := range it.NumMethods() {
						ifaceMethods[it.Method(i).Name()] = true
					}
				}
			}
			if v, ok := obj.(*types.Var); ok && v.Embedded() {
				embedded[v.Name()] = true
			}
		}
		for _, f := range p.Syntax {
			ast.Inspect(f, func(n ast.Node) bool {
				st, ok := n.(*ast.StructType)
				if !ok {
					return true
				}
				owner := w.fset.Position(st.Pos()).String()
				for _, fld := range st.Fields.List {
					for _, name := range fld.Names {
						k := w.key(p.TypesInfo.Defs[name])
						fieldOwner[k] = owner
						if fld.Tag != nil {
							tagged[k] = true
						}
					}
				}
				return true
			})
		}
	}
	cands := map[string]*cand{}
	for _, p := range w.variants(target) {
		for id, obj := range p.TypesInfo.Defs {
			if obj == nil || !isInternalName(obj.Name()) || strings.HasSuffix(w.fset.Position(id.Pos()).Filename, "_test.go") {
				continue
			}
			k := w.key(obj)
			if cands[k] != nil {
				continue
			}
			c := &cand{obj: obj, newName: lowerName(strings.TrimPrefix(obj.Name(), "Internal")), owner: target}
			switch o := obj.(type) {
			case *types.Var:
				if o.IsField() {
					c.owner = fieldOwner[k]
					if tagged[k] {
						c.blocked = append(c.blocked, "struct tag")
					}
					if o.Embedded() {
						c.blocked = append(c.blocked, "embedded field")
					}
				} else if o.Parent() != o.Pkg().Scope() {
					continue
				}
			case *types.Func:
				sig := o.Type().(*types.Signature)
				if recv := sig.Recv(); recv != nil {
					if types.IsInterface(recv.Type()) {
						c.blocked = append(c.blocked, "interface method")
					}
					if ifaceMethods[o.Name()] {
						c.blocked = append(c.blocked, "an interface declares this method name")
					}
					c.owner = types.TypeString(recv.Type(), nil)
					if m, _, _ := types.LookupFieldOrMethod(recv.Type(), true, o.Pkg(), c.newName); m != nil {
						c.blocked = append(c.blocked, "receiver already has "+c.newName)
					}
				}
			case *types.TypeName:
				if embedded[o.Name()] {
					c.blocked = append(c.blocked, "embedded somewhere")
				}
			case *types.Const:
			default:
				continue
			}
			if c.owner == target && obj.Parent() == obj.Pkg().Scope() {
				// Test files add declarations, so check every variant's scope.
				for _, v := range w.variants(target) {
					if v.Types.Scope().Lookup(c.newName) != nil {
						c.blocked = append(c.blocked, "package already declares "+c.newName)
						break
					}
				}
			}
			if token.IsKeyword(c.newName) || types.Universe.Lookup(c.newName) != nil {
				c.blocked = append(c.blocked, c.newName+" is predeclared")
			}
			cands[k] = c
		}
	}
	// Two candidates renamed to the same name in the same scope.
	seen := map[[2]string]*cand{}
	for _, c := range cands {
		sk := [2]string{c.owner, c.newName}
		if d := seen[sk]; d != nil {
			d.blocked = append(d.blocked, "same new name as "+c.obj.Name())
			c.blocked = append(c.blocked, "same new name as "+d.obj.Name())
		}
		seen[sk] = c
	}
	for _, p := range w.pkgs {
		for id, obj := range p.TypesInfo.Uses {
			c := cands[w.key(obj)]
			if c == nil {
				continue
			}
			if p.Types.Path() != target {
				c.blocked = append(c.blocked, "used from "+p.Types.Path())
				continue
			}
			if c.owner == target {
				if got := lookup(p, id.Pos(), c.newName); got != nil {
					c.blocked = append(c.blocked, fmt.Sprintf("%s shadowed at %s", c.newName, w.fset.Position(id.Pos())))
				}
			}
		}
		for _, sel := range p.TypesInfo.Selections {
			c := cands[w.key(sel.Obj())]
			if c == nil {
				continue
			}
			if m, _, _ := types.LookupFieldOrMethod(sel.Recv(), true, c.obj.Pkg(), c.newName); m != nil {
				c.blocked = append(c.blocked, fmt.Sprintf("%s reachable on %s", c.newName, sel.Recv()))
			}
		}
	}
	for _, c := range cands {
		if len(c.blocked) > 0 {
			fmt.Fprintf(os.Stderr, "blocked %s: %s\n", c.obj.Name(), strings.Join(slices.Compact(slices.Sorted(slices.Values(c.blocked))), "; "))
		}
	}
	for _, p := range w.variants(target) {
		for _, m := range []map[*ast.Ident]types.Object{p.TypesInfo.Defs, p.TypesInfo.Uses} {
			for id, obj := range m {
				if c := cands[w.key(obj)]; c != nil && len(c.blocked) == 0 {
					if err := e.replace(id.Pos(), id.End(), c.newName); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func isInternalName(s string) bool {
	return len(s) > len("Internal") && strings.HasPrefix(s, "Internal") && unicode.IsUpper(rune(s[len("Internal")]))
}

// lowerName lower-cases the leading initialism: Logger→logger, TCPConn→tcpConn,
// ID→id, V2Foo→v2Foo.
func lowerName(s string) string {
	r := []rune(s)
	n := 0
	for n < len(r) && unicode.IsUpper(r[n]) {
		n++
	}
	switch {
	case n == 0:
	case n == len(r) || n == 1:
		for i := range n {
			r[i] = unicode.ToLower(r[i])
		}
	default:
		for i := range n - 1 {
			r[i] = unicode.ToLower(r[i])
		}
	}
	return string(r)
}

// ---- methodize ----

// publicTypes returns the keys of named types that packages outside internal/
// (other than target) alias or embed, whose methods users can call.
func publicTypes(w *world, target string) map[string]bool {
	out := map[string]bool{}
	add := func(t types.Type) {
		if p, ok := t.(*types.Pointer); ok {
			t = p.Elem()
		}
		if n, ok := types.Unalias(t).(*types.Named); ok {
			out[w.key(n.Origin().Obj())] = true
		}
	}
	for _, p := range w.pkgs {
		if p.Types.Path() == target || strings.Contains(p.Types.Path()+"/", "/internal/") {
			continue
		}
		for _, obj := range p.TypesInfo.Defs {
			switch o := obj.(type) {
			case *types.TypeName:
				if o.IsAlias() {
					add(o.Type())
				}
			case *types.Var:
				if o.Embedded() {
					add(o.Type())
				}
			}
		}
	}
	return out
}

// legacyPrefixes are receiver names the code used before types were renamed
// (Core was Session).
var legacyPrefixes = map[string][]string{"Core": {"Session"}, "InternalClusterSettings": {"ClusterSettings"}}

func methodize(w *world, e *edits, target string) error {
	type fn struct {
		decl    *ast.FuncDecl
		p       *packages.Package
		obj     *types.Func
		recv    *types.Named
		newName string
		blocked []string
	}
	fns := map[string]*fn{}
	for _, p := range w.variants(target) {
		for _, f := range p.Syntax {
			if strings.HasSuffix(w.filename(f), "_test.go") {
				continue
			}
			for _, d := range f.Decls {
				d, ok := d.(*ast.FuncDecl)
				if !ok || d.Recv != nil || d.Type.TypeParams != nil || !isInternalName(d.Name.Name) || len(d.Type.Params.List) == 0 {
					continue
				}
				first := d.Type.Params.List[0]
				if len(first.Names) != 1 || first.Names[0].Name == "_" {
					continue
				}
				obj := p.TypesInfo.Defs[d.Name].(*types.Func)
				pt := obj.Type().(*types.Signature).Params().At(0).Type()
				if ptr, ok := pt.(*types.Pointer); ok {
					pt = ptr.Elem()
				}
				named, ok := types.Unalias(pt).(*types.Named)
				if !ok || named.Obj().Pkg() != obj.Pkg() || types.IsInterface(named) || named.TypeParams().Len() > 0 {
					continue
				}
				k := w.key(obj)
				if fns[k] != nil {
					continue
				}
				rest := strings.TrimPrefix(d.Name.Name, "Internal")
				for _, pre := range append([]string{named.Obj().Name()}, legacyPrefixes[named.Obj().Name()]...) {
					if after, ok := strings.CutPrefix(rest, pre); ok && after != "" && unicode.IsUpper(rune(after[0])) {
						rest = after
						break
					}
				}
				fns[k] = &fn{decl: d, p: p, obj: obj, recv: named, newName: rest}
			}
		}
	}
	type call struct {
		p *packages.Package
		c *ast.CallExpr
		f *fn
	}
	var calls []call
	for _, p := range w.pkgs {
		parents := map[ast.Node]ast.Node{}
		for _, f := range p.Syntax {
			var stack []ast.Node
			ast.Inspect(f, func(n ast.Node) bool {
				if n == nil {
					stack = stack[:len(stack)-1]
					return false
				}
				if len(stack) > 0 {
					parents[n] = stack[len(stack)-1]
				}
				stack = append(stack, n)
				return true
			})
		}
		{
			for id, obj := range p.TypesInfo.Uses {
				x := fns[w.key(obj)]
				if x == nil {
					continue
				}
				var fun ast.Node = id
				if sel, ok := parents[id].(*ast.SelectorExpr); ok && sel.Sel == id {
					fun = sel
				}
				c, ok := parents[fun].(*ast.CallExpr)
				if !ok || c.Fun != fun {
					x.blocked = append(x.blocked, "used as a value at "+w.fset.Position(id.Pos()).String())
					continue
				}
				if c.Ellipsis.IsValid() && len(c.Args) == 1 {
					x.blocked = append(x.blocked, "variadic receiver call")
				}
				calls = append(calls, call{p, c, x})
			}
		}
	}
	public := publicTypes(w, target)
	for _, x := range fns {
		exported := false
		for _, c := range calls {
			if c.f == x && c.p.Types.Path() != target {
				exported = true
			}
		}
		if !exported {
			x.newName = lowerName(x.newName)
		} else if public[w.key(x.recv.Obj())] {
			x.blocked = append(x.blocked, "an exported method would reach the public API through "+x.recv.Obj().Name())
		}
		if m, _, _ := types.LookupFieldOrMethod(types.NewPointer(x.recv), true, x.obj.Pkg(), x.newName); m != nil {
			x.blocked = append(x.blocked, "receiver already has "+x.newName)
		}
		if token.IsKeyword(x.newName) {
			x.blocked = append(x.blocked, x.newName+" is a keyword")
		}
	}
	// A call whose receiver argument contains another candidate call is left
	// for the next run, so that edits never overlap.
	for _, c := range calls {
		ast.Inspect(c.c.Args[0], func(n ast.Node) bool {
			if inner, ok := n.(*ast.CallExpr); ok {
				for _, d := range calls {
					if d.c == inner && len(d.f.blocked) == 0 {
						c.f.blocked = append(c.f.blocked, "nested call; rerun")
					}
				}
			}
			return true
		})
	}
	for _, x := range fns {
		if len(x.blocked) > 0 {
			fmt.Fprintf(os.Stderr, "blocked %s: %s\n", x.decl.Name.Name, strings.Join(slices.Compact(slices.Sorted(slices.Values(x.blocked))), "; "))
		}
	}
	src := func(from, to token.Pos) string {
		pos := w.fset.Position(from)
		b, _ := os.ReadFile(pos.Filename)
		return string(b[pos.Offset:w.fset.Position(to).Offset])
	}
	for _, x := range fns {
		if len(x.blocked) > 0 {
			continue
		}
		d := x.decl
		params := d.Type.Params
		first := params.List[0]
		if err := e.replace(d.Type.Func, params.Opening+1, "func ("+src(first.Pos(), first.End())+") "+x.newName+"("); err != nil {
			return err
		}
		end := first.End()
		if len(params.List) > 1 {
			end = params.List[1].Pos()
		}
		if err := e.replace(first.Pos(), end, ""); err != nil {
			return err
		}
	}
	for _, c := range calls {
		if len(c.f.blocked) > 0 {
			continue
		}
		recv := src(c.c.Args[0].Pos(), c.c.Args[0].End())
		switch c.c.Args[0].(type) {
		case *ast.Ident, *ast.SelectorExpr, *ast.CallExpr, *ast.IndexExpr, *ast.ParenExpr:
		default:
			recv = "(" + recv + ")"
		}
		if err := e.replace(c.c.Pos(), c.c.Lparen+1, recv+"."+c.f.newName+"("); err != nil {
			return err
		}
		end := c.c.Args[0].End()
		if len(c.c.Args) > 1 {
			end = c.c.Args[1].Pos()
		}
		if err := e.replace(c.c.Args[0].Pos(), end, ""); err != nil {
			return err
		}
	}
	return nil
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package main

import (
	"fmt"
	"go/ast"
	"go/types"
	"strconv"

	"github.com/scylladb/gocql/v2/internal/structmap"
)

// sequenceTags are the `codec=` tags that choose the collection kind of the
// outermost slice or array, as package cql's derivation reads them.
var sequenceTags = map[string]string{"ListCodec": "ListCodec", "SetCodec": "SetCodec", "VectorCodec": "VectorCodec"}

// derivedSrc spells a field type's derived codec: codec is the Codec
// expression, empty for a type that only binds parameters, and enc and dec
// the Encoder and Decoder expressions a field uses, which for Nullable are
// NullableEncoder and NullableDecoder. canonical is the Go type the
// expressions code, with every leaf replaced by its table type.
type derivedSrc struct {
	canonical       types.Type
	codec, enc, dec string
}

// typeDeriver derives the codec source of one field's type, applying the
// rules of package cql's reflective derivation (cql/derive.go).
type typeDeriver struct {
	g       *generator
	leafTag string // a leaf codec name from the field's `codec=` tag
	tagUsed bool
}

// derive returns the codec source of buffered field type typ with `codec=`
// tag tag, which names a built-in codec or is empty, or false when typ has
// no derived codec.
func (g *generator) derive(typ types.Type, tag string) (derivedSrc, bool, error) {
	d := &typeDeriver{g: g}
	forced := ""
	if name, ok := sequenceTags[tag]; ok {
		forced, d.tagUsed = name, true
	} else {
		d.leafTag = tag
	}
	out, err := d.derive(typ, forced, true)
	if err != nil {
		return derivedSrc{}, false, err
	}
	if out.enc == "" {
		return derivedSrc{}, false, nil
	}
	if tag != "" && !d.tagUsed {
		return derivedSrc{}, false, fmt.Errorf("codec %q is not a built-in for %s", tag, types.TypeString(typ, nil))
	}
	return out, true, nil
}

// leaf returns typ's built-in leaf: the one the tag names, or, unless tagged,
// the default.
func (d *typeDeriver) leaf(typ types.Type, tagged bool) (derivedSrc, bool) {
	var candidates []builtinType
	for _, b := range d.g.builtins {
		if types.Identical(typ, b.typ) {
			candidates = append(candidates, b)
		}
	}
	if len(candidates) == 0 {
		for _, b := range d.g.builtins {
			// A predeclared leaf, such as string, or an unnamed one, such
			// as []byte, as package cql matches them.
			if _, named := b.typ.(*types.Named); !named && sameLayout(typ, b.typ) {
				candidates = append(candidates, b)
			}
		}
	}
	if len(candidates) == 0 {
		if a, ok := typ.Underlying().(*types.Array); ok && a.Len() == 16 && types.Identical(a.Elem().Underlying(), types.Typ[types.Uint8]) {
			for _, b := range d.g.builtins {
				if b.TypeSrc == "cql.UUID" {
					candidates = append(candidates, b)
				}
			}
		}
	}
	if len(candidates) == 0 {
		return derivedSrc{}, false
	}
	table := make([]structmap.BuiltinFieldType, len(candidates))
	for i, c := range candidates {
		table[i] = c.BuiltinFieldType
	}
	pick := func(i int) derivedSrc {
		src := d.g.qualify(candidates[i].EncoderSrc)
		return derivedSrc{codec: src, enc: src, dec: src, canonical: candidates[i].typ}
	}
	if d.leafTag != "" {
		if i, ok := structmap.SelectBuiltin(table, d.leafTag); ok {
			d.tagUsed = true
			return pick(i), true
		}
	}
	if tagged {
		return derivedSrc{}, false
	}
	i, _ := structmap.SelectBuiltin(table, "")
	return pick(i), true
}

// cqlCodec reports whether typ has a CQLCodec method, or gets one from this
// run (-udt), and spells the call.
func (d *typeDeriver) cqlCodec(typ types.Type) (derivedSrc, bool) {
	named, ok := types.Unalias(typ).(*types.Named)
	if !ok {
		return derivedSrc{}, false
	}
	has := named.Obj().Pkg() == d.g.pkg && d.g.udts[named.Obj().Name()]
	if !has {
		obj, _, _ := types.LookupFieldOrMethod(types.NewPointer(named), true, named.Obj().Pkg(), "CQLCodec")
		fn, ok := obj.(*types.Func)
		if !ok {
			return derivedSrc{}, false
		}
		sig := fn.Signature()
		if sig.Params().Len() != 0 || sig.Results().Len() != 1 {
			return derivedSrc{}, false
		}
		res, ok := types.Unalias(sig.Results().At(0).Type()).(*types.Named)
		if !ok || res.Obj().Pkg() == nil || res.Obj().Pkg().Path() != cqlPath || res.Obj().Name() != "Codec" || res.TypeArgs().Len() != 1 || !types.Identical(res.TypeArgs().At(0), named) {
			return derivedSrc{}, false
		}
	}
	src := "new(" + types.TypeString(named, d.g.qualifier) + ").CQLCodec()"
	return derivedSrc{codec: src, enc: src, dec: src, canonical: named}, true
}

// cqlGeneric returns the type arguments of typ when it instantiates the cql
// generic type name, such as Nullable.
func cqlGeneric(typ types.Type, name string) (*types.TypeList, bool) {
	named, ok := types.Unalias(typ).(*types.Named)
	if !ok || named.TypeArgs().Len() == 0 {
		return nil, false
	}
	obj := named.Origin().Obj()
	if obj.Pkg() == nil || obj.Pkg().Path() != cqlPath || obj.Name() != name {
		return nil, false
	}
	return named.TypeArgs(), true
}

// instantiate returns the cql generic type name instantiated with args.
func (d *typeDeriver) instantiate(name string, args ...types.Type) (types.Type, error) {
	generic := d.g.cqlPkg.Scope().Lookup(name)
	if generic == nil {
		return nil, fmt.Errorf("cql.%s not found", name)
	}
	return types.Instantiate(nil, generic.Type(), args, false)
}

func (d *typeDeriver) derive(typ types.Type, forced string, top bool) (derivedSrc, error) {
	typ = types.Unalias(typ)
	if forced == "" {
		if out, ok := d.leaf(typ, true); ok {
			return out, nil
		}
		for _, b := range d.g.builtins {
			if types.Identical(typ, b.typ) {
				out, _ := d.leaf(typ, false)
				return out, nil
			}
		}
		if out, ok := d.cqlCodec(typ); ok {
			return out, nil
		}
		if out, ok := d.leaf(typ, false); ok {
			return out, nil
		}
	}
	none := func() (derivedSrc, error) {
		return derivedSrc{}, fmt.Errorf("no codec for type %s; add a `codec=` tag or a CQLCodec method", types.TypeString(typ, nil))
	}
	elemCodec := func(e derivedSrc) (string, error) {
		if e.codec == "" {
			return "", fmt.Errorf("type %s: BindValue maps only a field's own type", types.TypeString(typ, nil))
		}
		return e.codec, nil
	}
	cql := d.g.cqlName
	switch u := typ.Underlying().(type) {
	case *types.Slice:
		elem, err := d.derive(u.Elem(), "", false)
		if err != nil {
			return derivedSrc{}, err
		}
		ec, err := elemCodec(elem)
		if err != nil {
			return derivedSrc{}, err
		}
		ctor := forced
		if ctor == "" {
			ctor = "SliceCodec"
		}
		src := cql + "." + ctor + "(" + ec + ")"
		return derivedSrc{codec: src, enc: src, dec: src, canonical: types.NewSlice(elem.canonical)}, nil
	case *types.Array:
		elem, err := d.derive(u.Elem(), "", false)
		if err != nil {
			return derivedSrc{}, err
		}
		ec, err := elemCodec(elem)
		if err != nil {
			return derivedSrc{}, err
		}
		ctor := forced
		if ctor == "" {
			ctor = "SliceCodec"
		}
		canonical := types.NewArray(elem.canonical, u.Len())
		arr := types.TypeString(canonical, d.g.qualifier)
		sl := types.TypeString(types.NewSlice(elem.canonical), d.g.qualifier)
		n := strconv.FormatInt(u.Len(), 10)
		d.g.imports["fmt"] = importRef{path: "fmt", pkgName: "fmt"}
		src := fmt.Sprintf("%s.ConvertCodec(%s.%s(%s), func(a %s) (%s, error) { return a[:], nil }, func(s %s) (a %s, err error) { if len(s) != %s { return a, fmt.Errorf(\"gocql: %%d elements, %s needs %s\", len(s)) }; return %s(s), nil })",
			cql, cql, ctor, ec, arr, sl, sl, arr, n, arr, n, conversionType(arr))
		return derivedSrc{codec: src, enc: src, dec: src, canonical: canonical}, nil
	}
	if forced != "" {
		return derivedSrc{}, fmt.Errorf("codec %s needs a slice or array, not %s", forced, types.TypeString(typ, nil))
	}
	switch u := typ.Underlying().(type) {
	case *types.Pointer:
		elem, err := d.derive(u.Elem(), "", false)
		if err != nil {
			return derivedSrc{}, err
		}
		ec, err := elemCodec(elem)
		if err != nil {
			return derivedSrc{}, err
		}
		src := cql + ".PointerCodec(" + ec + ")"
		return derivedSrc{codec: src, enc: src, dec: src, canonical: types.NewPointer(elem.canonical)}, nil
	case *types.Map:
		key, err := d.derive(u.Key(), "", false)
		if err != nil {
			return derivedSrc{}, fmt.Errorf("map key: %w", err)
		}
		value, err := d.derive(u.Elem(), "", false)
		if err != nil {
			return derivedSrc{}, fmt.Errorf("map value: %w", err)
		}
		kc, err := elemCodec(key)
		if err != nil {
			return derivedSrc{}, err
		}
		vc, err := elemCodec(value)
		if err != nil {
			return derivedSrc{}, err
		}
		src := cql + ".MapCodec(" + kc + ", " + vc + ")"
		return derivedSrc{codec: src, enc: src, dec: src, canonical: types.NewMap(key.canonical, value.canonical)}, nil
	}
	if args, ok := cqlGeneric(typ, "Nullable"); ok {
		elem, err := d.derive(args.At(0), "", false)
		if err != nil {
			return derivedSrc{}, err
		}
		ec, err := elemCodec(elem)
		if err != nil {
			return derivedSrc{}, err
		}
		canonical, err := d.instantiate("Nullable", elem.canonical)
		if err != nil {
			return derivedSrc{}, err
		}
		return derivedSrc{
			codec: cql + ".NullableCodec(" + ec + ")", enc: cql + ".NullableEncoder(" + elem.enc + ")",
			dec: cql + ".NullableDecoder(" + elem.dec + ")", canonical: canonical,
		}, nil
	}
	if args, ok := cqlGeneric(typ, "BindValue"); ok {
		if !top {
			return derivedSrc{}, fmt.Errorf("type %s: BindValue maps only a field's own type", types.TypeString(typ, nil))
		}
		elem, err := d.derive(args.At(0), "", false)
		if err != nil {
			return derivedSrc{}, err
		}
		canonical, err := d.instantiate("BindValue", elem.canonical)
		if err != nil {
			return derivedSrc{}, err
		}
		return derivedSrc{enc: cql + ".BindEncoder(" + elem.enc + ")", canonical: canonical}, nil
	}
	if args, ok := cqlGeneric(typ, "Pair"); ok {
		a, err := d.derive(args.At(0), "", false)
		if err != nil {
			return derivedSrc{}, err
		}
		b, err := d.derive(args.At(1), "", false)
		if err != nil {
			return derivedSrc{}, err
		}
		ac, err := elemCodec(a)
		if err != nil {
			return derivedSrc{}, err
		}
		bc, err := elemCodec(b)
		if err != nil {
			return derivedSrc{}, err
		}
		canonical, err := d.instantiate("Pair", a.canonical, b.canonical)
		if err != nil {
			return derivedSrc{}, err
		}
		src := cql + ".PairCodec(" + ac + ", " + bc + ")"
		return derivedSrc{codec: src, enc: src, dec: src, canonical: canonical}, nil
	}
	return none()
}

// bufferedCodec picks a field's buffered codec source: a built-in named by
// the tag, else the tag's own Go expression, else the derived default.
// convert is how the accessors convert the field to the type the codec maps,
// when they differ.
func (g *generator) bufferedCodec(file *ast.File, typ types.Type, tag string) (enc, dec string, convert conversion, found bool, err error) {
	typ = types.Unalias(typ)
	if tag == "" || g.names[tag] {
		out, ok, err := g.derive(typ, tag)
		if err != nil {
			if tag != "" {
				return "", "", conversion{}, false, err
			}
			return "", "", conversion{}, false, nil
		}
		if !ok {
			return "", "", conversion{}, false, nil
		}
		if !types.Identical(typ, out.canonical) {
			convert = conversion{src: types.TypeString(out.canonical, g.qualifier), unsafe: !types.Identical(typ.Underlying(), out.canonical)}
		}
		return out.enc, out.dec, convert, true, nil
	}
	if file == nil {
		return "", "", conversion{}, false, fmt.Errorf("codec %q: a codec expression on a field of a struct from another package cannot be resolved here; use WithCodec or generate in that package", tag)
	}
	src, err := g.expression(file, tag, "codec")
	if err != nil {
		return "", "", conversion{}, false, err
	}
	return src, src, conversion{}, true, nil
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package builtins

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/structmap"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// reflectedBuffered and reflectedStreamed have the fields and tags of
// Buffered and Streamed but not their generated methods, so cql maps them
// by reflection.
type (
	reflectedBuffered Buffered
	reflectedBound    Bound
	reflectedStreamed Streamed
)

// scalarTypes returns every scalar column type. candidateTypes tries each of
// them for a leaf, and builds the column types of a derived field from them:
// a list, set or vector of its element's, a map of its key's and value's, a
// tuple of a Pair's members'.
func scalarTypes(t *testing.T) []cql.DataType {
	t.Helper()
	var out []cql.DataType
	for kind := cql.KindASCII; kind <= cql.KindDuration; kind++ {
		typ, err := cql.ScalarType(kind)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, typ)
	}
	return out
}

func candidateTypes(t *testing.T, field reflect.Type) []cql.DataType {
	t.Helper()
	return candidates(t, field, true)
}

// candidates returns the column types to try for Go type field; top is set
// for a field's own type, whose leaves are tried against every scalar. A
// nested leaf is tried against the scalars of its kind only, which keeps the
// product of a map or a tuple small.
func candidates(t *testing.T, field reflect.Type, top bool) []cql.DataType {
	t.Helper()
	scalars := scalarTypes(t)
	must := func(typ cql.DataType, err error) cql.DataType {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return typ
	}
	generic := func(name string) bool {
		return field.PkgPath() == reflect.TypeFor[cql.Pair[int, int]]().PkgPath() && strings.HasPrefix(field.Name(), name+"[")
	}
	switch {
	case field.Kind() == reflect.Array && field.Len() == 16 && field.Elem().Kind() == reflect.Uint8,
		field == reflect.TypeFor[[]byte](), field == reflect.TypeFor[*big.Int]():
		return leafCandidates(field, scalars, top)
	case field.Kind() == reflect.Slice || field.Kind() == reflect.Array:
		elems := candidates(t, field.Elem(), false)
		if top && isLeaf(field.Elem()) {
			elems = scalars
		}
		var out []cql.DataType
		for _, e := range elems {
			out = append(out, must(cql.ListType(e)), must(cql.SetType(e)))
			if field.Kind() == reflect.Array {
				out = append(out, must(cql.VectorType(e, uint16(field.Len()))))
			} else {
				out = append(out, must(cql.VectorType(e, 2)))
			}
		}
		if top {
			out = append(out, scalars...)
		}
		return out
	case field.Kind() == reflect.Pointer:
		return candidates(t, field.Elem(), top)
	case field.Kind() == reflect.Map:
		var out []cql.DataType
		keys, values := candidates(t, field.Key(), false), candidates(t, field.Elem(), false)
		if top && isLeaf(field.Key()) && isLeaf(field.Elem()) {
			keys, values = scalars, scalars
		}
		for _, k := range keys {
			for _, v := range values {
				out = append(out, must(cql.MapType(k, v)))
			}
		}
		return out
	case generic("Nullable") || generic("BindValue"):
		return candidates(t, field.Field(0).Type, top)
	case generic("Pair"):
		var out []cql.DataType
		for _, a := range candidates(t, field.Field(0).Type, false) {
			for _, b := range candidates(t, field.Field(1).Type, false) {
				out = append(out, must(cql.TupleType(a, b)))
			}
		}
		return out
	}
	return leafCandidates(field, scalars, top)
}

// isLeaf reports whether t is coded by a leaf codec, not derived.
func isLeaf(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Slice:
		return t == reflect.TypeFor[[]byte]()
	case reflect.Map, reflect.Pointer:
		return t == reflect.TypeFor[*big.Int]()
	case reflect.Array:
		return t.Len() == 16 && t.Elem().Kind() == reflect.Uint8
	case reflect.Struct:
		return !strings.Contains(t.Name(), "[")
	}
	return true
}

// leafCandidates returns every scalar at the top, or a nested leaf's own
// scalar kinds: those its default codec accepts.
func leafCandidates(field reflect.Type, scalars []cql.DataType, top bool) []cql.DataType {
	if top {
		return scalars
	}
	var out []cql.DataType
	for _, s := range scalars {
		if nestedKinds[field] == nil || nestedKinds[field][s.Kind()] {
			out = append(out, s)
		}
	}
	return out
}

// nestedKinds narrows the scalars a nested leaf is tried against.
var nestedKinds = map[reflect.Type]map[cql.Kind]bool{
	reflect.TypeFor[int32]():    {cql.KindInt: true, cql.KindSmallInt: true, cql.KindBlob: true},
	reflect.TypeFor[int64]():    {cql.KindBigInt: true, cql.KindInt: true, cql.KindBlob: true},
	reflect.TypeFor[string]():   {cql.KindText: true, cql.KindASCII: true, cql.KindInt: true},
	reflect.TypeFor[cql.UUID](): {cql.KindUUID: true, cql.KindTimeUUID: true, cql.KindInt: true},
}

// fill sets v to a value with every exported part nonzero where it can.
func fill(v reflect.Value) {
	switch v.Type() {
	case reflect.TypeFor[netip.Addr]():
		v.Set(reflect.ValueOf(netip.MustParseAddr("10.0.0.1")))
		return
	case reflect.TypeFor[cql.TimeUUID]():
		v.Set(reflect.ValueOf(cql.TimeUUIDFromTime(time.Unix(1, 0))))
		return
	case reflect.TypeFor[time.Time]():
		v.Set(reflect.ValueOf(time.UnixMilli(1500).UTC()))
		return
	}
	switch v.Kind() {
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(1)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(1.5)
	case reflect.String:
		v.SetString("ab")
	case reflect.Uint8:
		v.SetUint(1)
	case reflect.Array:
		for i := range v.Len() {
			if v.Type().Elem().Kind() == reflect.Uint8 {
				v.Index(i).SetUint(uint64(i + 1))
			} else {
				fill(v.Index(i))
			}
		}
	case reflect.Slice:
		s := reflect.MakeSlice(v.Type(), 2, 2)
		for i := range 2 {
			fill(s.Index(i))
		}
		if v.Type().Elem().Kind() == reflect.Uint8 {
			s.Index(1).SetUint(2)
		}
		v.Set(s)
	case reflect.Map:
		m := reflect.MakeMap(v.Type())
		key, value := reflect.New(v.Type().Key()).Elem(), reflect.New(v.Type().Elem()).Elem()
		fill(key)
		fill(value)
		m.SetMapIndex(key, value)
		v.Set(m)
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		fill(v.Elem())
	case reflect.Struct:
		for i := range v.NumField() {
			if v.Type().Field(i).IsExported() {
				fill(v.Field(i))
			}
		}
	}
}

type fieldOf struct {
	index  int
	column string
	typ    reflect.Type
}

func fieldsOf[T any](t *testing.T) []fieldOf {
	t.Helper()
	var out []fieldOf
	st := reflect.TypeFor[T]()
	for i := range st.NumField() {
		f := st.Field(i)
		tag, ok, err := structmap.ParseFieldTag(f.Name, f.IsExported(), f.Tag)
		if err != nil || !ok {
			t.Fatalf("field %s: %v", f.Name, err)
		}
		out = append(out, fieldOf{index: i, column: tag.Column, typ: f.Type})
	}
	return out
}

// encode encodes value as the one column of cols.
func encode[T any](params cql.ParamEncoder[T], cols cql.Columns, value T) (wire.Chain, error) {
	plan, err := params.Compile(cols)
	if err != nil {
		return wire.Chain{}, err
	}
	builder := wire.NewBuilder(8)
	w := codec.NewParamWriter(builder, cols, true)
	if err := plan.Encode(w, value); err != nil {
		return wire.Chain{}, err
	}
	if err := codec.FinishParamWriter(w); err != nil {
		return wire.Chain{}, err
	}
	return builder.Freeze()
}

func decode[T any](rows cql.RowDecoder[T], cols cql.Columns, chain wire.Chain) (T, error) {
	var zero T
	plan, err := rows.Compile(cols)
	if err != nil {
		return zero, err
	}
	reader := codec.NewRowReader(cols, chain, wire.NewLease(chain, nil))
	var row T
	err = plan.DecodeInto(reader, &row)
	if err == nil {
		err = codec.FinishRowReader(reader)
	}
	if err != nil {
		return zero, err
	}
	return rows.Clone(row), nil
}

func chainBytes(t *testing.T, chain wire.Chain) []byte {
	t.Helper()
	b := make([]byte, chain.Len())
	if _, err := chain.CopyTo(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func sameError(a, b error) bool { return (a == nil) == (b == nil) }

// TestGeneratedMatchesReflection checks every generated field against the
// reflected mapping of the same field: both accept the same column types,
// encode a filled value to the same bytes, and decode it to the same value.
func TestGeneratedMatchesReflection(t *testing.T) {
	if f := (Buffered{}).CQLFields(); f.Params == nil || f.Columns == nil {
		t.Fatal("no generated buffered mapping")
	}
	if (Streamed{}).CQLStreamColumns() == nil {
		t.Fatal("no generated stream mapping")
	}
	genParams, genRows := cql.MustParamsOf[Buffered](), cql.MustRowOf[Buffered]()
	refParams, refRows := cql.MustParamsOf[reflectedBuffered](), cql.MustRowOf[reflectedBuffered]()
	var value Buffered
	fill(reflect.ValueOf(&value).Elem())

	// cells holds, for each column type, a cell some buffered field encodes,
	// which the stream decoders then decode.
	cells := map[string][]byte{}
	for _, f := range fieldsOf[Buffered](t) {
		accepted := 0
		for _, typ := range candidateTypes(t, f.typ) {
			cols := cql.NewColumns(cql.ColumnSpec{Name: f.column, Type: typ})
			where := fmt.Sprintf("%s %s as %s", f.column, f.typ, typ)
			genChain, genErr := encode(genParams, cols, value)
			refChain, refErr := encode(refParams, cols, reflectedBuffered(value))
			if !sameError(genErr, refErr) {
				t.Errorf("%s: encode: generated %v, reflected %v", where, genErr, refErr)
				continue
			}
			_, genCompile := genRows.Compile(cols)
			_, refCompile := refRows.Compile(cols)
			if !sameError(genCompile, refCompile) {
				t.Errorf("%s: compile rows: generated %v, reflected %v", where, genCompile, refCompile)
			}
			if genErr != nil || genCompile != nil {
				continue
			}
			accepted++
			gen, ref := chainBytes(t, genChain), chainBytes(t, refChain)
			if !bytes.Equal(gen, ref) {
				t.Errorf("%s: encodings differ:\ngenerated %x\nreflected %x", where, gen, ref)
				continue
			}
			if _, ok := cells[typ.String()]; !ok {
				cells[typ.String()] = gen
			}
			genRow, genErr := decode(genRows, cols, genChain)
			refRow, refErr := decode(refRows, cols, genChain)
			if !sameError(genErr, refErr) || !reflect.DeepEqual(genRow, Buffered(refRow)) {
				t.Errorf("%s: decoded rows differ:\ngenerated %+v (%v)\nreflected %+v (%v)", where,
					reflect.ValueOf(genRow).Field(f.index), genErr, reflect.ValueOf(refRow).Field(f.index), refErr)
			}
		}
		if accepted == 0 {
			t.Errorf("%s %s: no column type round-trips", f.column, f.typ)
		}
	}

	// Bound's fields only bind parameters: its generated mapping has no
	// Columns, and RowOf rejects it.
	if f := (Bound{}).CQLFields(); f.Params == nil || f.Columns != nil {
		t.Fatal("generated Bound mapping is not parameters only")
	}
	if _, err := cql.RowOf[Bound](); err == nil {
		t.Error("RowOf accepted a BindValue field")
	}
	genBound, refBound := cql.MustParamsOf[Bound](), cql.MustParamsOf[reflectedBound]()
	var bound Bound
	fill(reflect.ValueOf(&bound).Elem())
	for _, f := range fieldsOf[Bound](t) {
		accepted := 0
		for _, typ := range candidateTypes(t, f.typ) {
			cols := cql.NewColumns(cql.ColumnSpec{Name: f.column, Type: typ})
			where := fmt.Sprintf("%s %s as %s", f.column, f.typ, typ)
			genChain, genErr := encode(genBound, cols, bound)
			refChain, refErr := encode(refBound, cols, reflectedBound(bound))
			if !sameError(genErr, refErr) {
				t.Errorf("%s: encode: generated %v, reflected %v", where, genErr, refErr)
				continue
			}
			if genErr != nil {
				continue
			}
			accepted++
			if gen, ref := chainBytes(t, genChain), chainBytes(t, refChain); !bytes.Equal(gen, ref) {
				t.Errorf("%s: encodings differ:\ngenerated %x\nreflected %x", where, gen, ref)
			}
		}
		if accepted == 0 {
			t.Errorf("%s %s: no column type encodes", f.column, f.typ)
		}
	}

	genStream, refStream := cql.MustStreamRowOf[Streamed](), cql.MustStreamRowOf[reflectedStreamed]()
	for _, f := range fieldsOf[Streamed](t) {
		accepted := 0
		for _, typ := range candidateTypes(t, f.typ) {
			cols := cql.NewColumns(cql.ColumnSpec{Name: f.column, Type: typ})
			where := fmt.Sprintf("%s %s as %s", f.column, f.typ, typ)
			genPlan, genErr := genStream.Compile(cols)
			refPlan, refErr := refStream.Compile(cols)
			if !sameError(genErr, refErr) {
				t.Errorf("%s: compile: generated %v, reflected %v", where, genErr, refErr)
				continue
			}
			cell, ok := cells[typ.String()]
			if genErr != nil || !ok {
				continue
			}
			accepted++
			var gen Streamed
			var ref reflectedStreamed
			genErr = streamDecode(cols, genPlan, cell, &gen)
			refErr = streamDecode(cols, refPlan, cell, &ref)
			if !sameError(genErr, refErr) || !reflect.DeepEqual(gen, Streamed(ref)) {
				t.Errorf("%s: decoded rows differ:\ngenerated %+v (%v)\nreflected %+v (%v)", where,
					reflect.ValueOf(gen).Field(f.index), genErr, reflect.ValueOf(ref).Field(f.index), refErr)
			}
		}
		if accepted == 0 {
			t.Errorf("%s %s: no column type decodes", f.column, f.typ)
		}
	}
}

func streamDecode[R any](cols cql.Columns, plan cql.CompiledStreamRow[R], data []byte, dst *R) error {
	scope, err := wire.NewStream(context.Background(), bytes.NewReader(data), int64(len(data)), wire.StreamLimits{MaxDepth: 8})
	if err != nil {
		return err
	}
	reader, err := codec.NewStreamRowReader(cols, scope)
	if err != nil {
		return err
	}
	plan.Reset(dst)
	if err := plan.DecodeInto(reader, dst); err != nil {
		return err
	}
	return scope.RequireEnd()
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package fixture

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestGeneratedMappingCodecOptions(t *testing.T) {
	cols, input := columns(t), value()
	input.MaybeText = cql.Nullable[string]{Value: "nullable", Valid: true}
	codecFor := func(prefix string) cql.Codec[string] {
		return cql.NamedScalar(cql.StringCodec, func(v string) string { return prefix + v }, func(v string) string { return strings.TrimPrefix(v, prefix) })
	}
	first := cql.WithCodec[Fixture](codecFor("first/"))
	second := cql.WithCodec[Fixture](codecFor("second/"))
	firstParams, secondParams := cql.MustParamsOf(first), cql.MustParamsOf(second)
	firstRows, secondRows := cql.MustRowOf(first), cql.MustRowOf(second)
	firstBody, _ := encode(t, cols, firstParams, input)
	secondBody, _ := encode(t, cols, secondParams, input)
	for _, tc := range []struct {
		prefix string
		body   wire.Chain
		rows   cql.RowDecoder[Fixture]
	}{
		{"first/", firstBody, firstRows}, {"second/", secondBody, secondRows},
	} {
		raw := decode(t, cols, cql.MustRowOf[Fixture](), tc.body)
		if raw.Text != tc.prefix+input.Text || raw.MaybeText.Value != tc.prefix+input.MaybeText.Value || raw.ASCII != input.ASCII {
			t.Fatalf("override or built-in tag precedence: %+v", raw)
		}
		if raw.Origin != input.Origin || raw.Corner != input.Corner || raw.Attrs[0] != input.Attrs[0] {
			t.Fatal("untouched custom-expression codecs changed")
		}
		got := decode(t, cols, tc.rows, tc.body)
		if got.Text != input.Text || got.MaybeText != input.MaybeText || got.ASCII != input.ASCII {
			t.Fatalf("decoder override not applied: %+v", got)
		}
	}
	// A later construction must not mutate the mappings already built or the registry.
	_, again := encode(t, cols, firstParams, input)
	_, original := encode(t, cols, cql.MustParamsOf[Fixture](), input)
	_, expectedFirst := encode(t, cols, cql.MustParamsOf(first), input)
	if !bytes.Equal(again, expectedFirst) || bytes.Equal(again, original) {
		t.Fatal("mapping options leaked into shared registration")
	}
	// Compare against reflection with the custom-expression types supplied explicitly.
	reflected, err := codec.ReflectParams(first, cql.WithCodec[Fixture](PointCodec), cql.WithCodec[Fixture](cql.MapCodec(cql.StringCodec, cql.Int64Codec)))
	if err != nil {
		t.Fatal(err)
	}
	_, ref := encode(t, cols, reflected, input)
	if !bytes.Equal(again, ref) {
		t.Fatal("generated and reflected option precedence differ")
	}
	// Optional fields and embedded offsets use column names, independently of
	// both declaration order and the physical order selected by the server.
	subset := cql.NewColumns(cql.ColumnSpec{Name: "createdat", Type: cql.TimestampType}, cql.ColumnSpec{Name: "text", Type: cql.TextType})
	shift := cql.WithCodec[Fixture](cql.NamedScalar(cql.TimestampCodec,
		func(v cql.Timestamp) cql.Timestamp { return v + 100 }, func(v cql.Timestamp) cql.Timestamp { return v - 100 }))
	partial, _ := encode(t, subset, cql.MustParamsOf(first, shift), input)
	raw := decode(t, subset, cql.MustRowOf[Fixture](), partial)
	if raw.CreatedAt != input.CreatedAt+100 || raw.Text != "first/"+input.Text || !raw.ID.IsEmpty() || raw.Skipped != "" {
		t.Fatalf("embedded/subset mapping: %+v", raw)
	}
	got := decode(t, subset, cql.MustRowOf(first, shift), partial)
	if got.CreatedAt != input.CreatedAt || got.Text != input.Text {
		t.Fatalf("embedded decoder override: %+v", got)
	}
	for _, check := range []func() error{
		func() error { _, err := cql.ParamsOf[Fixture](nil); return err },
		func() error { _, err := cql.RowOf[Fixture](nil); return err },
		func() error { _, err := cql.StreamRowOf[StreamFixture](nil); return err },
	} {
		if err := check(); err == nil {
			t.Fatal("generated mapping accepted a nil option")
		}
	}
}

type offsetStreamInt[T ~int32 | ~int64] struct {
	base   cql.StreamDecoder[T]
	amount T
}
type offsetStreamIntPlan[T ~int32 | ~int64] struct {
	base   cql.StreamValuePlan[T]
	amount T
}

func (d offsetStreamInt[T]) Compile(typ cql.DataType) (cql.StreamValuePlan[T], error) {
	base, err := d.base.Compile(typ)
	return offsetStreamIntPlan[T]{base: base, amount: d.amount}, err
}
func (d offsetStreamInt[T]) Reset(dst *T)     { *dst = 0 }
func (p offsetStreamIntPlan[T]) Reset(dst *T) { p.base.Reset(dst) }
func (p offsetStreamIntPlan[T]) DecodeInto(reader *cql.StreamValueReader, dst *T) error {
	if err := p.base.DecodeInto(reader, dst); err != nil {
		return err
	}
	*dst += p.amount
	return nil
}

func TestGeneratedStreamMappingOptions(t *testing.T) {
	set, err := cql.SetType(cql.IntType)
	if err != nil {
		t.Fatal(err)
	}
	cols := cql.NewColumns(cql.ColumnSpec{Name: "id", Type: cql.IntType}, cql.ColumnSpec{Name: "tags", Type: set}, cql.ColumnSpec{Name: "maybe", Type: cql.BigIntType})
	params := cql.Params(
		cql.Param("id", cql.Int32Codec, func(v StreamFixture) int32 { return v.ID }),
		cql.Param("tags", cql.SetEncoder(cql.Int32Codec), func(v StreamFixture) []int32 { return v.Tags }),
		cql.Param("maybe", cql.NullableEncoder(cql.Int64Codec), func(v StreamFixture) cql.Nullable[int64] { return v.Maybe }),
	)
	plan, err := params.Compile(cols)
	if err != nil {
		t.Fatal(err)
	}
	builder := wire.NewBuilder(8)
	if err := plan.Encode(codec.NewParamWriter(builder, cols, false), StreamFixture{ID: 7, Tags: []int32{1, 2}, Maybe: cql.Nullable[int64]{Value: 5, Valid: true}}); err != nil {
		t.Fatal(err)
	}
	chain, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, chain.Len())
	if _, err := chain.CopyTo(data); err != nil {
		t.Fatal(err)
	}
	for _, amount := range []int32{10, 20, 0} {
		rows := cql.MustStreamRowOf(
			cql.WithStreamCodec[StreamFixture](offsetStreamInt[int32]{base: cql.StreamInt32, amount: amount}),
			cql.WithStreamCodec[StreamFixture](offsetStreamInt[int64]{base: cql.StreamInt64, amount: int64(amount)}),
		)
		scope, err := wire.NewStream(context.Background(), bytes.NewReader(data), int64(len(data)), wire.StreamLimits{MaxDepth: 8})
		if err != nil {
			t.Fatal(err)
		}
		reader, err := codec.NewStreamRowReader(cols, scope)
		if err != nil {
			t.Fatal(err)
		}
		compiled, err := rows.Compile(cols)
		if err != nil {
			t.Fatal(err)
		}
		dst := StreamFixture{Tags: make([]int32, 0, 2)}
		if err := compiled.DecodeInto(reader, &dst); err != nil {
			t.Fatal(err)
		}
		if dst.ID != 7+amount || dst.Tags[0] != 1 || dst.Tags[1] != 2 || !dst.Maybe.Valid || dst.Maybe.Value != 5+int64(amount) {
			t.Fatalf("stream override or pinned set tag: %+v", dst)
		}
	}
}

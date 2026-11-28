// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package fixture

import (
	"bytes"
	"context"
	"math/big"
	"net/netip"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func mapType(t *testing.T) cql.DataType {
	t.Helper()
	text := cql.TextType
	big := cql.BigIntType
	typ, err := cql.MapType(text, big)
	if err != nil {
		t.Fatal(err)
	}
	return typ
}

func columns(t *testing.T) cql.Columns {
	t.Helper()
	collection := func(name string, of func(cql.DataType) (cql.DataType, error), kind cql.Kind) cql.ColumnSpec {
		element, _ := cql.ScalarType(kind)
		typ, err := of(element)
		if err != nil {
			t.Fatal(err)
		}
		return cql.ColumnSpec{Name: name, Type: typ}
	}
	spec := func(name string, kind cql.Kind) cql.ColumnSpec {
		typ, err := cql.ScalarType(kind)
		if err != nil {
			t.Fatal(err)
		}
		return cql.ColumnSpec{Name: name, Type: typ}
	}
	return cql.NewColumns(
		spec("id", cql.KindUUID), spec("text", cql.KindText), spec("bool", cql.KindBoolean),
		spec("tiny", cql.KindTinyInt), spec("small", cql.KindSmallInt), spec("int", cql.KindInt),
		spec("big_value", cql.KindBigInt), spec("float", cql.KindFloat), spec("double", cql.KindDouble),
		spec("ascii", cql.KindASCII), spec("blob", cql.KindBlob), spec("bytes", cql.KindBlob),
		spec("time_id", cql.KindTimeUUID), spec("addr", cql.KindInet), spec("hits", cql.KindCounter),
		spec("varint", cql.KindVarint), spec("decimal", cql.KindDecimal), spec("day", cql.KindDate),
		spec("clock", cql.KindTime), spec("at", cql.KindTimestamp), spec("span", cql.KindDuration),
		spec("maybe_int", cql.KindInt), spec("maybe_text", cql.KindText),
		spec("origin", cql.KindBigInt), spec("corner", cql.KindBigInt),
		collection("names", cql.ListType, cql.KindText), collection("scores", cql.SetType, cql.KindInt),
		cql.ColumnSpec{Name: "attrs", Type: mapType(t)}, spec("created_at", cql.KindTimestamp),
		collection("labels", cql.ListType, cql.KindText), spec("label", cql.KindText),
		spec("maybe_tag", cql.KindText), cql.ColumnSpec{Name: "label_hits", Type: mapType(t)},
	)
}

func value() Fixture {
	label := Label("one")
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	return Fixture{
		Bool: true, Tiny: -8, Small: 1600, Int: -32000, Big: 1 << 40, Float: 1.5, Double: -2.25,
		Text: "héllo", ASCII: "plain", Blob: []byte{1, 2, 3}, Bytes: cql.BorrowBytes([]byte{4, 5}),
		ID:     cql.UUID{1, 2, 3, 4, 5, 6, 0x40, 8, 0x80, 10, 11, 12, 13, 14, 15, 16},
		TimeID: cql.TimeUUIDFromTime(now), Addr: netip.MustParseAddr("10.0.0.1"), Hits: 7,
		Varint:  cql.VarintFromBigInt(big.NewInt(-12345)),
		Decimal: cql.Decimal{Unscaled: cql.VarintFromBigInt(big.NewInt(314)), Scale: 2},
		Day:     12345, Clock: 3600e9, At: cql.Timestamp(now.UnixMilli()),
		Span:     cql.Duration{Months: 1, Days: 2, Nanoseconds: 3},
		MaybeInt: cql.Nullable[int32]{Value: 9, Valid: true},
		Names:    []string{"a", "b"}, Scores: []int32{4, 2}, Attrs: []cql.Entry[string, int64]{{Key: "k", Value: 1}},
		Origin: Point{1, -2}, Corner: cql.Nullable[Point]{Value: Point{3, 4}, Valid: true},
		Labels: []Label{"l1", "l2"}, Label: &label, MaybeTag: cql.Nullable[Label]{Value: "m", Valid: true},
		LabelHits: map[Label]int{"h": 5},
		Audit:     Audit{CreatedAt: 42, ID: cql.UUID{9}},
		Skipped:   "not mapped", internal: 1,
	}
}

func encode[P any](t *testing.T, cols cql.Columns, params cql.ParamEncoder[P], v P) (wire.Chain, []byte) {
	t.Helper()
	plan, err := params.Compile(cols)
	if err != nil {
		t.Fatal(err)
	}
	builder := wire.NewBuilder(8)
	w := codec.NewParamWriter(builder, cols, true)
	if err := plan.Encode(w, v); err != nil {
		t.Fatal(err)
	}
	if err := codec.FinishParamWriter(w); err != nil {
		t.Fatal(err)
	}
	chain, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	raw := make([]byte, chain.Len())
	if _, err := chain.CopyTo(raw); err != nil {
		t.Fatal(err)
	}
	return chain, raw
}

func decode[R any](t *testing.T, cols cql.Columns, rows cql.RowDecoder[R], chain wire.Chain) R {
	t.Helper()
	plan, err := rows.Compile(cols)
	if err != nil {
		t.Fatal(err)
	}
	reader := codec.NewRowReader(cols, chain, wire.NewLease(chain, nil))
	var row R
	if err := plan.DecodeInto(reader, &row); err != nil {
		t.Fatal(err)
	}
	if err := codec.FinishRowReader(reader); err != nil {
		t.Fatal(err)
	}
	return rows.Clone(row)
}

func streamColumns(t *testing.T) cql.Columns {
	t.Helper()
	scalar := func(kind cql.Kind) cql.DataType {
		typ, err := cql.ScalarType(kind)
		if err != nil {
			t.Fatal(err)
		}
		return typ
	}
	blobs, err := cql.ListType(scalar(cql.KindBlob))
	if err != nil {
		t.Fatal(err)
	}
	ints, err := cql.SetType(scalar(cql.KindInt))
	if err != nil {
		t.Fatal(err)
	}
	return cql.NewColumns(
		cql.ColumnSpec{Name: "id", Type: scalar(cql.KindInt)},
		cql.ColumnSpec{Name: "key", Type: scalar(cql.KindUUID)},
		cql.ColumnSpec{Name: "payload", Type: scalar(cql.KindBlob)},
		cql.ColumnSpec{Name: "name", Type: scalar(cql.KindText)},
		cql.ColumnSpec{Name: "pieces", Type: blobs},
		cql.ColumnSpec{Name: "tags", Type: ints},
		cql.ColumnSpec{Name: "maybe", Type: scalar(cql.KindBigInt)},
		cql.ColumnSpec{Name: "at", Type: scalar(cql.KindTimestamp)},
		cql.ColumnSpec{Name: "addr", Type: scalar(cql.KindInet)},
	)
}

// reflectedFixture and reflectedStreamFixture have the fields of Fixture and
// StreamFixture but not their generated mapping methods, so cql.ParamsOf,
// cql.RowOf and stream.RowOf map them by reflection.
type (
	reflectedFixture       Fixture
	reflectedStreamFixture StreamFixture
)

// reflectParams is Fixture's parameter mapping built by reflection.
func reflectParams(t *testing.T, opts ...cql.StructOption) cql.ParamEncoder[Fixture] {
	t.Helper()
	params, err := cql.ParamsOf[reflectedFixture](opts...)
	if err != nil {
		t.Fatal(err)
	}
	return cql.ConvertParams(params, func(v Fixture) (reflectedFixture, error) { return reflectedFixture(v), nil })
}

// reflectRows is Fixture's row mapping built by reflection.
func reflectRows(t *testing.T, opts ...cql.StructOption) cql.RowDecoder[Fixture] {
	t.Helper()
	rows, err := cql.RowOf[reflectedFixture](opts...)
	if err != nil {
		t.Fatal(err)
	}
	return cql.ConvertRows(rows, func(v reflectedFixture) (Fixture, error) { return Fixture(v), nil },
		func(v Fixture) Fixture { return Fixture(rows.Clone(reflectedFixture(v))) })
}

func streamDecode(t *testing.T, cols cql.Columns, rows cql.StreamRowDecoder[StreamFixture], data []byte) StreamFixture {
	t.Helper()
	// Streaming decoders write into the destination's storage.
	dst := StreamFixture{Payload: make([]byte, 0, 16), Name: make([]byte, 0, 16),
		Pieces: [][]byte{make([]byte, 0, 8), make([]byte, 0, 8)}[:0], Tags: make([]int32, 0, 4)}
	streamDecodeInto(t, cols, rows, data, &dst)
	return dst
}

// reflectStreamDecode decodes data with StreamFixture's stream mapping built
// by reflection.
func reflectStreamDecode(t *testing.T, cols cql.Columns, data []byte) StreamFixture {
	t.Helper()
	rows, err := cql.StreamRowOf[reflectedStreamFixture]()
	if err != nil {
		t.Fatal(err)
	}
	dst := reflectedStreamFixture{Payload: make([]byte, 0, 16), Name: make([]byte, 0, 16),
		Pieces: [][]byte{make([]byte, 0, 8), make([]byte, 0, 8)}[:0], Tags: make([]int32, 0, 4)}
	streamDecodeInto(t, cols, rows, data, &dst)
	return StreamFixture(dst)
}

func streamDecodeInto[R any](t *testing.T, cols cql.Columns, rows cql.StreamRowDecoder[R], data []byte, dst *R) {
	t.Helper()
	plan, err := rows.Compile(cols)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := wire.NewStream(context.Background(), bytes.NewReader(data), int64(len(data)), wire.StreamLimits{MaxDepth: 8})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := codec.NewStreamRowReader(cols, scope)
	if err != nil {
		t.Fatal(err)
	}
	plan.Reset(dst)
	if err := plan.DecodeInto(reader, dst); err != nil {
		t.Fatal(err)
	}
	if err := scope.RequireEnd(); err != nil {
		t.Fatal(err)
	}
}

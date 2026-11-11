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
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func mapType(t *testing.T) cqltype.DataType {
	t.Helper()
	text := cqltype.TextType
	big := cqltype.BigIntType
	typ, err := cqltype.MapType(text, big)
	if err != nil {
		t.Fatal(err)
	}
	return typ
}

func columns(t *testing.T) cqltype.Columns {
	t.Helper()
	collection := func(name string, of func(cqltype.DataType) (cqltype.DataType, error), kind cqltype.Type) cqltype.ColumnSpec {
		element, _ := cqltype.ScalarType(kind)
		typ, err := of(element)
		if err != nil {
			t.Fatal(err)
		}
		return cqltype.ColumnSpec{Name: name, Type: typ}
	}
	spec := func(name string, kind cqltype.Type) cqltype.ColumnSpec {
		typ, err := cqltype.ScalarType(kind)
		if err != nil {
			t.Fatal(err)
		}
		return cqltype.ColumnSpec{Name: name, Type: typ}
	}
	return cqltype.NewColumns(
		spec("id", cqltype.TypeUUID), spec("text", cqltype.TypeText), spec("bool", cqltype.TypeBoolean),
		spec("tiny", cqltype.TypeTinyInt), spec("small", cqltype.TypeSmallInt), spec("int", cqltype.TypeInt),
		spec("big_value", cqltype.TypeBigInt), spec("float", cqltype.TypeFloat), spec("double", cqltype.TypeDouble),
		spec("ascii", cqltype.TypeAscii), spec("blob", cqltype.TypeBlob), spec("bytes", cqltype.TypeBlob),
		spec("timeid", cqltype.TypeTimeUUID), spec("addr", cqltype.TypeInet), spec("hits", cqltype.TypeCounter),
		spec("varint", cqltype.TypeVarint), spec("decimal", cqltype.TypeDecimal), spec("day", cqltype.TypeDate),
		spec("clock", cqltype.TypeTime), spec("at", cqltype.TypeTimestamp), spec("span", cqltype.TypeDuration),
		spec("maybeint", cqltype.TypeInt), spec("maybe_text", cqltype.TypeText),
		spec("origin", cqltype.TypeBigInt), spec("corner", cqltype.TypeBigInt),
		collection("names", cqltype.ListType, cqltype.TypeText), collection("scores", cqltype.SetType, cqltype.TypeInt),
		cqltype.ColumnSpec{Name: "attrs", Type: mapType(t)}, spec("createdat", cqltype.TypeTimestamp),
	)
}

func value() Fixture {
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
		Audit:   Audit{CreatedAt: 42, ID: cql.UUID{9}},
		Skipped: "not mapped", internal: 1,
	}
}

func encode(t *testing.T, cols cqltype.Columns, params codec.ParamEncoder[Fixture], v Fixture) (wire.Chain, []byte) {
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

func decode(t *testing.T, cols cqltype.Columns, rows codec.RowDecoder[Fixture], chain wire.Chain) Fixture {
	t.Helper()
	plan, err := rows.Compile(cols)
	if err != nil {
		t.Fatal(err)
	}
	reader := codec.NewRowReader(cols, chain, wire.NewLease(chain, nil))
	row, err := plan.Decode(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := codec.FinishRowReader(reader); err != nil {
		t.Fatal(err)
	}
	return rows.Clone(row)
}

func streamColumns(t *testing.T) cqltype.Columns {
	t.Helper()
	scalar := func(kind cqltype.Type) cqltype.DataType {
		typ, err := cqltype.ScalarType(kind)
		if err != nil {
			t.Fatal(err)
		}
		return typ
	}
	blobs, err := cqltype.ListType(scalar(cqltype.TypeBlob))
	if err != nil {
		t.Fatal(err)
	}
	ints, err := cqltype.SetType(scalar(cqltype.TypeInt))
	if err != nil {
		t.Fatal(err)
	}
	return cqltype.NewColumns(
		cqltype.ColumnSpec{Name: "id", Type: scalar(cqltype.TypeInt)},
		cqltype.ColumnSpec{Name: "key", Type: scalar(cqltype.TypeUUID)},
		cqltype.ColumnSpec{Name: "payload", Type: scalar(cqltype.TypeBlob)},
		cqltype.ColumnSpec{Name: "name", Type: scalar(cqltype.TypeText)},
		cqltype.ColumnSpec{Name: "pieces", Type: blobs},
		cqltype.ColumnSpec{Name: "tags", Type: ints},
		cqltype.ColumnSpec{Name: "maybe", Type: scalar(cqltype.TypeBigInt)},
		cqltype.ColumnSpec{Name: "at", Type: scalar(cqltype.TypeTimestamp)},
		cqltype.ColumnSpec{Name: "addr", Type: scalar(cqltype.TypeInet)},
	)
}

func streamDecode(t *testing.T, cols cqltype.Columns, rows codec.StreamRowDecoder[StreamFixture], data []byte) StreamFixture {
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
	// Streaming decoders write into the destination's storage.
	dst := StreamFixture{Payload: make([]byte, 0, 16), Name: make([]byte, 0, 16),
		Pieces: [][]byte{make([]byte, 0, 8), make([]byte, 0, 8)}[:0], Tags: make([]int32, 0, 4)}
	plan.Reset(&dst)
	if err := plan.DecodeInto(reader, &dst); err != nil {
		t.Fatal(err)
	}
	if err := scope.RequireEnd(); err != nil {
		t.Fatal(err)
	}
	return dst
}

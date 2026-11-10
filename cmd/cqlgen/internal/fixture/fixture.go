// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package fixture holds a struct covering every built-in field type, mapped
// by cqlgen; its tests check the generated mapping against reflection.
package fixture

import (
	"net/netip"

	"github.com/scylladb/gocql/v2/cql"
)

//go:generate go run github.com/scylladb/gocql/v2/cmd/cqlgen -type=Fixture,Point,StreamFixture

type Fixture struct { //nolint:govet // field order mirrors the column list, not memory layout
	Bool      bool
	Tiny      int8  `cql:"tiny"`
	Small     int16 `cql:"small"`
	Int       int32
	Big       int64 `cql:"big_value"`
	Float     float32
	Double    float64
	Text      string
	ASCII     string `cql:"ascii,codec=ASCIIStringCodec"`
	Blob      []byte
	Bytes     cql.Bytes
	ID        Key `cql:"id"` // an alias maps like the type it names
	TimeID    cql.TimeUUID
	Addr      netip.Addr
	Hits      cql.Counter
	Varint    cql.Varint
	Decimal   cql.Decimal
	Day       cql.Date
	Clock     cql.TimeOfDay
	At        cql.Timestamp
	Span      cql.Duration
	Names     []string                   `cql:"names"`
	Scores    []int32                    `cql:"scores,codec=SetCodec"`
	Attrs     []cql.Entry[string, int64] `cql:"attrs,codec=cql.MapCodec(cql.StringCodec, cql.Int64Codec)"`
	MaybeInt  cql.Nullable[int32]
	MaybeText cql.Nullable[string] `cql:"maybe_text"`
	Origin    Point                `cql:"origin,codec=PointCodec"`
	Corner    cql.Nullable[Point]  `cql:"corner,codec=cql.NullableCodec(PointCodec)"`
	Audit                          // flattened: createdat; its ID is hidden by Fixture.ID
	Skipped   string               `cql:"-"`
	internal  int
}

// Point packs into a bigint, standing in for a type with no built-in codec.
type Point struct{ X, Y int32 }

var PointCodec cql.Codec[Point] = cql.NamedScalar(cql.Int64Codec,
	func(p Point) int64 { return int64(p.X)<<32 | int64(uint32(p.Y)) },
	func(v int64) Point { return Point{X: int32(v >> 32), Y: int32(v)} })

// Key is an alias of cql.UUID, so it maps with UUIDCodec.
type Key = cql.UUID

// StreamFixture covers the streaming decoders. Name has no buffered codec for
// its text column, so only its stream mapping can be used with this table.
type StreamFixture struct { //nolint:govet // field order mirrors the column list, not memory layout
	ID      int32
	Key     Key
	Payload []byte
	Name    []byte `cql:"name,stream=StreamText"`
	Pieces  [][]byte
	Tags    []int32 `cql:"tags,codec=SetCodec,stream=StreamSet"`
	Maybe   cql.Nullable[int64]
	At      cql.Timestamp
	Addr    netip.Addr
}

// Audit is embedded in Fixture.
type Audit struct {
	CreatedAt cql.Timestamp
	ID        cql.UUID
}

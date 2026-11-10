// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package fixture

import (
	"bytes"
	"net/netip"
	"reflect"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestGeneratedCodec(t *testing.T) {
	// The generated mapping and the reflected one must encode and decode alike.
	t.Run("Fixture", func(t *testing.T) {
		if _, ok := codec.RegisteredStruct[Fixture](); !ok {
			t.Fatal("generated mapping is not registered")
		}
		cols := columns(t)
		v := value()
		withPoint := cql.WithCodec[Fixture](PointCodec)
		withAttrs := cql.WithCodec[Fixture](cql.MapCodec(cql.StringCodec, cql.Int64Codec))
		reflectedParams, err := codec.ReflectParams(withPoint, withAttrs)
		if err != nil {
			t.Fatal(err)
		}
		reflectedRows, err := codec.ReflectRow(withPoint, withAttrs)
		if err != nil {
			t.Fatal(err)
		}
		genChain, gen := encode(t, cols, cql.MustParamsOf[Fixture](), v)
		refChain, ref := encode(t, cols, reflectedParams, v)
		if !bytes.Equal(gen, ref) {
			t.Fatalf("encodings differ:\ngenerated %x\nreflected %x", gen, ref)
		}
		genRow := decode(t, cols, cql.MustRowOf[Fixture](), genChain)
		refRow := decode(t, cols, reflectedRows, refChain)
		if !reflect.DeepEqual(genRow, refRow) {
			t.Fatalf("decoded rows differ:\ngenerated %+v\nreflected %+v", genRow, refRow)
		}
		v.Skipped, v.internal, v.Audit.ID = "", 0, cql.UUID{}
		if genRow.CreatedAt != 42 || genRow.Audit.ID != (cql.UUID{}) {
			t.Fatalf("embedded fields: %+v", genRow.Audit)
		}
		if genRow.Varint.BigInt().Cmp(v.Varint.BigInt()) != 0 || genRow.Text != v.Text || genRow.Corner != v.Corner || genRow.TimeID != v.TimeID {
			t.Fatalf("round trip:\nwant %+v\ngot  %+v", v, genRow)
		}
	})

	// The generated streaming mapping and the reflected one decode alike.
	t.Run("StreamFixture", func(t *testing.T) {
		f, ok := codec.RegisteredStruct[StreamFixture]()
		if !ok || f.StreamColumns == nil {
			t.Fatal("expected a generated stream mapping for StreamFixture")
		}
		cols := streamColumns(t)
		want := StreamFixture{
			ID: 7, Key: Key{1, 2, 3}, Payload: []byte("payload"), Name: []byte("héllo"),
			Pieces: [][]byte{[]byte("a"), []byte("bc")}, Tags: []int32{1, 2},
			Maybe: cql.Nullable[int64]{Value: 5, Valid: true}, At: 1234, Addr: netip.MustParseAddr("::1"),
		}
		type source struct {
			ID      int32
			Key     cql.UUID
			Payload []byte
			Name    string
			Pieces  [][]byte
			Tags    []int32
			Maybe   cql.Nullable[int64]
			At      cql.Timestamp
			Addr    netip.Addr
		}
		params := cql.Params(
			cql.Param("id", cql.Int32Codec, func(v source) int32 { return v.ID }),
			cql.Param("key", cql.UUIDCodec, func(v source) cql.UUID { return v.Key }),
			cql.Param("payload", cql.BytesEncoder, func(v source) []byte { return v.Payload }),
			cql.Param("name", cql.StringCodec, func(v source) string { return v.Name }),
			cql.Param("pieces", cql.ListEncoder(cql.BytesEncoder), func(v source) [][]byte { return v.Pieces }),
			cql.Param("tags", cql.SetEncoder(cql.Int32Codec), func(v source) []int32 { return v.Tags }),
			cql.Param("maybe", cql.NullableEncoder(cql.Int64Codec), func(v source) cql.Nullable[int64] { return v.Maybe }),
			cql.Param("at", cql.TimestampCodec, func(v source) cql.Timestamp { return v.At }),
			cql.Param("addr", cql.InetCodec, func(v source) netip.Addr { return v.Addr }),
		)
		plan, err := params.Compile(cols)
		if err != nil {
			t.Fatal(err)
		}
		builder := wire.NewBuilder(8)
		w := codec.NewParamWriter(builder, cols, true)
		if err := plan.Encode(w, source{want.ID, want.Key, want.Payload, string(want.Name), want.Pieces, want.Tags, want.Maybe, want.At, want.Addr}); err != nil {
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
		reflected, err := codec.ReflectStreamRow[StreamFixture]()
		if err != nil {
			t.Fatal(err)
		}
		gen := streamDecode(t, cols, cql.MustStreamRowOf[StreamFixture](), data)
		ref := streamDecode(t, cols, reflected, data)
		if !reflect.DeepEqual(gen, ref) || !reflect.DeepEqual(gen, want) {
			t.Fatalf("stream rows differ:\ngenerated %+v\nreflected %+v\nwant      %+v", gen, ref, want)
		}
	})
}

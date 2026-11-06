// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestValue(t *testing.T) {
	t.Run("BytesView", func(t *testing.T) {
		t.Run("Borrowed", func(t *testing.T) {
			first, second := []byte("ab"), []byte("cdef")
			chain := FoundationChain(t, first, second)
			released := 0
			lease := wire.NewLease(chain, func() { released++ })
			view := cqltype.BytesView(chain, wire.RefOf(lease))
			slice, err := view.Slice(1, 4)
			if err != nil {
				t.Fatal(err)
			}
			if view.Len() != 6 || slice.Len() != 4 {
				t.Fatal("wrong view length")
			}
			var spans [][]byte
			if err := cqltype.EachBytes(slice, func(s wire.Span) error { spans = append(spans, s.Bytes()); return nil }); err != nil {
				t.Fatal(err)
			}
			if len(spans) != 2 || &spans[0][0] != &first[1] || &spans[1][0] != &second[0] {
				t.Fatal("slice copied payload")
			}
			var out bytes.Buffer
			if n, err := slice.WriteTo(&out); err != nil || n != 4 || out.String() != "bcde" {
				t.Fatalf("WriteTo: %d %v %q", n, err, out.String())
			}
			short := make([]byte, 2)
			if n, err := slice.CopyTo(short); n != 2 || !errors.Is(err, io.ErrShortBuffer) || string(short) != "bc" {
				t.Fatalf("short copy: %d %v %q", n, err, short)
			}
			cloned := slice.Clone()
			if !lease.Retain() {
				t.Fatal("retain failed")
			}
			lease.Release()
			if b, err := slice.At(0); err != nil || b != 'b' {
				t.Fatalf("retained view: %c %v", b, err)
			}
			lease.Release()
			if released != 1 {
				t.Fatalf("release count %d", released)
			}
			if slice.Len() != 4 {
				t.Fatal("released length changed")
			}
			if _, err := slice.At(0); !errors.Is(err, cqltype.ErrReleased) {
				t.Fatalf("At after release: %v", err)
			}
			if _, err := slice.Slice(0, 0); !errors.Is(err, cqltype.ErrReleased) {
				t.Fatalf("Slice after release: %v", err)
			}
			if _, err := slice.CopyTo(nil); !errors.Is(err, cqltype.ErrReleased) {
				t.Fatalf("CopyTo after release: %v", err)
			}
			if _, err := slice.WriteTo(io.Discard); !errors.Is(err, cqltype.ErrReleased) {
				t.Fatalf("WriteTo after release: %v", err)
			}
			foundationPanicReleased(t, func() { slice.Clone() })
			foundationPanicReleased(t, func() { slice.OwnedBytes() })
			if got := string(cloned.OwnedBytes()); got != "bcde" {
				t.Fatalf("clone lost ownership: %q", got)
			}
		})
		t.Run("ConcurrentRetained", func(t *testing.T) {
			data := []byte("immutable retained page")
			chain := FoundationChain(t, data)
			lease := wire.NewLease(chain, nil)
			view := cqltype.BytesView(chain, wire.RefOf(lease))
			var wg sync.WaitGroup
			for i := 0; i < 12; i++ {
				if !lease.Retain() {
					t.Fatal("retain failed")
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer lease.Release()
					for j := 0; j < 500; j++ {
						v, err := view.At(2)
						if err != nil || v != 'm' {
							t.Errorf("retained read %c: %v", v, err)
							return
						}
					}
				}()
			}
			lease.Release()
			wg.Wait()
			if _, err := view.At(0); !errors.Is(err, cqltype.ErrReleased) {
				t.Fatalf("final release: %v", err)
			}
		})
	})
	t.Run("Codec", func(t *testing.T) {
		t.Run("PublicChecks", func(t *testing.T) {
			typ := FoundationScalar(t, cqltype.TypeBlob)
			checks, encodes, decodes := 0, 0, 0
			codec := foundationCustomBlob{checks: &checks, encodes: &encodes, decodes: &decodes}
			encoded, err := EncodeValueBorrowed(typ, codec, []byte("value"))
			if err != nil {
				t.Fatal(err)
			}
			if checks != 1 || encodes != 1 {
				t.Fatalf("encode checks/calls: %d/%d", checks, encodes)
			}
			value, err := DecodeValue(codec, encoded)
			if err != nil || string(value) != "value" {
				t.Fatalf("decode: %q %v", value, err)
			}
			if checks != 2 || decodes != 1 {
				t.Fatalf("decode checks/calls: %d/%d", checks, decodes)
			}
			codec.reject = true
			if _, err := EncodeValueBorrowed(typ, codec, nil); err == nil || encodes != 1 {
				t.Fatal("rejected type entered encoder")
			}
			if _, err := DecodeValue(codec, encoded); err == nil || decodes != 1 {
				t.Fatal("rejected type entered decoder")
			}
			codec.reject = false
			codec.errorAfterWrite = true
			failed, err := EncodeValueBorrowed(typ, codec, []byte("partial"))
			if err == nil || cqltype.DescriptorOf(failed.Type()) != nil {
				t.Fatal("failed encoder published a value")
			}
			var nilEncoder Encoder[[]byte]
			if _, err := EncodeValueBorrowed(typ, nilEncoder, nil); err == nil {
				t.Fatal("nil encoder accepted")
			}
			var nilDecoder Decoder[[]byte]
			if _, err := DecodeValue(nilDecoder, encoded); err == nil {
				t.Fatal("nil decoder accepted")
			}
		})
	})
	t.Run("EncodeValue", func(t *testing.T) {
		t.Run("OwnedAndBorrowed", func(t *testing.T) {
			typ := FoundationScalar(t, cqltype.TypeBlob)
			source := []byte("source")
			borrowed, err := EncodeValueBorrowed(typ, BytesEncoder, source)
			if err != nil {
				t.Fatal(err)
			}
			if err := cqltype.EachBytes(borrowed.Bytes(), func(s wire.Span) error {
				if &s.Bytes()[0] != &source[0] {
					t.Error("borrowed encode copied payload")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			owned, err := EncodeValue(typ, BytesEncoder, source)
			if err != nil {
				t.Fatal(err)
			}
			clone := borrowed.Clone()
			source[0] = 'X'
			if got := string(owned.Bytes().OwnedBytes()); got != "source" {
				t.Fatalf("owned data retained source: %q", got)
			}
			if got := string(clone.Bytes().OwnedBytes()); got != "source" {
				t.Fatalf("cloned data retained source: %q", got)
			}
			// An owned parent propagates its mode through nested child writers.
			source = []byte("nested")
			builder := wire.NewBuilder(0)
			w := newOwnedValueWriter(builder, typ, false)
			if err := w.Child(typ, func(child *ValueWriter) error { return child.BorrowBytes(source) }); err != nil {
				t.Fatal(err)
			}
			if err := FinishValueWriter(w); err != nil {
				t.Fatal(err)
			}
			chain, _ := builder.Freeze()
			source[0] = 'X'
			parent, err := ReadValue(wire.NewCursor(chain), typ, nil, false)
			if err != nil {
				t.Fatal(err)
			}
			child, err := parent.Cursor().Child(typ)
			if err != nil {
				t.Fatal(err)
			}
			if got := string(child.Bytes().OwnedBytes()); got != "nested" {
				t.Fatalf("owned child retained source: %q", got)
			}
		})
		t.Run("RejectsOversized", func(t *testing.T) {
			typ := FoundationScalar(t, cqltype.TypeBlob)
			// Reuse immutable storage to represent 2 GiB without allocating its payload.
			block := make([]byte, 1<<16)
			spans := make([]wire.Span, 1<<15)
			for i := range spans {
				spans[i] = wire.Borrow(block)
			}
			chain, err := wire.NewChain(spans...)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := NewEncodedValue(typ, cqltype.Present, cqltype.BytesView(chain, wire.Ref{})); err == nil {
				t.Fatal("oversized standalone cell accepted")
			}
			builder := wire.NewBuilder(0)
			w := NewValueWriter(builder, typ, false)
			if err := w.borrowView(cqltype.BytesView(chain, wire.Ref{})); err != nil {
				t.Fatal(err)
			}
			if err := FinishValueWriter(w); err == nil {
				t.Fatal("oversized framed cell accepted")
			}
			if builder.Len() != 0 {
				t.Fatal("oversized cell was not rolled back")
			}
		})
	})
	t.Run("EncodedValue", func(t *testing.T) {
		t.Run("StatesAndLifetime", func(t *testing.T) {
			blob := FoundationScalar(t, cqltype.TypeBlob)
			integer := FoundationScalar(t, cqltype.TypeInt)
			for _, test := range []struct {
				state cqltype.CellState
				body  []byte
				typ   cqltype.DataType
			}{{cqltype.Null, []byte{1}, blob}, {cqltype.Unset, []byte{1}, blob}, {cqltype.EmptyCell, []byte{1}, integer}, {cqltype.CellState(42), nil, blob}, {cqltype.EmptyCell, nil, blob}} {
				if _, err := NewEncodedValue(test.typ, test.state, cqltype.BorrowBytes(test.body)); err == nil {
					t.Fatalf("invalid state/body accepted: %+v", test)
				}
			}
			empty, err := EncodeValueBorrowed(integer, EmptyCodec(Int32), cqltype.EmptyValueOf[int32]{Empty: true})
			if err != nil {
				t.Fatal(err)
			}
			if empty.State() != cqltype.EmptyCell || empty.Bytes().Len() != 0 {
				t.Fatal("explicit empty state lost")
			}
			decoded, err := DecodeValue(EmptyCodec(Int32), empty)
			if err != nil || !decoded.Empty {
				t.Fatalf("empty decode: %+v %v", decoded, err)
			}
			unset, err := NewEncodedValue(blob, cqltype.Unset, cqltype.BorrowBytes(nil))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeValue(BlobCodec, unset); err == nil {
				t.Fatal("unset result accepted")
			}
			chain := FoundationChain(t, []byte("leased"))
			lease := wire.NewLease(chain, nil)
			leased, err := NewEncodedValue(blob, cqltype.Present, cqltype.BytesView(chain, wire.RefOf(lease)))
			if err != nil {
				t.Fatal(err)
			}
			detached := leased.Clone()
			lease.Release()
			if _, err := DecodeValue(BlobCodec, leased); !errors.Is(err, cqltype.ErrReleased) {
				t.Fatalf("decode released: %v", err)
			}
			if _, err := NewEncodedValue(blob, cqltype.Present, leased.Bytes()); !errors.Is(err, cqltype.ErrReleased) {
				t.Fatalf("construct released: %v", err)
			}
			foundationPanicReleased(t, func() { leased.Clone() })
			if got, err := DecodeValue(BlobCodec, detached); err != nil || string(got.OwnedBytes()) != "leased" {
				t.Fatalf("owned decode: %v", err)
			}
		})
	})
	t.Run("TextView", func(t *testing.T) {
		t.Run("BorrowValidates", func(t *testing.T) {
			_, err := cqltype.BorrowText(string([]byte{0xff}))
			var validation *cqltype.ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("invalid UTF-8: %v", err)
			}
			if _, err := TextValue(string([]byte{0xff})); !errors.As(err, &validation) {
				t.Fatalf("invalid dynamic text: %v", err)
			}
		})
		t.Run("Chunks", func(t *testing.T) {
			data := []byte("a€界z")
			chain := FoundationChain(t, data[:2], data[2:4], data[4:6], data[6:])
			lease := wire.NewLease(chain, nil)
			text := cqltype.TextView(cqltype.BytesView(chain, wire.RefOf(lease)))
			if got := text.String(); got != string(data) {
				t.Fatalf("String: %q", got)
			}
			clone := text.Clone()
			lease.Release()
			foundationPanicReleased(t, func() { _ = text.String() })
			foundationPanicReleased(t, func() { text.Clone() })
			if clone.String() != string(data) {
				t.Fatal("text clone did not detach")
			}
			borrowed := CodecTestMust(cqltype.BorrowText("source"))
			if borrowed.String() != "source" {
				t.Fatal("external string view failed")
			}
			if cqltype.BorrowBytes(nil).Len() != 0 || cqltype.BorrowBytes([]byte{}).Len() != 0 {
				t.Fatal("empty borrowed bytes")
			}
		})
	})
	t.Run("ValueReader", func(t *testing.T) {
		t.Run("MalformedFraming", func(t *testing.T) {
			typ := foundationScalar(t, cqltype.TypeBlob)
			for _, data := range [][]byte{{}, {0}, {0, 0, 0}, {0, 0, 0, 2, 1}, {0x7f, 0xff, 0xff, 0xff}, {0xff, 0xff, 0xff, 0xfe}, {0xff, 0xff, 0xff, 0xfd}, {0x80, 0, 0, 0}} {
				c := wire.NewCursor(foundationChain(t, data))
				before := c.Remaining()
				_, err := ReadValue(c, typ, nil, false)
				var p *cqltype.ProtocolError
				if !errors.As(err, &p) {
					t.Fatalf("%x: expected protocol error, got %v", data, err)
				}
				if c.Remaining() != before {
					t.Fatalf("%x: failed framing advanced cursor", data)
				}
			}
			reader := NewValueReader(typ, cqltype.Present, foundationChain(t, []byte{1, 2, 3}), nil)
			c := reader.Cursor()
			if _, err := c.Uint32(); err == nil {
				t.Fatal("truncated scalar accepted")
			}
			if c.Remaining() != 3 {
				t.Fatal("failed scalar advanced")
			}
			if _, err := c.Uint8(); err == nil {
				t.Fatal("failure was not sticky")
			}
			c = reader.Cursor()
			if _, err := c.Take(-1); err == nil {
				t.Fatal("negative range accepted")
			}
			c = reader.Cursor()
			if err := c.RequireEnd(); err == nil {
				t.Fatal("trailing bytes accepted")
			}
		})
	})
	t.Run("ValueWriter", func(t *testing.T) {
		t.Run("CellStates", func(t *testing.T) {
			typ := foundationScalar(t, cqltype.TypeBlob)
			builder := wire.NewBuilder(4)
			encoders := []func(*ValueWriter) error{func(w *ValueWriter) error { return w.BorrowBytes(nil) }, func(w *ValueWriter) error { return w.BorrowBytes([]byte{}) }, func(w *ValueWriter) error { return w.Null() }, func(w *ValueWriter) error { return w.Unset() }}
			for _, encode := range encoders {
				w := NewValueWriter(builder, typ, true)
				if err := encode(w); err != nil {
					t.Fatal(err)
				}
				if err := FinishValueWriter(w); err != nil {
					t.Fatal(err)
				}
			}
			chain, _ := builder.Freeze()
			c := wire.NewCursor(chain)
			for i, want := range []cqltype.CellState{cqltype.Present, cqltype.Present, cqltype.Null, cqltype.Unset} {
				reader, err := ReadValue(c, typ, nil, true)
				if err != nil || reader.State() != want || reader.Bytes().Len() != 0 {
					t.Fatalf("cell %d state=%d err=%v", i, reader.State(), err)
				}
			}
			if err := c.RequireEnd(); err != nil {
				t.Fatal(err)
			}
		})
		t.Run("FramingTransactions", func(t *testing.T) {
			typ := foundationScalar(t, cqltype.TypeBlob)
			builder := wire.NewBuilder(3)
			payload := []byte("payload")
			writer := NewValueWriter(builder, typ, true)
			if err := writer.BorrowBytes(payload); err != nil {
				t.Fatal(err)
			}
			if err := FinishValueWriter(writer); err != nil {
				t.Fatal(err)
			}
			for _, test := range []struct {
				name   string
				encode func(*ValueWriter) error
			}{
				{"payload-null", func(w *ValueWriter) error { _ = w.BorrowBytes(nil); return w.Null() }},
				{"null-payload", func(w *ValueWriter) error { _ = w.Null(); return w.BorrowString("") }},
				{"double-null", func(w *ValueWriter) error { _ = w.Null(); return w.Null() }},
				{"nested-unset", func(w *ValueWriter) error { return w.Child(typ, func(c *ValueWriter) error { return c.Unset() }) }},
				{"child-error", func(w *ValueWriter) error {
					return w.Child(typ, func(c *ValueWriter) error { _ = c.BorrowString("partial"); return fmt.Errorf("custom failure") })
				}},
			} {
				t.Run(test.name, func(t *testing.T) {
					before := builder.Len()
					w := NewValueWriter(builder, typ, true)
					if err := test.encode(w); err == nil {
						t.Fatal("malformed cell accepted")
					}
					if err := FinishValueWriter(w); err == nil {
						t.Fatal("failed writer committed")
					}
					if builder.Len() != before {
						t.Fatal("failed cell not rolled back")
					}
				})
			}
			chain, err := builder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			out := make([]byte, chain.Len())
			chain.CopyTo(out)
			if !bytes.Equal(out, append([]byte{0, 0, 0, 7}, payload...)) {
				t.Fatalf("wire: %x", out)
			}
			borrowed := false
			chain.Each(func(s wire.Span) error {
				if len(s.Bytes()) > 0 && &s.Bytes()[0] == &payload[0] {
					borrowed = true
				}
				return nil
			})
			if !borrowed {
				t.Fatal("writer copied borrowed payload")
			}
		})
		t.Run("SegmentedCursor", func(t *testing.T) {
			typ := foundationScalar(t, cqltype.TypeBlob)
			builder := wire.NewBuilder(1)
			w := NewValueWriter(builder, typ, false)
			if err := w.Uint16(0x1234); err != nil {
				t.Fatal(err)
			}
			if err := w.Child(typ, func(c *ValueWriter) error { return c.BorrowString("abc") }); err != nil {
				t.Fatal(err)
			}
			if err := w.Uint64(0x123456789abcdef0); err != nil {
				t.Fatal(err)
			}
			if err := FinishValueWriter(w); err != nil {
				t.Fatal(err)
			}
			wireBytes := make([]byte, builder.Len())
			chain, _ := builder.Freeze()
			chain.CopyTo(wireBytes)
			parts := make([][]byte, len(wireBytes))
			for i := range wireBytes {
				parts[i] = wireBytes[i : i+1]
			}
			chain = foundationChain(t, parts...)
			lease := wire.NewLease(chain, nil)
			reader, err := ReadValue(wire.NewCursor(chain), typ, lease, false)
			if err != nil {
				t.Fatal(err)
			}
			a, b := reader.Cursor(), reader.Cursor()
			v, err := a.Uint16()
			if err != nil || v != 0x1234 {
				t.Fatalf("scalar %x %v", v, err)
			}
			child, err := a.Child(typ)
			if err != nil || string(child.Bytes().OwnedBytes()) != "abc" {
				t.Fatalf("child: %v", err)
			}
			large, err := a.Uint64()
			if err != nil || large != 0x123456789abcdef0 {
				t.Fatalf("scalar %x %v", large, err)
			}
			if err := a.RequireEnd(); err != nil {
				t.Fatal(err)
			}
			if b.Remaining() != reader.Bytes().Len() {
				t.Fatal("independent cursor advanced")
			}
			lease.Release()
			if _, err := b.Uint8(); !errors.Is(err, cqltype.ErrReleased) {
				t.Fatalf("cursor after release: %v", err)
			}
		})
	})
}

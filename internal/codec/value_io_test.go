package codec

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestV2FoundationFramingTransactions(t *testing.T) {
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
}
func TestV2FoundationCellStates(t *testing.T) {
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
}
func TestV2FoundationSegmentedCursor(t *testing.T) {
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
}
func TestV2FoundationMalformedFraming(t *testing.T) {
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
}

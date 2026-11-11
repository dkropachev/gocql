// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"fmt"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type textRow struct {
	ID    int32
	Name  cqltype.Text
	Code  cqltype.Text
	Other cqltype.Text
}

var textRows = RowInto(nil,
	Column("id", Int32, func(r *textRow) *int32 { return &r.ID }),
	Column("name", TextCodec, func(r *textRow) *cqltype.Text { return &r.Name }),
	Column("code", ASCIICodec, func(r *textRow) *cqltype.Text { return &r.Code }),
	Column("other", TextCodec, func(r *textRow) *cqltype.Text { return &r.Other }),
)

func textColumns(t testing.TB, other cqltype.Type) cqltype.Columns {
	return cqltype.NewColumns(
		cqltype.ColumnSpec{Name: "id", Type: rowTestType(t, cqltype.TypeInt)},
		cqltype.ColumnSpec{Name: "name", Type: rowTestType(t, cqltype.TypeText)},
		cqltype.ColumnSpec{Name: "code", Type: rowTestType(t, cqltype.TypeAscii)},
		cqltype.ColumnSpec{Name: "other", Type: rowTestType(t, other)},
	)
}

// textRowString renders a decoded row, copying its views while they are alive.
func textRowString(r textRow) string {
	text := func(v cqltype.Text) string {
		s, err := v.Copy()
		if err != nil {
			return "<" + err.Error() + ">"
		}
		return s
	}
	return fmt.Sprintf("%d %q %q %q", r.ID, text(r.Name), text(r.Code), text(r.Other))
}

// Text and ASCII fields compile into direct ops that decode the same views and
// fail with the same errors as their codec, across fragmented responses.
func TestDirectTextMatchesCodec(t *testing.T) {
	for _, other := range []cqltype.Type{cqltype.TypeText, cqltype.TypeVarchar} {
		columns := textColumns(t, other)
		plan := CodecTestMust(textRows.Compile(columns))
		want := []directKind{directInt, directText, directText, directText}
		for i, op := range plan.(rowPlan[textRow]).ops {
			if op.kind != want[i] {
				t.Fatalf("column %d: op kind %d, want %d", i, op.kind, want[i])
			}
		}
		reference := interfaceOnlyRows(plan)
		full := func(edit func(i int, b []byte) ([]byte, bool)) []byte {
			bodies := [][]byte{{0, 0, 0, 7}, []byte("tëxt"), []byte("ascii"), []byte("other")}
			var out []byte
			for i, b := range bodies {
				null := false
				if edit != nil {
					b, null = edit(i, b)
				}
				out = cell(out, b, null)
			}
			return out
		}
		cases := map[string][]byte{
			"values":     full(nil),
			"empty":      full(func(i int, b []byte) ([]byte, bool) { return map[bool][]byte{true: {}, false: b}[i > 0], false }),
			"null text":  full(func(i int, b []byte) ([]byte, bool) { return b, i == 1 }),
			"null ascii": full(func(i int, b []byte) ([]byte, bool) { return b, i == 2 }),
			"bad utf8": full(func(i int, b []byte) ([]byte, bool) {
				return map[bool][]byte{true: {0xff, 0xfe}, false: b}[i == 1], false
			}),
			"bad ascii": full(func(i int, b []byte) ([]byte, bool) {
				return map[bool][]byte{true: []byte("ä"), false: b}[i == 2], false
			}),
			"split rune": full(func(i int, b []byte) ([]byte, bool) {
				return map[bool][]byte{true: []byte("ëëëë"), false: b}[i == 3], false
			}),
			"truncated":   full(nil)[:14],
			"short frame": full(nil)[:2],
		}
		for name, body := range cases {
			for _, fragment := range []int{1, 3, 1024} {
				for _, expired := range []bool{false, true} {
					var spans []wire.Span
					for left := body; len(left) > 0; {
						n := min(fragment, len(left))
						spans = append(spans, wire.Borrow(left[:n]))
						left = left[n:]
					}
					chain, err := wire.NewChain(spans...)
					if err != nil {
						t.Fatal(err)
					}
					run := func(p RowPlan[textRow]) (string, string, int) {
						lease := wire.NewLease(chain, nil)
						if expired {
							lease.Release()
						} else {
							defer lease.Release()
						}
						var dst textRow
						reader := NewRowReader(columns, chain, lease)
						msg := ""
						if err := p.DecodeInto(reader, &dst); err != nil {
							msg = err.Error()
						}
						return textRowString(dst), msg, reader.index
					}
					got, gotErr, gotIndex := run(plan)
					ref, refErr, refIndex := run(reference)
					if gotErr != refErr || gotIndex != refIndex || got != ref {
						t.Fatalf("%v %s fragment=%d expired=%v:\n got %s %q %d\nwant %s %q %d", other, name, fragment, expired, got, gotErr, gotIndex, ref, refErr, refIndex)
					}
					if name == "values" && !expired && (gotErr != "" || got != `7 "tëxt" "ascii" "other"`) {
						t.Fatalf("values decoded as %s, %q", got, gotErr)
					}
				}
			}
		}
	}
}

// A text field compiled for one kind keeps the codec's own error when the
// decoded column has another kind.
func TestDirectTextForeignColumn(t *testing.T) {
	plan := CodecTestMust(textRows.Compile(textColumns(t, cqltype.TypeText)))
	foreign := textColumns(t, cqltype.TypeInt)
	body := cell(cell(cell(cell(nil, []byte{0, 0, 0, 1}, false), []byte("a"), false), []byte("b"), false), []byte{0, 0, 0, 2}, false)
	chain, _ := wire.NewChain(wire.Borrow(body))
	run := func(p RowPlan[textRow]) (string, int) {
		reader := NewRowReader(foreign, chain, nil)
		var dst textRow
		err := p.DecodeInto(reader, &dst)
		return fmt.Sprint(err), reader.index
	}
	got, gotIndex := run(plan)
	want, wantIndex := run(interfaceOnlyRows(plan))
	if got != want || gotIndex != wantIndex || got == "<nil>" {
		t.Fatalf("direct %q %d, codec %q %d", got, gotIndex, want, wantIndex)
	}
}

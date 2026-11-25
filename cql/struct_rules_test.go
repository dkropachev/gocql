// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql_test

import (
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type ruleNames struct {
	UserID   int32 // column "user_id"
	Renamed  int32 `cql:"renamed_id"` // column "renamed_id"
	Default  int32 `cql:",codec=Int32Codec"`
	Skipped  int32 `cql:"-"`
	internal int32 // unexported: never mapped
}

type ruleName string

type ruleEmbeddedNonStruct struct {
	ruleName // an unexported non-struct embed is an unexported field
	RuleText
}

// RuleText is an exported non-struct type; embedded, it is an ordinary
// field named after the type.
type RuleText string

type ruleBase struct {
	ID    int32
	Owner int32
}

type ruleOuter struct {
	ruleBase       // unexported embedded struct: its exported fields map
	Owner    int32 // hides ruleBase.Owner
}

// ruleLate declares ID after two embeds that both map id: ID hides them, as
// in Go, although they come first.
type ruleLate struct {
	ruleBase
	ruleOther
	ID int32
}

type ruleOther struct{ ID int32 }

type ruleDeep struct {
	ruleOuter
	ID int32 // hides ruleOuter.ruleBase.ID
}

// TestStructMappingRules pins how ParamsOf and RowOf match struct fields to
// columns, next to the explicit Params and Row they are equivalent to.
func TestStructMappingRules(t *testing.T) {
	integer := ruleType(t, cql.TypeInt)
	text := ruleType(t, cql.TypeText)
	cols := func(names ...string) cql.Columns {
		specs := make([]cql.ColumnSpec, len(names))
		for i, name := range names {
			specs[i] = cql.ColumnSpec{Name: name, Type: integer}
		}
		return cql.NewColumns(specs...)
	}

	t.Run("Names", func(t *testing.T) {
		columns := cols("user_id", "renamed_id", "default")
		value := ruleNames{UserID: 1, Renamed: 2, Default: 3, Skipped: 4, internal: 5}
		row := ruleDecode(t, columns, cql.MustRowOf[ruleNames](), ruleEncode(t, columns, cql.MustParamsOf[ruleNames](), value))
		want := ruleNames{UserID: 1, Renamed: 2, Default: 3}
		if row != want {
			t.Fatalf("got %+v, want %+v", row, want)
		}
		// The explicit form of the same mapping.
		explicit := cql.Row(
			cql.Column("user_id", cql.Int32Codec, func(r *ruleNames) *int32 { return &r.UserID }),
			cql.Column("renamed_id", cql.Int32Codec, func(r *ruleNames) *int32 { return &r.Renamed }),
			cql.Column("default", cql.Int32Codec, func(r *ruleNames) *int32 { return &r.Default }),
		)
		if got := ruleDecode(t, columns, explicit, ruleEncode(t, columns, cql.MustParamsOf[ruleNames](), value)); got != want {
			t.Fatalf("explicit: got %+v, want %+v", got, want)
		}
		// Names match exactly: a quoted mixed-case column needs a tag.
		for _, name := range []string{"UserID", "skipped", "internal"} {
			if _, err := cql.MustRowOf[ruleNames]().Compile(cols(name)); err == nil {
				t.Errorf("column %q matched a field", name)
			}
		}
	})
	t.Run("EmbeddedNonStruct", func(t *testing.T) {
		columns := cql.NewColumns(cql.ColumnSpec{Name: "rule_text", Type: text})
		// A named type maps like its underlying type, and a WithCodec
		// option for it takes precedence.
		value := ruleEmbeddedNonStruct{ruleName: "hidden", RuleText: "shown"}
		if row := ruleDecode(t, columns, cql.MustRowOf[ruleEmbeddedNonStruct](), ruleEncode(t, columns, cql.MustParamsOf[ruleEmbeddedNonStruct](), value)); row != (ruleEmbeddedNonStruct{RuleText: "shown"}) {
			t.Fatalf("named type: %+v", row)
		}
		opt := cql.WithCodec(cql.ConvertCodec(cql.StringCodec,
			func(v RuleText) string { return string(v) }, func(v string) RuleText { return RuleText(v) }))
		row := ruleDecode(t, columns, cql.MustRowOf[ruleEmbeddedNonStruct](opt), ruleEncode(t, columns, cql.MustParamsOf[ruleEmbeddedNonStruct](opt), value))
		if row != (ruleEmbeddedNonStruct{RuleText: "shown"}) {
			t.Fatalf("got %+v", row)
		}
	})
	t.Run("Promotion", func(t *testing.T) {
		columns := cols("id", "owner")
		value := ruleDeep{ID: 1, ruleOuter: ruleOuter{Owner: 2, ruleBase: ruleBase{ID: 3, Owner: 4}}}
		row := ruleDecode(t, columns, cql.MustRowOf[ruleDeep](), ruleEncode(t, columns, cql.MustParamsOf[ruleDeep](), value))
		if want := (ruleDeep{ID: 1, ruleOuter: ruleOuter{Owner: 2}}); row != want {
			t.Fatalf("got %+v, want %+v", row, want)
		}
		outer := ruleDecode(t, columns, cql.MustRowOf[ruleOuter](), ruleEncode(t, columns, cql.MustParamsOf[ruleOuter](), ruleOuter{Owner: 2, ruleBase: ruleBase{ID: 3, Owner: 4}}))
		if want := (ruleOuter{Owner: 2, ruleBase: ruleBase{ID: 3}}); outer != want {
			t.Fatalf("unexported embed: got %+v, want %+v", outer, want)
		}
		late := ruleDecode(t, columns, cql.MustRowOf[ruleLate](), ruleEncode(t, columns, cql.MustParamsOf[ruleLate](), ruleLate{ID: 1, ruleBase: ruleBase{ID: 2, Owner: 3}, ruleOther: ruleOther{ID: 4}}))
		if want := (ruleLate{ID: 1, ruleBase: ruleBase{Owner: 3}}); late != want {
			t.Fatalf("late shallower field: got %+v, want %+v", late, want)
		}
	})
	// A field without a column is skipped in both directions; a column
	// without a field fails. Explicit Params and Row require both.
	t.Run("Subsets", func(t *testing.T) {
		subset := cols("user_id")
		if _, err := cql.MustParamsOf[ruleNames]().Compile(subset); err != nil {
			t.Fatalf("ParamsOf: %v", err)
		}
		if _, err := cql.MustRowOf[ruleNames]().Compile(subset); err != nil {
			t.Fatalf("RowOf: %v", err)
		}
		extra := cols("user_id", "other")
		for name, compile := range map[string]func() error{
			"ParamsOf": func() error { _, err := cql.MustParamsOf[ruleNames]().Compile(extra); return err },
			"RowOf":    func() error { _, err := cql.MustRowOf[ruleNames]().Compile(extra); return err },
		} {
			if err := compile(); err == nil || !strings.Contains(err.Error(), "other") {
				t.Errorf("%s: extra column: %v", name, err)
			}
		}
		explicitRow := cql.Row(
			cql.Column("user_id", cql.Int32Codec, func(r *ruleNames) *int32 { return &r.UserID }),
			cql.Column("renamed_id", cql.Int32Codec, func(r *ruleNames) *int32 { return &r.Renamed }),
		)
		if _, err := explicitRow.Compile(subset); err == nil {
			t.Error("Row: a field without a column compiled")
		}
		explicitParams := cql.Params(
			cql.Param("user_id", cql.Int32Codec, func(r ruleNames) int32 { return r.UserID }),
			cql.Param("renamed_id", cql.Int32Codec, func(r ruleNames) int32 { return r.Renamed }),
		)
		if _, err := explicitParams.Compile(subset); err == nil {
			t.Error("Params: a field without a column compiled")
		}
		wider := cols("user_id", "renamed_id", "extra")
		if _, err := explicitRow.Compile(wider); err == nil {
			t.Error("Row: a column without a field compiled")
		}
		if _, err := explicitParams.Compile(wider); err == nil {
			t.Error("Params: a column without a field compiled")
		}
	})
	// A named marker repeated in the statement binds the same field.
	t.Run("RepeatedMarker", func(t *testing.T) {
		columns := cols("user_id", "user_id")
		chain := ruleEncode(t, columns, cql.MustParamsOf[ruleNames](), ruleNames{UserID: 9})
		pair := ruleDecode(t, columns, cql.PairRows(cql.OneColumn(cql.Int32Codec), cql.OneColumn(cql.Int32Codec)), chain)
		if pair.First != 9 || pair.Second != 9 {
			t.Fatalf("got %+v", pair)
		}
		if _, err := cql.MustRowOf[ruleNames]().Compile(columns); err == nil {
			t.Fatal("RowOf mapped a duplicate result column")
		}
	})
	t.Run("Rejected", func(t *testing.T) {
		type tagUnexported struct {
			hidden int32 `cql:"hidden"` // the field under test
		}
		type embeddedPointer struct{ *ruleBase }
		type embeddedTagged struct {
			ruleBase `cql:"base"`
		}
		type sameDepth struct {
			A int32 `cql:"x"`
			B int32 `cql:"x"`
		}
		type unknownOption struct {
			A int32 `cql:"a,omitempty"`
		}
		type noCodec struct{ A complex64 }
		type wrongCodec struct {
			A string `cql:"a,codec=Int32Codec"`
		}
		for name, err := range map[string]error{
			"tagged unexported field": func() error { _, err := cql.RowOf[tagUnexported](); return err }(),
			"embedded pointer":        func() error { _, err := cql.ParamsOf[embeddedPointer](); return err }(),
			"only the tag":            func() error { _, err := cql.RowOf[embeddedTagged](); return err }(),
			"both map column":         func() error { _, err := cql.ParamsOf[sameDepth](); return err }(),
			"unknown cql tag option":  func() error { _, err := cql.RowOf[unknownOption](); return err }(),
			"no codec for type":       func() error { _, err := cql.ParamsOf[noCodec](); return err }(),
			"not a built-in codec":    func() error { _, err := cql.RowOf[wrongCodec](); return err }(),
			"not a struct":            func() error { _, err := cql.RowOf[int](); return err }(),
			"nil option":              func() error { _, err := cql.RowOf[ruleNames](nil); return err }(),
		} {
			if err == nil || !strings.Contains(err.Error(), name) {
				t.Errorf("%s: got %v", name, err)
			}
		}
		// A tagged unexported field fails inside an embedded struct too.
		type embedsTagUnexported struct{ tagUnexported }
		if _, err := cql.ParamsOf[embedsTagUnexported](); err == nil || !strings.Contains(err.Error(), "tagged unexported field") {
			t.Errorf("embedded tagged unexported field: got %v", err)
		}
		func() {
			defer func() {
				if recover() == nil {
					t.Error("MustParamsOf did not panic")
				}
			}()
			cql.MustParamsOf[noCodec]()
		}()
		func() {
			defer func() {
				if recover() == nil {
					t.Error("MustRowOf did not panic")
				}
			}()
			cql.MustRowOf[noCodec]()
		}()
	})
}

func ruleType(t *testing.T, kind cql.Type) cql.DataType {
	t.Helper()
	typ, err := cql.ScalarType(kind)
	if err != nil {
		t.Fatal(err)
	}
	return typ
}

func ruleEncode[P any](t *testing.T, columns cql.Columns, params cql.ParamEncoder[P], value P) wire.Chain {
	t.Helper()
	plan, err := params.Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	chain, err := cql.EncodeParamsForTest(columns, plan, value, true)
	if err != nil {
		t.Fatal(err)
	}
	return chain
}

func ruleDecode[R any](t *testing.T, columns cql.Columns, rows cql.RowDecoder[R], chain wire.Chain) R {
	t.Helper()
	plan, err := rows.Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	row, err := cql.DecodeRowForTest(columns, plan, chain)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

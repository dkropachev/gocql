// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import "testing"

// TestStatementConstructors checks that NewStatement, Select and Write infer
// their type arguments from the codecs, NoParams included, and set the kind
// and idempotence each documents.
func TestStatementConstructors(t *testing.T) {
	params := OneParam(Int32Codec)
	rows := OneColumn(StringCodec)

	var s Statement[int32, string] = NewStatement("SELECT v FROM t WHERE k=?", params, rows)
	if s.CQL != "SELECT v FROM t WHERE k=?" || s.Params == nil || s.Rows == nil || s.Kind != UnknownStatement || s.Idempotent {
		t.Errorf("NewStatement = %+v", s)
	}

	var sel Statement[EmptyParams, string] = Select("SELECT v FROM t", NoParams, rows)
	if sel.Params == nil || sel.Rows == nil || sel.Kind != OrdinaryStatement || !sel.Idempotent {
		t.Errorf("Select = %+v", sel)
	}

	var w Statement[int32, EmptyRow] = Write("DELETE FROM t WHERE k=?", params)
	if w.Params == nil || w.Rows == nil || w.Kind != UnknownStatement || w.Idempotent {
		t.Errorf("Write = %+v", w)
	}
	if _, err := w.Rows.Compile(Columns{}); err != nil {
		t.Errorf("Write rows: %v", err)
	}

	c := Command("TRUNCATE t")
	if c.Params == nil || c.Rows == nil || c.Kind != UnknownStatement {
		t.Errorf("Command = %+v", c)
	}
	if _, err := NoParams.Compile(Columns{}); err != nil {
		t.Errorf("NoParams: %v", err)
	}
}

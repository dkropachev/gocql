// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import "testing"

// TestStatementConstructors checks that NewStatement, Select and Write infer
// their type arguments from the codecs, NoParams included, and set the kind
// and idempotence each documents, and that WithIdempotent copies.
func TestStatementConstructors(t *testing.T) {
	params := OneParam(Int32Codec)
	rows := OneColumn(StringCodec)

	var s Statement[int32, string] = NewStatement("SELECT v FROM t WHERE k=?", params, rows)
	if s.CQL != "SELECT v FROM t WHERE k=?" || s.Params == nil || s.Rows == nil || s.Idempotent {
		t.Errorf("NewStatement = %+v", s)
	}

	var sel Statement[EmptyParams, string] = Select("SELECT v FROM t", NoParams, rows)
	if sel.Params == nil || sel.Rows == nil || !sel.Idempotent {
		t.Errorf("Select = %+v", sel)
	}

	var w Statement[int32, EmptyRow] = Write("DELETE FROM t WHERE k=?", params)
	if w.Params == nil || w.Rows == nil || w.Idempotent {
		t.Errorf("Write = %+v", w)
	}
	if _, err := w.Rows.Compile(Columns{}); err != nil {
		t.Errorf("Write rows: %v", err)
	}

	if i := w.WithIdempotent(true); !i.Idempotent || w.Idempotent || i.CQL != w.CQL {
		t.Errorf("WithIdempotent = %+v, original %+v", i, w)
	}
	if m := w.WithBindMarkers(ColumnSpec{Name: "k", Type: IntType}); m.BindMarkers.Len() != 1 || w.BindMarkers.Len() != 0 {
		t.Errorf("WithBindMarkers = %+v, original %+v", m, w)
	}
	stream := NewStreamStatement("SELECT v FROM t WHERE k=?", params, StreamOneColumn(StreamInt32))
	if stream.CQL != "SELECT v FROM t WHERE k=?" || stream.Params == nil || stream.Rows == nil || stream.Idempotent {
		t.Errorf("NewStreamStatement = %+v", stream)
	}
	if i := stream.WithIdempotent(true); !i.Idempotent || stream.Idempotent {
		t.Errorf("StreamStatement.WithIdempotent = %+v", i)
	}
	if _, err := NoParams.Compile(Columns{}); err != nil {
		t.Errorf("NoParams: %v", err)
	}
}

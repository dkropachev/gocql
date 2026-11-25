// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

import (
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/link"
)

func TestMirrorLayouts(t *testing.T) {
	for name, err := range map[string]error{
		"Bytes":     link.CheckLayout[cql.Bytes, bytesRep](),
		"Columns":   link.CheckLayout[cql.Columns, columnsRep](),
		"DataType":  link.CheckLayout[cql.DataType, dataTypeRep](),
		"PageState": link.CheckLayout[cql.PageState, pageStateRep](),
	} {
		if err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestPageStateMirror(t *testing.T) {
	if p := PageStateOwned([]byte("x")); !p.More() || string(PageToken(p)) != "x" {
		t.Fatalf("PageStateOwned = %+v", p)
	}
	if p := CompletePageState(); !p.Complete() || p.More() {
		t.Fatalf("CompletePageState = %+v", p)
	}
	b := cql.BorrowBytes([]byte("abc"))
	if v := BytesView(BytesChain(b), BytesLease(b)); string(ownedBytesForTest(v)) != "abc" {
		t.Fatalf("BytesView(BytesChain(b)) = %q", ownedBytesForTest(v))
	}
}

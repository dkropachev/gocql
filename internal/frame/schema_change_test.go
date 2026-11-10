// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package frame

import "testing"

func TestSchemaChangeImplementers(t *testing.T) {
	for _, f := range []any{
		&SchemaChangeKeyspace{Keyspace: "k"},
		&SchemaChangeTable{Keyspace: "k"},
		&SchemaChangeType{Keyspace: "k"},
		&SchemaChangeFunction{Keyspace: "k"},
		&SchemaChangeAggregate{Keyspace: "k"},
	} {
		sc, ok := f.(SchemaChange)
		if !ok {
			t.Fatalf("%T does not implement SchemaChange", f)
		}
		if sc.SchemaKeyspace() != "k" {
			t.Fatalf("%T.SchemaKeyspace() = %q", f, sc.SchemaKeyspace())
		}
	}
	for _, f := range []any{&TopologyChangeEventFrame{}, &StatusChangeEventFrame{}, &ClientRoutesChanged{}} {
		if _, ok := f.(SchemaChange); ok {
			t.Fatalf("%T must not implement SchemaChange", f)
		}
	}
}

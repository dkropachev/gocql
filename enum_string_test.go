// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql_test

import (
	"fmt"
	"testing"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/endpoint"
	"github.com/scylladb/gocql/v2/event"
	"github.com/scylladb/gocql/v2/exec"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/retry"
)

// TestEnumStrings pins the names the enum-like types print, and the
// Type(n) fallback for a value a type does not define. cqlerr.ErrorCode is
// an error rather than a Stringer and prints an unknown code in hexadecimal.
func TestEnumStrings(t *testing.T) {
	for _, tc := range []struct {
		value fmt.Stringer
		want  string
	}{
		{cql.NoCompression, "none"},
		{cql.SnappyCompression, "snappy"},
		{cql.Compression(9), "Compression(9)"},
		{cql.UnloggedBatch, "unlogged"},
		{cql.BatchKind(9), "BatchKind(9)"},
		{cql.ConditionalStatement, "conditional"},
		{cql.StatementKind(9), "StatementKind(9)"},
		{cql.EmptyCell, "empty"},
		{cql.CellState(9), "CellState(9)"},
		{event.Kind(0), "unknown"},
		{event.ClientRoute, "client route"},
		{event.Kind(99), "Kind(99)"},
		{metadata.SchemaFull, "full"},
		{metadata.Mode(9), "Mode(9)"},
		{retry.IgnoreWrite, "ignore write"},
		{retry.Action(9), "Action(9)"},
		{exec.SerialLocal, "local serial"},
		{exec.SerialOverride(9), "SerialOverride(9)"},
		{endpoint.ConvictionControl, "control"},
		{endpoint.ConvictionSource(9), "ConvictionSource(9)"},
		{cqlerr.OpTypeWrite, "write"},
		{cqlerr.OpType(9), "OpType(9)"},
		{gocql.ScyllaV4, "ScyllaV4"},
		{gocql.Dialect(0), "Dialect(0)"},
		{cql.LocalQuorum, "LOCAL_QUORUM"},
		{cql.Consistency(66), "Consistency(66)"},
		{cql.TypeUDT, "udt"},
		{cql.TypeBigInt, "bigint"},
		{cql.Type(255), "Type(255)"},
		{cqlerr.MaybeExecuted, "maybe executed"},
		{cqlerr.ExecutionStatus(9), "ExecutionStatus(9)"},
		{cqlerr.KindUnknown, "unknown"},
		{cqlerr.KindServer, "server"},
		{cqlerr.Kind(99), "Kind(99)"},
		{metadata.ColumnKindUnknown, "unknown"},
		{metadata.ColumnStatic, "static"},
		{metadata.ColumnKind(99), "ColumnKind(99)"},
	} {
		if got := tc.value.String(); got != tc.want {
			t.Errorf("%T(%d).String() = %q, want %q", tc.value, tc.value, got, tc.want)
		}
	}
}

// TestEventKindZeroRejected checks that the zero event.Kind is not a server
// event.
func TestEventKindZeroRejected(t *testing.T) {
	if _, err := config.New("127.0.0.1").WithIgnoreEvents(event.Kind(0)).Build(); err == nil {
		t.Fatal("WithIgnoreEvents accepted the zero event.Kind")
	}
}

// TestErrorCodeUnknown checks how an ErrorCode the driver does not define
// prints.
func TestErrorCodeUnknown(t *testing.T) {
	if got := cqlerr.ErrorCode(0x1234).Error(); got != "server error 0x1234" {
		t.Fatalf("ErrorCode(0x1234).Error() = %q", got)
	}
}

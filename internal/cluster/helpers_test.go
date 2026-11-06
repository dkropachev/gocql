//go:build unit
// +build unit

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cluster

import (
	"reflect"
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/protocol"
)

// assertResolvedType compares got against want, except for a VectorType's
// NativeType: that one carries the bare VectorType prefix instead of the column's
// full spec, which is #1085. Pinning it here would make closing that issue an edit
// to these tests. Everything the guards under test actually decide -- the element
// type and the dimensions -- is still compared exactly.
func assertResolvedType(t *testing.T, typ string, got, want cqltype.TypeInfo) {
	t.Helper()

	wantVec, ok := want.(cqltype.VectorType)
	if !ok {
		if !reflect.DeepEqual(got, want) {
			t.Errorf("getCassandraLongType(%q) = %#v, want %#v", typ, got, want)
		}
		return
	}

	gotVec, ok := got.(cqltype.VectorType)
	if !ok {
		t.Fatalf("getCassandraLongType(%q) = %#v, want a VectorType", typ, got)
	}
	if !reflect.DeepEqual(gotVec.SubType, wantVec.SubType) || gotVec.Dimensions != wantVec.Dimensions {
		t.Errorf("getCassandraLongType(%q) resolved to element type %#v with %d dimensions, want %#v with %d",
			typ, gotVec.SubType, gotVec.Dimensions, wantVec.SubType, wantVec.Dimensions)
	}
}

// TestGetCassandraLongTypeRejectsShortSplits pins that a composite type name with
// a missing or truncated argument list degrades to an opaque custom type instead
// of panicking. The well-formed names keep the degrade path from swallowing them.
func TestGetCassandraLongTypeRejectsShortSplits(t *testing.T) {
	t.Parallel()

	const p = cqltype.ApacheCassandraTypePrefix

	tests := []struct {
		name string
		typ  string
		want cqltype.TypeInfo
	}{
		{name: "vector without arguments", typ: p + "VectorType", want: cqltype.NewCustomType(0, cqltype.TypeCustom, p+"VectorType")},
		{name: "vector without dimensions", typ: p + "VectorType(" + p + "FloatType)", want: cqltype.NewCustomType(0, cqltype.TypeCustom, p+"VectorType("+p+"FloatType)")},
		{name: "udt without arguments", typ: p + "UserType", want: cqltype.NewCustomType(0, cqltype.TypeCustom, p+"UserType")},
		{name: "udt without a name", typ: p + "UserType(gocql_test)", want: cqltype.NewCustomType(0, cqltype.TypeCustom, p+"UserType(gocql_test)")},
		{name: "udt field without a type", typ: p + "UserType(gocql_test,706572736f6e,616765)", want: cqltype.NewCustomType(0, cqltype.TypeCustom, p+"UserType(gocql_test,706572736f6e,616765)")},
		{name: "vector without an element type", typ: p + "VectorType(, 3)", want: cqltype.NewCustomType(0, cqltype.TypeCustom, p+"VectorType(, 3)")},
		{
			// The outer vector is well-formed; only its element type degrades.
			name: "vector whose element has no element type",
			typ:  p + "VectorType(" + p + "VectorType(, 3), 2)",
			want: cqltype.VectorType{
				SubType:    cqltype.NewCustomType(0, cqltype.TypeCustom, p+"VectorType(, 3)"),
				Dimensions: 2,
			},
		},
		{
			name: "vector of float",
			typ:  p + "VectorType(" + p + "FloatType, 3)",
			want: cqltype.VectorType{
				SubType:    cqltype.NewNativeType(0, cqltype.TypeFloat),
				Dimensions: 3,
			},
		},
		{
			name: "udt",
			typ:  p + "UserType(gocql_test,706572736f6e,616765:" + p + "Int32Type)",
			want: cqltype.UDTTypeInfo{
				NativeType: cqltype.NewNativeType(0, cqltype.TypeUDT),
				KeySpace:   "gocql_test",
				Name:       "person",
				Elements:   []cqltype.UDTField{{Name: "age", Type: cqltype.NewNativeType(0, cqltype.TypeInt)}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			assertResolvedType(t, test.typ, InternalGetCassandraLongType(test.typ, 0, logging.InternalNopLogger{}), test.want)
		})
	}
}

// TestGetCassandraLongTypeRejectsNonPositiveDimensions pins that a vector
// dimension must be positive. A nested vector is where a negative one gets in,
// since readVectorTypeInfo validates only the outer spec.
func TestGetCassandraLongTypeRejectsNonPositiveDimensions(t *testing.T) {
	t.Parallel()

	const p = cqltype.ApacheCassandraTypePrefix

	tests := []struct {
		name string
		typ  string
		want cqltype.TypeInfo
	}{
		{name: "negative dimensions", typ: p + "VectorType(" + p + "FloatType, -3)", want: cqltype.NewCustomType(0, cqltype.TypeCustom, p+"VectorType("+p+"FloatType, -3)")},
		{name: "zero dimensions", typ: p + "VectorType(" + p + "FloatType, 0)", want: cqltype.NewCustomType(0, cqltype.TypeCustom, p+"VectorType("+p+"FloatType, 0)")},
		{
			// The outer vector is well-formed; only its element type degrades.
			name: "negative dimensions nested in a valid vector",
			typ:  p + "VectorType(" + p + "VectorType(" + p + "FloatType, -3), 2)",
			want: cqltype.VectorType{
				SubType:    cqltype.NewCustomType(0, cqltype.TypeCustom, p+"VectorType("+p+"FloatType, -3)"),
				Dimensions: 2,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			assertResolvedType(t, test.typ, InternalGetCassandraLongType(test.typ, 0, logging.InternalNopLogger{}), test.want)
		})
	}
}

// TestAsVectorTypeRejectsNonPositiveDimensions covers the third parser of the same
// grammar, which backs MapScan and SliceMap through goType.
func TestAsVectorTypeRejectsNonPositiveDimensions(t *testing.T) {
	t.Parallel()

	const p = cqltype.ApacheCassandraTypePrefix

	tests := []struct {
		name    string
		typ     string
		wantOK  bool
		wantDim int
	}{
		{name: "negative dimensions", typ: p + "VectorType(" + p + "FloatType, -3)"},
		{name: "zero dimensions", typ: p + "VectorType(" + p + "FloatType, 0)"},
		{name: "vector of float", typ: p + "VectorType(" + p + "FloatType, 3)", wantOK: true, wantDim: 3},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, ok := InternalAsVectorType(cqltype.NewCustomType(protocol.ProtoVersion4, cqltype.TypeCustom, test.typ))
			if ok != test.wantOK {
				t.Fatalf("asVectorType(%q) ok = %t, want %t (got %#v)", test.typ, ok, test.wantOK, got)
			}
			if ok && got.Dimensions != test.wantDim {
				t.Errorf("asVectorType(%q).Dimensions = %d, want %d", test.typ, got.Dimensions, test.wantDim)
			}
		})
	}
}

// TestGetCassandraLongTypeLogsWhyADimensionWasRejected pins the reason, not just the
// rejection, so that a caller passing a real logger can tell the two apart.
func TestGetCassandraLongTypeLogsWhyADimensionWasRejected(t *testing.T) {
	t.Parallel()

	const p = cqltype.ApacheCassandraTypePrefix

	tests := []struct {
		name string
		typ  string
		want string
	}{
		{name: "not a number", typ: p + "VectorType(" + p + "FloatType, abc)", want: "strconv.Atoi"},
		{name: "negative dimensions", typ: p + "VectorType(" + p + "FloatType, -3)", want: "expecting a positive value"},
		{name: "zero dimensions", typ: p + "VectorType(" + p + "FloatType, 0)", want: "expecting a positive value"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logger := &logging.InternalTestLogger{}
			InternalGetCassandraLongType(test.typ, 0, logger)

			got := logger.String()
			if !strings.Contains(got, test.want) {
				t.Errorf("getCassandraLongType(%q) logged %q, want it to contain %q", test.typ, got, test.want)
			}
			if strings.Contains(got, "<nil>") {
				t.Errorf("getCassandraLongType(%q) logged %q, which reports no reason", test.typ, got)
			}
		})
	}
}

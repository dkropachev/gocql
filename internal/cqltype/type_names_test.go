// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

import (
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/logging"
)

const (
	marshalInt  = ApacheCassandraTypePrefix + "Int32Type"
	marshalList = ApacheCassandraTypePrefix + "ListType("
)

// nestedLongType nests levels list types around int, wrapped in a vector of
// dimension 2 when vector is set.
func nestedLongType(levels int, vector bool) string {
	name := strings.Repeat(marshalList, levels) + marshalInt + strings.Repeat(")", levels)
	if vector {
		name = ApacheCassandraTypePrefix + "VectorType(" + name + ", 2)"
	}
	return name
}

func TestSplitCompositeTypes(t *testing.T) {
	t.Parallel()
	const tuple = ApacheCassandraTypePrefix + "TupleType"
	for _, test := range []struct {
		name string
		want []string
	}{
		{tuple + "(a, b)", []string{"a", "b"}},
		{tuple + "(a(b,c), d)", []string{"a(b,c)", "d"}},
		{tuple + "( a(b,c) ,, d(e) )", []string{"a(b,c)", "d(e)"}},
		{tuple + "(a(b(c,d),e),f)", []string{"a(b(c,d),e)", "f"}},
		{tuple + "(,a(b))", []string{"a(b)"}},
	} {
		if got := SplitJavaCompositeTypes(test.name, tuple); !reflect.DeepEqual(got, test.want) {
			t.Errorf("%s: got %q, want %q", test.name, got, test.want)
		}
	}
}

// TestLongTypeNestingDepth checks that a type name nested deeper than
// MaxLongTypeDepth parses as a custom type below that depth instead of
// recursing without bound.
func TestLongTypeNestingDepth(t *testing.T) {
	t.Parallel()
	depthOf := func(info TypeInfo) (int, TypeInfo) {
		depth := 0
		for {
			c, ok := info.(CollectionType)
			if !ok {
				return depth, info
			}
			depth, info = depth+1, c.Elem
		}
	}
	for _, levels := range []int{MaxLongTypeDepth, MaxLongTypeDepth + 1, 10000} {
		depth, leaf := depthOf(GetCassandraLongType(nestedLongType(levels, false), 4, logging.Nop()))
		if levels <= MaxLongTypeDepth {
			if depth != levels || leaf.Type() != cql.TypeInt {
				t.Fatalf("%d levels: parsed %d levels down to %v", levels, depth, leaf.Type())
			}
			continue
		}
		if depth > MaxLongTypeDepth+1 || leaf.Type() != cql.TypeCustom {
			t.Fatalf("%d levels: parsed %d levels down to %v, want a custom type within the limit", levels, depth, leaf.Type())
		}
	}
	vector, ok := AsVectorType(NewCustomType(4, cql.TypeCustom, nestedLongType(10000, true)))
	if !ok || vector.Dimensions != 2 {
		t.Fatalf("deep vector: %v %v", vector, ok)
	}
}

// TestLongTypeParseLinear checks that parsing a long type name does work
// proportional to its length: the names come from the server, which can
// send up to 64 KiB, and a quadratic parser spent gigabytes on one.
func TestLongTypeParseLinear(t *testing.T) {
	const tuple = ApacheCassandraTypePrefix + "TupleType("
	for name, input := range map[string]string{
		"deep":  nestedLongType(1600, true),
		"wide":  tuple + strings.Repeat(nestedLongType(3, false)+",", 400) + marshalInt + ")",
		"nests": tuple + strings.Repeat(tuple+marshalInt+",", 60) + marshalInt + strings.Repeat(")", 61),
	} {
		t.Run(name, func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			info := GetCassandraLongType(input, 4, logging.Nop())
			_, _ = AsVectorType(info)
			runtime.ReadMemStats(&after)
			// Parsed types and part slices cost a few hundred bytes per
			// level; copying segments a character at a time cost the
			// square of the name length.
			if allocated := after.TotalAlloc - before.TotalAlloc; allocated > uint64(64*len(input)) {
				t.Fatalf("parsing %d bytes allocated %d bytes", len(input), allocated)
			}
		})
	}
}

func BenchmarkLongTypeParse(b *testing.B) {
	name := nestedLongType(1600, true)
	b.SetBytes(int64(len(name)))
	b.ReportAllocs()
	for b.Loop() {
		GetCassandraLongType(name, 4, logging.Nop())
	}
}

// FuzzCassandraLongType feeds server type names to the long type parsers,
// which must not panic.
func FuzzCassandraLongType(f *testing.F) {
	f.Add(nestedLongType(3, true))
	f.Add(ApacheCassandraTypePrefix + "MapType(" + marshalInt + "," + marshalInt + ")")
	f.Add(ApacheCassandraTypePrefix + "UserType(ks,61,62:" + marshalInt + ")")
	f.Add(ApacheCassandraTypePrefix + "TupleType(" + marshalInt + "," + nestedLongType(2, false) + ")")
	f.Add(ApacheCassandraTypePrefix + "FrozenType(" + marshalInt + ")")
	f.Add(ApacheCassandraTypePrefix + "VectorType(" + marshalInt + ",-1)")
	f.Add(ApacheCassandraTypePrefix + "ListType")
	f.Fuzz(func(t *testing.T, name string) {
		if len(name) > 1<<16 {
			return // a [string] holds at most 64 KiB
		}
		info := GetCassandraLongType(name, 4, logging.Nop())
		_, _ = AsVectorType(info)
		_, _ = AsVectorType(NewCustomType(4, cql.TypeCustom, name))
	})
}

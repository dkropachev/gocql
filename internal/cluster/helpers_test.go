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
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
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

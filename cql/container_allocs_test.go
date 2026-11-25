//go:build !race

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cql

import (
	"testing"
)

// sync.Pool deliberately drops entries under -race. Measure allocation counts
// without that instrumentation; ownership and concurrency tests also run with it.
func TestContainerAllocs(t *testing.T) {
	t.Run("TupleDecode", func(t *testing.T) {
		integer := FoundationScalar(t, KindInt)
		tuple := CodecTestMust(TupleType(integer))
		list := CodecTestMust(ListType(tuple))
		allocated := func(count int) float64 {
			encoded := CodecTestMust(encodeValueBorrowed(list, ListEncoder(TupleEncoder(OneParam(Int32Codec))), make([]int32, count)))
			decoder := CodecTestMust(compileValueDecoder(ListDecoder(TupleDecoder(OneColumn(Int32Codec))), list))
			reader := valueReaderFromLease(list, Present, bytesChain(encoded.body), nil)
			return testing.AllocsPerRun(20, func() {
				values, err := decoder.Decode(reader)
				if err != nil || len(values) != count {
					t.Fatal(values, err)
				}
			})
		}
		small, large := allocated(1), allocated(1024)
		if large > small {
			t.Fatalf("tuple scratch allocations scale with elements: %g -> %g", small, large)
		}
	})
}

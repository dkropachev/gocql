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

package wire

import (
	"fmt"
	"testing"
)

func BenchmarkCursor(b *testing.B) {
	b.Run("CrossSpanScalar", func(b *testing.B) {
		c := segmented(b, []byte{1, 2, 3, 4, 5, 6, 7, 8}, 1)
		b.ReportAllocs()
		b.SetBytes(8)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cur := NewCursor(c)
			allocationScalar, _ = cur.Uint64()
		}
	})
	b.Run("CellsAndFragments", func(b *testing.B) {
		for _, cells := range []int{256, 8192} {
			for _, fragments := range []int{1, 256, 65536} {
				b.Run(fmt.Sprintf("cells=%d/fragments=%d", cells, fragments), func(b *testing.B) {
					data := make([]byte, 65536)
					spans := make([]Span, fragments)
					width := len(data) / fragments
					for i := range spans {
						spans[i] = Borrow(data[i*width : (i+1)*width])
					}
					c, _ := NewChain(spans...)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						cursor := NewCursor(c)
						for j := 0; j < cells; j++ {
							if _, e := cursor.Take(int64(len(data) / cells)); e != nil {
								b.Fatal(e)
							}
						}
					}
				})
			}
		}
	})
}

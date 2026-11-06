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

func BenchmarkBuilder(b *testing.B) {
	b.Run("BorrowLargePayload", func(b *testing.B) {
		data := make([]byte, 1<<20)
		builder := NewBuilder(16)
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = builder.Borrow(Borrow(data))
			allocationChain, _ = builder.Freeze()
		}
	})
	b.Run("ScalarAndBorrow", func(b *testing.B) {
		data := make([]byte, 1<<20)
		builder := NewBuilder(16)
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			mark := builder.Mark()
			_ = builder.Uint32(0)
			_ = builder.Borrow(Borrow(data))
			_ = builder.PatchUint32(mark, uint32(len(data)))
			allocationChain, _ = builder.Freeze()
		}
	})
	// Reports arena/slab/descriptor work separately from per-scalar serialization.
	b.Run("ScalarArena", func(b *testing.B) {
		for _, count := range []int{1, 1024} {
			b.Run(fmt.Sprint(count), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(8 * count))
				for i := 0; i < b.N; i++ {
					builder := NewBuilder(8 * count)
					for j := 0; j < count; j++ {
						if e := builder.Uint64(uint64(j)); e != nil {
							b.Fatal(e)
						}
					}
					if _, e := builder.Freeze(); e != nil {
						b.Fatal(e)
					}
				}
				b.ReportMetric(float64(count), "scalar-writes/op")
			})
		}
	})
}

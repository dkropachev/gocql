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

func BenchmarkRangeLookupScaling(b *testing.B) {
	for _, fragments := range []int{1, 1024, 65536} {
		b.Run(fmt.Sprint(fragments), func(b *testing.B) {
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
				off := (i * 7919) % (len(data) - 16)
				v, e := c.Slice(int64(off), 16)
				if e != nil || v.Len() != 16 {
					b.Fatal(e)
				}
			}
		})
	}
}

func BenchmarkBorrowLargePayload(b *testing.B) {
	data := make([]byte, 1<<20)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		allocationChain, _ = NewChain(Borrow(data))
	}
}

func BenchmarkChainSlice(b *testing.B) {
	data := make([]byte, 1<<20)
	c := chain(b, Borrow(data[:1<<19]), Borrow(data[1<<19:]))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		allocationChain, _ = c.Slice(1, int64(len(data)-2))
	}
}

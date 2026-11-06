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

package wire

import (
	"context"
	"fmt"
	"math"
	"testing"
)

func BenchmarkStream(b *testing.B) {
	b.Run("FinalRanges", func(b *testing.B) {
		for _, size := range []int{32, 4096, 500 << 10} {
			b.Run(fmt.Sprint(size), func(b *testing.B) {
				dst := make([]byte, size)
				source := &repeatedStreamReader{bytes: math.MaxInt64}
				s, _ := NewStream(context.Background(), source, math.MaxInt32, StreamLimits{MaxBytes: math.MaxInt32})
				if e := s.Child(int64(size), func(c *Stream) error { return c.ReadFull(dst) }); e != nil {
					b.Fatal(e)
				}
				b.ReportAllocs()
				b.SetBytes(int64(size))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					s.remaining = math.MaxInt32
					if e := s.Child(int64(size), func(c *Stream) error { return c.ReadFull(dst) }); e != nil {
						b.Fatal(e)
					}
				}
			})
		}
	})
}

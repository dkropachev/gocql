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

package wirecompress

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

func BenchmarkSpanCompression(b *testing.B) {
	b.Run("Append", func(b *testing.B) {
		for _, codec := range codecs {
			for _, size := range []int{1 << 10, 1 << 20} {
				for _, spans := range []int{1, 16, 1024} {
					for _, random := range []bool{false, true} {
						b.Run(fmt.Sprintf("%s/%dB/%dspans/random=%v", codec.name, size, spans, random), func(b *testing.B) {
							data := bytes.Repeat([]byte{'x'}, size)
							if random {
								_, _ = rand.New(rand.NewSource(17)).Read(data)
							}
							chain := splitChain(b, data, size/spans)
							dst := make([]byte, 0, size+size/6+64)
							b.SetBytes(int64(size))
							b.ReportAllocs()
							b.ResetTimer()
							var encoded []byte
							for i := 0; i < b.N; i++ {
								var err error
								if encoded, err = codec.append(dst, chain); err != nil {
									b.Fatal(err)
								}
							}
							b.StopTimer()
							b.ReportMetric(float64(len(encoded)), "transformed-output-B/op")
							b.ReportMetric(float64(size), "transform-input-B/op")
							b.ReportMetric(float64(len(encoded))/float64(size), "compression-ratio")
						})
					}
				}
			}
		}
	})
}

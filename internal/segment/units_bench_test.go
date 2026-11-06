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

package segment

import (
	"bytes"
	"context"

	"fmt"

	"testing"
)

type cyclicUnitReader struct {
	unit   []byte
	offset int
}

func (r *cyclicUnitReader) Read(p []byte) (int, error) {
	if r.offset == len(r.unit) {
		r.offset = 0
	}
	n := copy(p, r.unit[r.offset:])
	r.offset += n
	return n, nil
}

func BenchmarkValidatedUnits(b *testing.B) {
	for _, compressed := range []bool{false, true} {
		for _, size := range []int{32, 4096, MaxPayloadSize} {
			b.Run(fmt.Sprintf("compressed=%v/%d", compressed, size), func(b *testing.B) {
				encoded := encodedUnit(b, bytes.Repeat([]byte("x"), size), compressed, true)
				var comp Compressor
				if compressed {
					comp = independentLZ4{}
				}
				source := &cyclicUnitReader{unit: encoded}
				r, _ := NewUnitReader(context.Background(), source, comp, UnitLimits{})
				if _, _, e := r.Next(); e != nil {
					b.Fatal(e)
				}
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, out, e := r.Next(); e != nil || len(out) != size {
						b.Fatal(e)
					}
				}
			})
		}
	}
}

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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package murmur

import (
	"testing"
)

func BenchmarkMurmur3(b *testing.B) {
	// Benchmark of the performance of the murmur3 implementation
	b.Run("H1", func(b *testing.B) {
		data := make([]byte, 1024)
		for i := 0; i < 1024; i++ {
			data[i] = byte(i)
		}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				h1 := Murmur3H1(data)
				if h1 != int64(7627370222079200297) {
					b.Fatalf("expected %d got %d", int64(7627370222079200297), h1)
				}
			}
		})
	})
}

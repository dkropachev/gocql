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

package wirecompress

import (
	"testing"
)

func BenchmarkCompressor(b *testing.B) {
	b.Run("LZ4", func(b *testing.B) {
		original := []byte("My Test String")
		var c LZ4Compressor

		b.Run("Encode", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if _, err := c.Encode(original); err != nil {
					b.Fatal(err)
				}
			}
		})

		compressed, err := c.Encode(original)
		if err != nil {
			b.Fatal(err)
		}

		b.Run("Decode", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if _, err := c.Decode(compressed); err != nil {
					b.Fatal(err)
				}
			}
		})
	})

	b.Run("Snappy", func(b *testing.B) {
		c := SnappyCompressor{}
		b.Run("Decode", func(b *testing.B) {
			for _, frame := range frameExamples.Responses {
				b.Run(frame.Name, func(b *testing.B) {
					for x := 0; x < b.N; x++ {
						_, _ = c.Decode(frame.Frame)
					}
				})
			}
		})

		b.Run("Encode", func(b *testing.B) {
			for _, frame := range frameExamples.Requests {
				b.Run(frame.Name, func(b *testing.B) {
					for x := 0; x < b.N; x++ {
						_, _ = c.Encode(frame.Frame)
					}
				})
			}
		})
	})
}

/*
Copyright 2015 To gocql authors
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

package lru

import (
	"fmt"
	"testing"
)

func BenchmarkCache(b *testing.B) {
	b.Run("Add", func(b *testing.B) {
		// Inserting entries with concatenated
		// string keys, including the per-query allocation cost of key construction.
		b.Run("StringKey", func(b *testing.B) {
			c := New[string](1000)

			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				k := fmt.Sprintf("%s%s%s", "550e8400-e29b-41d4-a716-446655440000", "my_keyspace", fmt.Sprintf("SELECT id FROM users WHERE id = %d", i))
				c.Add(k, "prepared-id")
			}
		})

		// Inserting entries with struct keys,
		// including eviction when the cache is full.
		b.Run("StructKey", func(b *testing.B) {
			c := New[stmtKey](1000)

			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				k := stmtKey{
					hostID:    "550e8400-e29b-41d4-a716-446655440000",
					keyspace:  "my_keyspace",
					statement: fmt.Sprintf("SELECT id FROM users WHERE id = %d", i),
				}
				c.Add(k, "prepared-id")
			}
		})
	})

	b.Run("Get", func(b *testing.B) {
		// The old approach: looking up a
		// concatenated string key in a populated cache.
		b.Run("StringKey", func(b *testing.B) {
			c := New[string](1000)
			key := "550e8400-e29b-41d4-a716-446655440000" + "my_keyspace" + "SELECT id, name, email FROM users WHERE id = ?"
			c.Add(key, "prepared-id")

			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				c.Get(key)
			}
		})

		// The hot path: looking up a struct key
		// in a populated cache.
		b.Run("StructKey", func(b *testing.B) {
			c := New[stmtKey](1000)
			key := stmtKey{
				hostID:    "550e8400-e29b-41d4-a716-446655440000",
				keyspace:  "my_keyspace",
				statement: "SELECT id, name, email FROM users WHERE id = ?",
			}
			c.Add(key, "prepared-id")

			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				c.Get(key)
			}
		})
	})
}

type stmtKey struct {
	hostID    string
	keyspace  string
	statement string
}

// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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

package metadata

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
)

func TestPartitioner(t *testing.T) {
	t.Run("Murmur3", func(t *testing.T) {
		// Tests of the murmur3Patitioner
		t.Run("Hash", func(t *testing.T) {
			t.Parallel()

			token := endpoint.InternalMurmur3Partitioner{}.ParseString("-1053604476080545076")

			if "-1053604476080545076" != token.String() {
				t.Errorf("Expected '-1053604476080545076' but was '%s'", token)
			}

			// at least verify that the partitioner
			// doesn't return nil
			pk, _ := encodeFixtureCell(cqltype.NewNativeType(4, cqltype.TypeInt), 1)
			token = endpoint.InternalMurmur3Partitioner{}.Hash(pk)
			if token == nil {
				t.Fatal("token was nil")
			}
		})
		// Verifies hashInt64 matches Hash().
		t.Run("HashInt64MatchesHash", func(t *testing.T) {
			t.Parallel()

			var p endpoint.InternalMurmur3Partitioner
			var _ endpoint.InternalInt64Hasher = p // compile-time contract check, mirrors token.go's var _ assertion

			keys := [][]byte{
				nil,
				{},
				{0},
				{1, 2, 3},
				[]byte("routing-key"),
				[]byte("a-somewhat-longer-partition-key-value-1234567890"),
			}
			for i := 0; i < 32; i++ {
				buf := make([]byte, i)
				for j := range buf {
					buf[j] = byte(i*7 + j)
				}
				keys = append(keys, buf)
			}

			for _, k := range keys {
				want := p.Hash(k)
				wantInt64, ok := want.(endpoint.InternalInt64Token)
				if !ok {
					t.Fatalf("Hash(%v) returned %T, want int64Token", k, want)
				}
				got := endpoint.InternalInt64Token(p.InternalHashInt64(k))
				if got != wantInt64 {
					t.Errorf("hashInt64(%v) = %d, want %d (from Hash)", k, got, wantInt64)
				}
			}
		})
		t.Run("HashInt64ZeroAlloc", func(t *testing.T) {
			var p endpoint.InternalMurmur3Partitioner
			pk := []byte("routing-key")

			var sink int64
			allocs := testing.AllocsPerRun(1000, func() {
				sink = p.InternalHashInt64(pk)
			})
			if allocs != 0 {
				t.Errorf("hashInt64 allocated %.2f allocs/op, want 0", allocs)
			}
			_ = sink

			// Contrast: Hash() itself allocates once, boxing the int64 into a Token.
			var tokSink endpoint.Token
			hashAllocs := testing.AllocsPerRun(1000, func() {
				tokSink = p.Hash(pk)
			})
			_ = tokSink
			if hashAllocs != 1 {
				t.Logf("Hash() allocated %.2f allocs/op (expected ~1 from Token boxing); "+
					"int64Hasher fast path may no longer be needed if this is 0", hashAllocs)
			}
		})
	})
	// Tests of the orderedPartitioner
	t.Run("Ordered", func(t *testing.T) {
		t.Parallel()

		// at least verify that the partitioner
		// doesn't return nil
		p := endpoint.InternalOrderedPartitioner{}
		pk, _ := encodeFixtureCell(cqltype.NewNativeType(4, cqltype.TypeInt), 1)
		token := p.Hash(pk)
		if token == nil {
			t.Fatal("token was nil")
		}

		str := token.String()
		parsedToken := p.ParseString(str)

		if !bytes.Equal([]byte(token.(endpoint.InternalOrderedToken)), []byte(parsedToken.(endpoint.InternalOrderedToken))) {
			t.Errorf("Failed to convert to and from a string %s expected %x but was %x",
				str,
				[]byte(token.(endpoint.InternalOrderedToken)),
				[]byte(parsedToken.(endpoint.InternalOrderedToken)),
			)
		}
	})
	t.Run("Random", func(t *testing.T) {
		t.Parallel()

		// Tests of the randomPartitioner
		t.Run("Hash", func(t *testing.T) {
			t.Parallel()

			// at least verify that the partitioner
			// doesn't return nil
			p := endpoint.InternalRandomPartitioner{}
			pk, _ := encodeFixtureCell(cqltype.NewNativeType(4, cqltype.TypeInt), 1)
			token := p.Hash(pk)
			if token == nil {
				t.Fatal("token was nil")
			}

			str := token.String()
			parsedToken := p.ParseString(str)

			if (*big.Int)(token.(*endpoint.InternalRandomToken)).Cmp((*big.Int)(parsedToken.(*endpoint.InternalRandomToken))) != 0 {
				t.Errorf("Failed to convert to and from a string %s expected %v but was %v",
					str,
					token,
					parsedToken,
				)
			}
		})
		t.Run("MatchesReference", func(t *testing.T) {
			t.Parallel()

			// example taken from datastax python driver
			//    >>> from cassandra.metadata import MD5Token
			//    >>> MD5Token.hash_fn("test")
			//    12707736894140473154801792860916528374L
			var p endpoint.InternalRandomPartitioner
			expect := "12707736894140473154801792860916528374"
			actual := p.Hash([]byte("test")).String()
			if actual != expect {
				t.Errorf("expected random partitioner to generate tokens in the same way as the reference"+
					" python client. Expected %s, but got %s", expect, actual)
			}
		})
	})
}

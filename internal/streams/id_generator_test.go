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

package streams

import (
	"sync"
	"testing"
)

func TestIDGenerator(t *testing.T) {
	t.Parallel()

	t.Run("Clear", func(t *testing.T) {
		t.Run("AllStreams", func(t *testing.T) {
			t.Parallel()

			streams := New()
			for i := 0; i < streams.NumStreams; i++ {
				if _, ok := streams.GetStream(); !ok {
					t.Fatalf("unable to get stream %d", i)
				}
			}
			for i := 0; i < streams.NumStreams; i++ {
				if !streams.Clear(i) {
					t.Fatalf("stream %d was not in use", i)
				}
			}
			for i := range streams.state[1:] {
				if word := streams.state[1+i].Load(); word != 0 {
					t.Errorf("did not clear streams in word %d: %x", i, word)
				}
			}
			if streams.InUse() != 0 || streams.Available() != streams.NumStreams {
				t.Fatalf("InUse=%d Available=%d", streams.InUse(), streams.Available())
			}
		})

		t.Run("Double", func(t *testing.T) {
			t.Parallel()

			streams := New()
			stream, ok := streams.GetStream()
			if !ok {
				t.Fatal("did not get stream")
			}

			if !streams.Clear(stream) {
				t.Fatalf("stream not indicated as in use: %d", stream)
			}
			if streams.Clear(stream) {
				t.Fatalf("stream not as in use after clear: %d", stream)
			}
			if streams.Clear(-1) || streams.Clear(streams.NumStreams) {
				t.Fatal("out-of-range streams must not clear")
			}
			if streams.InUse() != 0 {
				t.Fatalf("InUse=%d after a double clear", streams.InUse())
			}
			// A double clear must not put the ID on the free stack twice.
			x, _ := streams.GetStream()
			y, _ := streams.GetStream()
			if x == y {
				t.Fatalf("stream %d handed out twice", x)
			}
		})
	})

	t.Run("GetStream", func(t *testing.T) {
		t.Run("ReusesLowestReleased", func(t *testing.T) {
			t.Parallel()

			streams := New()
			a, _ := streams.GetStream()
			b, _ := streams.GetStream()
			c, _ := streams.GetStream()
			if a != 0 || b != 1 || c != 2 {
				t.Fatalf("fresh streams = %d %d %d, want 0 1 2", a, b, c)
			}
			streams.Clear(c)
			streams.Clear(a)
			if got, _ := streams.GetStream(); got != a {
				t.Fatalf("got %d, want the lowest released %d", got, a)
			}
			if got, _ := streams.GetStream(); got != c {
				t.Fatalf("got %d, want %d", got, c)
			}
			if got, _ := streams.GetStream(); got != 3 {
				t.Fatalf("got %d, want the next fresh stream 3", got)
			}
		})

		t.Run("ReusesBelowHint", func(t *testing.T) {
			t.Parallel()

			// Fill the first word so the hint moves past it, then free an ID in
			// it: the next allocation must find it again.
			streams := NewLimited(128)
			for range 65 {
				streams.GetStream()
			}
			streams.Clear(5)
			if got, _ := streams.GetStream(); got != 5 {
				t.Fatalf("got %d, want 5", got)
			}
		})

		t.Run("UsesAllStreams", func(t *testing.T) {
			t.Parallel()

			for _, limit := range []int{1, 63, 64, 65, 32768} {
				streams := NewLimited(limit)
				got := make(map[int]struct{})
				for i := 0; i < streams.NumStreams; i++ {
					stream, ok := streams.GetStream()
					if !ok {
						t.Fatalf("limit %d: unable to get stream %d", limit, i)
					}
					if stream < 0 || stream >= limit {
						t.Fatalf("limit %d: stream %d out of range", limit, stream)
					}
					if _, ok = got[stream]; ok {
						t.Fatalf("limit %d: got an already allocated stream: %d", limit, stream)
					}
					got[stream] = struct{}{}
					if !streams.isSet(stream) {
						t.Fatalf("limit %d: stream not set: %d", limit, stream)
					}
				}
				if stream, ok := streams.GetStream(); ok {
					t.Fatalf("limit %d: got stream %d when all are in use", limit, stream)
				}
				if streams.InUse() != limit || streams.Available() != 0 {
					t.Fatalf("limit %d: InUse=%d Available=%d", limit, streams.InUse(), streams.Available())
				}
			}
		})
	})
}

// Allocations and releases from many goroutines never hand out an ID that is
// still in use, and the in-use count returns to zero.
func TestStreamBitmapConcurrent(t *testing.T) {
	t.Parallel()

	for _, limit := range []int{40, 200, 32768} {
		streams := NewLimited(limit)
		owner := make([]sync.Mutex, limit)
		held := make([]bool, limit)
		const workers, rounds = 16, 2000
		var wg sync.WaitGroup
		for w := range workers {
			wg.Go(func() {
				var mine []int
				for i := range rounds {
					// Hold up to a few IDs at once so the bitmap fills and drains.
					if len(mine) < 1+(w+i)%5 {
						stream, ok := streams.GetStream()
						if !ok {
							if len(mine) == 0 {
								continue
							}
						} else {
							owner[stream].Lock()
							dup := held[stream]
							held[stream] = true
							owner[stream].Unlock()
							if dup {
								t.Errorf("limit %d: stream %d handed out twice", limit, stream)
								return
							}
							mine = append(mine, stream)
							continue
						}
					}
					stream := mine[len(mine)-1]
					mine = mine[:len(mine)-1]
					owner[stream].Lock()
					held[stream] = false
					owner[stream].Unlock()
					if !streams.Clear(stream) {
						t.Errorf("limit %d: stream %d was not in use", limit, stream)
						return
					}
				}
				for _, stream := range mine {
					owner[stream].Lock()
					held[stream] = false
					owner[stream].Unlock()
					streams.Clear(stream)
				}
			})
		}
		wg.Wait()
		if streams.InUse() != 0 || streams.Available() != limit {
			t.Fatalf("limit %d: InUse=%d Available=%d", limit, streams.InUse(), streams.Available())
		}
	}
}

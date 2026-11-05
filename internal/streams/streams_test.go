//go:build unit
// +build unit

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

func TestUsesAllStreams(t *testing.T) {
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
}

func TestClearStreams(t *testing.T) {
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
	for i, word := range streams.used {
		if word != 0 {
			t.Errorf("did not clear streams in word %d: %x", i, word)
		}
	}
	if streams.InUse() != 0 || streams.Available() != streams.NumStreams {
		t.Fatalf("InUse=%d Available=%d", streams.InUse(), streams.Available())
	}
}

func TestReusesLastReleased(t *testing.T) {
	t.Parallel()

	streams := New()
	a, _ := streams.GetStream()
	b, _ := streams.GetStream()
	c, _ := streams.GetStream()
	if a != 0 || b != 1 || c != 2 {
		t.Fatalf("fresh streams = %d %d %d, want 0 1 2", a, b, c)
	}
	streams.Clear(a)
	streams.Clear(c)
	if got, _ := streams.GetStream(); got != c {
		t.Fatalf("got %d, want the last released %d", got, c)
	}
	if got, _ := streams.GetStream(); got != a {
		t.Fatalf("got %d, want %d", got, a)
	}
	if got, _ := streams.GetStream(); got != 3 {
		t.Fatalf("got %d, want the next fresh stream 3", got)
	}
}

func TestDoubleClear(t *testing.T) {
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
}

// BenchmarkConcurrentUse allocates and releases under a shared lock, as a
// connection does.
func BenchmarkConcurrentUse(b *testing.B) {
	streams := New()
	var mu sync.Mutex

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			stream, ok := streams.GetStream()
			mu.Unlock()
			if !ok {
				b.Error("unable to get stream")
				return
			}
			mu.Lock()
			ok = streams.Clear(stream)
			mu.Unlock()
			if !ok {
				b.Errorf("stream was already cleared: %d", stream)
				return
			}
		}
	})
}

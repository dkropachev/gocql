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
	"math/bits"
	"sync/atomic"
)

// IDGenerator allocates the stream IDs of one connection without a lock.
//
// IDs live in an atomic bitmap: GetStream sets a clear bit with a CAS and
// Clear clears it. The scan starts at a hint word and takes the lowest clear
// bit, so a connection keeps reusing the same few low IDs and the call-table
// slots they index stay warm. Bits beyond NumStreams in the last word are set
// at construction and never cleared.
//
// state[0] counts the IDs in use and state[1:] is the bitmap. The count shares
// a cache line with the lowest bitmap words, which carry the happy-path
// allocations, so an allocation or release touches one contended line.
type IDGenerator struct {
	NumStreams int
	state      []atomic.Uint64
	// hint is the lowest bitmap word that may have a clear bit. It is only a
	// starting point: GetStream wraps around, so a stale hint costs a longer
	// scan, never an ID.
	hint atomic.Uint32
}

func New() *IDGenerator {
	return NewLimited(32768)
}

func NewLimited(maxStreams int) *IDGenerator {
	if maxStreams < 1 || maxStreams > 32768 {
		panic("streams: capacity must be 1..32768")
	}
	words := (maxStreams + 63) / 64
	s := &IDGenerator{NumStreams: maxStreams, state: make([]atomic.Uint64, 1+words)}
	if tail := maxStreams & 63; tail != 0 {
		s.state[words].Store(^uint64(0) << uint(tail))
	}
	return s
}

// GetStream allocates a stream ID.
func (s *IDGenerator) GetStream() (int, bool) {
	words := s.state[1:]
	start := int(s.hint.Load())
	if start >= len(words) {
		start = 0
	}
	for n, i := 0, start; n < len(words); n++ {
		word := &words[i]
		for w := word.Load(); w != ^uint64(0); w = word.Load() {
			bit := bits.TrailingZeros64(^w)
			if word.CompareAndSwap(w, w|1<<uint(bit)) {
				s.state[0].Add(1)
				return i<<6 | bit, true
			}
		}
		// Word i is full: later scans start past it.
		if i == start {
			s.hint.CompareAndSwap(uint32(start), uint32(i+1))
		}
		if i++; i == len(words) {
			i = 0
		}
	}
	return 0, false
}

// Clear releases stream and reports whether it was in use.
func (s *IDGenerator) Clear(stream int) (inuse bool) {
	inuse, _ = s.Release(stream)
	return inuse
}

// Release releases stream. It reports whether the stream was in use and, if
// so, whether no stream was left in use right after this release.
func (s *IDGenerator) Release(stream int) (inuse, idle bool) {
	if stream < 0 || stream >= s.NumStreams {
		return false, false
	}
	i, bit := stream>>6, uint64(1)<<uint(stream&63)
	if s.state[1+i].And(^bit)&bit == 0 {
		// already cleared
		return false, false
	}
	if uint32(i) < s.hint.Load() {
		s.hint.Store(uint32(i))
	}
	return true, s.state[0].Add(^uint64(0)) == 0
}

func (s *IDGenerator) isSet(stream int) bool {
	return s.state[1+(stream>>6)].Load()&(1<<uint(stream&63)) != 0
}

func (s *IDGenerator) Available() int {
	return s.NumStreams - s.InUse()
}

func (s *IDGenerator) InUse() int {
	return int(s.state[0].Load())
}

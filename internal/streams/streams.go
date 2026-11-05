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

import "sync/atomic"

// IDGenerator allocates the stream IDs of one connection.
//
// GetStream and Clear must be called under a lock the caller holds for the
// connection (Conn.InternalMu): stream allocation happens together with the
// registration of the call that owns it, so a lock-free allocator would only add
// atomic traffic to a section that is serialised anyway. InUse and Available may
// be read without that lock.
//
// Released IDs are reused last-in first-out, so a connection keeps reusing the
// same few low IDs and the call-table slots they index stay warm. IDs that were
// never handed out sit above a watermark rather than in the free stack, which
// grows only as far as the connection's peak concurrency.
type IDGenerator struct {
	NumStreams int
	inUse      atomic.Int32
	next       int32   // IDs in [next, NumStreams) have never been handed out
	free       []int16 // released IDs, reused LIFO
	used       []uint64
}

func New() *IDGenerator {
	return NewLimited(32768)
}

func NewLimited(maxStreams int) *IDGenerator {
	if maxStreams < 1 || maxStreams > 32768 {
		panic("streams: capacity must be 1..32768")
	}
	return &IDGenerator{NumStreams: maxStreams, used: make([]uint64, (maxStreams+63)/64)}
}

// GetStream allocates a stream ID. The caller must hold the connection lock.
func (s *IDGenerator) GetStream() (int, bool) {
	var stream int
	if n := len(s.free); n > 0 {
		stream = int(s.free[n-1])
		s.free = s.free[:n-1]
	} else if int(s.next) < s.NumStreams {
		stream = int(s.next)
		s.next++
	} else {
		return 0, false
	}
	s.used[stream>>6] |= 1 << uint(stream&63)
	s.inUse.Store(s.inUse.Load() + 1)
	return stream, true
}

// Clear releases stream and reports whether it was in use. The caller must
// hold the connection lock.
func (s *IDGenerator) Clear(stream int) (inuse bool) {
	if stream < 0 || stream >= s.NumStreams {
		return false
	}
	word, bit := &s.used[stream>>6], uint64(1)<<uint(stream&63)
	if *word&bit == 0 {
		// already cleared
		return false
	}
	*word &^= bit
	s.free = append(s.free, int16(stream))
	s.inUse.Store(s.inUse.Load() - 1)
	return true
}

func (s *IDGenerator) isSet(stream int) bool {
	return s.used[stream>>6]&(1<<uint(stream&63)) != 0
}

func (s *IDGenerator) Available() int {
	return s.NumStreams - int(s.inUse.Load())
}

func (s *IDGenerator) InUse() int {
	return int(s.inUse.Load())
}

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

package execution

import (
	"github.com/scylladb/gocql/v2/internal/segment"
)

// Segment builders for the tests in this package, which construct transport
// segments to feed the connection's receive path (segment_reassembly_test.go,
// conn_test.go) or to check what the send path produced (frame_test.go).
//
// The codec and its own tests live in internal/segment. What is left here is the
// bridge: these take this package's Compressor, so a test can pass the same
// compressor it configured a driverConn or framer with, and narrow it the way the
// production callers do.
//
// Each builds or reads one whole segment in a single call, with buffers of its own.
// No production caller works that way — the receive loop splits header and payload
// across two phases to re-arm its read deadline, and the send path encodes straight
// into the framer's wire buffer — so these model an allocation contract the driver
// deliberately does not follow.

// newUncompressedSegment returns payload as a standalone uncompressed segment.
func newUncompressedSegment(payload []byte, isSelfContained bool) ([]byte, error) {
	return segment.AppendUncompressed(nil, payload, isSelfContained)
}

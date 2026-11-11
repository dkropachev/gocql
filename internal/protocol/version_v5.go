// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

// V5 is native protocol version 5, which Cassandra 4.0 and later speak.
// ScyllaDB does not.
//
//   - Once the connection is ready, frames travel inside checksummed
//     transport segments (segment.go), and compression applies to segments.
//   - Query flags are an [int].
//   - Requests may name their keyspace and set now_in_seconds.
//   - PREPARED and ROWS results carry a result metadata ID.
//   - READ_FAILURE and WRITE_FAILURE carry a reason map.
var V5 = Version{
	Number:           ProtoVersion5,
	WideFlags:        true,
	Segments:         true,
	ResultMetadataID: true,
	QueryKeyspace:    true,
	NowInSeconds:     true,
	FailureReasonMap: true,
}

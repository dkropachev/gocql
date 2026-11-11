// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

// V4 is native protocol version 4, the version ScyllaDB speaks and the one
// every supported Cassandra speaks.
//
//   - Frames are compressed one by one, flagged by FlagCompress.
//   - Query flags are a [byte].
//   - Requests cannot name a keyspace or set now_in_seconds.
//   - Results carry no result metadata ID; ScyllaDB adds one through the
//     SCYLLA_USE_METADATA_ID extension.
//   - READ_FAILURE and WRITE_FAILURE carry a failure count.
var V4 = Version{Number: ProtoVersion4}

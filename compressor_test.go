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

package gocql_test

import (
	gocql "github.com/scylladb/gocql/v2"

	"testing"

	lz4mod "github.com/scylladb/gocql/v2/internal/wirecompress"
)

// legacyCompressor implements only the master-era Compressor surface
// (Name/Encode/Decode). It guards backward compatibility: if the public
// gocql.Compressor interface ever regains required methods beyond these, this
// file stops compiling — which is the regression we are preventing.
type legacyCompressor struct{}

func (legacyCompressor) Name() string                       { return "legacy" }
func (legacyCompressor) Encode(data []byte) ([]byte, error) { return data, nil }
func (legacyCompressor) Decode(data []byte) ([]byte, error) { return data, nil }

var _ gocql.Compressor = legacyCompressor{}

var (
	_ gocql.Compressor        = lz4mod.LZ4Compressor{}
	_ gocql.SegmentCompressor = lz4mod.LZ4Compressor{}
)

func TestCompressorBackwardCompatibility(t *testing.T) {
	t.Parallel()

	// A compressor implementing only Name/Encode/Decode must satisfy
	// gocql.Compressor without implementing the optional v5 SegmentCompressor.
	var c gocql.Compressor = legacyCompressor{}
	if _, ok := c.(gocql.SegmentCompressor); ok {
		t.Fatal("legacyCompressor unexpectedly satisfies SegmentCompressor")
	}

	// The built-in SnappyCompressor must not satisfy SegmentCompressor: it is
	// the "capable Compressor, but not v5-segment-capable" case the driver
	// rejects up front on ProtoVersion >= 5.
	if _, ok := interface{}(gocql.SnappyCompressor{}).(gocql.SegmentCompressor); ok {
		t.Fatal("SnappyCompressor must not satisfy SegmentCompressor")
	}
}

// TestSnappyCompressorDecodeRejectsHugeLength is the snappy counterpart of
// TestLZ4Compressor_DecodeRejectsHugeLength in the lz4 module.
//
// A snappy block begins with a varint holding the decompressed length, and s2 allocates
// that many bytes before it validates a single byte of the stream. s2 itself only
// refuses a claim above 4 GiB, so without a bound here a handful of corrupt header bytes
// on a short frame buy a multi-gigabyte allocation: the frame reader bounds the
// compressed body it accepts, but nothing bounded what that body expanded into.

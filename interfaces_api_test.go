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

package gocql_test

import (
	"io"
	"testing"

	gocql "github.com/scylladb/gocql/v2"
	lz4mod "github.com/scylladb/gocql/v2/internal/wirecompress"
)

func TestInterfacesAPI(t *testing.T) {
	t.Parallel()

	t.Run("Compressor", func(t *testing.T) {
		t.Run("BackwardCompatibility", func(t *testing.T) {
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
		})
	})

	t.Run("Conn", func(t *testing.T) {
		// Documents the compile-time Conn guards in this file as an explicit
		// expectation, in the same spirit as Compressor/BackwardCompatibility.
		t.Run("ReadWriter", func(t *testing.T) {
			t.Parallel()

			var c interface{} = (*gocql.Conn)(nil)
			if _, ok := c.(io.Reader); !ok {
				t.Error("*gocql.Conn must satisfy io.Reader")
			}
			if _, ok := c.(io.Writer); !ok {
				t.Error("*gocql.Conn must satisfy io.Writer")
			}
		})
	})
}

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

// internal/wirecompress TestCompressor/Snappy/DecodeRejectsHugeLength is the snappy
// counterpart of TestCompressor/LZ4/DecodeRejectsHugeLength.
//
// A snappy block begins with a varint holding the decompressed length, and s2 allocates
// that many bytes before it validates a single byte of the stream. s2 itself only
// refuses a claim above 4 GiB, so without a bound here a handful of corrupt header bytes
// on a short frame buy a multi-gigabyte allocation: the frame reader bounds the
// compressed body it accepts, but nothing bounded what that body expanded into.

// *gocql.Conn has exposed both Read and Write since before the read-path
// refactor. This module has no /v2 import path, so dropping either one is a
// backward-incompatible removal for downstream code that uses a *Conn as an
// io.Reader/io.Writer. These guards make such a removal a build failure.
var (
	_ io.Reader = (*gocql.Conn)(nil)
	_ io.Writer = (*gocql.Conn)(nil)
)

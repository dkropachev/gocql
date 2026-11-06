/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package frame

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func streamHeader(version byte, flags byte, stream int16, op Op, size uint32) []byte {
	b := []byte{version, flags, 0, 0, byte(op)}
	binary.BigEndian.PutUint16(b[2:4], uint16(stream))
	return binary.BigEndian.AppendUint32(b, size)
}

type envelopeSource struct {
	data                []byte
	offset, chunk, gate int
}

func (s *envelopeSource) Read(p []byte) (int, error) {
	if s.offset >= s.gate {
		return 0, errors.New("read beyond supplied first row")
	}
	n := min(len(p), s.chunk, s.gate-s.offset)
	copy(p[:n], s.data[s.offset:s.offset+n])
	s.offset += n
	return n, nil
}
func FuzzStreamEnvelope(f *testing.F) {
	f.Add(streamHeader(0x84, 0, 0, OpResult, 0))
	f.Add(streamHeader(0x85, FlagWarning, -1, OpEvent, 100))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<16 {
			t.Skip()
		}
		_, s, e := ReadStreamEnvelope(context.Background(), bytes.NewReader(data), 4, wire.StreamLimits{MaxBytes: 1 << 16})
		if e == nil {
			_ = s.Skip()
		}
	})
}

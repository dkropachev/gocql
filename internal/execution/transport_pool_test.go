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

package execution

import (
	"github.com/scylladb/gocql/v2/internal/transport"

	"io"
	"net"
	"sync"
	"testing"

	"github.com/scylladb/gocql/v2/internal/segment"
)

func writeResponse(t *testing.T, c *transport.Conn, server net.Conn, stream int, body []byte) {
	t.Helper()
	frame := typedTestResponse(t, c.InternalVersion, stream, body)
	if c.InternalVersion < 5 {
		if _, err := server.Write(frame); err != nil {
			t.Error(err)
		}
		return
	}
	for offset := 0; offset < len(frame); {
		end := min(offset+segment.MaxPayloadSize, len(frame))
		part := typedTestChain(t, frame[offset:end])
		packet, err := segment.EncodeSpans(part, len(frame) <= segment.MaxPayloadSize, c.InternalSegCompressor != nil)
		if err != nil {
			t.Error(err)
			return
		}
		if _, err := server.Write(typedTestBytes(t, packet)); err != nil {
			t.Error(err)
			return
		}
		offset = end
	}
}

var _ io.Reader = (*transport.InternalVerifiedStreamUnits)(nil)

type acceptedWriter struct {
	net.Conn
	entered chan struct{}
	once    sync.Once
}

func (w *acceptedWriter) Write(b []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	return w.Conn.Write(b)
}

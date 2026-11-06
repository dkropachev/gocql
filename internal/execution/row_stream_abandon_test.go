/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package execution

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/protocol"
)

// An explicit Close of an incremental stream discards the unread rest of a
// small current response and keeps the connection, so that the next stream
// runs on it; a response above the drain limit still closes the connection.
func TestStreamAbandonedIncrementalCloseKeepsConnection(t *testing.T) {
	for _, mode := range []struct {
		name        string
		proto       byte
		compression protocol.Compressor
	}{{"v4", 4, nil}, {"v5raw", 5, nil}} {
		for _, tc := range []struct {
			name string
			rows []string
			read int  // rows read before Close
			keep bool // connection survives Close
		}{
			{"mid-response", []string{"a", "b", "c", "d"}, 1, true},
			{"response-boundary", []string{"a", "b"}, 2, true},
			{"over-drain-limit", []string{"a", string(bytes.Repeat([]byte{'x'}, 1<<20+1))}, 1, false},
		} {
			t.Run(mode.name+"/"+tc.name, func(t *testing.T) {
				s, c, peer := rowStreamSession(t, mode.proto, mode.compression)
				served := make(chan struct{})
				go func() {
					defer close(served)
					// The first stream's page has more pages; the second is final.
					for _, body := range [][]byte{runtimeRowsBody([]byte{7}, tc.rows...), runtimeRowsBody(nil, "next")} {
						stream := typedTestReadRequest(t, c, peer)
						if _, err := peer.Write(testResponse(t, c, stream, body)); err != nil {
							return
						}
						if !tc.keep {
							return
						}
					}
				}()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				b := runtimeQuery(t)
				defer b.Close()
				cursor, err := RunStream(ctx, s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true})
				if err != nil {
					t.Fatal(err)
				}
				dst := make([]byte, 0, 1<<20+2)
				for i := 0; i < tc.read; i++ {
					if !cursor.NextInto(&dst) {
						t.Fatal("row", i, cursor.Err())
					}
				}
				if err := cursor.Close(); err != nil {
					t.Fatal(err)
				}
				if c.Closed() == tc.keep {
					t.Fatalf("connection closed=%v, want %v", c.Closed(), !tc.keep)
				}
				if !tc.keep {
					return
				}
				next, err := RunStream(ctx, s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true})
				if err != nil {
					t.Fatal(err)
				}
				if !next.NextInto(&dst) || string(dst) != "next" {
					t.Fatal("next stream", next.Err())
				}
				if next.NextInto(&dst) || next.Err() != nil {
					t.Fatal(next.Err())
				}
				if err := next.Close(); err != nil {
					t.Fatal(err)
				}
				<-served
			})
		}
	}
}

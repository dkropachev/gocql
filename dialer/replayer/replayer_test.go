// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package replayer

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/scylladb/gocql/v2/dialer"
)

// optionsFrame builds a body-less OPTIONS frame with the given version byte.
func optionsFrame(version byte) []byte {
	return []byte{version, 0x00, 0x00, 0x01, 0x05, 0x00, 0x00, 0x00, 0x00}
}

// requestFrame builds a body-less v4 request with the given opcode and stream id.
//
// The opcode is what tells two requests apart here: GetFrameHash blanks the stream id
// before hashing, so two OPTIONS frames on different streams hash alike and would be
// served the same recorded response.
func requestFrame(opcode byte, streamID int) []byte {
	return []byte{0x04, 0x00, byte(streamID >> 8), byte(streamID), opcode, 0x00, 0x00, 0x00, 0x00}
}

// newTestReplayer builds a ConnectionReplayer the way NewConnectionReplayer does,
// without going through a file on disk -- the same constructor, so a test connection
// cannot drift from a real one.
func newTestReplayer(proto byte, frames ...*FrameRecorded) *ConnectionReplayer {
	return newTestReplayerWith(nil, proto, frames...)
}

// newTestReplayerWith is the same for a connection given a segment compressor.
func newTestReplayerWith(comp dialer.SegmentCompressor, proto byte, frames ...*FrameRecorded) *ConnectionReplayer {
	return newConnectionReplayer(frames, proto, comp)
}

// writeRecords writes the given records to a file in the recorder's format, one
// JSON object per line, and returns its path.
func writeRecords(t *testing.T, name string, records ...dialer.Record) string {
	t.Helper()
	return writeRecordsTo(t, filepath.Join(t.TempDir(), name), records...)
}

// writeRecordsTo writes the records to an exact path, for the tests that need both
// halves of one recording to share a directory.
func writeRecordsTo(t *testing.T, path string, records ...dialer.Record) string {
	t.Helper()

	var buf bytes.Buffer
	for _, record := range records {
		line, err := json.Marshal(record)
		if err != nil {
			t.Fatalf("marshalling record %d: %v", record.StreamID, err)
		}
		buf.Write(append(line, '\n'))
	}

	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	return path
}

// responseFrame builds a body-carrying response frame with the given stream id, so a
// test can check which stream id came back out.
func responseFrame(streamID int, bodyLen int) []byte {
	frame := []byte{
		0x84, 0x00,
		byte(streamID >> 8), byte(streamID),
		0x08, // RESULT
		byte(bodyLen >> 24), byte(bodyLen >> 16), byte(bodyLen >> 8), byte(bodyLen),
	}
	return append(frame, bytes.Repeat([]byte{0x5A}, bodyLen)...)
}

// readAll drains the replayer until it has delivered n bytes, using a buffer of
// exactly chunk bytes so the response has to be served across several calls.
func readAll(t *testing.T, c *ConnectionReplayer, n, chunk int) []byte {
	t.Helper()

	got := make([]byte, 0, n)
	buf := make([]byte, chunk)
	for len(got) < n {
		read, err := c.Read(buf)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if read == 0 {
			t.Fatal("Read returned 0 bytes without an error")
		}
		got = append(got, buf[:read]...)
	}
	return got
}

// failAfterWriting appends what it was given and then reports a failure. That is the shape
// segment.AppendCompressed warns about: by the time a compressor is called the segment
// header has already been reserved in the destination buffer, so an error leaves bytes
// behind whatever the compressor itself did.
type failAfterWriting struct{ err error }

func (c failAfterWriting) AppendCompressed(dst, src []byte) ([]byte, error) {
	return append(dst, src...), c.err
}

func (failAfterWriting) AppendDecompressed(dst, src []byte, _ uint32) ([]byte, error) {
	return append(dst, src...), nil
}

// startupV5 builds a v5 STARTUP naming a compression algorithm, which is what fixes the
// segment header layout for the rest of the connection.
func startupV5(streamID int, algorithm string) []byte {
	body := []byte{0x00, 0x01} // a [string map] of one entry
	for _, s := range []string{"COMPRESSION", algorithm} {
		body = append(body, byte(len(s)>>8), byte(len(s)))
		body = append(body, s...)
	}
	frame := []byte{
		0x05, 0x00, byte(streamID >> 8), byte(streamID), 0x01, // STARTUP
		byte(len(body) >> 24), byte(len(body) >> 16), byte(len(body) >> 8), byte(len(body)),
	}
	return append(frame, body...)
}

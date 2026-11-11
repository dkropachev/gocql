// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package replayer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/testkit/wirerecord"
)

func TestConnectionReplayer(t *testing.T) {
	t.Run("Close", func(t *testing.T) {
		// Idempotent pins that closing twice is not a crash.
		t.Run("Idempotent", func(t *testing.T) {
			c := newTestReplayer(0x04, &FrameRecorded{Response: responseFrame(0x0001, 8)})
			if c.Closed() {
				t.Fatal("Closed() = true before Close")
			}

			for i := 1; i <= 2; i++ {
				if err := c.Close(); err != nil {
					t.Fatalf("Close #%d: %v", i, err)
				}
				if !c.Closed() {
					t.Fatalf("Closed() = false after Close #%d", i)
				}
			}
		})

		// UnmatchedWriteAfterCloseFails pins that the Close
		// gate comes before the lookup: a request with no recorded response fails rather than
		// reaching the unmatched-request panic.
		t.Run("UnmatchedWriteAfterCloseFails", func(t *testing.T) {
			c := newTestReplayer(0x04, &FrameRecorded{
				Response: responseFrame(0x0001, 8),
				Hash:     wirerecord.GetFrameHash(requestFrame(0x05, 1), false),
			})

			if err := c.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			if _, err := c.Write(requestFrame(0x07, 1)); !errors.Is(err, net.ErrClosed) {
				t.Fatalf("unmatched Write after Close = %v, want net.ErrClosed", err)
			}
		})

		// WakesBlockedRead pins that Close ends a Read parked for
		// the next request -- the driver's ordinary teardown order (scylladb/gocql#1020).
		t.Run("WakesBlockedRead", func(t *testing.T) {
			req := requestFrame(0x05, 1)
			c := newTestReplayer(0x04, &FrameRecorded{
				Response: responseFrame(0x0001, 8),
				Hash:     wirerecord.GetFrameHash(req, false),
			})

			type readResult struct {
				n   int
				err error
			}
			done := make(chan readResult, 1)
			go func() {
				n, err := c.Read(make([]byte, 64))
				done <- readResult{n: n, err: err}
			}()

			// Lets the reader reach the wait; either ordering must end in io.EOF.
			time.Sleep(50 * time.Millisecond)

			if err := c.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			select {
			case got := <-done:
				if !errors.Is(got.err, io.EOF) {
					t.Errorf("Read after Close = %v, want io.EOF", got.err)
				}
				if got.n != 0 {
					t.Errorf("Read after Close served %d bytes, want 0", got.n)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Read did not return within 5s of Close; it is spinning on the wait loop")
			}
		})

		// WriteAfterCloseFails pins that a write past Close
		// fails that connection rather than panicking the process.
		t.Run("WriteAfterCloseFails", func(t *testing.T) {
			req := requestFrame(0x05, 1)
			c := newTestReplayer(0x04, &FrameRecorded{
				Response: responseFrame(0x0001, 8),
				Hash:     wirerecord.GetFrameHash(req, false),
			})

			if err := c.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			if _, err := c.Write(req); !errors.Is(err, net.ErrClosed) {
				t.Fatalf("Write after Close = %v, want net.ErrClosed", err)
			}

			// wirerecord.FrameSplitter.Feed latches the emit error, so the connection stays failed.
			if _, err := c.Write(req); !errors.Is(err, net.ErrClosed) {
				t.Errorf("second Write after Close = %v, want the latched net.ErrClosed", err)
			}
		})
	})

	t.Run("Materialise", func(t *testing.T) {
		// LeavesNothingServableOnEncodeFailure pins what materialise owes its
		// caller when the framing refuses to encode a response.
		//
		// EncodeResponse encodes into the buffer it is handed and can fail after having written
		// into it, so on the error path outgoing's length described bytes that were no longer the
		// response it named. Nothing serves them -- outgoingPos == len(outgoing) on the way in,
		// and Read latches the failure on the way out -- which is exactly why this is asserted on
		// the fields: there is no way left to observe it from outside, and an invariant nothing
		// can see is the kind that decays quietly.
		t.Run("LeavesNothingServableOnEncodeFailure", func(t *testing.T) {
			const previous = 64

			for _, tc := range []struct {
				name string
				// comp covers both ways EncodeResponse fails: nil is refused before anything is
				// written, the other fails after.
				comp wirerecord.SegmentCompressor
			}{
				{name: "no compressor for a compressed connection", comp: nil},
				{name: "compressor fails after writing", comp: failAfterWriting{err: errors.New("compressor failed")}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					c := newTestReplayerWith(tc.comp, 0x05)

					// Drive the framing to where a response is segmented: a STARTUP naming an
					// algorithm fixes the layout, a READY flips the switch.
					if err := c.framing.ObserveRequest(startupV5(1, "lz4")); err != nil {
						t.Fatalf("ObserveRequest: %v", err)
					}
					c.framing.ObserveResponse([]byte{0x85, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00})

					// The state materialise is entered in: a previous response, served and
					// drained, whose buffer is the one the next encode is handed.
					c.outgoing = make([]byte, previous, 4096)
					c.outgoingPos = previous

					response := responseFrame(0x0040, 32)
					response[0] = 0x85 // the connection is v5; a v4 frame here would be a lie
					if err := c.materialise(&FrameRecorded{Response: response}, 0x0007); err == nil {
						t.Fatal("a response the framing cannot encode was materialised")
					}

					if len(c.outgoing) != 0 {
						t.Errorf("outgoing still reports %d bytes after a failed encode; they are not the response it names", len(c.outgoing))
					}
					if c.outgoingPos != len(c.outgoing) {
						t.Errorf("outgoingPos = %d with len(outgoing) = %d: Read would serve bytes materialise never wrote",
							c.outgoingPos, len(c.outgoing))
					}
				})
			}
		})
	})

	t.Run("NewConnectionReplayer", func(t *testing.T) {
		// RefusesUnpairedRecording pins that a recording with
		// nothing to replay fails at the dial.
		//
		// The loader accepts one deliberately -- it still has a protocol version to report off a
		// truncated Reads file, and a version check that disarms itself exactly when the recording
		// is damaged is not a check -- so the refusal belongs here instead. Without it the dial
		// succeeds and the first request panics with "unable to find a response to replay", naming
		// neither file, which is what a mistyped directory or a source port this recording never
		// saw produces.
		t.Run("RefusesUnpairedRecording", func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "rec")
			writeRecordsTo(t, base+"Writes", wirerecord.Record{Data: optionsFrame(0x04), StreamID: 1, Proto: 0x04})
			writeRecordsTo(t, base+"Reads")

			conn, err := NewConnectionReplayer(base, nil)
			if err == nil {
				t.Fatalf("a recording pairing no requests with responses was replayed as %T", conn)
			}
			for _, want := range []string{base + "Reads", base + "Writes"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not name %q", err, want)
				}
			}
		})
	})

	t.Run("Pipelining", func(t *testing.T) {
		// CloseRaces pins, under -race, that Close landing
		// mid-stream ends both sides cleanly: the writer sees nil or net.ErrClosed, the reader
		// io.EOF, and nothing panics.
		t.Run("CloseRaces", func(t *testing.T) {
			const requests = 200

			reqs := make([][]byte, requests)
			frames := make([]*FrameRecorded, requests)
			for i := range reqs {
				reqs[i] = requestFrame(byte(i), i)
				frames[i] = &FrameRecorded{
					Response: responseFrame(i, 8),
					Hash:     wirerecord.GetFrameHash(reqs[i], false),
				}
			}

			for iter := 0; iter < 50; iter++ {
				c := newTestReplayer(0x04, frames...)

				writeErr := make(chan error, 1)
				go func() {
					for _, req := range reqs {
						if _, err := c.Write(req); err != nil {
							writeErr <- err
							return
						}
					}
					writeErr <- nil
				}()

				readErr := make(chan error, 1)
				go func() {
					buf := make([]byte, 7)
					for {
						if _, err := c.Read(buf); err != nil {
							readErr <- err
							return
						}
					}
				}()

				if err := c.Close(); err != nil {
					t.Fatalf("iteration %d: Close: %v", iter, err)
				}
				if err := <-writeErr; err != nil && !errors.Is(err, net.ErrClosed) {
					t.Fatalf("iteration %d: Write = %v, want nil or net.ErrClosed", iter, err)
				}
				select {
				case err := <-readErr:
					if !errors.Is(err, io.EOF) {
						t.Fatalf("iteration %d: Read = %v, want io.EOF", iter, err)
					}
				case <-time.After(5 * time.Second):
					t.Fatalf("iteration %d: Read did not return within 5s of Close", iter)
				}
			}
		})

		// Concurrent pins, under -race, that one writer and
		// one reader -- what the driver gives this connection -- get every response back in
		// order with its request's stream id.
		t.Run("Concurrent", func(t *testing.T) {
			const (
				requests    = 200
				bodyLen     = 8
				responseLen = wirerecord.FrameHeaderLen + bodyLen
			)

			// A distinct opcode per request: GetFrameHash blanks the stream id before hashing, so
			// that is what tells two of these apart and pairs each with its own response.
			reqs := make([][]byte, requests)
			frames := make([]*FrameRecorded, requests)
			for i := range reqs {
				reqs[i] = requestFrame(byte(i), i)
				frames[i] = &FrameRecorded{
					Response: responseFrame(i, bodyLen),
					Hash:     wirerecord.GetFrameHash(reqs[i], false),
				}
			}
			c := newTestReplayer(0x04, frames...)

			writeErr := make(chan error, 1)
			go func() {
				for _, req := range reqs {
					if _, err := c.Write(req); err != nil {
						writeErr <- err
						return
					}
				}
				writeErr <- nil
			}()

			// A buffer smaller than one response keeps the reader re-entering the handoff.
			got := make([]byte, 0, requests*responseLen)
			buf := make([]byte, 7)
			for len(got) < requests*responseLen {
				n, err := c.Read(buf)
				if err != nil {
					t.Fatalf("Read after %d of %d bytes: %v", len(got), requests*responseLen, err)
				}
				got = append(got, buf[:n]...)
			}

			if err := <-writeErr; err != nil {
				t.Fatalf("Write: %v", err)
			}

			for i := range reqs {
				frame := got[i*responseLen : (i+1)*responseLen]
				if stream := int(frame[2])<<8 | int(frame[3]); stream != i {
					t.Fatalf("response %d carries stream id %d, want %d", i, stream, i)
				}
			}

			if err := c.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	})

	t.Run("Protocol", func(t *testing.T) {
		// AcceptsV4 pins that a v4 frame with a matching recorded
		// hash replays normally.
		t.Run("AcceptsV4", func(t *testing.T) {
			req := optionsFrame(0x04)
			c := newTestReplayer(0x04, &FrameRecorded{
				Response: optionsFrame(0x84),
				Hash:     wirerecord.GetFrameHash(req, false),
			})

			n, err := c.Write(req)
			if err != nil {
				t.Fatalf("Write(v4 frame) error = %v, want nil", err)
			}
			if n != len(req) {
				t.Errorf("Write(v4 frame) = %d bytes, want %d", n, len(req))
			}
		})

		// RefusesPreV3 is the replayer's half of the protocol floor, and
		// it pins the worse of the two failures it prevents. A pre-v3 frame the splitter lets
		// through is not merely unmatched: Write reports every byte written and queues no
		// response, so the driver's next Read parks in awaitPendingFrame and the connection hangs
		// instead of failing.
		//
		// A bodyless v2 OPTIONS is what reached that state -- a complete v1/v2 frame one byte
		// short of the v3+ header the splitter slices on, from the request the control
		// connection heartbeats. The error has to be the splitter's own, raised ahead of
		// matchRequest, so it names the frame's version rather than reporting a recording
		// mismatch or a hash that matched nothing.
		t.Run("RefusesPreV3", func(t *testing.T) {
			c := newTestReplayer(0x04)

			// Eight bytes: version, flags, a 1-byte stream id, opOptions, and a zero body
			// length.
			n, err := c.Write([]byte{0x02, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00})
			if err == nil {
				t.Fatal("a protocol v2 frame was replayed")
			}
			if !strings.Contains(err.Error(), "protocol v2") {
				t.Errorf("error does not name the frame's version: %v", err)
			}
			// Distinguishes the floor from matchRequest's recording-mismatch check, which
			// would also name v2 -- and which a frame refused here never reaches.
			if !strings.Contains(err.Error(), "v3+ frame header") {
				t.Errorf("refusal did not come from the protocol floor: %v", err)
			}
			if n != 0 {
				t.Errorf("Write reported %d bytes written, want 0", n)
			}
		})

		// RejectsMismatch pins that replaying a recording at
		// the wrong protocol version says so, rather than serving responses the driver rejects
		// deep inside its own header parsing.
		t.Run("RejectsMismatch", func(t *testing.T) {
			c := newTestReplayer(0x05)

			_, err := c.Write(optionsFrame(0x04))
			if err == nil {
				t.Fatal("a v4 request against a v5 recording was accepted")
			}
			if !strings.Contains(err.Error(), "protocol v5") || !strings.Contains(err.Error(), "protocol v4") {
				t.Errorf("error %q does not name both protocol versions", err)
			}
		})

		// UnsegmentedV5 pins that a v5 request is matched
		// rather than refused.
		//
		// It used to be refused outright, because the replayer could neither hash a transport
		// segment for matching nor patch a stream id into one without invalidating its CRC.
		// The handshake is still unsegmented on v5, so this frame goes through the plain path;
		// what changed is that reaching it is no longer an error.
		t.Run("UnsegmentedV5", func(t *testing.T) {
			req := optionsFrame(0x05)
			c := newTestReplayer(0x05, &FrameRecorded{
				Response: optionsFrame(0x85),
				Hash:     wirerecord.GetFrameHash(append([]byte(nil), req...), false),
			})

			if _, err := c.Write(append([]byte(nil), req...)); err != nil {
				t.Fatalf("Write(v5 frame) error = %v, want nil", err)
			}
		})
	})

	t.Run("Read", func(t *testing.T) {
		// DoesNotMutateRecording pins that the patch goes into a copy.
		// A FrameRecorded is shared and served once per benchmark iteration, so patching it in
		// place would rewrite the recording with the first request's stream id and leave every
		// later iteration depending on that.
		t.Run("DoesNotMutateRecording", func(t *testing.T) {
			const bodyLen = 32

			recorded := responseFrame(0x0040, bodyLen)
			pristine := append([]byte(nil), recorded...)

			frame := &FrameRecorded{Response: recorded}

			for i, streamID := range []int{0x0100, 0x0200, 0x0300} {
				request := optionsFrame(0x04)
				request[2] = byte(streamID >> 8)
				request[3] = byte(streamID & 0xFF)

				c := newTestReplayer(0x04, frame)
				frame.Hash = wirerecord.GetFrameHash(append([]byte(nil), request...), false)

				if _, err := c.Write(append([]byte(nil), request...)); err != nil {
					t.Fatalf("iteration %d: Write: %v", i, err)
				}
				got := readAll(t, c, len(recorded), 4096)

				if gotID := int(got[2])<<8 | int(got[3]); gotID != streamID {
					t.Errorf("iteration %d: served stream id %#04x, want %#04x", i, gotID, streamID)
				}
				if !bytes.Equal(frame.Response, pristine) {
					t.Fatalf("iteration %d: the recorded response was mutated in place", i)
				}
			}
		})

		// LatchesMaterialiseFailure pins that a response that cannot be
		// materialised ends the connection rather than being served up as an error forever.
		//
		// frameIdx is advanced only once the response is ready, so a failure leaves Read's branch
		// condition -- outgoingPos == len(outgoing) -- still true and the same record still
		// pending: the next Read fetches it again and fails the same way. The recorder latches the
		// analogous failure; this is the same latch, on the one goroutine that owns Read.
		t.Run("LatchesMaterialiseFailure", func(t *testing.T) {
			full := responseFrame(0x0040, 32)

			for _, tc := range []struct {
				name     string
				response []byte
			}{
				{name: "empty record", response: nil},
				{name: "truncated record", response: full[:wirerecord.FrameHeaderLen+16]},
			} {
				t.Run(tc.name, func(t *testing.T) {
					request := optionsFrame(0x04)
					c := newTestReplayer(0x04, &FrameRecorded{
						Response: append([]byte(nil), tc.response...),
						Hash:     wirerecord.GetFrameHash(append([]byte(nil), request...), false),
					})
					if _, err := c.Write(append([]byte(nil), request...)); err != nil {
						t.Fatalf("Write: %v", err)
					}

					n, first := c.Read(make([]byte, 64))
					if first == nil {
						t.Fatalf("Read returned (%d, nil) for a record that is not one whole frame", n)
					}

					// The latch is reported even for a zero-length read, the way wirerecord.Feed
					// reports one for an empty buffer: a caller that took (0, nil) for "nothing
					// yet, still healthy" would park on a connection that is finished.
					if n, err := c.Read(nil); err == nil || !errors.Is(err, first) {
						t.Errorf("Read(nil) = (%d, %v), want the latched %v", n, err, first)
					}

					// And every later read is the same error value, not another one just like
					// it -- a fresh fmt.Errorf per call is exactly what the unlatched loop
					// produced, and errors.Is compares identity, which is what tells them apart.
					n, second := c.Read(make([]byte, 64))
					if !errors.Is(second, first) {
						t.Errorf("second Read = (%d, %v), want the latched %v", n, second, first)
					}
					if n != 0 {
						t.Errorf("second Read returned %d bytes alongside its error", n)
					}
				})
			}
		})

		// PatchesStreamIDAcrossPartialReads pins the stream-id patch
		// against a caller whose buffer is smaller than the response.
		//
		// The patch used to be applied to the caller's buffer, and only on the branch where
		// the whole response fitted — so a response larger than the buffer was replayed
		// carrying the stream id it was recorded with, and the driver matched it to the wrong
		// in-flight request or to none. It went unnoticed because the largest response in the
		// checked-in recordings is well under the driver's 4 KiB read buffer, so the short
		// branch never ran.
		t.Run("PatchesStreamIDAcrossPartialReads", func(t *testing.T) {
			const (
				recordedStreamID = 0x0040
				liveStreamID     = 0x01F4
				bodyLen          = 500
			)

			request := optionsFrame(0x04)
			request[2] = byte(liveStreamID >> 8)
			request[3] = byte(liveStreamID & 0xFF)

			for _, chunk := range []int{1, 7, 64, 512, 4096} {
				t.Run(fmt.Sprintf("buffer of %d", chunk), func(t *testing.T) {
					response := responseFrame(recordedStreamID, bodyLen)
					c := newTestReplayer(0x04, &FrameRecorded{
						Response: append([]byte(nil), response...),
						Hash:     wirerecord.GetFrameHash(append([]byte(nil), request...), false),
					})

					if _, err := c.Write(append([]byte(nil), request...)); err != nil {
						t.Fatalf("Write: %v", err)
					}

					got := readAll(t, c, len(response), chunk)

					if len(got) != len(response) {
						t.Fatalf("served %d bytes, want %d", len(got), len(response))
					}
					if gotID := int(got[2])<<8 | int(got[3]); gotID != liveStreamID {
						t.Errorf("served stream id %#04x, want the live request's %#04x", gotID, liveStreamID)
					}
					// Everything except the stream id must be byte-for-byte the recording.
					if !bytes.Equal(got[4:], response[4:]) || got[0] != response[0] || got[1] != response[1] {
						t.Error("the response body was altered")
					}
				})
			}
		})

		// RejectsEmptyRecord pins the one damaged record that cannot
		// be served at all.
		//
		// A record with no bytes would have Read copy nothing into a buffer with room in it and
		// return (0, nil), having already counted the response as delivered -- so the driver
		// retries, finds nothing pending, and blocks for a request whose response is gone. A
		// line like {"data":null,"stream_id":5} decodes cleanly, so only this check stands
		// between a damaged recording and a hung test. It is the same check that catches a
		// record truncated part-way through a frame; see the test below.
		t.Run("RejectsEmptyRecord", func(t *testing.T) {
			request := optionsFrame(0x04)

			c := newTestReplayer(0x04, &FrameRecorded{
				Response: nil,
				Hash:     wirerecord.GetFrameHash(append([]byte(nil), request...), false),
			})

			if _, err := c.Write(append([]byte(nil), request...)); err != nil {
				t.Fatalf("Write: %v", err)
			}

			n, err := c.Read(make([]byte, 16))
			if err == nil {
				t.Fatalf("Read returned (%d, nil) for an empty record, want an error", n)
			}
			if n != 0 {
				t.Errorf("Read returned %d bytes alongside its error", n)
			}
			if !strings.Contains(err.Error(), "not one whole CQL frame") {
				t.Errorf("error %q does not say the recording is at fault", err)
			}
		})

		// RejectsTruncatedRecord pins that a record which is not one
		// whole frame is refused rather than served.
		//
		// Serving it used to be the documented behaviour, on the grounds that the driver would
		// reject it as a protocol error. It cannot: the driver reads a frame with io.ReadFull
		// twice -- the nine header bytes, then the body length that header declares -- and this
		// connection's SetReadDeadline does nothing, so a record stopping short of a whole frame
		// leaves it blocked mid-frame while Read waits for a request that is never coming.
		//
		// The second case is the one a length-only check misses: a complete header whose body
		// arrived short hangs exactly the same way.
		t.Run("RejectsTruncatedRecord", func(t *testing.T) {
			full := responseFrame(0x0040, 32)

			for _, tc := range []struct {
				name     string
				response []byte
			}{
				{"shorter than a header", []byte{0x84, 0x00}},
				{"header declares more body than arrived", full[:wirerecord.FrameHeaderLen+16]},
			} {
				t.Run(tc.name, func(t *testing.T) {
					request := optionsFrame(0x04)

					c := newTestReplayer(0x04, &FrameRecorded{
						Response: append([]byte(nil), tc.response...),
						Hash:     wirerecord.GetFrameHash(append([]byte(nil), request...), false),
					})

					if _, err := c.Write(append([]byte(nil), request...)); err != nil {
						t.Fatalf("Write: %v", err)
					}

					n, err := c.Read(make([]byte, 64))
					if err == nil {
						t.Fatalf("Read returned (%d, nil) for a truncated record, want an error", n)
					}
					if n != 0 {
						t.Errorf("Read returned %d bytes alongside its error", n)
					}
					if !strings.Contains(err.Error(), "not one whole CQL frame") {
						t.Errorf("error %q does not say the recording is at fault", err)
					}
				})
			}
		})

		// ReleasesOutsizedBuffers pins that neither reused buffer
		// keeps an outlier's capacity for the life of the connection.
		//
		// Both are reused so that serving a response allocates nothing, and a recording may hold
		// a response of any size the driver's frame limit allows -- so without a bound one large
		// response pins it twice over, patched and encoded, for as long as the connection lives.
		t.Run("ReleasesOutsizedBuffers", func(t *testing.T) {
			const (
				big   = 2 * maxRetainedResponse
				small = 8
			)

			first := requestFrame(0x05, 0x0001)  // OPTIONS
			second := requestFrame(0x0B, 0x0002) // REGISTER

			c := newTestReplayer(0x04,
				&FrameRecorded{
					Response: responseFrame(0x0040, big),
					Hash:     wirerecord.GetFrameHash(append([]byte(nil), first...), false),
				},
				&FrameRecorded{
					Response: responseFrame(0x0041, small),
					Hash:     wirerecord.GetFrameHash(append([]byte(nil), second...), false),
				},
			)

			if _, err := c.Write(append([]byte(nil), first...)); err != nil {
				t.Fatalf("Write: %v", err)
			}
			readAll(t, c, wirerecord.FrameHeaderLen+big, 4096)

			// patched is released as soon as the response has been encoded out of it.
			if cap(c.patched) > maxRetainedResponse {
				t.Errorf("patched still holds %d bytes after an outsized response", cap(c.patched))
			}
			// outgoing is what is being served, so it is still here until the next response
			// needs the buffer.
			if cap(c.outgoing) <= maxRetainedResponse {
				t.Fatalf("outgoing holds %d bytes, want the response just served", cap(c.outgoing))
			}

			if _, err := c.Write(append([]byte(nil), second...)); err != nil {
				t.Fatalf("Write: %v", err)
			}
			readAll(t, c, wirerecord.FrameHeaderLen+small, 4096)

			if cap(c.outgoing) > maxRetainedResponse {
				t.Errorf("outgoing still holds %d bytes after the outsized response drained", cap(c.outgoing))
			}
			// The bound must not cost the reuse it exists to protect: an ordinary response keeps
			// its buffer.
			if cap(c.outgoing) == 0 {
				t.Error("outgoing was released although it is within the bound")
			}
		})
	})
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package recorder

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/endpoint"
)

func TestConnectionRecorder(t *testing.T) {
	t.Run("Close", func(t *testing.T) {
		// ClosesEverything pins that one failing close does not
		// strand the rest. Close used to return at the first error, leaving the other
		// recording file and the socket itself open -- under the driver's redial loop, one
		// descriptor and one connection per attempt, which is the leak NewConnectionRecorder
		// was fixed for on its setup path.
		t.Run("ClosesEverything", func(t *testing.T) {
			conn := &stubConn{}
			rec, err := NewConnectionRecorder(filepath.Join(t.TempDir(), "conn"), conn, nil)
			if err != nil {
				t.Fatalf("NewConnectionRecorder: %v", err)
			}
			recorder := rec.(*ConnectionRecorder)

			// Closing the Writes file underneath is what a full disk or an I/O error looks
			// like from Close: the first close it makes reports one.
			if err := recorder.fd_writes.Close(); err != nil {
				t.Fatalf("closing the Writes file: %v", err)
			}

			if err := rec.Close(); err == nil {
				t.Error("Close reported success although the Writes file was already closed")
			}
			if !conn.closed {
				t.Error("Close left the wrapped connection open")
			}
			if err := recorder.fd_reads.Close(); err == nil {
				t.Error("Close left the Reads file open")
			}
		})

		// DoesNotLatchOwnTeardown pins that the close's own
		// casualties are not reported as the reason the recording ended.
		//
		// gocql closes a connection from whichever goroutine noticed it was done, while the
		// write coalescer may still be inside WriteTo, so a graceful close routinely fails an
		// in-flight write with net.ErrClosed. Latched, that outranks everything else Close has
		// to say -- and closeWithError hands Close's error to errorHandler.HandleError, so a
		// complete recording reported its own teardown as a connection failure.
		t.Run("DoesNotLatchOwnTeardown", func(t *testing.T) {
			conn := &closeRacingConn{
				inWrite:  make(chan struct{}),
				release:  make(chan struct{}),
				observed: make(chan struct{}),
			}

			rec, err := NewConnectionRecorder(filepath.Join(t.TempDir(), "conn"), conn, nil)
			if err != nil {
				t.Fatalf("NewConnectionRecorder: %v", err)
			}

			writeErr := make(chan error, 1)
			go func() {
				_, err := rec.Write(startupFrame())
				close(conn.observed)
				writeErr <- err
			}()

			<-conn.inWrite
			if err := rec.Close(); err != nil {
				t.Errorf("Close reported %v; nothing was wrong with the recording", err)
			}

			// The caller still learns its write failed. Only the latch is skipped.
			if err := <-writeErr; !errors.Is(err, net.ErrClosed) {
				t.Errorf("Write returned %v, want the socket error to reach the caller", err)
			}
		})

		// Idempotent pins that a second Close runs no second
		// teardown and reports what the first one did. Closing an already-closed everything
		// turned a healthy recording's nil into os.ErrClosed for each file joined with the
		// socket's net.ErrClosed -- and on the driver route Conn.closeWithError hands that to
		// errorHandler.HandleError, so a complete recording was reported as the reason the
		// connection ended. An explicit Close alongside a deferred one is the ordinary shape.
		t.Run("Idempotent", func(t *testing.T) {
			startup := startupFrame()

			for _, tc := range []struct {
				name  string
				write []byte
				want  string
			}{
				{name: "a complete recording", write: startup},
				{name: "a truncated recording", write: startup[:len(startup)-3], want: "truncated"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					conn := &stubConn{}
					rec, err := NewConnectionRecorder(filepath.Join(t.TempDir(), "conn"), conn, nil)
					if err != nil {
						t.Fatalf("NewConnectionRecorder: %v", err)
					}
					if _, err := rec.Write(tc.write); err != nil {
						t.Fatalf("Write: %v", err)
					}

					first := rec.Close()
					if tc.want == "" && first != nil {
						t.Fatalf("Close reported %v for a complete recording", first)
					}
					if tc.want != "" && (first == nil || !strings.Contains(first.Error(), tc.want)) {
						t.Fatalf("Close reported %v, want one mentioning %q", first, tc.want)
					}

					if second := rec.Close(); second != first {
						t.Errorf("the second Close reported %v, want the first one's %v", second, first)
					}
					if conn.closes != 1 {
						t.Errorf("the socket was closed %d times, want once", conn.closes)
					}
				})
			}
		})

		// ReportsEveryFailure pins that the close that failed first
		// does not silence the ones after it. Close kept only the first file error and then
		// dropped the socket's entirely -- and the socket's is the one closeWithError hands to
		// errorHandler.HandleError, so a Writes file that would not close hid the connection
		// failure the driver was about to be told about.
		t.Run("ReportsEveryFailure", func(t *testing.T) {
			sockErr := errors.New("closing the socket")
			conn := &stubConn{closeErr: sockErr}
			rec, err := NewConnectionRecorder(filepath.Join(t.TempDir(), "conn"), conn, nil)
			if err != nil {
				t.Fatalf("NewConnectionRecorder: %v", err)
			}
			recorder := rec.(*ConnectionRecorder)

			if err := recorder.fd_writes.Close(); err != nil {
				t.Fatalf("closing the Writes file: %v", err)
			}

			closeErr := rec.Close()
			if closeErr == nil {
				t.Fatal("Close reported success although the Writes file was already closed")
			}
			if !strings.Contains(closeErr.Error(), "Writes file") {
				t.Errorf("Close reported %v, which does not say which file it was", closeErr)
			}
			if !errors.Is(closeErr, sockErr) {
				t.Errorf("Close reported %v, which drops the socket's own failure", closeErr)
			}
		})

		// ReportsTruncatedStream pins that a stream ending in
		// the middle of a frame is reported at Close. On disk that recording looks complete,
		// and at load time the loss surfaces only as an unpaired stream or an unmatched
		// hash, with nothing pointing at the session that was cut short.
		t.Run("ReportsTruncatedStream", func(t *testing.T) {
			startup := startupFrame()

			rec, err := NewConnectionRecorder(filepath.Join(t.TempDir(), "conn"), &stubConn{}, nil)
			if err != nil {
				t.Fatalf("NewConnectionRecorder: %v", err)
			}

			if _, err := rec.Write(startup[:len(startup)-3]); err != nil {
				t.Fatalf("Write: %v", err)
			}

			err = rec.Close()
			if err == nil {
				t.Fatal("closing a connection mid-frame reported nothing")
			}
			if !strings.Contains(err.Error(), "truncated") {
				t.Errorf("error %q does not say the recording is truncated", err)
			}
		})

		// SafeWhileReading pins that Close may run while the read
		// goroutine is still inside Read. Worth running under -race, which is what catches it.
		//
		// gocql closes a connection from whichever goroutine noticed it was finished while
		// serve() can still be in Read, so a net.Conn has to tolerate the overlap. Close used to
		// shut the recording files before the socket and then read decoder state that Feed was
		// concurrently appending to: the in-flight frames were lost to an "file already closed"
		// that named nothing, and the truncation verdict came out differently depending on which
		// goroutine got there first.
		t.Run("SafeWhileReading", func(t *testing.T) {
			response := optionsFrame(0x84)
			conn := &endlessConn{frame: response}

			rec, err := NewConnectionRecorder(filepath.Join(t.TempDir(), "conn"), conn, nil)
			if err != nil {
				t.Fatalf("NewConnectionRecorder: %v", err)
			}

			done := make(chan struct{})
			go func() {
				defer close(done)
				buf := make([]byte, len(response))
				for {
					if _, err := rec.Read(buf); err != nil {
						return
					}
				}
			}()

			// Long enough for the reader to be somewhere inside Read.
			time.Sleep(time.Millisecond)

			if err := rec.Close(); err != nil {
				t.Errorf("Close while reading: %v", err)
			}
			<-done

			// Spelled through the embedded struct on purpose: that is the field a helper
			// written against stubConn reads, and endlessConn used to declare a second one
			// that shadowed it, so its Close left this one false with nothing to notice.
			if !conn.stubConn.closed {
				t.Error("Close left the wrapped connection open")
			}
		})
	})

	t.Run("Fail", func(t *testing.T) {
		// KeepsOneCause pins that fail reports the latched failure
		// rather than its argument. Read and Write hand what fail returns straight back to the
		// driver while Close ranks the latched value first, so returning the argument would have
		// the driver's error handler and the Close caller naming different causes for the same
		// recording -- and the later one is always the symptom, never the cause.
		t.Run("KeepsOneCause", func(t *testing.T) {
			var c ConnectionRecorder

			first := errors.New("the segment payload failed its CRC32")
			later := errors.New("write tcp: broken pipe")

			if got := c.fail(first); !errors.Is(got, first) {
				t.Fatalf("fail(first) = %v, want the first failure back", got)
			}
			if got := c.fail(later); !errors.Is(got, first) {
				t.Errorf("fail(later) = %v, want the latched %v", got, first)
			}
			if got := c.failed(); !errors.Is(got, first) {
				t.Errorf("failed() = %v, want the latched %v", got, first)
			}
		})
	})

	t.Run("NewConnectionRecorder", func(t *testing.T) {
		t.Run("RefusesSymlink", func(t *testing.T) {
			if recordingNoFollow == 0 {
				t.Skip("O_NOFOLLOW is not available on this platform")
			}

			dir := t.TempDir()
			target := filepath.Join(dir, "target")
			const targetContents = "must stay untouched\n"
			if err := os.WriteFile(target, []byte(targetContents), 0o666); err != nil {
				t.Fatalf("creating symlink target: %v", err)
			}
			if err := os.Chmod(target, 0o666); err != nil {
				t.Fatalf("making symlink target permissive: %v", err)
			}

			fname := filepath.Join(dir, "conn")
			if err := os.Symlink(target, fname+"Reads"); err != nil {
				t.Fatalf("creating recording symlink: %v", err)
			}

			rec, err := NewConnectionRecorder(fname, &stubConn{}, nil)
			if err == nil {
				rec.Close()
				t.Fatal("NewConnectionRecorder followed a recording symlink")
			}
			if rec != nil {
				t.Fatalf("NewConnectionRecorder returned %T alongside error %v", rec, err)
			}

			info, err := os.Stat(target)
			if err != nil {
				t.Fatalf("stat symlink target: %v", err)
			}
			if got := info.Mode().Perm(); got != 0o666 {
				t.Errorf("symlink target permissions = %04o, want 0666", got)
			}
			contents, err := os.ReadFile(target)
			if err != nil {
				t.Fatalf("read symlink target: %v", err)
			}
			if got := string(contents); got != targetContents {
				t.Errorf("symlink target contents = %q, want %q", got, targetContents)
			}

			// The failed setup left no recording to keep.
			if err := os.Remove(fname + "Writes"); err != nil {
				t.Errorf("removing Writes file after setup failure: %v", err)
			}
		})

		// RestrictsFilePermissions pins both halves of the recording
		// file permission contract: a new file starts owner-only, and reopening an older,
		// permissive file tightens it. The Writes file can hold AUTH_RESPONSE credentials in
		// plaintext, so relying on the process umask is not enough.
		t.Run("RestrictsFilePermissions", func(t *testing.T) {
			if runtime.GOOS == "windows" {
				t.Skip("Windows does not expose Unix permission bits")
			}
			const existingContents = "existing recording\n"
			suffixes := [...]string{"Writes", "Reads"}

			for _, tc := range []struct {
				name     string
				existing bool
			}{
				{name: "new files"},
				{name: "existing permissive files", existing: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					fname := filepath.Join(t.TempDir(), "conn")
					if tc.existing {
						for _, suffix := range suffixes {
							path := fname + suffix
							if err := os.WriteFile(path, []byte(existingContents), 0o666); err != nil {
								t.Fatalf("creating %s: %v", suffix, err)
							}
							if err := os.Chmod(path, 0o666); err != nil {
								t.Fatalf("making %s permissive: %v", suffix, err)
							}
						}
					}

					rec, err := NewConnectionRecorder(fname, &stubConn{}, nil)
					if err != nil {
						t.Fatalf("NewConnectionRecorder: %v", err)
					}
					t.Cleanup(func() {
						if err := rec.Close(); err != nil {
							t.Errorf("Close: %v", err)
						}
					})

					for _, suffix := range suffixes {
						info, err := os.Stat(fname + suffix)
						if err != nil {
							t.Fatalf("stat %s: %v", suffix, err)
						}
						if got := info.Mode().Perm(); got != 0o600 {
							t.Errorf("%s permissions = %04o, want 0600", suffix, got)
						}
						if tc.existing {
							contents, err := os.ReadFile(fname + suffix)
							if err != nil {
								t.Fatalf("read %s: %v", suffix, err)
							}
							if got := string(contents); got != existingContents {
								t.Errorf("%s contents = %q, want %q", suffix, got, existingContents)
							}
						}
					}
				})
			}
		})
	})

	t.Run("Protocol", func(t *testing.T) {
		// AcceptsV4 pins that the rejection is scoped to
		// v5+: a v4 frame (with and without the direction bit) is recorded normally.
		t.Run("AcceptsV4", func(t *testing.T) {
			fname := filepath.Join(t.TempDir(), "conn")
			rec, err := NewConnectionRecorder(fname, &stubConn{readData: optionsFrame(0x84)}, nil)
			if err != nil {
				t.Fatalf("NewConnectionRecorder: %v", err)
			}
			defer rec.Close()

			if _, err := rec.Write(optionsFrame(0x04)); err != nil {
				t.Fatalf("Write(v4 frame) error = %v, want nil", err)
			}
			buf := make([]byte, 64)
			if _, err := rec.Read(buf); err != nil {
				t.Fatalf("Read(v4 response) error = %v, want nil", err)
			}

			for _, suffix := range []string{"Writes", "Reads"} {
				data, err := os.ReadFile(fname + suffix)
				if err != nil {
					t.Fatalf("reading the %s record file: %v", suffix, err)
				}
				if len(data) == 0 {
					t.Errorf("the v4 frame was not recorded to the %s file", suffix)
				}
			}
		})

		// RefusesPreV3 is the other side of AcceptsProtoV4: a connection
		// a caller pinned to protocol v1 or v2 is refused by name, and nothing reaches the
		// recording.
		//
		// What used to happen was worse than an error. Every offset here reads the v3+ header,
		// so a v2 stream was sliced a byte late, and the write succeeded -- the file filled
		// with records whose frames were not frames, and whose stream ids came from the middle
		// of a body. Nothing said so until a replay of them failed to match anything.
		//
		// Both a bodyful frame and a bodyless one, because Write gates the socket on the
		// recording: a frame the splitter fails to refuse is forwarded to the server and
		// recorded nowhere. The bodyless case is the one that got that far.
		t.Run("RefusesPreV3", func(t *testing.T) {
			for _, tc := range []struct {
				name  string
				frame []byte
			}{
				{name: "a bodyful STARTUP", frame: startupFrameV2()},
				{name: "a bodyless OPTIONS", frame: optionsFrameV2()},
			} {
				t.Run(tc.name, func(t *testing.T) {
					fname := filepath.Join(t.TempDir(), "conn")
					conn := &stubConn{}
					rec, err := NewConnectionRecorder(fname, conn, nil)
					if err != nil {
						t.Fatalf("NewConnectionRecorder: %v", err)
					}
					defer rec.Close()

					n, err := rec.Write(tc.frame)
					if err == nil {
						t.Fatal("a protocol v2 connection was recorded")
					}
					if !strings.Contains(err.Error(), "protocol v2") {
						t.Errorf("error does not name the version: %v", err)
					}
					if n != 0 {
						t.Errorf("Write reported %d bytes written, want 0", n)
					}
					if len(conn.written) != 0 {
						t.Errorf("%d bytes reached the server from a refused connection", len(conn.written))
					}
					if got := recordedFrames(t, fname+"Writes"); len(got) != 0 {
						t.Errorf("recorded %d frames from a refused connection, want 0", len(got))
					}
				})
			}
		})

		// RefusesPreV5Compression pins that a compressed connection below protocol
		// v5 is refused where the cause is still visible.
		//
		// Below v5 a negotiated compressor compresses frame bodies, not transport segments, so
		// nothing here would fail: the recorder would write the frames out with compressed
		// bodies and look successful. The cost lands on whoever replays that recording, as a
		// panic from GetFrameHash finding no match -- because a compressed body carries the
		// encoded default timestamp, so the hash differs on every run -- with nothing in it
		// pointing at compression.
		t.Run("RefusesPreV5Compression", func(t *testing.T) {
			// A v4 STARTUP naming lz4: same body as the v5 one, one version byte apart.
			startup := startupV5("lz4")
			startup[0] = 0x04

			fname := filepath.Join(t.TempDir(), "conn")
			rec, err := NewConnectionRecorder(fname, &stubConn{}, passthroughCompressor{})
			if err != nil {
				t.Fatalf("NewConnectionRecorder: %v", err)
			}
			defer rec.Close()

			_, err = rec.Write(startup)
			if err == nil {
				t.Fatal("a compressed protocol v4 connection was recorded")
			}
			for _, want := range []string{"lz4", "v4"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q", err, want)
				}
			}
		})

		// UnsegmentedV5 pins that a v5 connection is
		// recorded rather than refused.
		//
		// It used to be refused outright, because the recorder sliced the byte stream on fixed
		// CQL header offsets and a v5 transport segment has none of them. The handshake is
		// still unsegmented on v5, so these frames go through the plain path; what changed is
		// that reaching them is no longer an error.
		t.Run("UnsegmentedV5", func(t *testing.T) {
			fname := filepath.Join(t.TempDir(), "conn")
			rec, err := NewConnectionRecorder(fname, &stubConn{readData: optionsFrame(0x85)}, nil)
			if err != nil {
				t.Fatalf("NewConnectionRecorder: %v", err)
			}
			defer rec.Close()

			if _, err := rec.Write(optionsFrame(0x05)); err != nil {
				t.Fatalf("Write(v5 frame) error = %v, want nil", err)
			}
			buf := make([]byte, 64)
			if _, err := rec.Read(buf); err != nil {
				t.Fatalf("Read(v5 response) error = %v, want nil", err)
			}

			for _, suffix := range []string{"Writes", "Reads"} {
				got := recordedFrames(t, fname+suffix)
				if len(got) != 1 {
					t.Fatalf("%s: recorded %d frames, want 1", suffix, len(got))
				}
			}
		})
	})

	t.Run("Read", func(t *testing.T) {
		// HandsOnNothingUnrecorded pins the read path's half of the
		// contract TestConnectionRecorder/Write/SendsNothingUnrecorded pins for writes: a frame that
		// could not be recorded does not reach the driver either. It used to return the bytes
		// alongside the error, and a bufio.Reader hands those on before it reports the error
		// -- so the driver parsed and acted on frames the recording does not hold, and the
		// Reads and Writes files disagreed about what crossed the wire.
		t.Run("HandsOnNothingUnrecorded", func(t *testing.T) {
			rec, err := NewConnectionRecorder(filepath.Join(t.TempDir(), "conn"), &stubConn{readData: optionsFrame(0x84)}, nil)
			if err != nil {
				t.Fatalf("NewConnectionRecorder: %v", err)
			}
			recorder := rec.(*ConnectionRecorder)

			// Closing the Reads file underneath is what a full disk looks like from record:
			// the frame decodes, and writing it out is what fails.
			if err := recorder.fd_reads.Close(); err != nil {
				t.Fatalf("closing the Reads file: %v", err)
			}

			n, err := rec.Read(make([]byte, 64))
			if err == nil {
				t.Fatal("a frame that could not be recorded was handed to the driver")
			}
			if n != 0 {
				t.Errorf("Read = (%d, %v), want (0, err)", n, err)
			}
			if _, err := rec.Write(optionsFrame(0x04)); err == nil {
				t.Error("the recording failure was not latched")
			}
		})

		// PropagatesEOF pins that a server-closed connection reports
		// io.EOF. The recorder must record on EOF too -- a final read can carry data -- but
		// it used to return the recording step's error in its place, which is nil, so a dead
		// connection read as (0, nil) forever: the driver reads through io.ReadFull, which
		// loops while err is nil, and spun at full speed instead of tearing the
		// connection down.
		t.Run("PropagatesEOF", func(t *testing.T) {
			response := optionsFrame(0x84)
			rec, err := NewConnectionRecorder(filepath.Join(t.TempDir(), "conn"), &stubConn{readData: response}, nil)
			if err != nil {
				t.Fatalf("NewConnectionRecorder: %v", err)
			}
			defer rec.Close()

			buf := make([]byte, len(response))
			if n, err := rec.Read(buf); err != nil || n != len(response) {
				t.Fatalf("Read = (%d, %v), want (%d, nil)", n, err, len(response))
			}
			if _, err := rec.Read(buf); err != io.EOF {
				t.Fatalf("Read at end of stream = %v, want io.EOF", err)
			}
		})

		// RecordsPartialErroredRead pins that bytes delivered
		// alongside a non-EOF error still reach the recording.
		//
		// A net.Conn may return n > 0 together with an error, and in this driver that is
		// routine rather than terminal: connReader.Read arms a deadline per attempt and resumes
		// a read that timed out while still making progress, up to maxReadAttempts times, so a
		// large frame body over a slow link arrives in pieces on a connection that carries
		// straight on. Returning before recording those bytes dropped them from the recording
		// and left the read decoder mid-frame at a stale offset, so every frame after them was
		// assembled from the wrong bytes -- and nothing was latched, so neither the driver nor
		// Close ever said so.
		t.Run("RecordsPartialErroredRead", func(t *testing.T) {
			response := optionsFrame(0x84)
			const split = 4 // mid-header, so a dropped prefix mis-frames everything after it

			conn := &scriptedConn{reads: []scriptedRead{
				{data: response[:split], err: timeoutErr{}},
				{data: response[split:]},
			}}

			dir := t.TempDir()
			rec, err := NewConnectionRecorder(filepath.Join(dir, "conn"), conn, nil)
			if err != nil {
				t.Fatalf("NewConnectionRecorder: %v", err)
			}

			buf := make([]byte, len(response))
			n, err := rec.Read(buf)
			if n != split {
				t.Fatalf("first Read = %d bytes, want %d", n, split)
			}
			if netErr, ok := errors.AsType[net.Error](err); !ok || !netErr.Timeout() {
				t.Fatalf("first Read error = %v, want a timeout the driver would resume", err)
			}

			if n, err := rec.Read(buf); err != nil || n != len(response)-split {
				t.Fatalf("second Read = (%d, %v), want (%d, nil)", n, err, len(response)-split)
			}

			if err := rec.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			records := recordedFrames(t, filepath.Join(dir, "connReads"))
			if len(records) != 1 {
				t.Fatalf("recorded %d frames, want 1", len(records))
			}
			if !bytes.Equal(records[0].Data, response) {
				t.Errorf("recorded frame = %x, want %x", records[0].Data, response)
			}
		})

		// SplitsCoalescedFrames pins the other half of the framing:
		// a call carrying more than one frame is recorded as the frames it holds, not as
		// one blob leaving its successor to start mid-frame.
		//
		// The read side is where this happens for real. Conn wraps the dialed connection in
		// a bufio.Reader (conn.go), which fills with reads of up to its buffer size, so two
		// responses that arrive together reach the recorder in a single call.
		t.Run("SplitsCoalescedFrames", func(t *testing.T) {
			t.Run("two whole frames in one read", func(t *testing.T) {
				first, second := optionsFrame(0x84), optionsFrame(0x84)
				second[3] = 0x02 // a different stream id, so the two records are telling apart

				fname := filepath.Join(t.TempDir(), "conn")
				rec, err := NewConnectionRecorder(fname, &stubConn{readData: append(append([]byte{}, first...), second...)}, nil)
				if err != nil {
					t.Fatalf("NewConnectionRecorder: %v", err)
				}
				defer rec.Close()

				if _, err := rec.Read(make([]byte, 4096)); err != nil {
					t.Fatalf("Read: %v", err)
				}

				records := recordedFrames(t, fname+"Reads")
				if len(records) != 2 {
					t.Fatalf("recorded %d frames, want 2", len(records))
				}
				for i, want := range [][]byte{first, second} {
					if !bytes.Equal(records[i].Data, want) {
						t.Errorf("record %d = % X, want % X", i, records[i].Data, want)
					}
					if wantID := int(want[2])<<8 | int(want[3]); records[i].StreamID != wantID {
						t.Errorf("record %d: stream id = %d, want %d", i, records[i].StreamID, wantID)
					}
				}
			})

			t.Run("a frame and part of the next", func(t *testing.T) {
				startup, options := startupFrame(), optionsFrame(0x04)

				fname := filepath.Join(t.TempDir(), "conn")
				rec, err := NewConnectionRecorder(fname, &stubConn{}, nil)
				if err != nil {
					t.Fatalf("NewConnectionRecorder: %v", err)
				}
				defer rec.Close()

				// The boundary falls inside the OPTIONS header, so the second frame is picked
				// up mid-header rather than at a clean start.
				for _, chunk := range [][]byte{append(append([]byte{}, startup...), options[:4]...), options[4:]} {
					if _, err := rec.Write(chunk); err != nil {
						t.Fatalf("Write: %v", err)
					}
				}

				records := recordedFrames(t, fname+"Writes")
				if len(records) != 2 {
					t.Fatalf("recorded %d frames, want 2", len(records))
				}
				if !bytes.Equal(records[0].Data, startup) {
					t.Errorf("STARTUP recorded as % X, want % X", records[0].Data, startup)
				}
				if !bytes.Equal(records[1].Data, options) {
					t.Errorf("OPTIONS recorded as % X, want % X", records[1].Data, options)
				}
				// The latch fired on the STARTUP even though the call did not end with it.
				for i, record := range records {
					if !record.UseMetadataID {
						t.Errorf("record %d: UseMetadataID = false, want true", i)
					}
				}
			})
		})
	})

	t.Run("RecordDialer", func(t *testing.T) {
		// ClosesSocketWhenRecordingCannotStart pins that a dial whose
		// recording cannot be opened does not strand the connection it has already made.
		//
		// NewConnectionRecorder returns (nil, err) for anything os.OpenFile refuses -- a
		// missing recording directory, a read-only volume, a full disk, EMFILE -- and hands
		// the caller no net.Conn to close. The driver answers a failed dial by redialing, so
		// a socket left behind here is one stranded pair per attempt.
		t.Run("ClosesSocketWhenRecordingCannotStart", func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("Listen: %v", err)
			}
			defer ln.Close()

			accepted := make(chan net.Conn, 1)
			go func() {
				conn, err := ln.Accept()
				if err != nil {
					close(accepted)
					return
				}
				accepted <- conn
			}()

			d := NewRecordDialer(filepath.Join(t.TempDir(), "no-such-directory"))
			if conn, err := d.DialContext(context.Background(), endpoint.DialTarget{Network: "tcp", Address: ln.Addr().String()}); err == nil {
				conn.Close()
				t.Fatal("DialContext succeeded although the recording directory does not exist")
			}

			server := <-accepted
			if server == nil {
				t.Fatal("the dial never reached the listener")
			}
			defer server.Close()

			// The far end reading EOF is the only thing that can be observed about a socket
			// the dialer no longer hands out. Deadlined so a leak fails this test rather than
			// hanging it until the package timeout.
			if err := server.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatalf("SetReadDeadline: %v", err)
			}
			if _, err := server.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
				t.Errorf("server-side read = %v, want io.EOF; the dialed socket was left open", err)
			}
		})
	})

	t.Run("Write", func(t *testing.T) {
		// LatchesSendFailure pins that a request recorded but not sent ends the
		// recording rather than being left behind in it.
		//
		// Recording happens before the send, so that record is already on disk and cannot be
		// recalled. The loader keys records by stream id and keeps the last one, and the
		// driver reuses stream ids, so left to run the connection would hand the loader an
		// unsent request paired with an earlier exchange's response -- served on replay as a
		// plausible answer to a question nobody asked.
		t.Run("LatchesSendFailure", func(t *testing.T) {
			sendErr := errors.New("connection reset by peer")
			conn := &stubConn{readData: optionsFrame(0x84), writeErr: sendErr}
			rec, err := NewConnectionRecorder(filepath.Join(t.TempDir(), "conn"), conn, nil)
			if err != nil {
				t.Fatalf("NewConnectionRecorder: %v", err)
			}
			defer rec.Close()

			if _, err := rec.Write(startupFrame()); !errors.Is(err, sendErr) {
				t.Fatalf("Write error = %v, want %v", err, sendErr)
			}

			// The connection healing afterwards is what the latch is for: recording resumes
			// only if nothing remembers that a request went unsent.
			conn.writeErr = nil
			if _, err := rec.Write(optionsFrame(0x04)); !errors.Is(err, sendErr) {
				t.Errorf("Write after a failed send = %v, want the latched %v", err, sendErr)
			}
			if _, err := rec.Read(make([]byte, 64)); !errors.Is(err, sendErr) {
				t.Errorf("Read after a failed send = %v, want the latched %v", err, sendErr)
			}
		})

		// ReassemblesSplitFrames pins that a frame delivered over
		// several calls is recorded once and whole.
		//
		// The metadata-ID latch is what makes this visible: it looks for the opt-in key in
		// a completed STARTUP, so a record cut short at an arbitrary byte misses it, every
		// later EXECUTE is stamped UseMetadataID=false, and replay fails as silent hash
		// mismatches rather than as an error. The recorder used to restart its record on
		// every call, so any frame that did not arrive in one piece was truncated.
		t.Run("ReassemblesSplitFrames", func(t *testing.T) {
			startup := startupFrame()

			for _, tc := range []struct {
				name  string
				split int
			}{
				// Before the 9-byte header is complete the declared length is unknown, so
				// this exercises the path that returns without deciding anything.
				{"split inside the header", 5},
				{"split inside the body", 30},
			} {
				t.Run(tc.name, func(t *testing.T) {
					fname := filepath.Join(t.TempDir(), "conn")
					rec, err := NewConnectionRecorder(fname, &stubConn{}, nil)
					if err != nil {
						t.Fatalf("NewConnectionRecorder: %v", err)
					}
					defer rec.Close()

					for _, chunk := range [][]byte{startup[:tc.split], startup[tc.split:], optionsFrame(0x04)} {
						if _, err := rec.Write(chunk); err != nil {
							t.Fatalf("Write: %v", err)
						}
					}

					records := recordedFrames(t, fname+"Writes")
					if len(records) != 2 {
						t.Fatalf("recorded %d frames, want 2 (the split STARTUP and the OPTIONS)", len(records))
					}

					if !bytes.Equal(records[0].Data, startup) {
						t.Errorf("STARTUP recorded as % X, want % X", records[0].Data, startup)
					}
					if want := int(startup[2])<<8 | int(startup[3]); records[0].StreamID != want {
						t.Errorf("STARTUP stream id = %d, want %d", records[0].StreamID, want)
					}
					if !bytes.Equal(records[1].Data, optionsFrame(0x04)) {
						t.Errorf("OPTIONS recorded as % X, want % X", records[1].Data, optionsFrame(0x04))
					}

					// The point of recording the frame whole: both the STARTUP itself and
					// every frame after it carry the negotiated state.
					for i, record := range records {
						if !record.UseMetadataID {
							t.Errorf("record %d: UseMetadataID = false, want true (the opt-in was in the STARTUP)", i)
						}
					}
				})
			}
		})

		// SendsNothingUnrecorded pins the write path's order: the frame
		// is recorded before it is sent, so a request the recording refuses -- here a
		// pre-v5 compressed STARTUP -- never reaches the server, Write reports (0, err)
		// rather than success with a trailing error the write coalescer would swallow, and
		// the failure is latched so both directions return it from then on: the connection
		// comes down at the failure instead of past it, recording a lie.
		t.Run("SendsNothingUnrecorded", func(t *testing.T) {
			startup := startupV5("lz4")
			startup[0] = 0x04

			conn := &stubConn{}
			rec, err := NewConnectionRecorder(filepath.Join(t.TempDir(), "conn"), conn, passthroughCompressor{})
			if err != nil {
				t.Fatalf("NewConnectionRecorder: %v", err)
			}
			defer rec.Close()

			n, err := rec.Write(startup)
			if err == nil {
				t.Fatal("a compressed protocol v4 connection was recorded")
			}
			if n != 0 {
				t.Errorf("Write = %d bytes, want 0", n)
			}
			if len(conn.written) != 0 {
				t.Errorf("%d bytes reached the connection after the recording refused the frame", len(conn.written))
			}

			if _, err := rec.Write(optionsFrame(0x04)); err == nil {
				t.Error("a Write after the recording failed was accepted")
			}
			if _, err := rec.Read(make([]byte, 8)); err == nil {
				t.Error("a Read after the recording failed was accepted")
			}
		})
	})
}

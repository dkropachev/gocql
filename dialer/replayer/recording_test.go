// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package replayer

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/dialer"
)

func TestRecording(t *testing.T) {
	t.Run("CheckedIn", func(t *testing.T) {
		// CarryProtocolStamp pins that the recordings were produced
		// by this tree's recorder, and that one directory is one connection at one version.
		//
		// FrameWriter stamps Record.Proto on every record it writes and has since the field
		// existed, so an unstamped record was captured by an older build. Replay tolerates that
		// -- loadResponseFramesFromFiles falls back to a response frame's own version byte --
		// which is exactly why it needs saying out loud: without this, a regeneration run from a
		// stale checkout silently produces recordings the checked-in recipe cannot reproduce,
		// and Record.Proto goes back to being exercised by no fixture at all.
		//
		// The stamps are compared against each other, not against their own frames. FrameWriter
		// derives the stamp by handing the frame to FrameProtoVersion, so re-reading byte 0 with
		// that same function could only ever agree: it would assert nothing. What is not
		// self-evident is that the records agree among themselves. The recorder appends to these
		// files (see NewConnectionRecorder), so a directory can hold one session's Reads beside
		// another's Writes, and that is the mixed state worth catching.
		//
		// loadResponseFramesFromFiles refuses such a disagreement too, but only among the Writes
		// records -- it never reads a response record's stamp at all. So the Reads half is this
		// test's own to cover, and comparing both halves in one pass is what makes them answer
		// to each other.
		t.Run("CarryProtocolStamp", func(t *testing.T) {
			for _, name := range fixtureRecordings {
				t.Run(name, func(t *testing.T) {
					base := benchRecordingBase(t, name)

					// The first stamped record a directory yields, and where it came from, so
					// a disagreement can name both sides.
					var (
						proto byte
						from  string
					)
					for _, half := range []string{"Reads", "Writes"} {
						for i, record := range readAllRecords(t, base+half) {
							where := fmt.Sprintf("%s record %d", half, i)
							if record.Proto == 0 {
								t.Errorf("%s carries no proto stamp, so the recording predates this tree's recorder; regenerate it (see the recipe above TestMain in tests/bench/e2e/single_conn_bench_test.go)", where)
								continue
							}
							if proto == 0 {
								proto, from = record.Proto, where
								continue
							}
							if record.Proto != proto {
								t.Errorf("%s is stamped proto %d but %s is stamped proto %d; a recording is one connection at one version", where, record.Proto, from, proto)
							}
						}
					}
				})
			}
		})

		// StillLoad pins the two properties the replay benchmarks
		// depend on, against the actual bytes they replay.
		//
		// Neither is about a particular hash value. The hash function may change — a recording
		// stores raw frames and is rehashed at load time with the same function applied to the
		// live request, so both sides move together. What it may not do is stop telling the
		// recording's own requests apart: the replayer scans for the first matching hash, so a
		// collision serves the wrong response, silently. The scan runs over a slice sorted by
		// stream id, so it is at least wrong the same way every run.
		t.Run("StillLoad", func(t *testing.T) {
			for _, name := range fixtureRecordings {
				t.Run(name, func(t *testing.T) {
					base := benchRecordingBase(t, name)

					frames, proto, err := loadResponseFramesFromFiles(base+"Reads", base+"Writes")
					if err != nil {
						t.Fatalf("loading the recording: %v", err)
					}
					if len(frames) == 0 {
						t.Fatal("the recording paired no requests with responses")
					}
					if proto == 0 {
						t.Error("no protocol version was read from the recording")
					}

					seen := make(map[int64]int, len(frames))
					for i, frame := range frames {
						if first, dup := seen[frame.Hash]; dup {
							t.Errorf("frames %d and %d hash alike (%d), so the replayer would serve whichever it reached first",
								first, i, frame.Hash)
							continue
						}
						seen[frame.Hash] = i

						// materialise refuses any record that is not one whole frame, and a
						// short one cannot be refused later: the driver reads a frame with
						// io.ReadFull and ConnectionReplayer's SetReadDeadline is a no-op, so
						// it blocks mid-frame forever. An empty response is only the loudest
						// case of that.
						if !wholeFrame(frame.Response) {
							t.Errorf("frame %d holds %d bytes, which is not one whole CQL frame", i, len(frame.Response))
						}
					}
				})
			}
		})
	})

	t.Run("LoadFramesFromFile", func(t *testing.T) {
		// LargeRecord pins that a record is bounded by the frame it
		// holds and nothing else. The recorder writes one record per frame, and Record.Data
		// is a []byte that encoding/json base64-inflates by 4/3, so a frame over ~48 KiB
		// produces a line past bufio.MaxScanTokenSize — which is what the loader used to
		// read records with, and it failed the whole recording with "token too long".
		t.Run("LargeRecord", func(t *testing.T) {
			big := dialer.Record{StreamID: 1, Data: bytes.Repeat([]byte{0xAB}, 128*1024)}
			small := dialer.Record{StreamID: 2, Data: optionsFrame(0x04), UseMetadataID: true}

			line, err := json.Marshal(big)
			if err != nil {
				t.Fatalf("marshalling the large record: %v", err)
			}
			if len(line) <= bufio.MaxScanTokenSize {
				t.Fatalf("the large record encodes to %d bytes, which does not exceed the old %d-byte cap", len(line), bufio.MaxScanTokenSize)
			}

			records, err := loadFramesFromFile(writeRecords(t, "Reads", big, small))
			if err != nil {
				t.Fatalf("loadFramesFromFile() error = %v, want nil", err)
			}
			if len(records) != 2 {
				t.Fatalf("loaded %d records, want 2", len(records))
			}
			if !bytes.Equal(records[1].Data, big.Data) {
				t.Errorf("record 1 holds %d bytes, want the %d recorded", len(records[1].Data), len(big.Data))
			}
			// The record that follows a large one must still be read, so a big frame cannot
			// silently truncate a recording.
			if !bytes.Equal(records[2].Data, small.Data) || !records[2].UseMetadataID {
				t.Errorf("record 2 = %+v, want %+v", records[2], small)
			}
		})

		// SkipsDamagedRecord pins the behaviour the read loop keeps: a
		// recording is a debugging artefact, so a record that does not decode is skipped and
		// the frames around it still load. The last record deliberately has no trailing
		// newline, which a truncated recording also looks like.
		t.Run("SkipsDamagedRecord", func(t *testing.T) {
			first := dialer.Record{StreamID: 1, Data: optionsFrame(0x04)}
			last := dialer.Record{StreamID: 3, Data: optionsFrame(0x04)}

			firstLine, err := json.Marshal(first)
			if err != nil {
				t.Fatalf("marshalling the first record: %v", err)
			}
			lastLine, err := json.Marshal(last)
			if err != nil {
				t.Fatalf("marshalling the last record: %v", err)
			}

			path := filepath.Join(t.TempDir(), "Reads")
			content := append(append(firstLine, "\n{\"data\":\"not json\n"...), lastLine...)
			if err := os.WriteFile(path, content, 0o600); err != nil {
				t.Fatalf("writing %s: %v", path, err)
			}

			records, err := loadFramesFromFile(path)
			if err != nil {
				t.Fatalf("loadFramesFromFile() error = %v, want nil", err)
			}
			if len(records) != 2 {
				t.Fatalf("loaded %d records, want 2 (the damaged one skipped)", len(records))
			}
			if !bytes.Equal(records[1].Data, first.Data) {
				t.Errorf("record 1 = %+v, want %+v", records[1], first)
			}
			if !bytes.Equal(records[3].Data, last.Data) {
				t.Errorf("record 3 = %+v, want %+v, so a record without a trailing newline is still read", records[3], last)
			}
		})
	})

	t.Run("LoadResponseFrames", func(t *testing.T) {
		// PrefersStampedProto pins where a recording's protocol version
		// comes from: the Proto field the recorder stamps, read from any request record,
		// rather than from the request/response pairing -- a truncated Reads file pairs
		// nothing, and a version check that disarms itself exactly when the recording is
		// damaged is not a check. A record that predates the field (Proto 0) still derives
		// the version from the frame's own bytes.
		t.Run("PrefersStampedProto", func(t *testing.T) {
			t.Run("stamped, nothing paired", func(t *testing.T) {
				writes := writeRecords(t, "Writes", dialer.Record{Data: optionsFrame(0x05), StreamID: 1, Proto: 0x05})
				reads := writeRecords(t, "Reads")

				_, proto, err := loadResponseFramesFromFiles(reads, writes)
				if err != nil {
					t.Fatalf("loadResponseFramesFromFiles: %v", err)
				}
				if proto != 0x05 {
					t.Errorf("proto = %d, want 5 (the stamped version, despite nothing pairing)", proto)
				}
			})

			t.Run("stamped versions disagree", func(t *testing.T) {
				writes := writeRecords(t, "Writes",
					dialer.Record{Data: optionsFrame(0x04), StreamID: 1, Proto: 0x04},
					dialer.Record{Data: optionsFrame(0x05), StreamID: 2, Proto: 0x05})
				reads := writeRecords(t, "Reads")

				_, _, err := loadResponseFramesFromFiles(reads, writes)
				if err == nil {
					t.Fatal("a file holding two protocol versions was loaded")
				}
				for _, want := range []string{"v4", "v5"} {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error %q does not name %q", err, want)
					}
				}
			})

			t.Run("derived versions disagree", func(t *testing.T) {
				writes := writeRecords(t, "Writes",
					dialer.Record{Data: optionsFrame(0x04), StreamID: 1},
					dialer.Record{Data: optionsFrame(0x05), StreamID: 2})
				reads := writeRecords(t, "Reads")

				if _, _, err := loadResponseFramesFromFiles(reads, writes); err == nil {
					t.Fatal("a file holding two protocol versions was loaded")
				}
			})

			t.Run("one stamped record does not answer for the unstamped ones", func(t *testing.T) {
				// What a recording directory reused across a recorder upgrade holds: the
				// recorder appends every connection to one addr-sourcePort file, so a v4
				// session recorded before the Proto field existed survives underneath a
				// later stamped one. Taking the version from the stamped record alone left
				// the rest unchecked.
				writes := writeRecords(t, "Writes",
					dialer.Record{Data: optionsFrame(0x05), StreamID: 1, Proto: 0x05},
					dialer.Record{Data: optionsFrame(0x04), StreamID: 2})
				reads := writeRecords(t, "Reads",
					dialer.Record{Data: optionsFrame(0x85), StreamID: 1},
					dialer.Record{Data: optionsFrame(0x84), StreamID: 2})

				_, _, err := loadResponseFramesFromFiles(reads, writes)
				if err == nil {
					t.Fatal("a file mixing a stamped v5 record with an unstamped v4 leftover was loaded")
				}
				for _, want := range []string{"v4", "v5"} {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error %q does not name %q", err, want)
					}
				}
			})

			t.Run("unstamped legacy record", func(t *testing.T) {
				writes := writeRecords(t, "Writes", dialer.Record{Data: optionsFrame(0x04), StreamID: 1})
				reads := writeRecords(t, "Reads", dialer.Record{Data: optionsFrame(0x84), StreamID: 1})

				_, proto, err := loadResponseFramesFromFiles(reads, writes)
				if err != nil {
					t.Fatalf("loadResponseFramesFromFiles: %v", err)
				}
				if proto != 0x04 {
					t.Errorf("proto = %d, want 4 (derived from the frame bytes)", proto)
				}
			})
		})

		// Current typed control startup repeats an identical query. Deduplication must
		// preserve the collision guard and reject ambiguous capture results.
		t.Run("RepeatedRequestsRequireIdenticalFrames", func(t *testing.T) {
			for _, mode := range []string{"identical", "request collision", "different response"} {
				t.Run(mode, func(t *testing.T) {
					a, b := requestFrame(1, 1), requestFrame(1, 2)
					first, second := optionsFrame(0x84), optionsFrame(0x84)
					first[3] = 1
					second[3] = 2
					if mode == "request collision" {
						b = append(b, 0)
						b[8] = 1
					}
					if mode == "different response" {
						second[4] = 2
					}
					writes := writeRecords(t, "Writes", dialer.Record{Data: a, StreamID: 1, Proto: 4}, dialer.Record{Data: b, StreamID: 2, Proto: 4})
					reads := writeRecords(t, "Reads", dialer.Record{Data: first, StreamID: 1, Proto: 4}, dialer.Record{Data: second, StreamID: 2, Proto: 4})
					frames, _, err := loadResponseFramesFromFiles(reads, writes)
					if mode == "identical" {
						if err != nil || len(frames) != 1 {
							t.Fatalf("deduplicated frames=%d err=%v", len(frames), err)
						}
					} else if err == nil {
						t.Fatal("ambiguous/colliding capture was accepted")
					}
				})
			}
		})
	})
}

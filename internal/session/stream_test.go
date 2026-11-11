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

package session

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/protocol"
	implProtocol "github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/testkit"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wire"
	"github.com/scylladb/gocql/v2/internal/wirecompress"
	lz4 "github.com/scylladb/gocql/v2/internal/wirecompress"
	"github.com/scylladb/gocql/v2/retry"
)

func TestStream(t *testing.T) {
	t.Run("Admission", func(t *testing.T) {
		t.Run("LargeEnvelope", func(t *testing.T) {
			for _, declared := range []uint32{256<<20 + 1, math.MaxInt32} {
				s, conn, peer := rowStreamSession(t, 4, nil)
				conn.InternalCfg.TypedLimits.MaxFrameBytes = int64(declared)
				conn.InternalCfg.TypedLimits.MaxDecompressedBytes = int64(declared)
				done := make(chan struct{})
				go func() {
					defer close(done)
					stream := typedTestReadRequest(t, conn, peer)
					frame := typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "x"))
					binary.BigEndian.PutUint32(frame[5:9], declared)
					// Send only the metadata/row count. Admission must accept the high
					// envelope without gathering or allocating the advertised body.
					if _, err := peer.Write(frame[:len(frame)-5]); err != nil {
						t.Error(err)
					}
				}()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				bound := runtimeQuery(t)
				cursor, err := RunStream(ctx, s, bound, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true})
				if err != nil {
					t.Fatal("configured envelope rejected", declared, err)
				}
				if cursor.Info().Pages != 1 || cursor.Info().OutputStarted {
					t.Fatal("payload read before destination")
				}
				if err := cursor.Close(); err != nil {
					t.Fatal(err)
				}
				bound.Close()
				if err := bound.Wait(ctx); err != nil {
					t.Fatal(err)
				}
				cancel()
				<-done
			}
		})
	})

	t.Run("Capability", func(t *testing.T) {
		t.Run("BeforeQuery", func(t *testing.T) {
			s, c, _ := rowStreamSession(t, 4, wirecompress.SnappyCompressor{})
			b := runtimeQuery(t)
			defer b.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := RunStream(ctx, s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true})
			if _, ok := errors.AsType[*codec.StreamCapabilityError](err); !ok {
				t.Fatal(err)
			}
			if c.InternalStreams.InUse() != 0 {
				t.Fatal("capability leaked stream")
			}
			_, err = RunStream(ctx, s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true, Options: Options{Prefetch: cqltype.Option[int]{Set: true, Value: 1}}})
			if _, ok := errors.AsType[*cqltype.ValidationError](err); !ok {
				t.Fatal(err)
			}
		})
	})

	t.Run("Close", func(t *testing.T) {
		// An explicit Close of an incremental stream discards the unread rest of a
		// small current response and keeps the connection, so that the next stream
		// runs on it; a response above the drain limit still closes the connection.
		t.Run("AbandonedIncrementalKeepsConnection", func(t *testing.T) {
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
		})

		t.Run("DuringFinalCallerAccess", func(t *testing.T) {
			for _, mode := range fixtureModes {
				for _, action := range []string{"close", "cancel", "session"} {
					t.Run(mode.name+"-"+action, func(t *testing.T) {
						s, c, peer := fixtureStreamSession(t, mode)
						go func() {
							stream := typedTestReadRequest(t, c, peer)
							_, _ = peer.Write(fixturePacket(mode, testkit.BlobRows(mode.version, uint16(stream), []byte("final"))))
						}()
						b := runtimeQuery(t)
						defer b.Close()
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						decoder := &finalAccess{entered: make(chan struct{}), leave: make(chan struct{})}
						cursor, err := RunStream(ctx, s, b, decoder, StreamOptions{Incremental: true})
						if err != nil {
							t.Fatal(err)
						}
						dst := make([]byte, 0, 10)
						next := make(chan struct{})
						go func() { cursor.NextInto(&dst); close(next) }()
						<-decoder.entered
						closing := make(chan struct{})
						closed := make(chan struct{})
						go func() {
							close(closing)
							switch action {
							case "cancel":
								cancel()
								cursor.Close()
							case "session":
								s.Close()
							default:
								cursor.Close()
							}
							close(closed)
						}()
						<-closing
						for i := 0; i < 100; i++ {
							runtime.Gosched()
						}
						select {
						case <-closed:
							t.Fatal("close returned during final storage access")
						default:
						}
						close(decoder.leave)
						<-closed
						<-next
						if decoder.writes.Load() != 1 || !bytes.Equal(dst, []byte("Final")) {
							t.Fatal("missing joined final write", dst)
						}
						snapshot := append([]byte(nil), dst...)
						cursor.Close()
						s.Close()
						if !bytes.Equal(snapshot, dst) {
							t.Fatal("late write")
						}
					})
				}
			}
		})

		t.Run("GenerationRejectsLateCancellation", func(t *testing.T) {
			s, c, peer := rowStreamSession(t, 4, nil)
			go func() {
				id := typedTestReadRequest(t, c, peer)
				_, _ = peer.Write(testResponse(t, c, id, runtimeRowsBody(nil, "value")))
			}()
			decoder := &gatedDecoder{entered: make(chan struct{}), leave: make(chan struct{})}
			b := runtimeQuery(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			cursor, err := RunStream(ctx, s, b, decoder, StreamOptions{Incremental: true})
			if err != nil {
				t.Fatal(err)
			}
			dst := make([]byte, 0, 10)
			nextDone := make(chan struct{})
			go func() { cursor.NextInto(&dst); close(nextDone) }()
			<-decoder.entered
			first := make(chan error, 1)
			go func() { first <- cursor.Close() }()
			for !RowStreamData(cursor).closing.Load() {
				select {
				case <-ctx.Done():
					t.Fatal("close did not begin")
				default:
					runtime.Gosched()
				}
			}
			late := make(chan error, 1)
			go func() { late <- rowStreamCloseWithError(cursor, context.Canceled) }()
			select {
			case err := <-late:
				t.Fatal("duplicate close did not join", err)
			case <-time.After(10 * time.Millisecond):
			}
			close(decoder.leave)
			if err := <-first; err != nil {
				t.Fatal("explicit close acquired cancellation error", err)
			}
			if err := <-late; err != nil {
				t.Fatal("late cancellation replaced close", err)
			}
			<-nextDone
			b.Close()
			if err := b.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			if cursor.Err() != nil {
				t.Fatal(cursor.Err())
			}
		})

		t.Run("IdleSessionDrainAndPoolExhaustion", func(t *testing.T) {
			s, c, peer := rowStreamSession(t, 4, nil)
			go func() {
				stream := typedTestReadRequest(t, c, peer)
				_, _ = peer.Write(testResponse(t, c, stream, runtimeRowsBody(nil, "payload")))
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			b := runtimeQuery(t)
			cursor, err := RunStream(ctx, s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true})
			if err != nil {
				t.Fatal(err)
			}
			short, stop := context.WithTimeout(ctx, 10*time.Millisecond)
			defer stop()
			if second, err := RunStream(short, s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true}); second != nil || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(second, err)
			}
			b.Close()
			s.Close()
			if err := b.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			if cursor.NextInto(nil) || !errors.Is(cursor.Err(), runtimeerrors.ErrClosed) {
				t.Fatal(cursor.Err())
			}
			if len(InternalRuntimeFor(s).executions) != 0 {
				t.Fatal("idle operation leaked")
			}
		})

		t.Run("JoinsDestinationAndConcurrentRejection", func(t *testing.T) {
			for _, action := range []string{"close", "cancel", "session", "concurrent"} {
				t.Run(action, func(t *testing.T) {
					s, c, peer := rowStreamSession(t, 4, nil)
					go func() {
						stream := typedTestReadRequest(t, c, peer)
						_, _ = peer.Write(testResponse(t, c, stream, runtimeRowsBody(nil, "payload")))
					}()
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					b := runtimeQuery(t)
					bDone := make(chan struct{})
					decoder := &gatedDecoder{entered: make(chan struct{}), leave: make(chan struct{})}
					cursor, err := RunStream(ctx, s, b, decoder, StreamOptions{Incremental: true})
					if err != nil {
						t.Fatal(err)
					}
					dst := make([]byte, 0, 20)
					go func() { cursor.NextInto(&dst); close(bDone) }()
					<-decoder.entered
					closed := make(chan struct{})
					go func() {
						defer close(closed)
						switch action {
						case "close":
							cursor.Close()
						case "cancel":
							cancel()
							cursor.Close()
						case "session":
							s.Close()
						case "concurrent":
							other := []byte("unchanged")
							if cursor.NextInto(&other) || string(other) != "unchanged" {
								t.Error("rejected call touched storage")
							}
							cursor.Close()
						}
					}()
					select {
					case <-closed:
						t.Fatal("close returned during Reset")
					case <-time.After(10 * time.Millisecond):
					}
					close(decoder.leave)
					select {
					case <-closed:
					case <-time.After(time.Second):
						t.Fatal("close failed to join")
					}
					<-bDone
					if decoder.resets.Load() != 1 {
						t.Fatal("unexpected reset", decoder.resets.Load())
					}
					snapshot := append([]byte(nil), dst...)
					time.Sleep(time.Millisecond)
					if !bytes.Equal(snapshot, dst) {
						t.Fatal("late destination write")
					}
					b.Close()
					wait, stop := context.WithTimeout(context.Background(), time.Second)
					defer stop()
					if err := b.Wait(wait); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	})

	t.Run("Deadline", func(t *testing.T) {
		t.Run("PageEndsAtExactBoundary", func(t *testing.T) {
			s, c, peer := rowStreamSession(t, 4, nil)
			go func() {
				stream := typedTestReadRequest(t, c, peer)
				_, _ = peer.Write(testResponse(t, c, stream, runtimeRowsBody(nil, "value")))
			}()
			b := runtimeQuery(t)
			defer b.Close()
			cursor, err := RunStream(context.Background(), s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true, Options: Options{PageTimeout: cqltype.Option[time.Duration]{Set: true, Value: 20 * time.Millisecond}}})
			if err != nil {
				t.Fatal(err)
			}
			dst := make([]byte, 0, 10)
			if !cursor.NextInto(&dst) {
				t.Fatal(cursor.Err())
			}
			time.Sleep(30 * time.Millisecond)
			if cursor.NextInto(&dst) || cursor.Close() != nil {
				t.Fatal("completed page acquired idle timeout", cursor.Err())
			}
		})
	})

	t.Run("Decoder", func(t *testing.T) {
		t.Run("CustomErrorAfterWrite", func(t *testing.T) {
			for _, mode := range fixtureModes {
				t.Run(mode.name, func(t *testing.T) {
					s, c, peer := fixtureStreamSession(t, mode)
					done := make(chan struct{})
					go func() {
						defer close(done)
						stream := typedTestReadRequest(t, c, peer)
						_, _ = peer.Write(fixturePacket(mode, testkit.BlobRows(mode.version, uint16(stream), []byte("partial"))))
					}()
					cause := fmt.Errorf("custom decoder terminal failure")
					bound := runtimeQuery(t)
					defer bound.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					defer cancel()
					cursor, err := RunStream(ctx, s, bound, decoderError{cause}, StreamOptions{Incremental: true, Options: Options{Idempotent: true}})
					if err != nil {
						t.Fatal(err)
					}
					defer cursor.Close()
					dst := make([]byte, 0, 16)
					if cursor.NextInto(&dst) || !errors.Is(cursor.Err(), cause) {
						t.Fatal("custom decoder error published row", cursor.Err())
					}
					if string(dst) != "partial" || !cursor.Info().OutputStarted || cursor.Info().DeliveredRows != 0 || cursor.Info().Result.Attempts != 1 {
						t.Fatal("custom decoder replay/progress", dst, cursor.Info())
					}
					if cursor.NextInto(&dst) || string(dst) != "partial" || !errors.Is(cursor.Close(), cause) {
						t.Fatal("terminal custom failure changed destination/error")
					}
					bound.Close()
					if err := bound.Wait(ctx); err != nil {
						t.Fatal(err)
					}
					await(t, done)
				})
			}
		})
	})

	t.Run("Destination", func(t *testing.T) {
		t.Run("CapacityAndNilNoResetReplay", func(t *testing.T) {
			for _, nilDst := range []bool{false, true} {
				t.Run(map[bool]string{false: "capacity", true: "nil"}[nilDst], func(t *testing.T) {
					s, c, peer := rowStreamSession(t, 4, nil)
					done := make(chan struct{})
					go func() {
						defer close(done)
						stream := typedTestReadRequest(t, c, peer)
						_, _ = peer.Write(testResponse(t, c, stream, runtimeRowsBody(nil, "payload")))
					}()
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					b := runtimeQuery(t)
					cursor, err := RunStream(ctx, s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true})
					if err != nil {
						t.Fatal(err)
					}
					dst := make([]byte, 0, 2)
					var ok bool
					if nilDst {
						ok = cursor.NextInto(nil)
					} else {
						ok = cursor.NextInto(&dst)
					}
					if ok || cursor.Err() == nil {
						t.Fatal("invalid destination succeeded")
					}
					if !nilDst {
						if capacity, ok := errors.AsType[*codec.StreamCapacityError](cursor.Err()); !ok || capacity.Required != 7 || capacity.Available != 2 {
							t.Fatal(cursor.Err())
						}
					}
					if cursor.Info().OutputStarted == nilDst {
						t.Fatal(cursor.Info())
					}
					b.Close()
					if err := b.Wait(ctx); err != nil {
						t.Fatal(err)
					}
					cursor.Close()
					<-done
				})
			}
		})
	})

	t.Run("FirstRow", func(t *testing.T) {
		t.Run("BeforePageAndNoReplay", func(t *testing.T) {
			s, c, peer := rowStreamSession(t, 4, nil)
			first := make(chan struct{})
			later := make(chan struct{})
			done := make(chan struct{})
			go func() {
				defer close(done)
				stream := typedTestReadRequest(t, c, peer)
				body := runtimeRowsBody(nil, "first", "second")
				frame := testResponse(t, c, stream, body)
				split := len(frame) - len("second") - 4
				if _, err := peer.Write(frame[:split]); err != nil {
					t.Error(err)
					return
				}
				close(first)
				<-later
				peer.Close()
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			b := runtimeQuery(t)
			defer b.Close()
			cursor, err := RunStream(ctx, s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true, Options: Options{Idempotent: true}})
			if err != nil {
				t.Fatal(err)
			}
			dst := make([]byte, 0, 20)
			if !cursor.NextInto(&dst) || string(dst) != "first" {
				t.Fatal(cursor.Err())
			}
			<-first
			close(later)
			if cursor.NextInto(&dst) {
				t.Fatal("truncated row succeeded")
			}
			if cursor.Err() == nil || cursor.Info().Result.Attempts != 1 || cursor.Info().DeliveredRows != 1 {
				t.Fatal(cursor.Err(), cursor.Info())
			}
			firstErr := cursor.Err()
			cursor.NextInto(nil)
			cursor.Close()
			if cursor.Err() != firstErr {
				t.Fatal("first error replaced")
			}
			<-done
		})

		t.Run("IndependentGate", func(t *testing.T) {
			for _, mode := range fixtureModes {
				t.Run(mode.name, func(t *testing.T) {
					s, c, peer := fixtureStreamSession(t, mode)
					release := make(chan struct{})
					done := make(chan struct{})
					t.Cleanup(func() {
						select {
						case <-release:
						default:
							close(release)
						}
					})
					go func() {
						defer close(done)
						stream := typedTestReadRequest(t, c, peer)
						frame := testkit.BlobRows(mode.version, uint16(stream), []byte("first"), []byte("second"))
						cut := len(frame) - 10
						first, last := frame[:cut], frame[cut:]
						if mode.version == 5 {
							first = testkit.Unit(first, mode.compression != nil, false)
							last = testkit.Unit(last, mode.compression != nil, false)
						}
						_, _ = peer.Write(first)
						<-release
						_, _ = peer.Write(last)
					}()
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					b := runtimeQuery(t)
					defer b.Close()
					cursor, err := RunStream(ctx, s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true})
					if err != nil {
						t.Fatal(err)
					}
					dst := make([]byte, 0, 12)
					if !cursor.NextInto(&dst) || string(dst) != "first" {
						t.Fatal("first row waited beyond gate", cursor.Err())
					}
					close(release)
					if !cursor.NextInto(&dst) || string(dst) != "second" {
						t.Fatal(cursor.Err())
					}
					if cursor.NextInto(&dst) || cursor.Err() != nil {
						t.Fatal(cursor.Err())
					}
					cursor.Close()
					<-done
				})
			}
		})

		t.Run("PartialNeverReplays", func(t *testing.T) {
			s, c, peer := rowStreamSession(t, 4, nil)
			done := make(chan struct{})
			go func() {
				defer close(done)
				stream := typedTestReadRequest(t, c, peer)
				frame := testResponse(t, c, stream, runtimeRowsBody(nil, "payload"))
				_, _ = peer.Write(frame[:len(frame)-4])
				peer.Close()
			}()
			b := runtimeQuery(t)
			defer b.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			cursor, err := RunStream(ctx, s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true, Options: Options{Idempotent: true}})
			if err != nil {
				t.Fatal(err)
			}
			dst := make([]byte, 0, 20)
			if cursor.NextInto(&dst) || cursor.Err() == nil {
				t.Fatal("partial row succeeded")
			}
			if info := cursor.Info(); !info.OutputStarted || info.DeliveredRows != 0 || info.Result.Attempts != 1 {
				t.Fatal(info)
			}
			if string(dst[:3]) != "pay" {
				t.Fatal("fixture did not write partial caller storage", dst)
			}
			cursor.Close()
			<-done
		})
	})

	t.Run("Integrity", func(t *testing.T) {
		t.Run("BadV5CannotDeliver", func(t *testing.T) {
			s, c, peer := rowStreamSession(t, 5, nil)
			go func() {
				stream := typedTestReadRequest(t, c, peer)
				frame := testResponse(t, c, stream, runtimeRowsBody(nil, "payload"))
				frame[len(frame)-1] ^= 1
				_, _ = peer.Write(frame)
			}()
			b := runtimeQuery(t)
			defer b.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			cursor, err := RunStream(ctx, s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true})
			if err == nil || cursor != nil {
				t.Fatal("invalid checksum exposed cursor", cursor, err)
			}
		})

		t.Run("CorruptChecksums", func(t *testing.T) {
			for _, mode := range fixtureModes {
				if mode.version != 5 {
					continue
				}
				t.Run(mode.name, func(t *testing.T) {
					// Corrupt every byte of each header and payload checksum, independently.
					headerSize := 6
					if mode.compression != nil {
						headerSize = 8
					}
					for _, checksum := range []string{"header", "payload"} {
						for i := 0; i < map[string]int{"header": 3, "payload": 4}[checksum]; i++ {
							s, c, peer := fixtureStreamSession(t, mode)
							done := make(chan struct{})
							finish := make(chan struct{})
							var finishOnce sync.Once
							finishRead := func() { finishOnce.Do(func() { close(finish) }) }
							t.Cleanup(finishRead)
							go func() {
								defer close(done)
								stream := typedTestReadRequest(t, c, peer)
								packet := fixturePacket(mode, testkit.BlobRows(mode.version, uint16(stream), []byte("forbidden")))
								offset := headerSize - 3 + i
								if checksum == "payload" {
									offset = len(packet) - 4 + i
								}
								packet[offset] ^= 0x80
								_, _ = peer.Write(packet)
								peer.Close()
							}()
							b := runtimeQuery(t)
							ctx, cancel := context.WithTimeout(context.Background(), time.Second)
							cursor, err := RunStream(ctx, s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true})
							if err == nil {
								dst := make([]byte, 0, 20)
								if cursor.NextInto(&dst) {
									t.Fatal("corrupt unit delivered caller output")
								}
								err = cursor.Err()
								cursor.Close()
							}
							if err == nil {
								t.Fatal("corrupt checksum accepted")
							}
							finishRead()
							cancel()
							b.Close()
							peer.Close()
							s.Close()
							<-done
						}
					}
				})
			}
		})

		t.Run("EverySplitAndTruncation", func(t *testing.T) {
			for _, mode := range fixtureModes {
				t.Run(mode.name, func(t *testing.T) {
					sample := fixturePacket(mode, testkit.BlobRows(mode.version, 0, []byte("first"), []byte("last")))
					for _, truncate := range []bool{false, true} {
						t.Run(fmt.Sprintf("truncate=%v", truncate), func(t *testing.T) {
							for split := 0; split < len(sample); split++ {
								s, c, peer := fixtureStreamSession(t, mode)
								done := make(chan struct{})
								finish := make(chan struct{})
								var finishOnce sync.Once
								finishRead := func() { finishOnce.Do(func() { close(finish) }) }
								t.Cleanup(finishRead)
								go func() {
									defer close(done)
									stream := typedTestReadRequest(t, c, peer)
									packet := fixturePacket(mode, testkit.BlobRows(mode.version, uint16(stream), []byte("first"), []byte("last")))
									_, _ = peer.Write(packet[:split])
									if !truncate {
										_, _ = peer.Write(packet[split:])
										// net.Pipe rejects deadline setters after peer close,
										// unlike a TCP socket with a received FIN. Keep complete
										// TLS records readable until the consumer acknowledges.
										<-finish
									}
									peer.Close()
								}()
								b := runtimeQuery(t)
								ctx, cancel := context.WithTimeout(context.Background(), time.Second)
								cursor, err := RunStream(ctx, s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true})
								if err == nil {
									dst := make([]byte, 0, 10)
									var rows [][]byte
									for cursor.NextInto(&dst) {
										rows = append(rows, append([]byte(nil), dst...))
									}
									err = cursor.Err()
									cursor.Close()
									if !truncate && (len(rows) != 2 || string(rows[0]) != "first" || string(rows[1]) != "last") {
										t.Fatalf("split %d rows %q error %v", split, rows, err)
									}
								}
								if truncate && err == nil {
									t.Fatalf("accepted truncated prefix %d/%d", split, len(sample))
								}
								if !truncate && err != nil {
									t.Fatalf("split %d: %v", split, err)
								}
								finishRead()
								cancel()
								b.Close()
								peer.Close()
								s.Close()
								<-done
							}
						})
					}
				})
			}
		})
	})

	t.Run("Paging", func(t *testing.T) {
		t.Run("CycleAndLimits", func(t *testing.T) {
			for _, limit := range []bool{false, true} {
				t.Run(map[bool]string{false: "cycle", true: "limit"}[limit], func(t *testing.T) {
					s, c, peer := rowStreamSession(t, 4, nil)
					config := settings.DefaultConfig("localhost")
					if limit {
						config.Limits.MaxPagingStates = 1
					}
					InternalRuntimeFor(s).config = config
					done := make(chan struct{})
					go func() {
						defer close(done)
						tokens := [][]byte{{}, {1}, {}}
						if limit {
							tokens = tokens[:2]
						}
						for _, token := range tokens {
							stream := typedTestReadRequest(t, c, peer)
							if _, err := peer.Write(testResponse(t, c, stream, runtimeRowsBody(token))); err != nil {
								return
							}
						}
					}()
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					b := runtimeQuery(t)
					defer b.Close()
					cursor, err := RunStream(ctx, s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true})
					if err != nil {
						t.Fatal(err)
					}
					dst := []byte("untouched")
					if cursor.NextInto(&dst) {
						t.Fatal("invented row")
					}
					want := runtimeerrors.ErrPagingNoProgress
					if limit {
						want = runtimeerrors.ErrPagingStateLimit
					}
					if !errors.Is(cursor.Err(), want) || string(dst) != "untouched" || cursor.Info().OutputStarted {
						t.Fatal(cursor.Err(), cursor.Info())
					}
					<-done
				})
			}
		})

		t.Run("Modes", func(t *testing.T) {
			for _, mode := range []struct {
				name        string
				proto       byte
				compression implProtocol.Compressor
			}{{"v3", 3, nil}, {"v4", 4, nil}, {"v5raw", 5, nil}, {"v5lz4", 5, lz4.LZ4Compressor{}}} {
				t.Run(mode.name, func(t *testing.T) {
					s, c, peer := rowStreamSession(t, mode.proto, mode.compression)
					payload := bytes.Repeat([]byte("caller-owned"), 40000)
					done := make(chan struct{})
					go func() {
						defer close(done)
						for _, body := range [][]byte{runtimeRowsBody([]byte{}), runtimeRowsBody([]byte{1}, string(payload)), runtimeRowsBody(nil, "last")} {
							stream := typedTestReadRequest(t, c, peer)
							if _, err := peer.Write(testResponse(t, c, stream, body)); err != nil {
								t.Error(err)
								return
							}
						}
					}()
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					defer cancel()
					b := runtimeQuery(t)
					cursor, err := RunStream(ctx, s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true})
					if err != nil {
						t.Fatal(err)
					}
					if info := cursor.Info(); info.Pages != 1 || info.OutputStarted {
						t.Fatal(info)
					}
					b.Close()
					dst := make([]byte, 0, len(payload))
					backing := &dst[:cap(dst)][0]
					if !cursor.NextInto(&dst) || !bytes.Equal(dst, payload) || &dst[0] != backing {
						t.Fatal("first", len(dst), cursor.Err())
					}
					if info := cursor.Info(); info.Pages != 2 || info.DeliveredRows != 1 || !info.OutputStarted {
						t.Fatal(info)
					}
					owned := append([]byte(nil), dst...)
					if !cursor.NextInto(&dst) || string(dst) != "last" {
						t.Fatal(cursor.Err())
					}
					if cursor.NextInto(&dst) || cursor.Err() != nil {
						t.Fatal(cursor.Err())
					}
					if string(dst) != "last" {
						t.Fatal("exhaustion touched output")
					}
					if err := cursor.Close(); err != nil {
						t.Fatal(err)
					}
					if info := cursor.Info(); info.Pages != 3 || info.DeliveredRows != 2 || info.Result.Attempts != 3 {
						t.Fatal(info)
					}
					if err := b.Wait(ctx); err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(owned, payload) || string(dst) != "last" {
						t.Fatal("output lost at close")
					}
					<-done
				})
			}
		})
	})

	t.Run("Prepared", func(t *testing.T) {
		t.Run("MetadataFencesAndPlanIsolation", func(t *testing.T) {
			for _, mismatch := range []bool{false, true} {
				t.Run(map[bool]string{false: "matching", true: "count-mismatch"}[mismatch], func(t *testing.T) {
					s, c, peer := rowStreamSession(t, 4, nil)
					implCluster.SessionData(s).InternalCfg.Keyspace = "ks"
					columns := cqltype.NewColumns(cqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: cqltype.BlobType})
					state, _ := preparedState(t, s, columns)
					bound := func() *Bound[cqltype.Bytes] {
						opaqueStorageValuex := new(Bound[cqltype.Bytes])
						BoundData(opaqueStorageValuex).InternalRequest = InternalExecutionRequest{statement: state.InternalStatement, InternalSource: state.preparedSource(), InternalValues: model.EncodedValues{Columns: cqltype.NewColumns()}, InternalOwner: InternalNewInputOwner(), routing: model.RoutingInfo{Kind: cqltype.OrdinaryStatement}}
						BoundData(opaqueStorageValuex).InternalRows = codec.OneColumn(codec.BlobCodec)
						return opaqueStorageValuex
					}()
					decoder := &countStreamDecoder{}
					done := make(chan struct{})
					go func() {
						defer close(done)
						stream := typedTestReadRequest(t, c, peer)
						body := []byte{0, 0, 0, 2, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 3, 'a', 'b', 'c'}
						if mismatch {
							body[11] = 2
							body = append(body, 255, 255, 255, 255)
						}
						_, _ = peer.Write(testResponse(t, c, stream, body))
					}()
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					cursor, err := RunStream(ctx, s, bound, decoder, StreamOptions{Incremental: true})
					if mismatch {
						if _, ok := errors.AsType[*cqltype.ProtocolError](err); !ok || cursor != nil {
							t.Fatal(cursor, err)
						}
					} else {
						if err != nil {
							t.Fatal(err)
						}
						dst := make([]byte, 0, 3)
						if !cursor.NextInto(&dst) || string(dst) != "abc" {
							t.Fatal(cursor.Err())
						}
						cursor.NextInto(&dst)
						cursor.Close()
						if decoder.compiles.Load() != 1 {
							t.Fatal("unchanged metadata recompiled", decoder.compiles.Load())
						}
					}
					bound.Close()
					if err := bound.Wait(ctx); err != nil {
						t.Fatal(err)
					}
					<-done
				})
			}
		})

		t.Run("ReprepareCanonicalInputNoRepairAfterOutput", func(t *testing.T) {
			s, connection, peer := rowStreamSession(t, 4, nil)
			implCluster.SessionData(s).InternalCfg.Keyspace = "ks"
			typ := cqltype.BlobType
			columns := cqltype.NewColumns(cqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: typ})
			state, old := preparedState(t, s, columns)
			state.InternalParameters = columns
			state.InternalInfo.PartitionKeyIndexes = []int{0}
			count := 0
			params := codecTestMust(codec.OneParam[[]byte](countingBlobEncoder{calls: &count}).Compile(columns))
			prepared := func() *Prepared[[]byte,
				cqltype.Bytes] {
				opaqueStorageValuexx := new(Prepared[[]byte,
					cqltype.Bytes])
				PreparedData(opaqueStorageValuexx).InternalState = state
				PreparedData(opaqueStorageValuexx).kind = cqltype.OrdinaryStatement
				PreparedData(opaqueStorageValuexx).params = params
				PreparedData(opaqueStorageValuexx).rows = codec.OneColumn(codec.BlobCodec)
				return opaqueStorageValuexx
			}()
			input := []byte("canonical-input")
			bound := codecTestMust(BindBorrowed(prepared, input))
			spy := &canonicalWriter{testWriter: connection.InternalW, payload: input}
			connection.InternalW = spy
			done := make(chan error, 1)
			go func() {
				for step := 0; step < 4; step++ {
					op, id, body, err := readRequest(peer)
					if err != nil {
						done <- err
						return
					}
					if step == 1 {
						if op != 9 {
							done <- errors.New("missing reprepare")
							return
						}
						_, err = peer.Write(typedTestResponse(t, 4, id, preparedBody([]byte("new"), columns, columns, 0)))
					} else {
						if op != 10 || !bytes.Contains(body, input) {
							done <- errors.New("canonical input changed")
							return
						}
						switch step {
						case 0:
							_, err = peer.Write(errorResponse(t, id, implProtocol.ErrCodeUnprepared, old.InternalId))
						case 2:
							_, err = peer.Write(typedTestResponse(t, 4, id, runtimeRowsBody([]byte("next"), "first")))
						case 3:
							_, err = peer.Write(errorExtensions(errorResponse(t, id, implProtocol.ErrCodeUnprepared, []byte("new"))))
						}
					}
					if err != nil {
						done <- err
						return
					}
				}
				done <- nil
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			cursor, err := RunStream(ctx, s, bound, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true})
			if err != nil {
				t.Fatal(err)
			}
			dst := make([]byte, 0, 20)
			if !cursor.NextInto(&dst) || string(dst) != "first" {
				t.Fatal(cursor.Err())
			}
			if cursor.NextInto(&dst) {
				t.Fatal("unprepared replay after output")
			}
			if _, ok := errors.AsType[*implProtocol.RequestErrUnprepared](cursor.Err()); !ok {
				t.Fatal(cursor.Err())
			}
			if count != 1 || cursor.Info().DeliveredRows != 1 || cursor.Info().Result.Attempts != 3 {
				t.Fatal(count, cursor.Info())
			}
			if spy.bytes.Load() != int64(3*len(input)) {
				t.Fatal("canonical stream writer", spy.bytes.Load())
			}
			t.Logf("canonical stream attempts=3 payload_backing_bytes=%d", spy.bytes.Load())
			info := cursor.Info()
			if len(info.Result.Warnings()) != 1 || info.Result.Warnings()[0] != "terminal" || len(info.Result.TraceIDs()) != 1 || info.Result.TraceIDs()[0][15] != 7 || len(info.Result.CustomPayload()) != 1 || string(info.Result.CustomPayload()[0].Value.Value.OwnedBytes()) != "new" {
				t.Fatal("terminal attempt metadata missing", info)
			}
			bound.Close()
			cursor.Close()
			if err := bound.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	})

	t.Run("RawRead", func(t *testing.T) {
		t.Run("CallerBacking", func(t *testing.T) {
			s, c, peer := rowStreamSession(t, 4, nil)
			reader := c.InternalR.(*transport.InternalConnReader)
			spy := reader.InternalConn.(*testReadTargets)
			payload := bytes.Repeat([]byte{7}, 128<<10)
			go func() {
				stream := typedTestReadRequest(t, c, peer)
				_, _ = peer.Write(testResponse(t, c, stream, runtimeRowsBody(nil, string(payload))))
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			b := runtimeQuery(t)
			defer b.Close()
			cursor, err := RunStream(ctx, s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true})
			if err != nil {
				t.Fatal(err)
			}
			dst := make([]byte, 0, len(payload))
			if !cursor.NextInto(&dst) {
				t.Fatal(cursor.Err())
			}
			spy.mu.Lock()
			found := false
			for _, p := range spy.targets {
				prefix := len(dst) - len(p)
				if prefix >= 0 && prefix <= 4<<10 && &p[0] == &dst[prefix] {
					found = true
				}
			}
			spy.mu.Unlock()
			if !found || !bytes.Equal(dst, payload) {
				t.Fatal("body did not read directly into caller backing")
			}
			cursor.NextInto(&dst)
			cursor.Close()
		})

		t.Run("IndependentDirect", func(t *testing.T) {
			for _, mode := range fixtureModes {
				if mode.version == 5 {
					continue
				}
				t.Run(mode.name, func(t *testing.T) {
					s, c, peer := fixtureStreamSession(t, mode, true)
					payload := bytes.Repeat([]byte{0x71}, 400<<10)
					done := make(chan struct{})
					go func() {
						defer close(done)
						stream := typedTestReadRequest(t, c, peer)
						_, _ = peer.Write(testkit.BlobRows(mode.version, uint16(stream), payload))
					}()
					b := runtimeQuery(t)
					defer b.Close()
					cursor, err := RunStream(context.Background(), s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true})
					if err != nil {
						t.Fatal(err)
					}
					dst := make([]byte, 0, len(payload))
					backing := &dst[:cap(dst)][0]
					if !cursor.NextInto(&dst) || !bytes.Equal(dst, payload) || &dst[0] != backing {
						t.Fatal("lost final storage", cursor.Err())
					}
					spy := c.InternalR.(*transport.InternalConnReader).InternalConn.(*fixtureReadTargets)
					spy.mu.Lock()
					direct := 0
					otherBulk := 0
					start := uintptr(unsafe.Pointer(backing))
					end := start + uintptr(len(dst))
					for _, target := range spy.targets {
						if target.n == 0 {
							continue
						}
						address := uintptr(unsafe.Pointer(&target.p[0]))
						if address >= start && address+uintptr(target.n) <= end {
							direct += target.n
						} else if target.n >= 4096 {
							otherBulk += target.n
						}
					}
					spy.mu.Unlock()
					// Native read-ahead may stage one bounded prefix; the bulk must
					// still land in the caller backing without payload-sized scratch.
					if direct < len(payload)-(4<<10) || direct > len(payload) || otherBulk > 4<<10 {
						t.Fatal("unexpected raw payload staging", direct, otherBulk)
					}
					cursor.NextInto(&dst)
					cursor.Close()
					<-done
				})
			}
		})

		t.Run("ReadAheadUpperBound", func(t *testing.T) {
			s, c, peer := rowStreamSession(t, 4, nil)
			defer s.Close()
			spy := c.InternalR.(*transport.InternalConnReader).InternalConn.(*testReadTargets)
			payload := bytes.Repeat([]byte{17}, 64<<10)
			done := make(chan error, 1)
			go func() {
				stream := typedTestReadRequest(t, c, peer)
				_, err := peer.Write(testResponse(t, c, stream, runtimeRowsBody(nil, string(payload))))
				done <- err
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			bound := runtimeQuery(t)
			defer bound.Close()
			cursor := codecTestMust(RunStream(ctx, s, bound, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true, Limits: transport.StreamLimits{ReadAheadBytes: 1}}))
			spy.mu.Lock()
			for _, target := range spy.targets {
				if len(target) > 16 {
					t.Error("raw payload read ahead before destination", len(target))
				}
			}
			spy.mu.Unlock()
			dst := make([]byte, 0, len(payload))
			if !cursor.NextInto(&dst) || !bytes.Equal(dst, payload) {
				t.Fatal("positive read-ahead cap changed output", cursor.Err())
			}
			if cursor.NextInto(&dst) || cursor.Err() != nil {
				t.Fatal(cursor.Err())
			}
			if err := cursor.Close(); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	})

	t.Run("Retry", func(t *testing.T) {
		t.Run("LatestAttemptMetadataBudgeted", func(t *testing.T) {
			s, c, peer := rowStreamSession(t, 4, nil)
			profile := settings.InternalDefaultProfile()
			profile.Retry = retryOnce{}
			handle := new(profile)
			payload := bytes.Repeat([]byte{'x'}, 12000)
			extension := binary.BigEndian.AppendUint16(nil, 1)
			extension = binary.BigEndian.AppendUint16(extension, 3)
			extension = append(extension, "big"...)
			extension = binary.BigEndian.AppendUint32(extension, uint32(len(payload)))
			extension = append(extension, payload...)
			withPayload := func(frame []byte) []byte {
				h := append([]byte(nil), frame[:9]...)
				h[1] = 4
				binary.BigEndian.PutUint32(h[5:], uint32(len(extension)+len(frame)-9))
				h = append(h, extension...)
				return append(h, frame[9:]...)
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				id := typedTestReadRequest(t, c, peer)
				if _, err := peer.Write(typedTestResponse(t, 4, id, runtimeRowsBody([]byte{}))); err != nil {
					return
				}
				id = typedTestReadRequest(t, c, peer)
				if _, err := peer.Write(withPayload(errorResponse(t, id, implProtocol.ErrCodeServer, nil))); err != nil {
					return
				}
				id = typedTestReadRequest(t, c, peer)
				_, _ = peer.Write(withPayload(typedTestResponse(t, 4, id, runtimeRowsBody(nil, "value"))))
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			b := runtimeQuery(t)
			defer b.Close()
			cursor, err := RunStream(ctx, s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true, Options: Options{Profile: handle}, Limits: transport.StreamLimits{MaxMetadataBytes: 20000}})
			if err != nil {
				t.Fatal(err)
			}
			dst := make([]byte, 0, 10)
			if cursor.NextInto(&dst) {
				t.Fatal("metadata budget ignored old latest-attempt storage")
			}
			if _, ok := errors.AsType[*cqltype.ProtocolError](cursor.Err()); !ok {
				t.Fatal(cursor.Err())
			}
			cursor.Close()
			if budget := c.InternalStreamBudget.Bytes(); budget != 0 {
				t.Fatal("metadata owner leaked", budget)
			}
			<-done
		})
	})

	t.Run("Storage", func(t *testing.T) {
		// Every current streaming mode receives the same generated results. This
		// measures byte-storage capacity while decoding, including inside skip reads,
		// and samples it again while a slow caller is idle. It is not an allocation or
		// throughput benchmark: fixed structs/plans and TLS's own record buffers are
		// separate from the bounded driver byte-storage claim.
		t.Run("Matrix", func(t *testing.T) {
			for _, mode := range fixtureModes {
				t.Run(mode.name, func(t *testing.T) {
					for _, work := range []struct {
						name       string
						rows, size int
						skip, slow bool
					}{
						{"cell256K", 1, 256 << 10, false, false}, {"cell8M", 1, 8 << 20, false, false},
						{"page8M", 128, 64 << 10, false, false}, {"skip8M", 1, 8 << 20, true, false},
						{"slowpage8M", 128, 64 << 10, false, true},
					} {
						t.Run(work.name, func(t *testing.T) {
							source := testRows(mode.version, work.rows, work.size)
							// v5's independent producer retains just one bounded source unit.
							var input io.Reader = source
							if mode.version == 5 {
								input = &unitSource{input: source, compressed: mode.compression != nil}
							}
							memory := &generatedConn{memoryConn: &memoryConn{}, input: input}
							var socket net.Conn = memory
							var peer net.Conn
							done := make(chan error, 1)
							if mode.tls {
								a, b := net.Pipe()
								cert, conf := testTLS(t)
								client := tls.Client(a, conf)
								server := tls.Server(b, &tls.Config{Certificates: []tls.Certificate{cert}})
								go func() { done <- server.Handshake() }()
								if err := client.Handshake(); err != nil {
									t.Fatal(err)
								}
								if err := <-done; err != nil {
									t.Fatal(err)
								}
								socket = &tlsConn{Conn: client, raw: a}
								peer = &tlsConn{Conn: server, raw: b}
							}
							r := memoryReservation(mode.version, mode.compression, socket)
							defer r.InternalClose(false)
							if peer != nil {
								defer peer.Close()
								go func() {
									typedTestReadRequest(t, r.InternalConn, peer)
									_, err := io.Copy(peer, input)
									done <- err
								}()
							}
							response, err := r.InternalExchange(context.Background(), &model.AttemptRequest{Statement: "SELECT generated", Consistency: cqltype.One})
							if err != nil {
								t.Fatal(err)
							}
							defer response.InternalReleaseMetadata()
							if err = response.InternalInstallColumns(response.InternalColumns); err != nil {
								t.Fatal(err)
							}
							var peak int64
							sample := func() {
								n := response.InternalWorkingStorageBytes()
								peak = max(peak, n)
								if n > 320<<10 {
									t.Fatalf("unbounded driver byte storage: %d", n)
								}
							}
							// Sampling the actual source Read catches simultaneous scratch during skip,
							// not merely the smaller storage retained after decode returns.
							r.InternalConn.InternalR.(*transport.InternalConnReader).InternalConn = &observedConn{Conn: socket, observe: sample}
							sample()
							if work.skip {
								plan := codecTestMust(codec.StreamRow(func(*codec.EmptyRecord) {}).Compile(response.InternalColumns))
								var dst codec.EmptyRecord
								for i := 0; i < work.rows; i++ {
									if ok, err := transport.InternalDecodeStreamRow(response, plan, &dst); !ok || err != nil {
										t.Fatal(ok, err)
									}
									sample()
								}
								if ok, err := transport.InternalDecodeStreamRow(response, plan, &dst); ok || err != nil {
									t.Fatal(ok, err)
								}
							} else {
								plan := codecTestMust(codec.StreamOneColumn(codec.StreamBlob).Compile(response.InternalColumns))
								dst := make([]byte, 0, work.size)
								backing := unsafe.SliceData(dst[:cap(dst)])
								for i := 0; i < work.rows; i++ {
									if ok, err := transport.InternalDecodeStreamRow(response, plan, &dst); !ok || err != nil {
										t.Fatal(ok, err)
									}
									if len(dst) != work.size || unsafe.SliceData(dst) != backing || bytes.Count(dst, []byte{0x71}) != len(dst) {
										t.Fatal("incorrect final output/backing")
									}
									sample()
									if work.slow {
										time.Sleep(time.Millisecond)
										sample()
									}
								}
								if ok, err := transport.InternalDecodeStreamRow(response, plan, &dst); ok || err != nil {
									t.Fatal(ok, err)
								}
							}
							if peer != nil {
								if err := <-done; err != nil {
									t.Fatal(err)
								}
							}
							copies := int64(0)
							if r.InternalUnits != nil {
								copies = r.InternalUnits.InternalCopies
							}
							t.Logf("mode=%s rows=%d cell=%d skipped=%v slow=%v peak_driver_byte_storage=%d validated_unit_to_parser_bytes=%d source_unit_payload<=130000 source_unit_encoded<=131071 final_output=%d", mode.name, work.rows, work.size, work.skip, work.slow, peak, copies, func() int {
								if work.skip {
									return 0
								}
								return work.size
							}())
						})
					}
				})
			}
		})
	})

	t.Run("Validation", func(t *testing.T) {
		t.Run("ContinuationMetadataAndCallbacks", func(t *testing.T) {
			s, c, peer := rowStreamSession(t, 4, nil)
			implCluster.SessionData(s).InternalCfg.Keyspace = "ks"
			columns := cqltype.NewColumns(cqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: cqltype.BlobType})
			state, _ := preparedState(t, s, columns)
			bound := new(Bound[cqltype.Bytes])
			BoundData(bound).InternalRequest = InternalExecutionRequest{statement: state.InternalStatement, InternalSource: state.preparedSource(), InternalValues: model.EncodedValues{Columns: cqltype.NewColumns()}, InternalOwner: InternalNewInputOwner(), routing: model.RoutingInfo{Kind: cqltype.OrdinaryStatement}}
			BoundData(bound).InternalRows = codec.OneColumn(codec.BlobCodec)
			defer bound.Close()
			// The first page is empty. The next omits metadata, then an equal full
			// metadata page is canonicalized, then a renamed column changes the plan.
			noMetadata := binary.BigEndian.AppendUint32(nil, 2)
			noMetadata = binary.BigEndian.AppendUint32(noMetadata, 4|2)
			noMetadata = binary.BigEndian.AppendUint32(noMetadata, 1)
			noMetadata = binary.BigEndian.AppendUint32(noMetadata, 1)
			noMetadata = append(noMetadata, 1)
			noMetadata = binary.BigEndian.AppendUint32(noMetadata, 1)
			noMetadata = binary.BigEndian.AppendUint32(noMetadata, 3)
			noMetadata = append(noMetadata, "abc"...)
			changed := bytes.Replace(runtimeRowsBody(nil, "ghi"), []byte("value"), []byte("other"), 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				for _, body := range [][]byte{runtimeRowsBody([]byte{}), noMetadata, runtimeRowsBody([]byte{2}, "def"), changed} {
					id := typedTestReadRequest(t, c, peer)
					if _, err := peer.Write(testResponse(t, c, id, body)); err != nil {
						t.Error(err)
						return
					}
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			decoder := new(pageValidationDecoder)
			cursor, err := RunStream(ctx, s, bound, decoder, StreamOptions{Incremental: true, Limits: transport.StreamLimits{MaxMetadataBytes: 1024}})
			if err != nil {
				t.Fatal(err)
			}
			defer cursor.Close()
			if decoder.compiles != 1 || decoder.resets != 0 || decoder.decodes != 0 {
				t.Fatal("empty page invoked application callbacks", decoder)
			}
			dst := make([]byte, 0, 3)
			for index, want := range []string{"abc", "def", "ghi"} {
				if !cursor.NextInto(&dst) || string(dst) != want {
					t.Fatal("continuation decode", index, string(dst), cursor.Err())
				}
				if decoder.resets != index+1 || decoder.decodes != index+1 {
					t.Fatal("application row callbacks changed", decoder)
				}
				if decoder.compiles != 1+index/2 {
					t.Fatal("metadata compile policy changed", decoder)
				}
				if budget := c.InternalStreamBudget.Bytes(); budget <= 0 || budget > 1024 {
					t.Fatal("live page metadata lost budget ownership", budget)
				}
			}
			if cursor.NextInto(&dst) || cursor.Err() != nil {
				t.Fatal(cursor.Err())
			}
			if err := cursor.Close(); err != nil {
				t.Fatal(err)
			}
			if decoder.compiles != 2 || decoder.resets != 3 || decoder.decodes != 3 || cursor.Info().Pages != 4 {
				t.Fatal(decoder, cursor.Info())
			}
			if budget := c.InternalStreamBudget.Bytes(); budget != 0 {
				t.Fatal("released page metadata retained its budget", budget)
			}
			if string(dst) != "ghi" || cursor.Columns().Len() != 1 {
				t.Fatal("closed stream lost caller output or metadata")
			}
			bound.Close()
			if err := bound.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			<-done
		})

		t.Run("IncrementalScalarParity", func(t *testing.T) {
			scalarParity(t, cqltype.TypeBoolean, codec.Bool, codec.StreamBool, []byte{0}, []byte{1}, []byte{2})
			scalarParity(t, cqltype.TypeDate, codec.DateCodec, codec.StreamDate, []byte{0, 0, 0, 0}, []byte{255, 255, 255, 255})
			scalarParity(t, cqltype.TypeTime, codec.TimeCodec, codec.StreamTime, []byte{255, 255, 255, 255, 255, 255, 255, 255}, []byte{0, 0, 78, 148, 145, 79, 0, 0})
			scalarParity(t, cqltype.TypeTimeUUID, codec.TimeUUIDCodec, codec.StreamTimeUUID, make([]byte, 16), []byte{0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0})
			scalarParity(t, cqltype.TypeInet, codec.InetCodec, codec.StreamInet, []byte{127, 0, 0, 1}, make([]byte, 16))
			scalarParity(t, cqltype.TypeDuration, codec.DurationCodec, codec.StreamDuration,
				[]byte{0, 0, 0}, []byte{1, 2, 0}, []byte{128, 0, 0, 0}, // accepted nonminimal zero prefix
				[]byte{240, 255, 255, 255, 255, 240, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
				[]byte{248, 1, 0, 0, 0, 0, 0, 0}, // months overflow
				[]byte{0, 0, 0, 0},               // trailing byte
			)
		})

		t.Run("SurvivesPriorResponseRelease", func(t *testing.T) {
			columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "value", Type: cqltype.BlobType})
			data := binary.BigEndian.AppendUint32(nil, 3)
			data = append(data, "abc"...)
			budget := &protocol.StreamMetadataBudget{Limit: 200}
			response := func() *transport.InternalStreamResponse {
				if err := budget.Reserve(100); err != nil {
					t.Fatal(err)
				}
				scope, err := wire.NewStream(context.Background(), bytes.NewReader(data), int64(len(data)), wire.StreamLimits{})
				if err != nil {
					t.Fatal(err)
				}
				return &transport.InternalStreamResponse{InternalBody: scope, InternalColumnCount: 1, InternalBudget: &protocol.StreamMetadataCursor{Budget: budget, Used: 100}}
			}
			prior := response()
			if err := prior.InternalInstallColumns(columns); err != nil {
				t.Fatal(err)
			}
			next := response()
			if err := next.InternalInstallColumnsReusing(columns, prior); err != nil {
				t.Fatal(err)
			}
			if budget.Bytes() != 200 {
				t.Fatal("simultaneous metadata owners lost charges", budget.Bytes())
			}
			prior.InternalReleaseMetadata()
			if prior.InternalReader != nil || budget.Bytes() != 100 {
				t.Fatal("old reader/budget not released", budget.Bytes())
			}
			plan := codecTestMust(codec.StreamOneColumn(codec.StreamBlob).Compile(columns))
			dst := make([]byte, 0, 3)
			plan.Reset(&dst)
			if err := plan.DecodeInto(next.InternalReader, &dst); err != nil || string(dst) != "abc" {
				t.Fatal("new reader depended on released prior owner", string(dst), err)
			}
			if err := next.InternalReader.RequireEnd(); err != nil {
				t.Fatal(err)
			}
			next.InternalReleaseMetadata()
			if budget.Bytes() != 0 || string(dst) != "abc" {
				t.Fatal("metadata charge leaked or caller output invalidated", budget.Bytes())
			}
		})
	})

	t.Run("Write", func(t *testing.T) {
		t.Run("Interrupted", func(t *testing.T) {
			for _, mode := range fixtureModes {
				for _, accepted := range []int{0, 1, 8} {
					t.Run(mode.name+fmt.Sprint(accepted), func(t *testing.T) {
						s, c, peer := fixtureStreamSession(t, mode)
						defer peer.Close()
						writer := &interruptedWriter{Conn: c.InternalR.(*transport.InternalConnReader).InternalConn, accepted: accepted}
						c.InternalW.(*transport.InternalDeadlineContextWriter).InternalW = writer
						b := runtimeQuery(t)
						ctx, cancel := context.WithTimeout(context.Background(), time.Second)
						defer cancel()
						cursor, err := RunStream(ctx, s, b, codec.StreamOneColumn(codec.StreamBlob), StreamOptions{Incremental: true, Options: Options{Retry: retry.Safe{MaxAttempts: 1}}})
						if cursor != nil {
							cursor.Close()
						}
						if err == nil {
							t.Fatal("interrupted write succeeded")
						}
						b.Close()
						if err := b.Wait(ctx); err != nil {
							t.Fatal("write failure retained input", err)
						}
						if !c.Closed() || writer.calls.Load() != 1 {
							t.Fatal("failed request connection not poisoned", writer.calls.Load())
						}
					})
				}
			}
		})
	})
}

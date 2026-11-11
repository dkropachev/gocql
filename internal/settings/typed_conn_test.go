// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math/rand"
	"testing"
	"time"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	implCqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/segment"
	transporttestkit "github.com/scylladb/gocql/v2/internal/testkit/transport"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wire"
	"github.com/scylladb/gocql/v2/internal/wirecompress"
	lz4 "github.com/scylladb/gocql/v2/internal/wirecompress"
)

func TestTypedConn(t *testing.T) {
	t.Run("BuildTypedRequest", func(t *testing.T) {
		t.Run("CompressedIndependentDecode", func(t *testing.T) {
			payload := bytes.Repeat([]byte("useful-compression-across-spans-"), 2000)
			var pieces [][]byte
			pieces = append(pieces, binary.BigEndian.AppendUint32(nil, uint32(len(payload))))
			for i := 0; i < len(payload); {
				n := min(43, len(payload)-i)
				pieces = append(pieces, payload[i:i+n])
				i += n
			}
			values := typedTestChain(t, pieces...)
			for _, compressor := range []protocol.Compressor{lz4.LZ4Compressor{}, wirecompress.SnappyCompressor{}} {
				c := &transport.Conn{InternalVersion: 4, InternalCompressor: compressor}
				c.InternalInitFramerCache()
				request := &model.AttemptRequest{Statement: "INSERT INTO tbl (v) VALUES (?)", Values: values, ValueCount: 1, Consistency: implCqltype.One}
				packet, err := c.InternalBuildTypedRequest(request, 8)
				if err != nil {
					t.Fatal(err)
				}
				encoded := typedTestBytes(t, packet)
				if encoded[1]&frm.FlagCompress == 0 {
					t.Fatal("compression flag missing")
				}
				if len(encoded) > len(payload)/2 {
					t.Fatal("span compressor failed to compress repetitive data")
				}
				decoded, err := compressor.Decode(encoded[9:])
				if err != nil {
					t.Fatal(err)
				}
				plain := &transport.Conn{InternalVersion: 4}
				plain.InternalInitFramerCache()
				oracle, err := plain.InternalBuildTypedRequest(request, 8)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(decoded, typedTestBytes(t, oracle)[9:]) {
					t.Fatal("independent decompressor observed wrong request bytes")
				}
			}
		})

		t.Run("ExecutePrepareBatchWireOracle", func(t *testing.T) {
			for _, proto := range []byte{4, 5} {
				payload := []byte("borrowed-batch-value")
				values := typedTestChain(t, binary.BigEndian.AppendUint32(nil, uint32(len(payload))), payload)
				for _, kind := range []string{"prepare", "execute", "batch"} {
					t.Run(string(rune('0'+proto))+kind, func(t *testing.T) {
						c := &transport.Conn{InternalVersion: proto}
						c.InternalInitFramerCache()
						r := &model.AttemptRequest{Statement: "INSERT INTO ks.tbl (v) VALUES (?)", Consistency: implCqltype.Quorum, SerialConsistency: implCqltype.LocalSerial, Timestamp: 123456, DefaultTimestamp: true}
						if proto >= 5 {
							r.Keyspace = "ks"
							r.HasNowInSeconds, r.NowInSeconds = true, 123
						}
						f := c.InternalGetWriteFramer()
						defer c.InternalReleaseWriteFramer(f)
						params := fixtureQueryParams{consistency: implCqltype.Quorum, serialConsistency: implCqltype.LocalSerial, defaultTimestamp: true, defaultTimestampValue: 123456, keyspace: r.Keyspace}
						if r.HasNowInSeconds {
							now := int(r.NowInSeconds)
							params.nowInSeconds = &now
						}
						var err error
						switch kind {
						case "prepare":
							r.Prepare = true
							r.HasNowInSeconds, r.NowInSeconds = false, 0
							err = (&writePrepareFrame{Statement: r.Statement, Keyspace: r.Keyspace}).BuildFrame(f, 23)
						case "execute":
							r.PreparedID = []byte{1, 2, 3}
							r.ResultMetadataID = []byte{4, 5, 6}
							r.Values = values
							r.ValueCount = 1
							params.values = []fixtureQueryValue{{value: payload}}
							empty := map[string][]byte(nil)
							err = fixtureWriteExecute(f, 23, r.PreparedID, r.ResultMetadataID, &params, &empty)
						case "batch":
							r.Batch = &model.TypedBatchRequest{Kind: implCqltype.LoggedBatch, Items: []model.TypedBatchItem{{Statement: r.Statement, Values: values, ValueCount: 1}, {PreparedID: []byte{1, 2}, Values: values, ValueCount: 1}}}
							err = (&fixtureBatchFrame{typ: implCqltype.LoggedBatch, statements: []fixtureBatchStatement{{statement: r.Statement, values: []fixtureQueryValue{{value: payload}}}, {preparedID: []byte{1, 2}, values: []fixtureQueryValue{{value: payload}}}}, consistency: implCqltype.Quorum, serialConsistency: implCqltype.LocalSerial, defaultTimestamp: true, defaultTimestampValue: 123456, keyspace: r.Keyspace, nowInSeconds: params.nowInSeconds}).BuildFrame(f, 23)
						}
						if err != nil {
							t.Fatal(err)
						}
						actual, err := c.InternalBuildTypedRequest(r, 23)
						if err != nil {
							t.Fatal(err)
						}
						encoded := typedTestBytes(t, actual)
						if proto >= 5 {
							reader := bytes.NewReader(encoded)
							h, err := segment.ReadHeader(reader, false)
							if err != nil {
								t.Fatal(err)
							}
							owner, err := segment.ReadOwnedPayload(reader, h, nil)
							if err != nil {
								t.Fatal(err)
							}
							encoded = typedTestBytes(t, owner.Chain())
							owner.Release()
						}
						if !bytes.Equal(encoded, f.Buf) {
							t.Fatalf("%s differs from legacy wire oracle\ngot %x\nwant %x", kind, encoded, f.Buf)
						}
					})
				}
			}
		})

		t.Run("MatchesLegacyWire", func(t *testing.T) {
			payload := bytes.Repeat([]byte("original-borrowed-payload"), 7000)
			length := binary.BigEndian.AppendUint32(nil, uint32(len(payload)))
			values := typedTestChain(t, length, payload)
			for _, proto := range []byte{4, 5} {
				c := &transport.Conn{InternalVersion: proto}
				c.InternalInitFramerCache()
				request := &model.AttemptRequest{Statement: "INSERT INTO ks.tbl (value) VALUES (?)", Values: values, ValueCount: 1, Consistency: implCqltype.Quorum, SerialConsistency: implCqltype.LocalSerial, PageSize: 83, PagingState: []byte{1, 2, 3}, Timestamp: 78321, DefaultTimestamp: true, Trace: true}
				if proto == 5 {
					request.Keyspace = "ks"
					request.HasNowInSeconds, request.NowInSeconds = true, 321
				}
				got, err := c.InternalBuildTypedRequest(request, 7)
				if err != nil {
					t.Fatal(err)
				}
				f := c.InternalGetWriteFramer()
				f.Trace()
				params := fixtureQueryParams{values: []fixtureQueryValue{{value: payload}}, consistency: implCqltype.Quorum, serialConsistency: implCqltype.LocalSerial, pageSize: 83, pagingState: request.PagingState, defaultTimestamp: true, defaultTimestampValue: 78321, keyspace: request.Keyspace}
				if request.HasNowInSeconds {
					now := int(request.NowInSeconds)
					params.nowInSeconds = &now
				}
				if err := fixtureWriteQuery(f, 7, request.Statement, &params, nil); err != nil {
					t.Fatal(err)
				}
				expected := append([]byte(nil), f.Buf...)
				if proto == 5 {
					var decoded []byte
					r := bytes.NewReader(typedTestBytes(t, got))
					for r.Len() > 0 {
						h, err := segment.ReadHeader(r, false)
						if err != nil {
							t.Fatal(err)
						}
						if h.IsSelfContained {
							t.Fatal("large split frame marked self contained")
						}
						owner, err := segment.ReadOwnedPayload(r, h, nil)
						if err != nil {
							t.Fatal(err)
						}
						decoded = append(decoded, typedTestBytes(t, owner.Chain())...)
						owner.Release()
					}
					if !bytes.Equal(decoded, expected) {
						t.Fatal("v5 wire differs from independent contiguous oracle")
					}
				} else if !bytes.Equal(typedTestBytes(t, got), expected) {
					t.Fatalf("v%d wire differs", proto)
				}
				// Request segments retain original payload backing even when boundaries cut it.
				retained := 0
				_ = got.Each(func(s wire.Span) error {
					b := s.Bytes()
					if len(b) > 1000 && bytes.Contains(payload, b) {
						found := false
						for i := 0; i+len(b) <= len(payload); i++ {
							if &b[0] == &payload[i] {
								found = true
								break
							}
						}
						if !found {
							t.Fatal("request payload gathered")
						}
						retained += len(b)
					}
					return nil
				})
				if retained != len(payload) {
					t.Fatalf("only %d/%d original bytes retained", retained, len(payload))
				}
				c.InternalReleaseWriteFramer(f)
			}
		})
	})

	t.Run("ExecuteTyped", func(t *testing.T) {
		t.Run("CancelKeepsStreamUntilOrphan", func(t *testing.T) {
			c, server := typedTestConn(t, 4, nil)
			go c.InternalServe(c.InternalCtx)
			ctx, cancel := context.WithCancel(context.Background())
			out := make(chan error, 1)
			go func() {
				_, err := c.InternalExecuteTyped(ctx, &model.AttemptRequest{Statement: "SELECT value FROM ks.tbl", Consistency: implCqltype.One})
				out <- err
			}()
			stream := typedTestReadRequest(t, c, server)
			cancel()
			if err := <-out; !errors.Is(err, context.Canceled) {
				t.Fatalf("got %v", err)
			}
			if c.InternalStreams.InUse() != 1 {
				t.Fatal("cancel reused stream before orphan arrived")
			}
			if _, err := server.Write(typedTestResponse(t, 4, stream, typedTestRowsBody([]byte("orphan")))); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(time.Second)
			for c.InternalStreams.InUse() != 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if c.InternalStreams.InUse() != 0 {
				t.Fatal("orphan did not release stream")
			}
		})

		t.Run("FatalCloseInterruptsBlockedWriter", func(t *testing.T) {
			c, _ := typedTestConn(t, 4, nil)
			out := make(chan error, 1)
			payload := bytes.Repeat([]byte("blocked"), 40000)
			values := typedTestChain(t, binary.BigEndian.AppendUint32(nil, uint32(len(payload))), payload)
			released := make(chan struct{})
			owner := wire.NewLease(values, func() { close(released) })
			go func() {
				_, err := c.InternalExecuteTyped(context.Background(), &model.AttemptRequest{Statement: "INSERT INTO ks.tbl (value) VALUES (?)", Values: values, ValueCount: 1, Lease: owner, Consistency: implCqltype.One})
				out <- err
			}()
			deadline := time.Now().Add(time.Second)
			for c.InternalStreams.InUse() == 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			closed := make(chan struct{})
			go func() { transporttestkit.Fail(c, io.ErrUnexpectedEOF); close(closed) }()
			select {
			case err := <-out:
				if err == nil {
					t.Fatal("blocked write succeeded")
				}
			case <-time.After(time.Second):
				t.Fatal("fatal close deadlocked writer")
			}
			owner.Release()
			select {
			case <-released:
			case <-time.After(time.Second):
				t.Fatal("write ownership leaked")
			}
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("fatal close deadlocked drain")
			}
		})

		t.Run("LargeFragmentedRows", func(t *testing.T) {
			payload := make([]byte, 500123)
			rand.New(rand.NewSource(42)).Read(payload)
			for _, pattern := range []string{"random", "repetitive"} {
				payload := payload
				if pattern == "repetitive" {
					payload = bytes.Repeat([]byte("compressible-row-cell"), 25007)
				}
				for _, proto := range []byte{4, 5} {
					for _, compressed := range []bool{false, true} {
						t.Run(pattern+"/"+string(rune('0'+proto))+map[bool]string{true: "LZ4", false: "raw"}[compressed], func(t *testing.T) {
							var comp protocol.Compressor
							if compressed {
								comp = lz4.LZ4Compressor{}
							}
							c, server := typedTestConn(t, proto, comp)
							go c.InternalServe(c.InternalCtx)
							peerDone := make(chan struct{})
							go func() {
								defer close(peerDone)
								stream := typedTestReadRequest(t, c, server)
								body := typedTestRowsBody(payload)
								if proto == 4 {
									response := typedTestResponse(t, proto, stream, body)
									if compressed {
										encoded, err := comp.Encode(body)
										if err != nil {
											t.Error(err)
											return
										}
										response = typedTestResponse(t, proto, stream, encoded)
										response[1] |= frm.FlagCompress
									}
									if _, err := server.Write(response); err != nil {
										t.Error(err)
									}
									return
								}
								frame := typedTestResponse(t, proto, stream, body)
								// Deliberately split inside the CQL header, value length, and payload.
								for offset := 0; offset < len(frame); {
									n := min(70001, len(frame)-offset)
									if offset == 0 {
										n = 3
									}
									source := typedTestChain(t, frame[offset:offset+n])
									packet, err := segment.EncodeSpans(source, false, compressed)
									if err != nil {
										t.Error(err)
										return
									}
									if _, err := server.Write(typedTestBytes(t, packet)); err != nil {
										t.Error(err)
										return
									}
									offset += n
								}
							}()
							page, err := c.InternalExecuteTyped(context.Background(), &model.AttemptRequest{Statement: "SELECT value FROM ks.tbl", Consistency: implCqltype.One, Timeout: 5 * time.Second})
							if err != nil {
								t.Fatal(err)
							}
							<-peerDone
							c.Close() // Delivered owners survive connection shutdown.
							cursor := wire.NewCursor(page.Rows)
							n, err := cursor.Uint32()
							if err != nil {
								t.Fatal(err)
							}
							chain, err := cursor.Take(int64(n))
							if err != nil {
								t.Fatal(err)
							}
							if !bytes.Equal(typedTestBytes(t, chain), payload) {
								t.Fatal("received row changed")
							}
							if proto == 5 {
								count := 0
								_ = chain.Each(func(wire.Span) error { count++; return nil })
								if count < 2 {
									t.Fatal("split row was reassembled")
								}
							}
							page.Owner.Release()
						})
					}
				}
			}
		})

		t.Run("ServerErrorKeepsMetadata", func(t *testing.T) {
			c, server := typedTestConn(t, 4, nil)
			go c.InternalServe(c.InternalCtx)
			go func() {
				stream := typedTestReadRequest(t, c, server)
				body := bytes.Repeat([]byte{9}, 16)
				body = binary.BigEndian.AppendUint16(body, 1)
				body = binary.BigEndian.AppendUint16(body, 4)
				body = append(body, "warn"...)
				body = binary.BigEndian.AppendUint16(body, 1)
				body = binary.BigEndian.AppendUint16(body, 3)
				body = append(body, "key"...)
				body = binary.BigEndian.AppendUint32(body, 3)
				body = append(body, "val"...)
				body = binary.BigEndian.AppendUint32(body, uint32(protocol.CodeUnprepared))
				body = binary.BigEndian.AppendUint16(body, 7)
				body = append(body, "missing"...)
				body = binary.BigEndian.AppendUint16(body, 2)
				body = append(body, 1, 2)
				response := typedTestResponse(t, 4, stream, body)
				response[1] = frm.FlagTracing | frm.FlagWarning | frm.FlagCustomPayload
				response[4] = byte(frm.OpError)
				if _, err := server.Write(response); err != nil {
					t.Error(err)
				}
			}()
			page, err := c.InternalExecuteTyped(context.Background(), &model.AttemptRequest{Statement: "SELECT value FROM ks.tbl", Consistency: implCqltype.One, Timeout: time.Second})
			if unprepared, ok := errors.AsType[*protocol.UnpreparedError](err); !ok || !bytes.Equal(unprepared.StatementID, []byte{1, 2}) {
				t.Fatalf("wrong error %v", err)
			}
			if page == nil || len(page.Warnings()) != 1 || len(page.TraceID()) != 16 || len(page.Payload()) != 1 {
				t.Fatal("error metadata lost")
			}
			if got := string(page.Payload()[0].Value.Value.OwnedBytes()); got != "val" {
				t.Fatalf("wrong payload %q", got)
			}
			page.Owner.Release()
			if _, err := page.Payload()[0].Value.Value.At(0); !errors.Is(err, implCqltype.ErrReleased) {
				t.Fatal("error payload owner did not expire")
			}
		})

		// A typed call waiting for its response returns once the connection's
		// context is cancelled without an explicit close: the cancellation closes
		// the connection, which delivers the closure to the call.
		t.Run("WaitEndsOnConnContextCancel", func(t *testing.T) {
			c, server := typedTestConn(t, 4, nil)
			go c.InternalServe(c.InternalCtx)
			out := make(chan error, 1)
			go func() {
				_, err := c.InternalExecuteTyped(context.Background(), &model.AttemptRequest{Statement: "SELECT value FROM ks.tbl", Consistency: implCqltype.One})
				out <- err
			}()
			typedTestReadRequest(t, c, server)
			c.InternalCancel()
			select {
			case err := <-out:
				if !errors.Is(err, runtimeerrors.ErrConnectionClosed) {
					t.Fatalf("got %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("waiting call missed the connection's cancellation")
			}
		})
	})

	t.Run("ParseTypedTabletHint", func(t *testing.T) {
		t.Run("AcrossEveryByte", func(t *testing.T) {
			e := protocol.NewTypedEnvelope()
			e.U32(8)
			e.U64(11)
			e.U32(8)
			e.U64(22)
			e.U32(36)
			e.U32(1)
			e.U32(28)
			e.U32(16)
			e.U64(2)
			e.U64(3)
			e.U32(4)
			e.U32(5)
			chain, err := e.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			body := typedTestBytes(t, chain)
			for split := 0; split <= len(body); split++ {
				chain := typedTestChain(t, body[:split], body[split:])
				owner := wire.NewLease(chain, nil)
				_, err := transport.InternalParseTypedTabletHint(cqltype.BytesView(chain, wire.RefOf(owner)), "ks", "tbl")
				owner.Release()
				if err != nil {
					t.Fatalf("split %d: %v", split, err)
				}
			}
			bad := append(append([]byte(nil), body...), 0)
			if _, err := transport.InternalParseTypedTabletHint(implCqltype.BorrowBytes(bad), "ks", "tbl"); err == nil {
				t.Fatal("accepted trailing tablet bytes")
			}
		})
	})

	t.Run("ProtocolLimits", func(t *testing.T) {
		t.Run("request", func(t *testing.T) {
			c := &transport.Conn{InternalVersion: 4, InternalCfg: &transport.ConnConfig{TypedLimits: ProtocolLimits{MaxFrameBytes: 8}}}
			if _, err := c.InternalBuildTypedRequest(&model.AttemptRequest{Statement: "SELECT value FROM table", Consistency: implCqltype.One}, 1); !errors.Is(err, protocol.ErrFrameTooBig) {
				t.Fatalf("got %v", err)
			}
		})
		t.Run("metadata depth", func(t *testing.T) {
			e := protocol.NewTypedEnvelope()
			e.U32(frm.ResultKindRows)
			e.U32(uint32(frm.FlagGlobalTableSpec))
			e.U32(1)
			e.Str("ks", false)
			e.Str("tbl", false)
			e.Str("v", false)
			e.U16(uint16(implCqltype.TypeList))
			e.U16(uint16(implCqltype.TypeList))
			e.U16(uint16(implCqltype.TypeInt))
			e.U32(0)
			body, _ := e.Freeze()
			owner := wire.NewLease(body, nil)
			defer owner.Release()
			c := &transport.Conn{InternalVersion: 4, InternalCfg: &transport.ConnConfig{TypedLimits: ProtocolLimits{MaxMetadataDepth: 1}}}
			if _, err := c.InternalParseTypedResponse(frm.FrameHeader{Version: 0x84, Op: frm.OpResult}, body, owner); err == nil {
				t.Fatal("metadata depth budget ignored")
			}
			c.InternalCfg.TypedLimits.MaxMetadataDepth = 2
			if _, err := c.InternalParseTypedResponse(frm.FrameHeader{Version: 0x84, Op: frm.OpResult}, body, owner); err != nil {
				t.Fatal(err)
			}
		})
		t.Run("v4 expansion before allocation", func(t *testing.T) {
			c, server := typedTestConn(t, 4, lz4.LZ4Compressor{})
			c.InternalCfg = &transport.ConnConfig{TypedLimits: ProtocolLimits{MaxDecompressedBytes: 64}}
			go c.InternalServe(c.InternalCtx)
			go func() {
				stream := typedTestReadRequest(t, c, server)
				response := typedTestResponse(t, 4, stream, binary.BigEndian.AppendUint32(nil, 1000))
				response[1] = frm.FlagCompress
				server.Write(response)
			}()
			page, err := c.InternalExecuteTyped(context.Background(), &model.AttemptRequest{Statement: "q", Consistency: implCqltype.One, Timeout: time.Second})
			if page != nil {
				page.Owner.Release()
			}
			if !errors.Is(err, protocol.ErrFrameTooBig) {
				t.Fatalf("got %v", err)
			}
		})
		t.Run("v5 expansion before payload read", func(t *testing.T) {
			c := &transport.Conn{InternalCfg: &transport.ConnConfig{TypedLimits: ProtocolLimits{MaxDecompressedBytes: 64}}}
			if _, err := c.InternalReadTypedSegmentPayload(segment.Header{PayloadLen: 1, UncompressedLen: 65}); !errors.Is(err, protocol.ErrFrameTooBig) {
				t.Fatalf("got %v", err)
			}
		})
	})

	t.Run("RecvLeasedSegment", func(t *testing.T) {
		t.Run("IndependentPageOwnership", func(t *testing.T) {
			c, server := typedTestConn(t, 5, nil)
			received := make(chan error, 1)
			go func() { received <- c.InternalRecvLeasedSegment(c.InternalCtx) }()
			type result struct {
				page *transport.InternalResponsePage
				err  error
			}
			out := make(chan result, 2)
			for i := 0; i < 2; i++ {
				go func() {
					page, err := c.InternalExecuteTyped(context.Background(), &model.AttemptRequest{Statement: "SELECT value FROM ks.tbl", Consistency: implCqltype.One, Timeout: time.Second})
					out <- result{page, err}
				}()
			}
			a, b := typedTestReadRequest(t, c, server), typedTestReadRequest(t, c, server)
			first := typedTestResponse(t, 5, a, typedTestRowsBody([]byte("first")))
			second := typedTestResponse(t, 5, b, typedTestRowsBody([]byte("second")))
			payload := typedTestChain(t, first, second)
			packet, err := segment.EncodeSpans(payload, true, false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := server.Write(typedTestBytes(t, packet)); err != nil {
				t.Fatal(err)
			}
			x, y := <-out, <-out
			// Both request callbacks can run before the segment dispatch returns. Join
			// that dispatch before testing that the receive loop released its share.
			if err := <-received; err != nil {
				t.Fatal(err)
			}
			if x.err != nil || y.err != nil {
				t.Fatalf("errors %v %v", x.err, y.err)
			}
			if x.page.Owner != y.page.Owner {
				t.Fatal("self-contained frames did not share segment lease")
			}
			x.page.Owner.Release()
			if !y.page.Owner.Alive() {
				t.Fatal("releasing first page invalidated second")
			}
			if len(typedTestBytes(t, y.page.Rows)) == 0 {
				t.Fatal("second page lost bytes")
			}
			y.page.Owner.Release()
			if y.page.Owner.Alive() {
				t.Fatal("receive loop retained completed segment")
			}
		})
	})
}

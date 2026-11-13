// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/wire"
	"github.com/scylladb/gocql/v2/internal/wirecompress"
)

func (c *Conn) receiveTypedCall(ctx context.Context, call *InternalCallReq, src InternalFrameSource, head frm.FrameHeader) error {
	var owner *wire.Lease
	var body wire.Chain
	var err error
	fatal := false
	limits := c.typedProtocolLimits()
	if int64(head.Length) > limits.MaxFrameBytes {
		err = protocol.ErrFrameTooBig
		if src.owner == nil {
			if discardErr := c.discardFrame(src.InternalR, head); discardErr != nil {
				err = discardErr
				fatal = true
			}
		}
	} else if src.owner != nil {
		if !src.owner.Retain() {
			err = cqltype.ErrReleased
		} else {
			owner = src.owner
			body = src.chain
		}
	} else {
		var cache *wire.ReceiveCache
		if c.InternalCfg != nil {
			cache = c.InternalCfg.InternalReceiveCache
		}
		// An unextended custom decoder may alias its input. Leave that original
		// ownership unpooled; built-ins support checked final-arena decoding.
		decoder, pooledDecode := wirecompress.FrameDecoderWithLimit(c.InternalCompressor, limits.MaxDecompressedBytes)
		if head.Flags&frm.FlagCompress != 0 && !pooledDecode {
			cache = nil
		}
		data, epoch := cache.Acquire(head.Length)
		cacheInput := data
		returned := false
		var output []byte
		var outputEpoch uint64
		outputReturned := false
		defer func() {
			if output != nil && !outputReturned {
				cache.Release(output, outputEpoch)
			}
			if !returned {
				cache.Release(cacheInput, epoch)
			}
		}()
		if _, err = io.ReadFull(src.InternalR, data); err != nil {
			fatal = true
		} else {
			if !c.InternalWire().Segments && head.Flags&frm.FlagCompress != 0 {
				if c.InternalCompressor == nil {
					err = fmt.Errorf("gocql: compressed frame without compressor")
				} else {
					var size uint64
					switch c.InternalCompressor.Name() {
					case "lz4":
						if len(data) < 4 {
							err = io.ErrUnexpectedEOF
						} else {
							size = uint64(binary.BigEndian.Uint32(data[:4]))
						}
					case "snappy":
						var n int
						size, n = binary.Uvarint(data)
						if n <= 0 {
							err = fmt.Errorf("gocql: invalid Snappy length")
						}
					default:
						err = fmt.Errorf("gocql: unsupported typed compressor %q", c.InternalCompressor.Name())
					}
					if err == nil && size > uint64(limits.MaxDecompressedBytes) {
						err = protocol.ErrFrameTooBig
					}
					if err == nil {
						if pooledDecode {
							checkedSize, checkErr := decoder.DecodedLen(data)
							if checkErr != nil {
								err = checkErr
							} else if uint64(checkedSize) != size {
								err = fmt.Errorf("gocql: decoded frame length mismatch")
							} else {
								output, outputEpoch = cache.Acquire(checkedSize)
								data, err = decoder.DecodeTo(output[:0], data)
							}
							if err == nil && len(data) > 0 && (len(output) == 0 || &data[0] != &output[0]) {
								err = segment.ErrDecoderStorage
							}
						} else {
							data, err = c.InternalCompressor.Decode(data)
						}
						if err == nil && uint64(len(data)) != size {
							err = fmt.Errorf("gocql: decoded frame length mismatch")
						}
					}
				}
			}
			if err == nil && int64(len(data)) > limits.MaxDecompressedBytes {
				err = protocol.ErrFrameTooBig
			}
			if err == nil {
				body, _ = wire.NewChain(wire.Borrow(data))
				if !c.InternalWire().Segments && head.Flags&frm.FlagCompress == 0 {
					returned = true
					owner = wire.NewCachedLease(body, cache, data, epoch)
				} else if pooledDecode && head.Flags&frm.FlagCompress != 0 {
					outputReturned = true
					owner = wire.NewCachedLease(body, cache, output, outputEpoch)
				} else {
					owner = wire.NewLease(body, nil)
				}
			}
		}
	}
	var page *InternalResponsePage
	if err == nil {
		if headerErr := frm.CheckResponseHeader(head, c.InternalVersion, c.typedResponseExtensionFlags()); headerErr != nil {
			err = headerErr
		} else {
			// The caller's storage may be used only while it still waits.
			var dest *InternalResponsePage
			if call.dest != nil && call.claim() {
				dest = call.dest
			}
			page, err = c.parseTypedResponseInto(head, body, owner, dest)
		}
	}
	if err != nil && owner != nil && page == nil {
		owner.Release()
		owner = nil
	}
	if !call.deliver(InternalCallResp{page: page, InternalErr: err, removedFromCalls: true}) {
		if page != nil {
			page.Owner.Release()
		}
		c.releaseStream(call)
		call.waitExecDone("receiveTypedCall")
		putCallReq(call)
	}
	if fatal {
		return err
	}
	return nil
}

func (c *Conn) InternalRecvLeasedSegment(ctx context.Context) error {
	start := c.observedNow()
	hdr, err := c.InternalReadFirstSegmentHeader()
	if err != nil {
		return err
	}
	first, err := c.InternalReadTypedSegmentPayload(hdr)
	if err != nil {
		return err
	}
	end := c.observedNow()
	if !hdr.IsSelfContained {
		return c.recvSplitLeasedFrame(ctx, first, start, end)
	}
	defer first.Release()
	cursor := wire.CursorOver(first.Chain())
	for cursor.Remaining() > 0 {
		frameHeader, err := cursor.Take(protocol.HeadSize)
		if err != nil {
			return fmt.Errorf("gocql: incomplete frame header in self-contained segment: %w", err)
		}
		_, _ = frameHeader.CopyTo(c.headerBuf[:])
		head, err := frm.DecodeHeader(c.headerBuf[:])
		if err != nil {
			return err
		}
		body, err := cursor.Take(int64(head.Length))
		if err != nil {
			return fmt.Errorf("gocql: frame header declares a %d byte body but only %d bytes remain in the self-contained segment", head.Length, cursor.Remaining())
		}
		if err := c.processFrameSource(ctx, InternalFrameSource{owner: first, chain: body, netStart: start, netEnd: end}, &head); err != nil {
			return err
		}
	}
	return nil
}

// splitFrame owns the segments one frame was split across. A retained page
// keeps it, and through it every segment payload, alive.
type splitFrame struct {
	owners []*wire.Lease
	wire.Lease
}

func (f *splitFrame) ReleaseLease() {
	for _, owner := range f.owners {
		owner.Release()
	}
	f.owners = nil
}

// splitFrameSegments is how many segments a split frame collects without
// allocating: 16 full segments carry a frame of up to 2 MiB less 16 bytes.
const splitFrameSegments = 16

// recvSplitLeasedFrame reassembles one frame the peer split across
// non-self-contained segments, the first of which is first, and processes it.
// It owns first's reference.
//
// Segment owners and span descriptors collect in stack arrays. The frame's
// lease, its owner list and its span index are allocated once per frame and
// belong to that frame alone: a page that retains it must never see them
// rewritten by a later frame.
func (c *Conn) recvSplitLeasedFrame(ctx context.Context, first *wire.Lease, start, end time.Time) error {
	var ownerBuf [splitFrameSegments]*wire.Lease
	var spanBuf [splitFrameSegments]wire.Span
	owners := append(ownerBuf[:0], first)
	spans := spanBuf[:0]
	defer func() {
		for _, owner := range owners {
			owner.Release()
		}
	}()
	spans = appendChainSpans(spans, first.Chain())
	total := first.Chain().Len()
	readNext := func() error {
		nextHdr, err := segment.ReadHeaderInto(c.InternalR, c.InternalSegCompressor != nil, c.headerBuf[:])
		if err != nil {
			return fmt.Errorf("gocql: failed to read continuation segment header: %w", err)
		}
		if nextHdr.IsSelfContained {
			return fmt.Errorf("gocql: received self-contained segment, but expected a continuation")
		}
		next, err := c.InternalReadTypedSegmentPayload(nextHdr)
		if err != nil {
			return fmt.Errorf("gocql: failed to read continuation segment payload: %w", err)
		}
		if next.Chain().Len() == 0 {
			next.Release()
			return fmt.Errorf("gocql: continuation segment made no progress (empty payload)")
		}
		owners = append(owners, next)
		spans = appendChainSpans(spans, next.Chain())
		total += next.Chain().Len()
		return nil
	}
	for total < protocol.HeadSize {
		if err := readNext(); err != nil {
			return err
		}
		end = c.observedNow()
	}
	n := 0
	for _, s := range spans {
		if n += copy(c.headerBuf[n:], s.Bytes()); n == protocol.HeadSize {
			break
		}
	}
	head, err := frm.DecodeHeader(c.headerBuf[:])
	if err != nil {
		return err
	}
	limits := c.typedProtocolLimits()
	if int64(head.Length) > min(limits.MaxFrameBytes, limits.MaxDecompressedBytes) {
		return fmt.Errorf("gocql: invalid frame body length in segmented frame: %d", head.Length)
	}
	expected := int64(head.Length) + protocol.HeadSize
	for total < expected {
		if err := readNext(); err != nil {
			return err
		}
	}
	if total != expected {
		return fmt.Errorf("gocql: segmented frame exceeds its declared length %d", expected)
	}
	chain, _ := wire.NewChain(spans...)
	body, _ := chain.Slice(protocol.HeadSize, int64(head.Length))
	frame := &splitFrame{owners: append([]*wire.Lease(nil), owners...)}
	owners = owners[:0]
	wire.InitLease(&frame.Lease, chain, frame)
	defer frame.Release()
	return c.processFrameSource(ctx, InternalFrameSource{owner: &frame.Lease, chain: body, netStart: start, netEnd: end}, &head)
}

// appendChainSpans appends c's spans to dst.
func appendChainSpans(dst []wire.Span, c wire.Chain) []wire.Span {
	_ = c.Each(func(s wire.Span) error { dst = append(dst, s); return nil })
	return dst
}

func (c *Conn) InternalReadTypedSegmentPayload(hdr segment.Header) (*wire.Lease, error) {
	if int64(hdr.UncompressedLen) > c.typedProtocolLimits().MaxDecompressedBytes {
		return nil, protocol.ErrFrameTooBig
	}
	var cache *wire.ReceiveCache
	if c.InternalCfg != nil {
		cache = c.InternalCfg.InternalReceiveCache
	}
	// The trailer goes through headerBuf: no header is being read meanwhile.
	return segment.ReadCachedPayload(c.InternalR, hdr, c.InternalSegCompressor, cache, c.headerBuf[:])
}

// typedResponseExtensionFlags returns the response header flags extensions
// add to the version's own. Every supported version already allows the custom
// payload that carries ScyllaDB's tablet feedback, so none do.
func (c *Conn) typedResponseExtensionFlags() byte {
	return 0
}

func (c *Conn) InternalParseTypedResponse(head frm.FrameHeader, body wire.Chain, owner *wire.Lease) (*InternalResponsePage, error) {
	return c.parseTypedResponseInto(head, body, owner, nil)
}

// parseTypedResponseInto parses into page, or into new storage when page is nil.
func (c *Conn) parseTypedResponseInto(head frm.FrameHeader, body wire.Chain, owner *wire.Lease, page *InternalResponsePage) (*InternalResponsePage, error) {
	if page == nil {
		page = new(InternalResponsePage)
	} else {
		page.InternalMetadata = nil
	}
	data, err := c.nativeProtocol().ParseTypedResponseInto(head, body, owner, &page.ResponseData)
	if data == nil {
		clearFailedDest(page)
		return nil, err
	}
	return page, err
}

// chainReader is used only for small frame headers and the migration adapter
// serving legacy callers. Typed response payloads pass through checked cursors.
type chainReader struct{ cursor *wire.Cursor }

func newChainReader(c wire.Chain) *chainReader {
	return &chainReader{cursor: wire.NewCursor(c)}
}

func (r *chainReader) Read(p []byte) (int, error) {
	if r.cursor.Remaining() == 0 {
		return 0, io.EOF
	}
	n := min(int64(len(p)), r.cursor.Remaining())
	part, err := r.cursor.Take(n)
	if err != nil {
		return 0, err
	}
	return part.CopyTo(p[:int(n)])
}

// clearFailedDest empties the page after a failed parse, so a caller's
// destination cannot keep half-decoded values or point at the lease that
// receiveTypedCall releases on the error path. It stays out of line to keep
// zeroing the large page out of the successful parse path.
//
//go:noinline
func clearFailedDest(page *InternalResponsePage) { *page = InternalResponsePage{} }

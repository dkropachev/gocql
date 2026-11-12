package segment

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/scylladb/gocql/v2/internal/crc"
	"github.com/scylladb/gocql/v2/internal/wire"
	"github.com/scylladb/gocql/v2/internal/wirecompress"
)

// EncodeSpans wraps immutable spans in a segment without gathering the payload.
// Compression writes its final output directly; raw fallback retains the input.
func EncodeSpans(payload wire.Chain, selfContained, compressed bool) (wire.Chain, error) {
	if payload.Len() > MaxPayloadSize {
		return wire.Chain{}, fmt.Errorf("gocql: segment payload exceeds %d", MaxPayloadSize)
	}
	originalLen := payload.Len()
	uncompressedLen := int64(0)
	if compressed {
		out, err := wirecompress.AppendLZ4(nil, payload)
		if err != nil {
			return wire.Chain{}, err
		}
		if len(out) > 0 && int64(len(out)) < originalLen {
			payload, _ = wire.NewChain(wire.Borrow(out))
			uncompressedLen = originalLen
		}
	}
	headerSize := HeaderSize(compressed)
	header := make([]byte, headerSize)
	putHeader(header, payload.Len(), uncompressedLen, selfContained, compressed)
	sum := crcSeed
	spans := []wire.Span{wire.Borrow(header)}
	_ = payload.Each(func(s wire.Span) error {
		sum = crc32.Update(sum, crc32.IEEETable, s.Bytes())
		spans = append(spans, s)
		return nil
	})
	trailer := make([]byte, 4)
	binary.LittleEndian.PutUint32(trailer, sum)
	spans = append(spans, wire.Borrow(trailer))
	return wire.NewChain(spans...)
}

// EncodeUncompressedInto is EncodeSpans for an uncompressed payload that fits
// one segment, writing the header, trailer and span index into sc: it does not
// allocate once sc has grown. The result aliases sc, so sc may be reused only
// after the returned chain drained.
func EncodeUncompressedInto(sc *wire.BuilderScratch, payload wire.Chain, selfContained bool) (wire.Chain, error) {
	if payload.Len() > MaxPayloadSize {
		return wire.Chain{}, fmt.Errorf("gocql: segment payload exceeds %d", MaxPayloadSize)
	}
	var hdr [UncompressedHeaderSize]byte
	putHeader(hdr[:], payload.Len(), 0, selfContained, false)
	sum := crcSeed
	_ = payload.Each(func(s wire.Span) error {
		sum = crc32.Update(sum, crc32.IEEETable, s.Bytes())
		return nil
	})
	frame := sc.FrameBytes()
	copy(frame, hdr[:])
	binary.LittleEndian.PutUint32(frame[UncompressedHeaderSize:], sum)
	return sc.Wrap(wire.Borrow(frame[:UncompressedHeaderSize]), payload,
		wire.Borrow(frame[UncompressedHeaderSize:UncompressedHeaderSize+Crc32Size]))
}

// crcSeed is the CRC32 state after the segment payload's fixed initial bytes.
var crcSeed = crc32.ChecksumIEEE([]byte{0xfa, 0x2d, 0x55, 0xca})

// putHeader writes a segment header, its CRC24 included, into header.
func putHeader(header []byte, payloadLen int64, uncompressedLen int64, selfContained, compressed bool) {
	fields := len(header) - Crc24Size
	packed := uint64(payloadLen)
	if compressed {
		packed |= uint64(uncompressedLen) << 17
		if selfContained {
			packed |= 1 << 34
		}
	} else if selfContained {
		packed |= 1 << 17
	}
	for i := 0; i < fields; i++ {
		header[i] = byte(packed >> (8 * i))
	}
	checksum := crc.Crc24(header[:fields])
	for i := 0; i < 3; i++ {
		header[fields+i] = byte(checksum >> (8 * i))
	}
}

// ReadOwnedPayload receives directly into final immutable storage. The returned
// lease can outlive both the connection and reads of subsequent segments.
func ReadOwnedPayload(r io.Reader, h Header, comp Compressor) (*wire.Lease, error) {
	var trailer [Crc32Size]byte
	return ReadCachedPayload(r, h, comp, nil, trailer[:])
}

// ReadCachedPayload is ReadOwnedPayload into storage acquired from cache, which
// the lease's final release returns there. It reads the CRC32 trailer through
// trailer, which holds at least Crc32Size bytes, so a caller-owned buffer keeps
// it from escaping. A nil cache allocates the storage.
func ReadCachedPayload(r io.Reader, h Header, comp Compressor, cache *wire.ReceiveCache, trailer []byte) (*wire.Lease, error) {
	if err := validatePayloadHeader(h, comp != nil); err != nil {
		return nil, err
	}
	payload, epoch := cache.Acquire(h.PayloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		cache.Release(payload, epoch)
		return nil, fmt.Errorf("gocql: failed reading segment payload: %w", err)
	}
	trailer = trailer[:Crc32Size]
	if _, err := io.ReadFull(r, trailer); err != nil {
		cache.Release(payload, epoch)
		return nil, fmt.Errorf("gocql: failed reading segment CRC: %w", err)
	}
	if crc.Crc32(payload) != binary.LittleEndian.Uint32(trailer) {
		cache.Release(payload, epoch)
		return nil, fmt.Errorf("gocql: crc32 mismatch in segment payload")
	}
	if h.UncompressedLen != 0 {
		if comp == nil {
			cache.Release(payload, epoch)
			return nil, fmt.Errorf("gocql: compressed segment without compressor")
		}
		out, outEpoch := cache.Acquire(h.UncompressedLen)
		decoded, err := comp.AppendDecompressed(out[:0], payload, uint32(h.UncompressedLen))
		if err == nil && len(decoded) != h.UncompressedLen {
			err = fmt.Errorf("gocql: segment decompressed to %d bytes, expected %d", len(decoded), h.UncompressedLen)
		}
		if err != nil {
			cache.Release(out, outEpoch)
			cache.Release(payload, epoch)
			return nil, err
		}
		if &decoded[0] != &out[0] {
			// The decoder returned storage of its own, which may alias its
			// input: neither buffer goes back to the cache.
			return wire.NewLease(wire.ChainOf(decoded), nil), nil
		}
		cache.Release(payload, epoch)
		return wire.NewCachedLease(wire.ChainOf(decoded), cache, out, outEpoch), nil
	}
	return wire.NewCachedLease(wire.ChainOf(payload), cache, payload, epoch), nil
}

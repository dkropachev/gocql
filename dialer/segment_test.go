package dialer

import (
	"encoding/binary"
	"testing"

	"github.com/scylladb/gocql/v2/internal/crc"
	"github.com/scylladb/gocql/v2/internal/segment"
)

// passthroughCompressor is a no-op compressor that still honours the append
// contract. Using a real lz4 would drag in a separate module; what these tests need
// is the compressed *layout*, not compression.
type passthroughCompressor struct{}

func (passthroughCompressor) AppendCompressed(dst, src []byte) ([]byte, error) {
	return append(dst, src...), nil
}

func (passthroughCompressor) AppendDecompressed(dst, src []byte, _ uint32) ([]byte, error) {
	return append(dst, src...), nil
}

// expandingCompressor decodes an empty wire payload into decompressedLength bytes.
// The structural SegmentCompressor contract permits that, and the driver's reader
// accepts what it produces. A real lz4 does not -- pierrec's UncompressBlock
// short-circuits on an empty source and returns nothing, which passthroughCompressor
// above happens to model exactly.
type expandingCompressor struct{ out []byte }

func (expandingCompressor) AppendCompressed(dst, src []byte) ([]byte, error) {
	return append(dst, src...), nil
}

func (c expandingCompressor) AppendDecompressed(dst, src []byte, decompressedLength uint32) ([]byte, error) {
	if len(src) == 0 {
		return append(dst, c.out[:decompressedLength]...), nil
	}
	return append(dst, src...), nil
}

// compressors is the set every decoder test runs against: both segment layouts.
func compressors() []struct {
	name string
	comp SegmentCompressor
} {
	return []struct {
		name string
		comp SegmentCompressor
	}{
		{name: "uncompressed", comp: nil},
		{name: "compressed", comp: passthroughCompressor{}},
	}
}

// mustSegment builds one segment, failing the test if the codec rejects it.
func mustSegment(t *testing.T, payload []byte, selfContained bool, comp SegmentCompressor) []byte {
	t.Helper()

	seg, err := segment.Append(nil, payload, selfContained, comp)
	if err != nil {
		t.Fatalf("build segment: %v", err)
	}
	return seg
}

// zeroWireSegment builds a compressed chain segment carrying no payload bytes on the
// wire that declares decodedLen bytes after decompression. The header is spelled out
// here because segment.Append cannot produce the shape: AppendCompressed rewrites a
// zero compressed length into the store-as-is fallback, on the grounds that a peer
// running a real compressor could not decode it.
func zeroWireSegment(decodedLen int) []byte {
	var wide [8]byte
	binary.LittleEndian.PutUint64(wide[:], uint64(decodedLen)<<17)

	seg := append([]byte(nil), wide[:5]...)
	checksum := crc.Crc24(seg)
	seg = append(seg, byte(checksum), byte(checksum>>8), byte(checksum>>16))
	return binary.LittleEndian.AppendUint32(seg, crc.Crc32(nil))
}

// decode feeds chunks to a splitter and returns the frames it recovered.
func decode(t *testing.T, s *SegmentSplitter, chunks ...[]byte) [][]byte {
	t.Helper()

	var got [][]byte
	for _, chunk := range chunks {
		if err := s.Feed(chunk, func(frame []byte) error {
			got = append(got, append([]byte(nil), frame...))
			return nil
		}); err != nil {
			t.Fatalf("Feed: %v", err)
		}
	}
	return got
}

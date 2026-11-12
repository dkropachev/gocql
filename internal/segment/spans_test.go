package segment

import (
	"bytes"
	"testing"

	lz4 "github.com/pierrec/lz4/v4"

	"github.com/scylladb/gocql/v2/internal/wire"
)

type independentLZ4 struct{}

func (independentLZ4) AppendCompressed(dst, src []byte) ([]byte, error) { panic("unused") }
func (independentLZ4) AppendDecompressed(dst, src []byte, n uint32) ([]byte, error) {
	start := len(dst)
	dst = append(dst, make([]byte, n)...)
	decoded, err := lz4.UncompressBlock(src, dst[start:])
	if err != nil {
		return nil, err
	}
	return dst[:start+decoded], nil
}
func flattenTest(t *testing.T, c wire.Chain) []byte {
	t.Helper()
	out := make([]byte, c.Len())
	if _, err := c.CopyTo(out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestEncodeUncompressedIntoMatchesEncodeSpans(t *testing.T) {
	sc := wire.NewBuilderScratch(16, 2)
	for _, sizes := range [][]int{{1}, {5, 100}, {3, 3, 3, 3, 3}, {MaxPayloadSize}} {
		var spans []wire.Span
		for _, n := range sizes {
			spans = append(spans, wire.Borrow(bytes.Repeat([]byte{byte(n)}, n)))
		}
		payload, _ := wire.NewChain(spans...)
		want, err := EncodeSpans(payload, true, false)
		if err != nil {
			t.Fatal(err)
		}
		for round := 0; round < 3; round++ {
			got, err := EncodeUncompressedInto(sc, payload, true)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(flattenTest(t, got), flattenTest(t, want)) {
				t.Fatalf("sizes %v round %d: segment bytes differ", sizes, round)
			}
			sc.Reset()
		}
	}
}

// foreignDecoder decompresses into storage of its own, ignoring dst.
type foreignDecoder struct{ independentLZ4 }

func (f foreignDecoder) AppendDecompressed(_, src []byte, n uint32) ([]byte, error) {
	return f.independentLZ4.AppendDecompressed(nil, src, n)
}

func TestReadCachedPayload(t *testing.T) {
	encode := func(t *testing.T, payload []byte, compressed bool) *bytes.Reader {
		chain, _ := wire.NewChain(wire.Borrow(payload))
		out, err := EncodeSpans(chain, true, compressed)
		if err != nil {
			t.Fatal(err)
		}
		return bytes.NewReader(flattenTest(t, out))
	}
	read := func(t *testing.T, r *bytes.Reader, compressed bool, comp Compressor, cache *wire.ReceiveCache) (*wire.Lease, error) {
		h, err := ReadHeader(r, compressed)
		if err != nil {
			t.Fatal(err)
		}
		var trailer [Crc32Size]byte
		return ReadCachedPayload(r, h, comp, cache, trailer[:])
	}
	compressible := bytes.Repeat([]byte("cached segment "), 600)

	t.Run("ReleaseReturnsStorage", func(t *testing.T) {
		cache := new(wire.ReceiveCache)
		first, err := read(t, encode(t, []byte("first"), false), false, nil, cache)
		if err != nil {
			t.Fatal(err)
		}
		if _, objects := cache.Storage(); objects != 0 {
			t.Fatalf("idle objects = %d while the payload is leased", objects)
		}
		first.Release()
		if _, objects := cache.Storage(); objects != 1 {
			t.Fatalf("idle objects = %d after release, want 1", objects)
		}
		second, err := read(t, encode(t, []byte("second"), false), false, nil, cache)
		if err != nil {
			t.Fatal(err)
		}
		if got := string(flattenTest(t, second.Chain())); got != "second" {
			t.Fatalf("payload = %q", got)
		}
		second.Release()
		if st := cache.Stats(); st.Hits != 1 {
			t.Fatalf("hits = %d, want 1", st.Hits)
		}
	})

	t.Run("CompressedKeepsOutputOnly", func(t *testing.T) {
		cache := new(wire.ReceiveCache)
		owner, err := read(t, encode(t, compressible, true), true, independentLZ4{}, cache)
		if err != nil {
			t.Fatal(err)
		}
		// The compressed input went back; the decoded output is leased.
		if _, objects := cache.Storage(); objects != 1 {
			t.Fatalf("idle objects = %d while leased, want 1", objects)
		}
		if !bytes.Equal(flattenTest(t, owner.Chain()), compressible) {
			t.Fatal("decoded payload mismatch")
		}
		owner.Release()
		if _, objects := cache.Storage(); objects != 2 {
			t.Fatalf("idle objects = %d after release, want 2", objects)
		}
	})

	t.Run("ForeignDecoderStorageStaysOut", func(t *testing.T) {
		cache := new(wire.ReceiveCache)
		owner, err := read(t, encode(t, compressible, true), true, foreignDecoder{}, cache)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(flattenTest(t, owner.Chain()), compressible) {
			t.Fatal("decoded payload mismatch")
		}
		owner.Release()
		if _, objects := cache.Storage(); objects != 0 {
			t.Fatalf("idle objects = %d, want 0: a foreign decoder may alias its input", objects)
		}
	})

	t.Run("CorruptReturnsStorage", func(t *testing.T) {
		cache := new(wire.ReceiveCache)
		r := encode(t, []byte("corrupt"), false)
		data := make([]byte, r.Len())
		_, _ = r.Read(data)
		data[len(data)-1] ^= 0xff
		if _, err := read(t, bytes.NewReader(data), false, nil, cache); err == nil || err.Error() != "gocql: crc32 mismatch in segment payload" {
			t.Fatalf("err = %v", err)
		}
		if _, objects := cache.Storage(); objects != 1 {
			t.Fatalf("idle objects = %d after a corrupt segment, want 1", objects)
		}
	})
}

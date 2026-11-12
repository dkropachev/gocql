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

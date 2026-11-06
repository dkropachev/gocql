package segment

import (
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

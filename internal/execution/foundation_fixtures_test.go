package execution

import (
	"context"
	"encoding/binary"
	"io"
	"testing"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func codecTestMust[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
func foundationScalar(t testing.TB, kind cqltype.Type) cqltype.DataType {
	t.Helper()
	d, err := cqltype.ScalarType(kind)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func scalarReader(t testing.TB, kind cqltype.Type, state cqltype.CellState, chunks ...[]byte) codec.ValueReader {
	t.Helper()
	spans := make([]wire.Span, len(chunks))
	for i, b := range chunks {
		spans[i] = wire.Borrow(b)
	}
	chain, err := wire.NewChain(spans...)
	if err != nil {
		t.Fatal(err)
	}
	return codec.NewValueReader(foundationScalar(t, kind), state, chain, nil)
}
func streamCell(body []byte) []byte {
	b := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(b, uint32(len(body)))
	copy(b[4:], body)
	return b
}
func streamDecodeFixture[T any](t testing.TB, typ cqltype.DataType, decoder codec.StreamDecoder[T], data []byte, dst *T, step int) error {
	t.Helper()
	columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: typ})
	r, scope := streamReaderFixture(t, columns, &streamChunkSource{data: data, step: step}, len(data))
	plan, err := codec.StreamOneColumn(decoder).Compile(columns)
	if err != nil {
		return err
	}
	plan.Reset(dst)
	if err := plan.DecodeInto(r, dst); err != nil {
		return err
	}
	if err := r.RequireEnd(); err != nil {
		return err
	}
	return scope.RequireEnd()
}

type streamChunkSource struct {
	data                []byte
	offset, step, reads int
	record              func([]byte, int)
}

func (s *streamChunkSource) Read(p []byte) (int, error) {
	if s.offset == len(s.data) {
		return 0, io.EOF
	}
	n := min(len(p), len(s.data)-s.offset)
	if s.step > 0 {
		n = min(n, s.step)
	}
	if s.record != nil {
		s.record(p[:n], s.offset)
	}
	copy(p, s.data[s.offset:s.offset+n])
	s.offset += n
	s.reads++
	return n, nil
}

func streamReaderFixture(t testing.TB, columns cqltype.Columns, source io.Reader, size int) (*codec.StreamRowReader, *wire.Stream) {
	t.Helper()
	scope, err := wire.NewStream(context.Background(), source, int64(size), wire.StreamLimits{MaxDepth: 130, DiscardBytes: 127})
	if err != nil {
		t.Fatal(err)
	}
	r, err := codec.NewStreamRowReader(columns, scope)
	if err != nil {
		t.Fatal(err)
	}
	return r, scope
}

// profileOf returns a pointer to a copy of p, so later changes to p do not
// reach options that already hold it.
func profileOf(p settings.Profile) *settings.Profile { return &p }

package session

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
)

// alternationRows is a ROWS result with full global-spec metadata of cols blob
// columns named name+i, and one row.
func alternationRows(name string, cols int) []byte {
	b := binary.BigEndian.AppendUint32(nil, 2)
	b = binary.BigEndian.AppendUint32(b, 1)
	b = binary.BigEndian.AppendUint32(b, uint32(cols))
	for _, s := range []string{"ks", "tbl"} {
		b = binary.BigEndian.AppendUint16(b, uint16(len(s)))
		b = append(b, s...)
	}
	for i := 0; i < cols; i++ {
		n := fmt.Sprintf("%s%d", name, i)
		b = binary.BigEndian.AppendUint16(b, uint16(len(n)))
		b = append(b, n...)
		b = binary.BigEndian.AppendUint16(b, uint16(cqltype.TypeBlob))
	}
	b = binary.BigEndian.AppendUint32(b, 1)
	for i := 0; i < cols; i++ {
		b = binary.BigEndian.AppendUint32(b, 1)
		b = append(b, 'x')
	}
	return b
}

type alternationFixture struct {
	s     *cluster.Core
	peer  net.Conn
	ctx   context.Context
	stop  context.CancelFunc
	ps    [2]*Prepared[int32, []byte]
	dst   []byte
	opts  CompiledOptions
	fatal func(args ...any)
}

func (f *alternationFixture) one(p *Prepared[int32, []byte]) {
	found, _, err := p.One(f.ctx, 1, &f.dst, f.opts)
	if err != nil || !found {
		f.fatal(found, err)
	}
}

func (f *alternationFixture) close() {
	f.stop()
	f.s.Close()
	f.peer.Close()
}

// newAlternationFixture prepares two SELECTs with different result metadata
// of cols columns on one connection. Every response carries full metadata, as
// on protocol v4 without a result metadata ID (Cassandra v4, or ScyllaDB
// without SCYLLA_USE_METADATA_ID).
func newAlternationFixture(tb testing.TB, cols int) *alternationFixture {
	s, _, peers := runtimeTestSession(tb, 1)
	peer := peers[0]
	key := cqltype.NewColumns(cqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "k", Type: cqltype.IntType})
	var prepared, frames [2][]byte
	for i, name := range []string{"a", "b"} {
		specs := make([]cqltype.ColumnSpec, cols)
		for c := range specs {
			specs[c] = cqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: fmt.Sprintf("%s%d", name, c), Type: cqltype.BlobType}
		}
		prepared[i] = preparedBody([]byte("id-"+name), key, cqltype.NewColumns(specs...), 0)
		frames[i] = alternationRows(name, cols)
	}
	go func() {
		out := make([]byte, 0, 4096)
		for {
			op, stream, body, err := readRequest(peer)
			if err != nil {
				return
			}
			var resp []byte
			if op == byte(frm.OpPrepare) {
				which := 0
				if body[len(body)-1] == 'b' { // CQL ends with the statement's tag
					which = 1
				}
				resp = prepared[which]
			} else {
				id := body[2 : 2+int(binary.BigEndian.Uint16(body))]
				resp = frames[0]
				if id[len(id)-1] == 'b' {
					resp = frames[1]
				}
			}
			out = append(out[:0], 0x84, 0, byte(stream>>8), byte(stream), byte(frm.OpResult))
			out = binary.BigEndian.AppendUint32(out, uint32(len(resp)))
			out = append(out, resp...)
			if _, err := peer.Write(out); err != nil {
				return
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	f := &alternationFixture{s: s, peer: peer, ctx: ctx, stop: cancel, dst: make([]byte, 0, 8), opts: testOptions(Options{}), fatal: tb.Fatal}
	for i, name := range []string{"a", "b"} {
		fields := make([]codec.OutputField[[]byte], cols)
		for c := range fields {
			fields[c] = codec.Column(fmt.Sprintf("%s%d", name, c), codec.ByteSliceCodec, func(dst *[]byte) *[]byte { return dst })
		}
		st := binding.Statement[int32, []byte]{Kind: cqltype.OrdinaryStatement, CQL: "SELECT * FROM ks.tbl WHERE k = ? -- " + name, Params: codec.OneParam(codec.Int32), Rows: codec.RowInto(func(*[]byte) {}, fields...)}
		p, err := Prepare(ctx, s, st, PrepareOptions{})
		if err != nil {
			f.close()
			tb.Fatal(err)
		}
		f.ps[i] = p
	}
	for range 20 {
		f.one(f.ps[0])
		f.one(f.ps[1])
	}
	return f
}

// TestPreparedAlternationAllocs checks that two statements alternating on one
// connection, each with full result metadata in every response, allocate no
// more per execution than one statement alone: the connection keeps both
// statements' metadata.
func TestPreparedAlternationAllocs(t *testing.T) {
	for _, cols := range []int{1, 4, 16} {
		t.Run(fmt.Sprintf("%dcols", cols), func(t *testing.T) {
			f := newAlternationFixture(t, cols)
			defer f.close()
			single := testing.AllocsPerRun(2000, func() { f.one(f.ps[0]) })
			i := 0
			alt := testing.AllocsPerRun(2000, func() { f.one(f.ps[i&1]); i++ })
			t.Logf("Prepared.One allocs/op: one statement=%.2f two alternating=%.2f", single, alt)
			if alt != single {
				t.Fatalf("two alternating statements allocate %.2f/op, one statement %.2f/op", alt, single)
			}
		})
	}
}

func BenchmarkPreparedAlternation(b *testing.B) {
	for _, stmts := range []int{1, 2} {
		b.Run(fmt.Sprintf("%dstmt", stmts), func(b *testing.B) {
			for _, cols := range []int{1, 4, 16} {
				b.Run(fmt.Sprintf("%dcols", cols), func(b *testing.B) {
					f := newAlternationFixture(b, cols)
					defer f.close()
					mask := stmts - 1
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; b.Loop(); i++ {
						f.one(f.ps[i&mask])
					}
				})
			}
		})
	}
}

// typedRowsBody is a ROWS result with one column "value" of type typ, a
// paging state when token is set, and one row per value.
func typedRowsBody(token []byte, typ cqltype.Type, values ...[]byte) []byte {
	b := binary.BigEndian.AppendUint32(nil, 2)
	flags := uint32(frm.FlagGlobalTableSpec)
	if token != nil {
		flags |= uint32(frm.FlagHasMorePages)
	}
	b = binary.BigEndian.AppendUint32(b, flags)
	b = binary.BigEndian.AppendUint32(b, 1)
	if token != nil {
		b = binary.BigEndian.AppendUint32(b, uint32(len(token)))
		b = append(b, token...)
	}
	for _, name := range []string{"ks", "tbl", "value"} {
		b = binary.BigEndian.AppendUint16(b, uint16(len(name)))
		b = append(b, name...)
	}
	b = binary.BigEndian.AppendUint16(b, uint16(typ))
	b = binary.BigEndian.AppendUint32(b, uint32(len(values)))
	for _, value := range values {
		b = binary.BigEndian.AppendUint32(b, uint32(len(value)))
		b = append(b, value...)
	}
	return b
}

// recordingDecoder records the columns of every plan it compiles and reads
// the single column whatever its type.
type recordingDecoder struct{ compiled *[]cqltype.Columns }

func (d recordingDecoder) Compile(c cqltype.Columns) (codec.RowPlan[codec.Value], error) {
	*d.compiled = append(*d.compiled, c)
	return codec.OneColumn(codec.DynamicCodec).Compile(c)
}
func (d recordingDecoder) Clone(v codec.Value) codec.Value { return v.Clone() }

// TestMetadataReuseDetectsChangedColumns pages one prepared statement whose
// result column changes type between pages. The connection's metadata reuse
// must not hand the second page the first page's (or the prepared) columns:
// the new columns are parsed and the second page decodes with a plan compiled
// for them.
func TestMetadataReuseDetectsChangedColumns(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	defer s.Close()
	peer := peers[0]
	defer peer.Close()
	key := cqltype.NewColumns(cqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "k", Type: cqltype.IntType})
	blob := cqltype.NewColumns(cqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: cqltype.BlobType})
	intValue := binary.BigEndian.AppendUint32(nil, 7)
	responses := [][]byte{
		preparedBody([]byte("id"), key, blob, 0),
		// Warm the connection's reuse with the old shape, unpaged.
		typedRowsBody(nil, cqltype.TypeBlob, []byte("warm")),
		typedRowsBody([]byte("p"), cqltype.TypeBlob, []byte("one")),
		typedRowsBody(nil, cqltype.TypeInt, intValue),
	}
	serverDone := make(chan error, 1)
	go func() {
		for _, resp := range responses {
			_, stream, _, err := readRequest(peer)
			if err != nil {
				serverDone <- err
				return
			}
			if _, err := peer.Write(typedTestResponse(t, 4, stream, resp)); err != nil {
				serverDone <- err
				return
			}
		}
		serverDone <- nil
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var compiled []cqltype.Columns
	st := binding.Statement[int32, codec.Value]{Kind: cqltype.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl WHERE k = ?", Params: codec.OneParam(codec.Int32), Rows: recordingDecoder{compiled: &compiled}}
	p, err := Prepare(ctx, s, st, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	read := func() []cqltype.Type {
		t.Helper()
		bound := codecTestMust(BindBorrowed(p, 1))
		defer bound.Close()
		rows := codecTestMust(Run(ctx, s, bound, testOptions(Options{})))
		defer rows.Close()
		var dst codec.Value
		var values []cqltype.Type
		for rows.NextInto(&dst) {
			values = append(values, dst.Type().Kind())
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return values
	}
	if got := read(); len(got) != 1 || got[0] != cqltype.TypeBlob {
		t.Fatal(got)
	}
	compiled = compiled[:0]
	if got := read(); len(got) != 2 || got[0] != cqltype.TypeBlob || got[1] != cqltype.TypeInt {
		t.Fatalf("value types %v", got)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	if len(compiled) == 0 {
		t.Fatal("no plan compiled for the changed page")
	}
	last := compiled[len(compiled)-1]
	if spec, _ := last.At(0); spec.Type.Kind() != cqltype.TypeInt {
		t.Fatalf("second page decoded with a plan for %v, want int", spec.Type)
	}
}

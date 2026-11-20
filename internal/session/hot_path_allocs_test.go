//go:build !race

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/binding"
	frm "github.com/scylladb/gocql/v2/internal/frame"
)

// hotPathPageRows is how many rows each page of the scan and stream fixtures
// carries.
const hotPathPageRows = 32

// hotPathToken is the paging state of the scan fixture's first page. A
// request that carries it gets the last page.
var hotPathToken = []byte("hot-path-next-page")

// hotPathRow decodes the blob column into the caller's slice, reusing its
// capacity, so a warm read allocates nothing for the value itself.
var hotPathRow = cql.RowInto(func(*[]byte) {}, cql.Column("value", cql.ByteSliceCodec, func(dst *[]byte) *[]byte { return dst }))

// hotPathSession returns a session on one fake connection serving prepared
// statements by the tag their CQL ends with: "E" answers EXECUTE with a void
// result, "O" with one row, "S" with two pages of hotPathPageRows rows and
// "T" with one page of hotPathPageRows rows. Every row is one blob column.
func hotPathSession(t *testing.T) *Session {
	t.Helper()
	s, _, peers := runtimeTestSession(t, 1)
	peer := peers[0]
	t.Cleanup(func() {
		s.Close()
		peer.Close()
	})
	key := cql.NewColumns(cql.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "k", Type: cql.IntType})
	value := cql.NewColumns(cql.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: cql.BlobType})
	rows := make([]string, hotPathPageRows)
	for i := range rows {
		rows[i] = fmt.Sprintf("row-%02d", i)
	}
	prepared := map[byte][]byte{
		'E': preparedBody([]byte("id-E"), key, cql.NewColumns(), 0),
		'O': preparedBody([]byte("id-O"), key, value, 0),
		'S': preparedBody([]byte("id-S"), key, value, 0),
		'T': preparedBody([]byte("id-T"), key, value, 0),
	}
	executed := map[byte][]byte{
		'E': binary.BigEndian.AppendUint32(nil, frm.ResultKindVoid),
		'O': runtimeRowsBody(nil, "x"),
		'S': runtimeRowsBody(hotPathToken, rows...),
		'T': runtimeRowsBody(nil, rows...),
	}
	lastPage := runtimeRowsBody(nil, rows...)
	// The server allocates nothing per request: AllocsPerRun counts every
	// goroutine's allocations, the server's too.
	go func() {
		var header [9]byte
		in := make([]byte, 1<<16)
		out := make([]byte, 0, 4096)
		for {
			if _, err := io.ReadFull(peer, header[:]); err != nil {
				return
			}
			body := in[:binary.BigEndian.Uint32(header[5:])]
			if _, err := io.ReadFull(peer, body); err != nil {
				return
			}
			op, stream := header[4], binary.BigEndian.Uint16(header[2:4])
			var resp []byte
			if op == byte(frm.OpPrepare) {
				resp = prepared[body[len(body)-1]] // the CQL ends with the tag
			} else {
				id := body[2 : 2+int(binary.BigEndian.Uint16(body))]
				tag := id[len(id)-1]
				resp = executed[tag]
				if tag == 'S' && bytes.Contains(body, hotPathToken) {
					resp = lastPage
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
	return InternalWrapSession(s)
}

func hotPathContext(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func hotPathPrepare[R any](t *testing.T, ctx context.Context, session *Session, tag string, rows cql.RowDecoder[R]) *Prepared[int32, R] {
	t.Helper()
	p, err := Prepare(ctx, session.Core(), binding.Statement[int32, R]{Kind: cql.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl WHERE k = ? -- " + tag, Params: cql.OneParam(cql.Int32Codec), Rows: rows}, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// TestHotPathAllocs gates what a warm execution allocates on each hot path of
// a prepared statement against a fake node: exec, select one, a paged scan and
// streaming, each through Prepared and through a reused Query. Batches have
// their own gates (TestBatchAllocs, TestRunBatchAllocs). The limits are
// ceilings: lowering one is progress, raising one needs a reason. It runs
// without the race detector, whose instrumentation allocates on its own.
func TestHotPathAllocs(t *testing.T) {
	t.Run("Exec", func(t *testing.T) {
		session, ctx := hotPathSession(t), hotPathContext(t)
		p := hotPathPrepare(t, ctx, session, "E", cql.EmptyRowDecoder)
		q := hotPathQuery(t, p, Options{})
		options := testOptions(Options{})
		gateAllocs(t, "Prepared.Exec", 0, func() {
			if _, err := p.Exec(ctx, 1, options); err != nil {
				t.Fatal(err)
			}
		})
		gateAllocs(t, "Query.Exec", 0, func() {
			if _, err := q.Exec(ctx, 1); err != nil {
				t.Fatal(err)
			}
		})
	})

	t.Run("One", func(t *testing.T) {
		session, ctx := hotPathSession(t), hotPathContext(t)
		p := hotPathPrepare(t, ctx, session, "O", hotPathRow)
		q := hotPathQuery(t, p, Options{})
		options := testOptions(Options{})
		dst := make([]byte, 0, 16)
		gateAllocs(t, "Prepared.One", 0, func() {
			if found, _, err := p.One(ctx, 1, &dst, options); err != nil || !found {
				t.Fatal(found, err)
			}
		})
		gateAllocs(t, "Query.One", 0, func() {
			if found, _, err := q.One(ctx, 1, &dst); err != nil || !found {
				t.Fatal(found, err)
			}
		})
	})

	// The one allocation is the first page's paging state, copied out of
	// the response so that it outlives it and goes into the next request.
	t.Run("PagedScan", func(t *testing.T) {
		const pagingState = 1
		session, ctx := hotPathSession(t), hotPathContext(t)
		p := hotPathPrepare(t, ctx, session, "S", hotPathRow)
		values := Options{PageSize: cql.Option[int32]{Set: true, Value: hotPathPageRows}, Prefetch: cql.Option[int]{Set: true, Value: 0}}
		q := hotPathQuery(t, p, values)
		options := testOptions(values)
		dst := make([]byte, 0, 16)
		count := 0
		countRow := func(*[]byte) error { count++; return nil }
		check := func(err error) {
			if err != nil || count != 2*hotPathPageRows {
				t.Fatal(count, err)
			}
			count = 0
		}
		gateAllocs(t, "Prepared.Each over two pages", pagingState, func() {
			_, err := p.Each(ctx, 1, &dst, countRow, options)
			check(err)
		})
		gateAllocs(t, "Query.Each over two pages", pagingState, func() {
			_, err := q.Each(ctx, 1, &dst, countRow)
			check(err)
		})
	})

	// A stream pays per execution, not per row: its row reader, cursor and
	// context. An incremental stream also reserves the connection, which
	// interrupts the connection's reader with a deadline. The limits are what
	// this costs today; rows may not add to it.
	t.Run("Stream", func(t *testing.T) {
		session, ctx := hotPathSession(t), hotPathContext(t)
		for _, tc := range []struct {
			incremental bool
			limit       float64
		}{{false, 25}, {true, 55}} {
			t.Run(fmt.Sprintf("incremental=%t", tc.incremental), func(t *testing.T) {
				run := func(tag string, rows int) func() {
					p, err := PrepareStream(ctx, session.Core(), StreamStatement[int32, []byte]{Kind: cql.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl WHERE k = ? -- " + tag, Params: cql.OneParam(cql.Int32Codec), Rows: cql.StreamOneColumn(cql.StreamBlob)}, PrepareOptions{})
					if err != nil {
						t.Fatal(err)
					}
					q, err := p.Query(StreamOptions{Incremental: tc.incremental})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(q.Close)
					dst := make([]byte, 0, 16)
					return func() {
						rs, err := q.Run(ctx, 1)
						if err != nil {
							t.Fatal(err)
						}
						count := 0
						for rs.NextInto(&dst) {
							count++
						}
						if err := rs.Close(); err != nil || count != rows {
							t.Fatal(count, err)
						}
					}
				}
				one := gateAllocs(t, "StreamQuery.Run of one row", tc.limit, run("O", 1))
				page := gateAllocs(t, fmt.Sprintf("StreamQuery.Run of %d rows", hotPathPageRows), tc.limit, run("T", hotPathPageRows))
				if page > one {
					t.Errorf("a stream of %d rows allocates %v times, of one row %v", hotPathPageRows, page, one)
				}
			})
		}
	})
}

func hotPathQuery[R any](t *testing.T, p *Prepared[int32, R], options Options) *Query[int32, R] {
	t.Helper()
	q, err := p.Query(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(q.Close)
	return q
}

// gateAllocs runs f once to warm it, then fails t when f allocates more than
// limit times per run. It returns the count.
func gateAllocs(t *testing.T, name string, limit float64, f func()) float64 {
	t.Helper()
	f()
	n := testing.AllocsPerRun(1000, f)
	t.Logf("%s allocates %v times per run", name, n)
	if n > limit {
		t.Errorf("%s allocates %v times per run, want at most %v", name, n, limit)
	}
	return n
}

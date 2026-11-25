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
	"math"
	"runtime"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/binding"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/observe"
)

// hotPathPageRows is how many rows each page of the scan and stream fixtures
// carries.
const hotPathPageRows = 32

// hotPathTokens are the paging states of a paged fixture's pages: a request
// that carries hotPathTokens[i] gets page i+1.
var hotPathTokens = [][]byte{[]byte("hot-path-page-1"), []byte("hot-path-page-2")}

// hotPathRow decodes the blob column into the caller's slice, reusing its
// capacity, so a warm read allocates nothing for the value itself.
var hotPathRow = cql.RowInto(func(*[]byte) {}, cql.Column("value", cql.ByteSliceCodec, func(dst *[]byte) *[]byte { return dst }))

// hotPathSession returns a session on one fake connection serving prepared
// statements by the tag their CQL ends with: "E" answers EXECUTE with a void
// result, "O" with one row, "T" with one page of hotPathPageRows rows, "S"
// with two such pages and "R" with three. Every row is one blob column. A
// request the fixture does not serve closes the connection.
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
	// pages returns n pages of rows, each but the last with the token of
	// the next.
	pages := func(n int) [][]byte {
		out := make([][]byte, n)
		for i := range out {
			var token []byte
			if i+1 < n {
				token = hotPathTokens[i]
			}
			out[i] = runtimeRowsBody(token, rows...)
		}
		return out
	}
	prepared := map[byte][]byte{}
	executed := map[byte][][]byte{
		'E': {binary.BigEndian.AppendUint32(nil, frm.ResultKindVoid)},
		'O': {runtimeRowsBody(nil, "x")},
		'T': pages(1),
		'S': pages(2),
		'R': pages(3),
	}
	for tag := range executed {
		columns := value
		if tag == 'E' {
			columns = cql.NewColumns()
		}
		prepared[tag] = preparedBody([]byte{'i', 'd', '-', tag}, key, columns, 0)
	}
	// The server allocates nothing per request: gateAllocs counts every
	// goroutine's allocations, the server's too.
	go func() {
		defer peer.Close()
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
			switch {
			case op == byte(frm.OpPrepare) && len(body) > 0:
				resp = prepared[body[len(body)-1]] // the CQL ends with the tag
			case op == byte(frm.OpExecute) && len(body) >= 2 && len(body) >= 2+int(binary.BigEndian.Uint16(body)):
				if id := body[2 : 2+int(binary.BigEndian.Uint16(body))]; len(id) > 0 {
					served := executed[id[len(id)-1]]
					page := 0
					for i, token := range hotPathTokens {
						if bytes.Contains(body, token) {
							page = i + 1
						}
					}
					if page < len(served) {
						resp = served[page]
					}
				}
			}
			if resp == nil {
				return
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

// hotPathRequest is a request hook that allocates nothing of its own.
func hotPathRequest(ctx context.Context, _ observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
	return ctx, nil
}

// TestHotPathAllocs gates what a warm execution allocates on each hot path of
// a prepared statement against a fake node: exec, select one and a paged
// scan, each through Prepared and through a reused Query, and streaming
// through a reused StreamQuery. Batches have their own gates
// (TestBatchAllocs, TestRunBatchAllocs). The limits are ceilings: lowering
// one is progress, raising one needs a reason. It runs without the race
// detector, whose instrumentation allocates on its own.
func TestHotPathAllocs(t *testing.T) {
	t.Run("Exec", func(t *testing.T) {
		session, ctx := hotPathSession(t), hotPathContext(t)
		p := hotPathPrepare(t, ctx, session, "E", cql.Row[cql.EmptyRow]())
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
		// A view's defaults, and the hooks of an observer in compiled
		// options, are resolved when the options are compiled, not per
		// call. (Joining the observer to a session observer costs what
		// observe.Join costs per request.)
		compiled, err := NewCompiledOptions(session.Core(), Options{Consistency: cql.Some(cql.Quorum)})
		if err != nil {
			t.Fatal(err)
		}
		view := p.With(compiled)
		gateAllocs(t, "Prepared.With(defaults).Exec", 0, func() {
			if _, err := view.Exec(ctx, 1, CompiledOptions{}); err != nil {
				t.Fatal(err)
			}
		})
		observed, err := NewCompiledOptions(session.Core(), Options{Observer: observe.Hooks{Request: hotPathRequest}})
		if err != nil {
			t.Fatal(err)
		}
		gateAllocs(t, "Prepared.Exec with an Observer", 0, func() {
			if _, err := p.Exec(ctx, 1, observed); err != nil {
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

	// Each page after the first costs one allocation: the previous page's
	// paging state, copied out of its response so that it outlives it and
	// goes into the next request.
	t.Run("PagedScan", func(t *testing.T) {
		session, ctx := hotPathSession(t), hotPathContext(t)
		values := Options{PageSize: hotPathPageRows, Prefetch: cql.Option[int]{Set: true, Value: 0}}
		options := testOptions(values)
		dst := make([]byte, 0, 16)
		count := 0
		countRow := func(*[]byte) error { count++; return nil }
		for _, tc := range []struct {
			tag   string
			pages int
		}{{"S", 2}, {"R", 3}} {
			p := hotPathPrepare(t, ctx, session, tc.tag, hotPathRow)
			q := hotPathQuery(t, p, values)
			check := func(err error) {
				if err != nil || count != tc.pages*hotPathPageRows {
					t.Fatal(count, err)
				}
				count = 0
			}
			limit := float64(tc.pages - 1)
			gateAllocs(t, fmt.Sprintf("Prepared.Each over %d pages", tc.pages), limit, func() {
				_, err := p.Each(ctx, 1, &dst, countRow, options)
				check(err)
			})
			gateAllocs(t, fmt.Sprintf("Query.Each over %d pages", tc.pages), limit, func() {
				_, err := q.Each(ctx, 1, &dst, countRow)
				check(err)
			})
		}
	})

	// A stream, run through a reused StreamQuery of a PreparedStream, pays
	// per execution and per page, not per row: its row reader, cursor and
	// context. An incremental stream also reserves the connection, which
	// interrupts the connection's reader with a deadline. The limits are what
	// this costs today: limit for the first page, perPage for each next one.
	// More rows in a page may not add to it.
	t.Run("Stream", func(t *testing.T) {
		session, ctx := hotPathSession(t), hotPathContext(t)
		for _, tc := range []struct {
			incremental    bool
			limit, perPage float64
		}{{false, 25, 14}, {true, 55, 27}} {
			t.Run(fmt.Sprintf("incremental=%t", tc.incremental), func(t *testing.T) {
				run := func(tag string, rows int) func() {
					p, err := PrepareStream(ctx, session.Core(), StreamStatement[int32, []byte]{Kind: cql.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl WHERE k = ? -- " + tag, Params: cql.OneParam(cql.Int32Codec), Rows: cql.StreamOneColumn(cql.StreamBlob)}, PrepareOptions{})
					if err != nil {
						t.Fatal(err)
					}
					q, err := p.Query(StreamOptions{Options: Options{PageSize: hotPathPageRows}, Incremental: tc.incremental})
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
				if page > one+allocTolerance {
					t.Errorf("a stream of %d rows allocates %v times, of one row %v", hotPathPageRows, page, one)
				}
				gateAllocs(t, "StreamQuery.Run of two pages", tc.limit+tc.perPage, run("S", 2*hotPathPageRows))
				gateAllocs(t, "StreamQuery.Run of three pages", tc.limit+2*tc.perPage, run("R", 3*hotPathPageRows))
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

// allocTolerance is how far above its limit gateAllocs lets an average go:
// a stray allocation every five runs, from goroutines the gate does not
// measure.
const allocTolerance = 0.2

// gateAllocs runs f once to warm it, then fails t when f allocates more than
// limit times per run on average. It returns that average.
//
// The average is a fraction, not testing.AllocsPerRun's truncated quotient,
// so an allocation on some runs only still counts. The connection's own
// goroutines can allocate during a run, so the best of three samples is
// kept and allocTolerance is allowed.
func gateAllocs(t *testing.T, name string, limit float64, f func()) float64 {
	t.Helper()
	const runs, samples = 1000, 3
	f()
	n := math.Inf(1)
	for range samples {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for range runs {
			f()
		}
		runtime.ReadMemStats(&after)
		n = min(n, float64(after.Mallocs-before.Mallocs)/runs)
	}
	t.Logf("%s allocates %.3f times per run", name, n)
	if n > limit+allocTolerance {
		t.Errorf("%s allocates %.3f times per run, want at most %v", name, n, limit)
	}
	return n
}

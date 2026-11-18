// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"iter"
	"net"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/exec"
	frm "github.com/scylladb/gocql/v2/internal/frame"
)

// gridRequest is a QUERY or EXECUTE the grid server received.
type gridRequest struct {
	stream      int
	consistency cql.Consistency
	pageSize    int32 // -1 without a page size
	key         int32
	pageState   string
}

// gridServer is a fake node for the execution grid tests. It answers
// PREPARE with a statement of one int parameter and, for a SELECT, one blob
// column, records each QUERY and EXECUTE, and answers it with answer.
type gridServer struct {
	mu       sync.Mutex
	requests []gridRequest
}

func newGridServer(t *testing.T, peer net.Conn, answer func(gridRequest) []byte) *gridServer {
	srv := &gridServer{}
	key := cql.NewColumns(cql.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "k", Type: cql.IntType})
	value := cql.NewColumns(cql.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: cql.BlobType})
	go func() {
		for {
			var h [9]byte
			if _, err := io.ReadFull(peer, h[:]); err != nil {
				return
			}
			body := make([]byte, binary.BigEndian.Uint32(h[5:]))
			if _, err := io.ReadFull(peer, body); err != nil {
				return
			}
			stream := int(binary.BigEndian.Uint16(h[2:4]))
			var response []byte
			switch frm.Op(h[4]) {
			case frm.OpPrepare:
				text := string(body[4 : 4+binary.BigEndian.Uint32(body)])
				columns := cql.NewColumns()
				if strings.HasPrefix(text, "SELECT") {
					columns = value
				}
				response = typedTestResponse(t, 4, stream, gridPreparedBody(key, columns))
			case frm.OpQuery, frm.OpExecute:
				r := parseGridRequest(frm.Op(h[4]), stream, body)
				srv.mu.Lock()
				srv.requests = append(srv.requests, r)
				srv.mu.Unlock()
				response = answer(r)
			default:
				t.Errorf("unexpected opcode %#x", h[4])
				return
			}
			if _, err := peer.Write(response); err != nil {
				return
			}
		}
	}()
	return srv
}

func (srv *gridServer) sent() []gridRequest {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return slices.Clone(srv.requests)
}

// gridPreparedBody is a v4 PREPARED result of parameters and columns.
func gridPreparedBody(parameters, columns cql.Columns) []byte {
	b := binary.BigEndian.AppendUint32(nil, 4)
	b = binary.BigEndian.AppendUint16(b, 4)
	b = append(b, "grid"...)
	for part, c := range []cql.Columns{parameters, columns} {
		b = binary.BigEndian.AppendUint32(b, 0)
		b = binary.BigEndian.AppendUint32(b, uint32(c.Len()))
		if part == 0 {
			b = binary.BigEndian.AppendUint32(b, 0)
		}
		for i := range c.Len() {
			col, _ := c.At(i)
			for _, text := range []string{col.Keyspace, col.Table, col.Name} {
				b = binary.BigEndian.AppendUint16(b, uint16(len(text)))
				b = append(b, text...)
			}
			b = binary.BigEndian.AppendUint16(b, uint16(col.Type.Kind()))
		}
	}
	return b
}

// parseGridRequest decodes a v4 QUERY or EXECUTE body of at most one int value.
func parseGridRequest(op frm.Op, stream int, b []byte) gridRequest {
	r := gridRequest{stream: stream, pageSize: -1}
	if op == frm.OpQuery {
		b = b[4+int(binary.BigEndian.Uint32(b)):] // query text
	} else {
		b = b[2+int(binary.BigEndian.Uint16(b)):] // statement id
	}
	r.consistency = cql.Consistency(binary.BigEndian.Uint16(b))
	flags := b[2]
	b = b[3:]
	if flags&0x01 != 0 {
		count := int(binary.BigEndian.Uint16(b))
		b = b[2:]
		for range count {
			n := int(int32(binary.BigEndian.Uint32(b)))
			if n == 4 {
				r.key = int32(binary.BigEndian.Uint32(b[4:]))
			}
			b = b[4+max(n, 0):]
		}
	}
	if flags&0x04 != 0 {
		r.pageSize = int32(binary.BigEndian.Uint32(b))
		b = b[4:]
	}
	if flags&0x08 != 0 {
		n := int(int32(binary.BigEndian.Uint32(b)))
		r.pageState = string(b[4 : 4+max(n, 0)])
	}
	return r
}

// gridError is a v4 ERROR response with code.
func gridError(t *testing.T, stream int, code cqlerr.ErrorCode) []byte {
	body := binary.BigEndian.AppendUint32(nil, uint32(code))
	body = binary.BigEndian.AppendUint16(body, 4)
	body = append(body, "grid"...)
	response := typedTestResponse(t, 4, stream, body)
	response[4] = byte(frm.OpError)
	return response
}

// gridMode runs the cells of one row of the execution grid. A nil func is a
// cell the row leaves empty by design.
type gridMode struct {
	name    string
	exec    func(context.Context, *Session, cql.Statement[int32, cql.EmptyRow], exec.Options) (cql.Result, error)
	one     func(context.Context, *Session, cql.Statement[int32, []byte], *[]byte, exec.Options) (bool, cql.Result, error)
	each    func(context.Context, *Session, cql.Statement[int32, []byte], *[]byte, func(*[]byte) error, exec.Options) (cql.Result, error)
	all     func(context.Context, *Session, cql.Statement[int32, []byte], *[]byte, exec.Options) iter.Seq2[*[]byte, error]
	run     func(context.Context, *Session, cql.Statement[int32, []byte], exec.Options) (Rows[[]byte], func(), error)
	runPage func(context.Context, *Session, cql.Statement[int32, []byte], cql.PageState, exec.Options) (*Page[[]byte], func(), error)
}

const gridKey int32 = 7

// gridQuery prepares statement and makes a worker-owned query with options.
func gridQuery[R any](ctx context.Context, s *Session, statement cql.Statement[int32, R], options exec.Options) (*Query[int32, R], error) {
	p, err := s.Prepare(ctx, statement)
	if err != nil {
		return nil, err
	}
	return p.Query(options)
}

// gridShared prepares statement and compiles options for it.
func gridShared[R any](ctx context.Context, s *Session, statement cql.Statement[int32, R], options exec.Options) (*Prepared[int32, R], exec.CompiledOptions, error) {
	p, err := s.Prepare(ctx, statement)
	if err != nil {
		return nil, exec.CompiledOptions{}, err
	}
	o, err := s.CompileOptions(options)
	return p, o, err
}

func gridSeqError[R any](err error) iter.Seq2[*R, error] {
	return func(yield func(*R, error) bool) { yield(nil, err) }
}

var gridModes = []gridMode{
	{
		name: "Throwaway",
		exec: func(ctx context.Context, s *Session, st cql.Statement[int32, cql.EmptyRow], o exec.Options) (cql.Result, error) {
			return s.UnpreparedExec(ctx, st, gridKey, o)
		},
		one: func(ctx context.Context, s *Session, st cql.Statement[int32, []byte], dst *[]byte, o exec.Options) (bool, cql.Result, error) {
			return s.UnpreparedOne(ctx, st, gridKey, dst, o)
		},
		each: func(ctx context.Context, s *Session, st cql.Statement[int32, []byte], dst *[]byte, fn func(*[]byte) error, o exec.Options) (cql.Result, error) {
			return s.UnpreparedEach(ctx, st, gridKey, dst, fn, o)
		},
		all: func(ctx context.Context, s *Session, st cql.Statement[int32, []byte], dst *[]byte, o exec.Options) iter.Seq2[*[]byte, error] {
			return s.UnpreparedAll(ctx, st, gridKey, dst, o)
		},
	},
	{
		name: "Worker-owned",
		exec: func(ctx context.Context, s *Session, st cql.Statement[int32, cql.EmptyRow], o exec.Options) (cql.Result, error) {
			q, err := gridQuery(ctx, s, st, o)
			if err != nil {
				return cql.Result{}, err
			}
			defer q.Close()
			return q.Exec(ctx, gridKey)
		},
		one: func(ctx context.Context, s *Session, st cql.Statement[int32, []byte], dst *[]byte, o exec.Options) (bool, cql.Result, error) {
			q, err := gridQuery(ctx, s, st, o)
			if err != nil {
				return false, cql.Result{}, err
			}
			defer q.Close()
			return q.One(ctx, gridKey, dst)
		},
		each: func(ctx context.Context, s *Session, st cql.Statement[int32, []byte], dst *[]byte, fn func(*[]byte) error, o exec.Options) (cql.Result, error) {
			q, err := gridQuery(ctx, s, st, o)
			if err != nil {
				return cql.Result{}, err
			}
			defer q.Close()
			return q.Each(ctx, gridKey, dst, fn)
		},
		all: func(ctx context.Context, s *Session, st cql.Statement[int32, []byte], dst *[]byte, o exec.Options) iter.Seq2[*[]byte, error] {
			q, err := gridQuery(ctx, s, st, o)
			if err != nil {
				return gridSeqError[[]byte](err)
			}
			return func(yield func(*[]byte, error) bool) {
				defer q.Close()
				q.All(ctx, gridKey, dst)(yield)
			}
		},
		run: func(ctx context.Context, s *Session, st cql.Statement[int32, []byte], o exec.Options) (Rows[[]byte], func(), error) {
			q, err := gridQuery(ctx, s, st, o)
			if err != nil {
				return Rows[[]byte]{}, func() {}, err
			}
			rows, err := q.Run(ctx, gridKey)
			return rows, q.Close, err
		},
		runPage: func(ctx context.Context, s *Session, st cql.Statement[int32, []byte], state cql.PageState, o exec.Options) (*Page[[]byte], func(), error) {
			q, err := gridQuery(ctx, s, st, o)
			if err != nil {
				return nil, func() {}, err
			}
			page, err := q.RunPage(ctx, gridKey, state, 0)
			return page, q.Close, err
		},
	},
	{
		name: "Shared",
		exec: func(ctx context.Context, s *Session, st cql.Statement[int32, cql.EmptyRow], o exec.Options) (cql.Result, error) {
			p, co, err := gridShared(ctx, s, st, o)
			if err != nil {
				return cql.Result{}, err
			}
			return p.Exec(ctx, gridKey, co)
		},
		one: func(ctx context.Context, s *Session, st cql.Statement[int32, []byte], dst *[]byte, o exec.Options) (bool, cql.Result, error) {
			p, co, err := gridShared(ctx, s, st, o)
			if err != nil {
				return false, cql.Result{}, err
			}
			return p.One(ctx, gridKey, dst, co)
		},
		each: func(ctx context.Context, s *Session, st cql.Statement[int32, []byte], dst *[]byte, fn func(*[]byte) error, o exec.Options) (cql.Result, error) {
			p, co, err := gridShared(ctx, s, st, o)
			if err != nil {
				return cql.Result{}, err
			}
			return p.Each(ctx, gridKey, dst, fn, co)
		},
		all: func(ctx context.Context, s *Session, st cql.Statement[int32, []byte], dst *[]byte, o exec.Options) iter.Seq2[*[]byte, error] {
			p, co, err := gridShared(ctx, s, st, o)
			if err != nil {
				return gridSeqError[[]byte](err)
			}
			return p.All(ctx, gridKey, dst, co)
		},
		run: func(ctx context.Context, s *Session, st cql.Statement[int32, []byte], o exec.Options) (Rows[[]byte], func(), error) {
			p, co, err := gridShared(ctx, s, st, o)
			if err != nil {
				return Rows[[]byte]{}, func() {}, err
			}
			rows, err := p.Run(ctx, gridKey, co)
			return rows, func() {}, err
		},
		runPage: func(ctx context.Context, s *Session, st cql.Statement[int32, []byte], state cql.PageState, o exec.Options) (*Page[[]byte], func(), error) {
			p, co, err := gridShared(ctx, s, st, o)
			if err != nil {
				return nil, func() {}, err
			}
			page, err := p.RunPage(ctx, gridKey, state, 0, co)
			return page, func() {}, err
		},
	},
}

func gridSelect() cql.Statement[int32, []byte] {
	return cql.Statement[int32, []byte]{
		CQL:         "SELECT value FROM ks.tbl WHERE k = ?",
		Params:      cql.OneParam(cql.Int32Codec),
		Rows:        cql.RowInto(func(*[]byte) {}, cql.Column("value", cql.ByteSliceCodec, func(dst *[]byte) *[]byte { return dst })),
		BindMarkers: cql.NewColumns(cql.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "k", Type: cql.IntType}),
		Kind:        cql.OrdinaryStatement,
	}
}

func gridUpdate(kind cql.StatementKind) cql.Statement[int32, cql.EmptyRow] {
	return cql.Statement[int32, cql.EmptyRow]{
		CQL:         "UPDATE ks.tbl SET value = 0x00 WHERE k = ?",
		Params:      cql.OneParam(cql.Int32Codec),
		Rows:        cql.EmptyRowDecoder,
		BindMarkers: cql.NewColumns(cql.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "k", Type: cql.IntType}),
		Kind:        kind,
	}
}

// gridOptions are the options every grid call runs with, so the requests
// show whether each row applies them the same way.
var gridOptions = exec.Options{Consistency: cql.Some(cql.Three), PageSize: cql.Some[int32](2)}

// gridPages answers the first page with a and b and a page state, and the
// page resumed from that state with c.
func gridPages(t *testing.T) func(gridRequest) []byte {
	return func(r gridRequest) []byte {
		if r.pageState == "" {
			return typedTestResponse(t, 4, r.stream, runtimeRowsBody([]byte("p1"), "a", "b"))
		}
		return typedTestResponse(t, 4, r.stream, runtimeRowsBody(nil, "c"))
	}
}

// gridOutcome is what a grid call observably did: its rows, its error's
// classification and the requests it sent.
type gridOutcome struct {
	rows     string
	found    bool
	resultN  int64
	errKind  cqlerr.Kind
	errCode  cqlerr.ErrorCode
	status   cqlerr.ExecutionStatus
	errMatch string
	sent     []gridRequest
}

func gridClassify(o *gridOutcome, err error, sentinels ...error) {
	if err == nil {
		return
	}
	o.errKind = cqlerr.KindOf(err)
	o.status = cqlerr.ExecutionStatusOf(err)
	var coded interface{ Code() cqlerr.ErrorCode }
	if errors.As(err, &coded) {
		o.errCode = coded.Code()
	}
	var validation *cql.ValidationError
	if errors.As(err, &validation) {
		o.errMatch = "validation"
	}
	for _, s := range sentinels {
		if errors.Is(err, s) {
			o.errMatch = s.Error()
		}
	}
	if o.errMatch == "" {
		o.errMatch = "other"
	}
}

var errGridCallback = errors.New("grid callback")

// gridCase is one scenario of a grid column. call runs it on one row and
// reports its outcome; it returns false when the row has no such cell.
type gridCase struct {
	name   string
	answer func(*testing.T) func(gridRequest) []byte
	call   func(context.Context, *Session, gridMode) (gridOutcome, bool)
}

func gridRowsOf(seq iter.Seq2[*[]byte, error], stopAfter int) (string, error) {
	var got []string
	for row, err := range seq {
		if err != nil {
			return strings.Join(got, ","), err
		}
		got = append(got, string(*row))
		if len(got) == stopAfter {
			break
		}
	}
	return strings.Join(got, ","), nil
}

func gridEach(ctx context.Context, s *Session, m gridMode, fn func(row []byte, n int) error, nilFn bool) (gridOutcome, bool) {
	var o gridOutcome
	var got []string
	var dst []byte
	cb := func(row *[]byte) error {
		got = append(got, string(*row))
		return fn(*row, len(got))
	}
	if nilFn {
		cb = nil
	}
	res, err := m.each(ctx, s, gridSelect(), &dst, cb, gridOptions)
	o.rows, o.resultN = strings.Join(got, ","), res.Rows
	gridClassify(&o, err, StopIteration, errGridCallback)
	return o, true
}

var gridCases = []gridCase{
	{
		name: "Exec",
		answer: func(t *testing.T) func(gridRequest) []byte {
			return func(r gridRequest) []byte {
				return typedTestResponse(t, 4, r.stream, binary.BigEndian.AppendUint32(nil, 1))
			}
		},
		call: func(ctx context.Context, s *Session, m gridMode) (gridOutcome, bool) {
			var o gridOutcome
			_, err := m.exec(ctx, s, gridUpdate(cql.OrdinaryStatement), gridOptions)
			gridClassify(&o, err)
			return o, true
		},
	},
	{
		name:   "ExecConditional",
		answer: func(t *testing.T) func(gridRequest) []byte { return gridPages(t) },
		call: func(ctx context.Context, s *Session, m gridMode) (gridOutcome, bool) {
			var o gridOutcome
			_, err := m.exec(ctx, s, gridUpdate(cql.ConditionalStatement), gridOptions)
			gridClassify(&o, err, cqlerr.ErrConditionalExec)
			return o, true
		},
	},
	{
		name: "ExecServerError",
		answer: func(t *testing.T) func(gridRequest) []byte {
			return func(r gridRequest) []byte { return gridError(t, r.stream, cqlerr.CodeInvalid) }
		},
		call: func(ctx context.Context, s *Session, m gridMode) (gridOutcome, bool) {
			var o gridOutcome
			_, err := m.exec(ctx, s, gridUpdate(cql.OrdinaryStatement), gridOptions)
			gridClassify(&o, err)
			return o, true
		},
	},
	{
		name:   "One",
		answer: gridPages,
		call: func(ctx context.Context, s *Session, m gridMode) (gridOutcome, bool) {
			var o gridOutcome
			var dst []byte
			found, _, err := m.one(ctx, s, gridSelect(), &dst, gridOptions)
			o.found, o.rows = found, string(dst)
			gridClassify(&o, err)
			return o, true
		},
	},
	{
		name: "OneNoRow",
		answer: func(t *testing.T) func(gridRequest) []byte {
			return func(r gridRequest) []byte { return typedTestResponse(t, 4, r.stream, runtimeRowsBody(nil)) }
		},
		call: func(ctx context.Context, s *Session, m gridMode) (gridOutcome, bool) {
			var o gridOutcome
			dst := []byte("stale")
			found, _, err := m.one(ctx, s, gridSelect(), &dst, gridOptions)
			o.found, o.rows = found, string(dst)
			gridClassify(&o, err)
			return o, true
		},
	},
	{
		name: "OneServerError",
		answer: func(t *testing.T) func(gridRequest) []byte {
			return func(r gridRequest) []byte { return gridError(t, r.stream, cqlerr.CodeInvalid) }
		},
		call: func(ctx context.Context, s *Session, m gridMode) (gridOutcome, bool) {
			var o gridOutcome
			var dst []byte
			found, _, err := m.one(ctx, s, gridSelect(), &dst, gridOptions)
			o.found = found
			gridClassify(&o, err)
			return o, true
		},
	},
	{
		name:   "Each",
		answer: gridPages,
		call: func(ctx context.Context, s *Session, m gridMode) (gridOutcome, bool) {
			return gridEach(ctx, s, m, func([]byte, int) error { return nil }, false)
		},
	},
	{
		name:   "EachStop",
		answer: gridPages,
		call: func(ctx context.Context, s *Session, m gridMode) (gridOutcome, bool) {
			return gridEach(ctx, s, m, func(_ []byte, n int) error {
				if n == 1 {
					return StopIteration
				}
				return nil
			}, false)
		},
	},
	{
		name:   "EachCallbackError",
		answer: gridPages,
		call: func(ctx context.Context, s *Session, m gridMode) (gridOutcome, bool) {
			return gridEach(ctx, s, m, func(_ []byte, n int) error {
				if n == 3 {
					return errGridCallback
				}
				return nil
			}, false)
		},
	},
	{
		name:   "EachNilCallback",
		answer: gridPages,
		call: func(ctx context.Context, s *Session, m gridMode) (gridOutcome, bool) {
			return gridEach(ctx, s, m, nil, true)
		},
	},
	{
		name: "EachServerError",
		answer: func(t *testing.T) func(gridRequest) []byte {
			return func(r gridRequest) []byte { return gridError(t, r.stream, cqlerr.CodeInvalid) }
		},
		call: func(ctx context.Context, s *Session, m gridMode) (gridOutcome, bool) {
			return gridEach(ctx, s, m, func([]byte, int) error { return nil }, false)
		},
	},
	{
		name:   "All",
		answer: gridPages,
		call: func(ctx context.Context, s *Session, m gridMode) (gridOutcome, bool) {
			var o gridOutcome
			var dst []byte
			rows, err := gridRowsOf(m.all(ctx, s, gridSelect(), &dst, gridOptions), 0)
			o.rows = rows
			gridClassify(&o, err)
			return o, true
		},
	},
	{
		name:   "AllBreak",
		answer: gridPages,
		call: func(ctx context.Context, s *Session, m gridMode) (gridOutcome, bool) {
			var o gridOutcome
			var dst []byte
			rows, err := gridRowsOf(m.all(ctx, s, gridSelect(), &dst, gridOptions), 1)
			o.rows = rows
			gridClassify(&o, err)
			return o, true
		},
	},
	{
		name: "AllServerError",
		answer: func(t *testing.T) func(gridRequest) []byte {
			return func(r gridRequest) []byte { return gridError(t, r.stream, cqlerr.CodeInvalid) }
		},
		call: func(ctx context.Context, s *Session, m gridMode) (gridOutcome, bool) {
			var o gridOutcome
			var dst []byte
			rows, err := gridRowsOf(m.all(ctx, s, gridSelect(), &dst, gridOptions), 0)
			o.rows = rows
			gridClassify(&o, err)
			return o, true
		},
	},
	{
		name:   "Run",
		answer: gridPages,
		call: func(ctx context.Context, s *Session, m gridMode) (gridOutcome, bool) {
			if m.run == nil {
				return gridOutcome{}, false
			}
			var o gridOutcome
			rows, done, err := m.run(ctx, s, gridSelect(), gridOptions)
			defer done()
			if err == nil {
				var got []string
				var dst []byte
				for rows.NextInto(&dst) {
					got = append(got, string(dst))
				}
				err = errors.Join(rows.Err(), rows.Close())
				o.rows, o.resultN = strings.Join(got, ","), rows.Info().Rows
			}
			gridClassify(&o, err)
			return o, true
		},
	},
	{
		name:   "RunPage",
		answer: gridPages,
		call: func(ctx context.Context, s *Session, m gridMode) (gridOutcome, bool) {
			if m.runPage == nil {
				return gridOutcome{}, false
			}
			var o gridOutcome
			var got []string
			state := cql.StartPage
			for range 3 {
				page, done, err := m.runPage(ctx, s, gridSelect(), state, gridOptions)
				if err != nil {
					done()
					gridClassify(&o, err)
					break
				}
				var dst []byte
				var rows []string
				for page.NextInto(&dst) {
					rows = append(rows, string(dst))
				}
				err = errors.Join(page.Err(), page.Close())
				done()
				got = append(got, strings.Join(rows, ","))
				if err != nil {
					gridClassify(&o, err)
					break
				}
				if state = page.State(); !state.More() {
					break
				}
			}
			o.rows = strings.Join(got, "|")
			return o, true
		},
	},
}

// TestExecutionGridBehavior runs the same statement through each row of the
// execution grid that has the cell and checks that the rows agree: the same
// rows, the same error classification, and the same requests on the wire,
// options applied included.
func TestExecutionGridBehavior(t *testing.T) {
	for _, c := range gridCases {
		t.Run(c.name, func(t *testing.T) {
			var want *gridOutcome
			var wantMode string
			for _, m := range gridModes {
				s, _, peers := runtimeTestSession(t, 1)
				srv := newGridServer(t, peers[0], c.answer(t))
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				got, ok := c.call(ctx, s, m)
				cancel()
				_ = s.Close()
				_ = peers[0].Close()
				if !ok {
					continue
				}
				got.sent = srv.sent()
				for i := range got.sent {
					got.sent[i].stream = 0
				}
				if want == nil {
					want, wantMode = &got, m.name
					continue
				}
				if !gridOutcomeEqual(*want, got) {
					t.Errorf("%s: %+v\n%s: %+v", wantMode, *want, m.name, got)
				}
			}
		})
	}
}

func gridOutcomeEqual(a, b gridOutcome) bool {
	sent := slices.Equal(a.sent, b.sent)
	a.sent, b.sent = nil, nil
	return sent && reflect.DeepEqual(a, b)
}

// TestExecutionGridTooManyOptions checks that every cell taking optional
// options rejects a second value before sending anything.
func TestExecutionGridTooManyOptions(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	defer func() { _ = s.Close(); _ = peers[0].Close() }()
	srv := newGridServer(t, peers[0], gridPages(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sel, upd := gridSelect(), gridUpdate(cql.OrdinaryStatement)
	p, err := s.Prepare(ctx, sel)
	if err != nil {
		t.Fatal(err)
	}
	pu, err := s.Prepare(ctx, upd)
	if err != nil {
		t.Fatal(err)
	}
	o, c := exec.Options{}, exec.CompiledOptions{}
	var dst []byte
	nop := func(*[]byte) error { return nil }
	firstErr := func(seq iter.Seq2[*[]byte, error]) error {
		_, err := gridRowsOf(seq, 0)
		return err
	}
	for name, err := range map[string]error{
		"Session.UnpreparedExec": second(s.UnpreparedExec(ctx, upd, gridKey, o, o)),
		"Session.UnpreparedOne":  third(s.UnpreparedOne(ctx, sel, gridKey, &dst, o, o)),
		"Session.UnpreparedEach": second(s.UnpreparedEach(ctx, sel, gridKey, &dst, nop, o, o)),
		"Session.UnpreparedAll":  firstErr(s.UnpreparedAll(ctx, sel, gridKey, &dst, o, o)),
		"Prepared.Exec":          second(pu.Exec(ctx, gridKey, c, c)),
		"Prepared.One":           third(p.One(ctx, gridKey, &dst, c, c)),
		"Prepared.Each":          second(p.Each(ctx, gridKey, &dst, nop, c, c)),
		"Prepared.All":           firstErr(p.All(ctx, gridKey, &dst, c, c)),
		"Prepared.Run":           second(p.Run(ctx, gridKey, c, c)),
		"Prepared.RunPage":       second(p.RunPage(ctx, gridKey, cql.StartPage, 0, c, c)),
	} {
		var validation *cql.ValidationError
		if !errors.As(err, &validation) {
			t.Errorf("%s with two options: err = %v, want a cql.ValidationError", name, err)
		}
	}
	if sent := srv.sent(); len(sent) != 0 {
		t.Fatalf("calls with two options sent %d requests", len(sent))
	}
}

func second[A any](_ A, err error) error { return err }

func third[A, B any](_ A, _ B, err error) error { return err }

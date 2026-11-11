//go:build !old

package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	g "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
)

// The v2 query-execution cases. cases.tsv matches each to a master case of
// old_cases.go; README-CASES.md describes them.

type lwtArgs struct{ V, K, Cur int32 }

var lwtParams = cql.Params(cql.ParamAt(0, cql.Int32Codec, func(a lwtArgs) int32 { return a.V }), cql.ParamAt(1, cql.Int32Codec, func(a lwtArgs) int32 { return a.K }), cql.ParamAt(2, cql.Int32Codec, func(a lwtArgs) int32 { return a.Cur }))
var lwtValue = cql.RowInto(func(*int32) {}, cql.Column("v", cql.Int32Codec, func(v *int32) *int32 { return v }))
var lwtRows = cql.CASRows(lwtValue)

// v2Case is the per-worker state of a case: an owned output row and a row
// validator whose method value is bound once.
type v2Case struct {
	a     *v2Adapter
	check rowCheck
	row   func(*row) error
	out   row
}

type rowSource interface{ NextInto(*row) bool }

// drain validates rows from src until it ends or limit rows were read.
func (c *v2Case) drain(src rowSource, limit int32) error {
	for c.check.seen < limit && src.NextInto(&c.out) {
		if err := c.row(&c.out); err != nil {
			return err
		}
	}
	return nil
}

func (a *v2Adapter) lwtPrepared() (*g.Prepared[lwtArgs, cql.CASResult[int32]], *g.Prepared[int32, int32], error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	ctx := context.Background()
	if a.lwt == nil {
		p, err := a.s.Prepare(ctx, cql.Statement[lwtArgs, cql.CASResult[int32]]{Kind: cql.ConditionalStatement, CQL: lwtUpdate, Params: lwtParams, Rows: lwtRows}, g.PrepareOptions{})
		if err != nil {
			return nil, nil, err
		}
		a.lwt = p
	}
	if a.lwtRead == nil {
		p, err := a.s.Prepare(ctx, cql.Statement[int32, int32]{Kind: cql.OrdinaryStatement, CQL: lwtRead, Params: keyParams, Rows: lwtValue}, g.PrepareOptions{})
		if err != nil {
			return nil, nil, err
		}
		a.lwtRead = p
	}
	return a.lwt, a.lwtRead, nil
}

func (a *v2Adapter) options(v g.Options) (g.CompiledOptions, error) { return a.s.CompileOptions(v) }

func (a *v2Adapter) Case(name string, id, n int) (op, error) {
	if len(name) == 0 {
		return nil, errors.New("empty case")
	}
	paged := name[0] == 'B' || name[0] == 'C' || name == "E1"
	read, write, err := a.prepared(paged)
	if err != nil {
		return nil, err
	}
	size := 1024
	if paged {
		size = 4096
	}
	c := &v2Case{a: a, out: row{P: make([]byte, 0, size)}}
	c.row = c.check.row
	intType := cql.IntType
	blobType := cql.BlobType
	keyColumns := cql.NewColumns(cql.ColumnSpec{Name: "k", Type: intType})
	rowColumns := cql.NewColumns(cql.ColumnSpec{Name: "k", Type: intType}, cql.ColumnSpec{Name: "r", Type: intType}, cql.ColumnSpec{Name: "p", Type: blobType})

	switch name {
	case "A1", "B5", "B7", "A3", "B1":
		opts := g.Options{}
		switch name {
		case "B5":
			opts = g.Options{Prefetch: cql.Some(1)}
		case "B7":
			opts = g.Options{PageSize: cql.Some[int32](5000)}
		}
		q, err := read.Query(opts)
		if err != nil {
			return nil, err
		}
		if name == "A1" {
			return c.one(q), nil
		}
		want := int32(1)
		if paged {
			want = rowsPerPartition
		}
		return func(ctx context.Context, seq uint64) error {
			k := c.key(paged, seq)
			c.check.reset(paged, k, 0)
			_, err := q.Each(ctx, k, &c.out, c.row)
			return c.check.done(want, err)
		}, nil
	case "A2":
		return c.preparedOne(read), nil
	case "A4", "B2", "B8":
		q, err := read.Query(g.Options{})
		if err != nil {
			return nil, err
		}
		want, limit := int32(1), allRows
		if paged {
			want = rowsPerPartition
		}
		if name == "B8" {
			want, limit = 16, 16
		}
		return func(ctx context.Context, seq uint64) error {
			k := c.key(paged, seq)
			c.check.reset(paged, k, 0)
			rows, err := q.Run(ctx, k)
			if err != nil {
				return err
			}
			err = c.drain(rows, limit)
			if name == "B8" {
				// Abandon the rest of the partition; the next Run waits for the
				// cancelled execution to drain.
				return c.check.done(want, errors.Join(err, rows.Close()))
			}
			return c.check.done(want, errors.Join(err, rows.Err(), rows.Close()))
		}, nil
	case "A5", "B3", "B4", "B9":
		readStream, err := a.preparedStream(paged)
		if err != nil {
			return nil, err
		}
		sq, err := readStream.Query(g.StreamOptions{Incremental: name == "B4" || name == "B9"})
		if err != nil {
			return nil, err
		}
		want, limit := int32(1), allRows
		if paged {
			want = rowsPerPartition
		}
		if name == "B9" {
			want, limit = 16, 16
		}
		return func(ctx context.Context, seq uint64) error {
			k := c.key(paged, seq)
			c.check.reset(paged, k, 0)
			rs, err := sq.Run(ctx, k)
			if err != nil {
				return err
			}
			err = c.drain(rs, limit)
			return c.check.done(want, errors.Join(err, rs.Err(), rs.Close()))
		}, nil
	case "A6", "B6":
		q, err := read.Query(g.Options{PageSize: cql.Some(cql.Unpaged)})
		if err != nil {
			return nil, err
		}
		want := int32(1)
		if paged {
			want = rowsPerPartition
		}
		return func(ctx context.Context, seq uint64) error {
			k := c.key(paged, seq)
			c.check.reset(paged, k, 0)
			page, err := q.RunPage(ctx, k, g.StartPage, 0)
			if err != nil {
				return err
			}
			err = c.drain(page, allRows)
			return c.check.done(want, errors.Join(err, page.Err(), page.Close()))
		}, nil
	case "A7":
		statement := cql.Statement[int32, row]{Kind: cql.OrdinaryStatement, CQL: pointRead, Params: keyParams, Rows: bufferedDecoder, BindMarkers: keyColumns}
		return func(ctx context.Context, seq uint64) error {
			k := pointKey(seq)
			c.check.reset(false, k, 0)
			found, _, err := a.s.UnpreparedOne(ctx, statement, k, &c.out)
			return c.found(found, err)
		}, nil
	case "C1":
		q, err := read.Query(g.Options{})
		if err != nil {
			return nil, err
		}
		cursor := &pageCursor{part: uint64(id)}
		cursor.k = partKey(cursor.part)
		return func(ctx context.Context, _ uint64) error {
			c.check.reset(true, cursor.k, cursor.next)
			want := cursor.expect()
			state := g.StartPage
			if len(cursor.state) > 0 {
				state = g.PageStateFromToken(cursor.state)
			}
			page, err := q.RunPage(ctx, cursor.k, state, 0)
			if err != nil {
				return err
			}
			err = c.drain(page, allRows)
			next := page.State().Token()
			if err = errors.Join(err, page.Err(), page.Close()); err != nil {
				return err
			}
			if err = c.check.done(want, nil); err != nil {
				return err
			}
			return cursor.advance(c.check.seen, next)
		}, nil
	case "D1":
		q, err := write.Query(a.metadata)
		if err != nil {
			return nil, err
		}
		return execOp(q), nil
	case "D2":
		return preparedExec(write, a.boundMetadata), nil
	case "D3":
		return func(ctx context.Context, seq uint64) error {
			k := pointKey(seq)
			opts, err := a.options(g.Options{IncludeMetadata: true, Idempotent: true, Timestamp: cql.Option[int64]{Set: true, Value: time.Now().UnixMicro()}})
			if err != nil {
				return err
			}
			_, err = write.Exec(ctx, row{K: k, P: payload(false, k, 0)}, opts)
			return err
		}, nil
	case "D4":
		statement := cql.Statement[row, cql.EmptyRow]{Kind: cql.OrdinaryStatement, CQL: pointWrite, Params: rowParams, Rows: cql.EmptyRowDecoder, BindMarkers: rowColumns}
		return func(ctx context.Context, seq uint64) error {
			k := pointKey(seq)
			_, err := a.s.UnpreparedExec(ctx, statement, row{K: k, P: payload(false, k, 0)}, a.metadata)
			return err
		}, nil
	case "E1", "E2":
		q, err := write.Query(a.metadata)
		if err != nil {
			return nil, err
		}
		var batch g.Batch
		return func(ctx context.Context, seq uint64) error {
			batch.Reset()
			kind := cql.UnloggedBatch
			if name == "E1" {
				k, base := partKey(seq), int32(seq%16)*8
				for r := base; r < base+8; r++ {
					if err := q.AddToBatch(&batch, row{K: k, R: r, P: payload(true, k, r)}); err != nil {
						return err
					}
				}
			} else {
				kind = cql.LoggedBatch
				for i := uint64(0); i < 4; i++ {
					k := pointKey(seq*4 + i)
					if err := q.AddToBatch(&batch, row{K: k, P: payload(false, k, 0)}); err != nil {
						return err
					}
				}
			}
			_, err := a.s.ExecBatch(ctx, kind, &batch)
			return err
		}, nil
	case "F1":
		return c.lwt(id, n)
	case "G1", "G2":
		var readOp, writeOp op
		if name == "G1" {
			rq, err := read.Query(g.Options{})
			if err != nil {
				return nil, err
			}
			wq, err := write.Query(a.metadata)
			if err != nil {
				return nil, err
			}
			readOp, writeOp = c.one(rq), execOp(wq)
		} else {
			readOp, writeOp = c.preparedOne(read), preparedExec(write, a.boundMetadata)
		}
		return func(ctx context.Context, seq uint64) error {
			if seq%2 == 0 {
				return readOp(ctx, seq)
			}
			return writeOp(ctx, seq)
		}, nil
	}
	return nil, fmt.Errorf("unknown v2 case %q", name)
}

func (c *v2Case) key(paged bool, seq uint64) int32 {
	if paged {
		return partKey(seq)
	}
	return pointKey(seq)
}

// found validates the single row a One-style read decoded.
func (c *v2Case) found(found bool, err error) error {
	if err == nil {
		if !found {
			return fmt.Errorf("key=%d not found", c.check.k)
		}
		err = c.row(&c.out)
	}
	return c.check.done(1, err)
}

func (c *v2Case) one(q *g.Query[int32, row]) op {
	return func(ctx context.Context, seq uint64) error {
		k := pointKey(seq)
		c.check.reset(false, k, 0)
		found, _, err := q.One(ctx, k, &c.out)
		return c.found(found, err)
	}
}

func (c *v2Case) preparedOne(p *g.Prepared[int32, row]) op {
	return func(ctx context.Context, seq uint64) error {
		k := pointKey(seq)
		c.check.reset(false, k, 0)
		found, _, err := p.One(ctx, k, &c.out)
		return c.found(found, err)
	}
}

func execOp(q *g.Query[row, cql.EmptyRow]) op {
	return func(ctx context.Context, seq uint64) error {
		k := pointKey(seq)
		_, err := q.Exec(ctx, row{K: k, P: payload(false, k, 0)})
		return err
	}
}

func preparedExec(p *g.Prepared[row, cql.EmptyRow], opts g.CompiledOptions) op {
	return func(ctx context.Context, seq uint64) error {
		k := pointKey(seq)
		_, err := p.Exec(ctx, row{K: k, P: payload(false, k, 0)}, opts)
		return err
	}
}

// lwt updates the worker's own keys in turn with a compare-and-set that
// must apply: the worker read each key's value when it was built.
func (c *v2Case) lwt(id, n int) (op, error) {
	if n > lwtKeys/lwtKeysPerWorker {
		return nil, fmt.Errorf("lwt case supports at most %d workers", lwtKeys/lwtKeysPerWorker)
	}
	update, read, err := c.a.lwtPrepared()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	var keys, values [lwtKeysPerWorker]int32
	for j := range keys {
		keys[j] = int32(id + n*j)
		found, _, err := read.One(ctx, keys[j], &values[j])
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("lwt key %d missing; run -mode prepare -scenario lwt", keys[j])
		}
	}
	q, err := update.Query(g.Options{SerialConsistency: cql.Some(cql.LocalSerial)})
	if err != nil {
		return nil, err
	}
	var result cql.CASResult[int32]
	next := 0
	return func(ctx context.Context, _ uint64) error {
		j := next % lwtKeysPerWorker
		next++
		cur := values[j]
		found, _, err := q.One(ctx, lwtArgs{V: cur + 1, K: keys[j], Cur: cur}, &result)
		if err != nil {
			return err
		}
		if !found || !result.Applied {
			return fmt.Errorf("lwt key=%d v=%d not applied (previous %+v)", keys[j], cur, result.Previous)
		}
		values[j] = cur + 1
		return nil
	}, nil
}

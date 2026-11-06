//go:build old

package main

import (
	"context"
	"fmt"
	"time"

	g "github.com/gocql/gocql"
)

// The master query-execution cases. cases.tsv matches v2 cases of
// v2_cases.go to them; README-CASES.md describes them.

// oldCase is the per-worker state of a case: reused argument and scan
// slices, an owned blob and a row validator whose method value is bound once.
type oldCase struct {
	s     *g.Session
	check rowCheck
	row   func(*row) error
	out   row
	data  blob
	scan  []any
	args  []any
	state []byte
}

// drain validates up to limit scanned rows and closes it.
func (c *oldCase) drain(it *g.Iter, limit int32) error {
	for c.check.seen < limit && it.Scan(c.scan...) {
		c.out.P = []byte(c.data)
		if err := c.row(&c.out); err != nil {
			it.Close()
			return err
		}
	}
	return it.Close()
}

func (c *oldCase) key(paged bool, seq uint64) int32 {
	if paged {
		return partKey(seq)
	}
	return pointKey(seq)
}

func (a *oldAdapter) Case(name string, id, n int) (op, error) {
	if len(name) < 2 {
		return nil, fmt.Errorf("unknown master case %q", name)
	}
	paged := name[1] == 'B' || name[1] == 'C' || name == "ME1"
	size := 1024
	rd, wr := pointRead, pointWrite
	if paged {
		size = 4096
		rd, wr = pageRead, pageWrite
	}
	c := &oldCase{s: a.s, data: make(blob, 0, size), args: make([]any, 3)}
	c.scan = []any{&c.out.K, &c.out.R, &c.data}
	c.row = c.check.row
	want := int32(1)
	if paged {
		want = rowsPerPartition
	}

	switch name {
	case "MA1":
		q := a.s.Query(rd).Prefetch(0)
		return func(ctx context.Context, seq uint64) error {
			k := pointKey(seq)
			c.check.reset(false, k, 0)
			c.args[0] = k
			err := q.Bind(c.args[:1]...).WithContext(ctx).Scan(c.scan...)
			return c.scanned(err)
		}, nil
	case "MA2":
		return c.idiomaticRead(), nil
	case "MA3", "MA4", "MB1", "MB2", "MB3", "MB4", "MB5":
		q := a.s.Query(rd).Prefetch(0)
		limit := allRows
		switch name {
		case "MA4", "MB3":
			q.PageSize(0)
		case "MB2":
			q.Prefetch(0.25)
		case "MB4":
			q.PageSize(5000)
		case "MB5":
			want, limit = 16, 16
		}
		return func(ctx context.Context, seq uint64) error {
			k := c.key(paged, seq)
			c.check.reset(paged, k, 0)
			c.args[0] = k
			return c.check.done(want, c.drain(q.Bind(c.args[:1]...).WithContext(ctx).Iter(), limit))
		}, nil
	case "MC1":
		q := a.s.Query(rd).Prefetch(0).PageSize(pageRows)
		cursor := &pageCursor{part: uint64(id)}
		cursor.k = partKey(cursor.part)
		return func(ctx context.Context, _ uint64) error {
			c.check.reset(true, cursor.k, cursor.next)
			want := cursor.expect()
			c.args[0] = cursor.k
			it := q.Bind(c.args[:1]...).PageState(cursor.state).WithContext(ctx).Iter()
			c.state = append(c.state[:0], it.PageState()...)
			if err := c.check.done(want, c.drain(it, allRows)); err != nil {
				return err
			}
			return cursor.advance(c.check.seen, c.state)
		}, nil
	case "MD1", "MD3":
		q := a.s.Query(wr)
		if name == "MD3" {
			q.Idempotent(true)
		}
		return func(ctx context.Context, seq uint64) error {
			k := pointKey(seq)
			c.args[0], c.args[1], c.args[2] = k, int32(0), payload(false, k, 0)
			q.Bind(c.args...)
			if name == "MD3" {
				q.WithTimestamp(time.Now().UnixMicro())
			}
			return q.WithContext(ctx).Exec()
		}, nil
	case "MD2":
		return idiomaticWrite(a.s), nil
	case "ME1", "ME2":
		return func(ctx context.Context, seq uint64) error {
			if name == "ME1" {
				b := a.s.BatchWithContext(ctx, g.UnloggedBatch)
				k, base := partKey(seq), int32(seq%16)*8
				for r := base; r < base+8; r++ {
					b.Query(pageWrite, k, r, payload(true, k, r))
				}
				return a.s.ExecuteBatch(b)
			}
			b := a.s.BatchWithContext(ctx, g.LoggedBatch)
			for i := uint64(0); i < 4; i++ {
				k := pointKey(seq*4 + i)
				b.Query(pointWrite, k, int32(0), payload(false, k, 0))
			}
			return a.s.ExecuteBatch(b)
		}, nil
	case "MF1":
		return c.lwt(id, n)
	case "MG1":
		rq, wq := a.s.Query(pointRead).Prefetch(0), a.s.Query(pointWrite)
		return func(ctx context.Context, seq uint64) error {
			k := pointKey(seq)
			if seq%2 == 0 {
				c.check.reset(false, k, 0)
				c.args[0] = k
				return c.scanned(rq.Bind(c.args[:1]...).WithContext(ctx).Scan(c.scan...))
			}
			c.args[0], c.args[1], c.args[2] = k, int32(0), payload(false, k, 0)
			return wq.Bind(c.args...).WithContext(ctx).Exec()
		}, nil
	case "MG2":
		read, write := c.idiomaticRead(), idiomaticWrite(a.s)
		return func(ctx context.Context, seq uint64) error {
			if seq%2 == 0 {
				return read(ctx, seq)
			}
			return write(ctx, seq)
		}, nil
	}
	return nil, fmt.Errorf("unknown master case %q", name)
}

// scanned validates the row Query.Scan decoded.
func (c *oldCase) scanned(err error) error {
	if err == nil {
		c.out.P = []byte(c.data)
		err = c.row(&c.out)
	}
	return c.check.done(1, err)
}

// idiomaticRead builds a new query for every read, as most applications do.
func (c *oldCase) idiomaticRead() op {
	return func(ctx context.Context, seq uint64) error {
		k := pointKey(seq)
		c.check.reset(false, k, 0)
		return c.scanned(c.s.Query(pointRead, k).WithContext(ctx).Scan(c.scan...))
	}
}

func idiomaticWrite(s *g.Session) op {
	return func(ctx context.Context, seq uint64) error {
		k := pointKey(seq)
		return s.Query(pointWrite, k, int32(0), payload(false, k, 0)).WithContext(ctx).Exec()
	}
}

// lwt updates the worker's own keys in turn with a compare-and-set that
// must apply: the worker read each key's value when it was built.
func (c *oldCase) lwt(id, n int) (op, error) {
	if n > lwtKeys/lwtKeysPerWorker {
		return nil, fmt.Errorf("lwt case supports at most %d workers", lwtKeys/lwtKeysPerWorker)
	}
	var keys, values [lwtKeysPerWorker]int32
	for j := range keys {
		keys[j] = int32(id + n*j)
		if err := c.s.Query(lwtRead, keys[j]).Scan(&values[j]); err != nil {
			return nil, fmt.Errorf("lwt key %d: %w; run -mode prepare -scenario lwt", keys[j], err)
		}
	}
	q := c.s.Query(lwtUpdate).SerialConsistency(g.LocalSerial)
	var previous int32
	next := 0
	return func(ctx context.Context, _ uint64) error {
		j := next % lwtKeysPerWorker
		next++
		cur := values[j]
		c.args[0], c.args[1], c.args[2] = cur+1, keys[j], cur
		applied, err := q.Bind(c.args...).WithContext(ctx).ScanCAS(&previous)
		if err != nil {
			return err
		}
		if !applied {
			return fmt.Errorf("lwt key=%d v=%d not applied (previous %d)", keys[j], cur, previous)
		}
		values[j] = cur + 1
		return nil
	}, nil
}

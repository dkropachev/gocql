//go:build !old

package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	g "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/retry"
)

const driverName = "v2"

type borrowedRow struct {
	K, R int32
	P    g.Bytes
}
type v2Adapter struct {
	s        *g.Session
	mu       sync.Mutex
	reads    [2]*g.Prepared[int32, row]
	writes   [2]*g.Prepared[row, g.Unit]
	lwt      *g.Prepared[lwtArgs, g.CASResult[int32]]
	lwtRead  *g.Prepared[int32, int32]
	metadata g.Options // includes result metadata, as master's writes request
}
type v2Worker struct {
	a           *v2Adapter
	read        *g.Query[int32, row]
	write       *g.Query[row, g.Unit]
	streamQuery *g.StreamQuery[int32, row]
	stream      bool
	out         row
}

var streamDecoder = g.StreamRow(func(*row) {}, g.StreamColumn("k", g.StreamInt32, func(r *row) *int32 { return &r.K }), g.StreamColumn("r", g.StreamInt32, func(r *row) *int32 { return &r.R }), g.StreamColumn("p", g.StreamBlob, func(r *row) *[]byte { return &r.P }))
var bufferedDecoder = g.RowInto(func(*row) {}, g.Column("k", g.Int32, func(r *row) *int32 { return &r.K }), g.Column("r", g.Int32, func(r *row) *int32 { return &r.R }), g.Column("p", g.OwnedBlobCodec, func(r *row) *[]byte { return &r.P }))

func connect(hosts []string) (adapter, error) {
	c := g.DefaultConfig(hosts...)
	c.Protocol = 4
	c.Metadata.PeriodicRefresh = *metadataRefresh
	c.Pool.ConnectionsPerShard = 1
	c.Pool.MaxExcessPerShard = 0
	p := *c.DefaultProfile
	p.Consistency = g.LocalQuorum
	p.SerialConsistency = g.Option[g.Consistency]{}
	p.Timeout = 10 * time.Second
	p.PageTimeout = 10 * time.Second
	p.PageSize = 32
	p.Prefetch = 0
	p.Retry = retry.Safe{MaxAttempts: 1}
	p.Speculation = nil
	p.Routing, _ = g.NewRoutingPolicy(g.RoutingOptions{TokenAware: true, Datacenter: "datacenter1"})
	c.DefaultProfile = &p
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, e := g.Connect(ctx, c)
	if e != nil {
		return nil, e
	}
	// Populate table/partitioner metadata before creating immutable prepared handles.
	if e = s.RefreshMetadata(ctx); e != nil {
		s.Close()
		return nil, e
	}
	metadata, e := g.NewOptions(s, g.OptionValues{IncludeMetadata: true})
	if e != nil {
		s.Close()
		return nil, e
	}
	return &v2Adapter{s: s, metadata: metadata}, nil
}
func (a *v2Adapter) Close() error { return a.s.Close() }
func (a *v2Adapter) Exec(ctx context.Context, q string) error {
	b, e := g.NewUnpreparedQuery(a.s, g.Statement[g.Unit, g.Unit]{Kind: g.OrdinaryStatement, CQL: q, Params: g.NoParams, Rows: g.NoRows}, g.NewColumns(), g.Options{})
	if e != nil {
		return e
	}
	defer b.Close()
	e = b.Exec(ctx, g.Unit{})
	return e
}
func (a *v2Adapter) Evidence() any {
	routing := []g.RoutingInfo{}
	for _, prepared := range a.reads {
		if prepared != nil {
			b, e := g.NewQuery(prepared, g.Options{})
			if e == nil {
				info, err := b.Routing(int32(0))
				if err == nil {
					routing = append(routing, info)
				}
				b.Close()
			}
		}
	}
	keyspace, _ := a.s.Metadata().Keyspace("bench_compare")
	return map[string]any{"keyspace_strategy": keyspace.StrategyOptions, "keyspace_strategy_class": keyspace.StrategyClass, "metrics": a.s.Metrics(), "routing_samples": routing, "hosts": a.s.Metadata().Hosts(), "protocol": 4, "consistency": "LOCAL_QUORUM", "page_size": 32, "prefetch": 0, "retries": 0, "compression": "none", "tls": false, "coalesce_wait_ns": 0, "incremental_receive": *incremental, "metadata_refresh_ns": int64(*metadataRefresh)}
}

var keyParams = g.Params(g.Param("k", g.Int32, func(k int32) int32 { return k }))
var rowParams = g.Params(g.Param("k", g.Int32, func(r row) int32 { return r.K }), g.Param("r", g.Int32, func(r row) int32 { return r.R }), g.Param("p", g.BytesEncoder, func(r row) []byte { return r.P }))

// prepared prepares, once, the read and write statements of the point or
// paged table and checks that the read is token routed.
func (a *v2Adapter) prepared(paged bool) (*g.Prepared[int32, row], *g.Prepared[row, g.Unit], error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	idx := 0
	rd, wr := pointRead, pointWrite
	if paged {
		idx = 1
		rd, wr = pageRead, pageWrite
	}
	ctx := context.Background()
	if a.reads[idx] == nil {
		p, e := g.Prepare(ctx, a.s, g.Statement[int32, row]{Kind: g.OrdinaryStatement, CQL: rd, Params: keyParams, Rows: bufferedDecoder}, g.PrepareOptions{})
		if e != nil {
			return nil, nil, e
		}
		q, e := g.NewQuery(p, g.Options{})
		if e != nil {
			return nil, nil, e
		}
		routing, e := q.Routing(int32(0))
		q.Close()
		if e != nil {
			return nil, nil, e
		}
		if !routing.Token.Set {
			return nil, nil, fmt.Errorf("prepared read lacks token routing for %s", rd)
		}
		a.reads[idx] = p
	}
	if a.writes[idx] == nil {
		p, e := g.Prepare(ctx, a.s, g.Statement[row, g.Unit]{Kind: g.OrdinaryStatement, CQL: wr, Params: rowParams, Rows: g.NoRows}, g.PrepareOptions{})
		if e != nil {
			return nil, nil, e
		}
		a.writes[idx] = p
	}
	return a.reads[idx], a.writes[idx], nil
}

func (a *v2Adapter) Worker(paged, stream bool) (worker, error) {
	read, write, e := a.prepared(paged)
	if e != nil {
		return nil, e
	}
	size := 1024
	if paged {
		size = 4096
	}
	readQuery, e := g.NewQuery(read, g.Options{})
	if e != nil {
		return nil, e
	}
	writeQuery, e := g.NewQuery(write, a.metadata)
	if e != nil {
		readQuery.Close()
		return nil, e
	}
	streamQuery, e := g.NewStreamQuery(read, streamDecoder, g.StreamOptions{Incremental: *incremental})
	if e != nil {
		readQuery.Close()
		writeQuery.Close()
		return nil, e
	}
	return &v2Worker{a: a, read: readQuery, write: writeQuery, streamQuery: streamQuery, stream: stream, out: row{P: make([]byte, 0, size)}}, nil
}

// Write executes the acknowledged upsert. A successful Exec returns with the
// query idle; after a failure the query may still drain an accepted write, so
// Wait joins it before the worker reuses the query.
func (w *v2Worker) Write(ctx context.Context, k, r int32, p []byte) error {
	if e := w.write.Exec(ctx, row{K: k, R: r, P: p}); e != nil {
		return errors.Join(e, w.write.Wait(ctx))
	}
	return nil
}
func (w *v2Worker) Read(ctx context.Context, k int32, consume consumer) (resultErr error) {
	if w.stream {
		rs, e := w.streamQuery.Run(ctx, k)
		if e != nil {
			return e
		}
		defer func() { resultErr = errors.Join(resultErr, rs.Close(), w.streamQuery.Wait(ctx)) }()
		for rs.NextInto(&w.out) {
			if e := consume(&w.out); e != nil {
				return e
			}
		}
		return rs.Err()
	}
	// Each decodes every row of every page into the worker's owned row and
	// returns with the query idle: no iterator to close. It keeps the exact
	// row count the consumer validates, which One (first row only) would not.
	if e := w.read.Each(ctx, k, &w.out, consume); e != nil {
		return errors.Join(e, w.read.Wait(ctx))
	}
	return nil
}

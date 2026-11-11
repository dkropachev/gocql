//go:build !old

package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	g "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/retry"
	"github.com/scylladb/gocql/v2/routing"
)

const driverName = "v2"

type borrowedRow struct {
	K, R int32
	P    cql.Bytes
}
type v2Adapter struct {
	s             *g.Session
	mu            sync.Mutex
	reads         [2]*g.Prepared[int32, row]
	writes        [2]*g.Prepared[row, cql.EmptyRow]
	lwt           *g.Prepared[lwtArgs, cql.CASResult[int32]]
	lwtRead       *g.Prepared[int32, int32]
	metadata      g.Options         // includes result metadata, as master's writes request
	boundMetadata g.CompiledOptions // metadata bound to the session, for one-shot executions
}
type v2Worker struct {
	a           *v2Adapter
	read        *g.Query[int32, row]
	write       *g.Query[row, cql.EmptyRow]
	streamQuery *g.StreamQuery[int32, row]
	stream      bool
	out         row
}

var streamDecoder = cql.StreamRow(func(*row) {}, cql.StreamColumn("k", cql.StreamInt32, func(r *row) *int32 { return &r.K }), cql.StreamColumn("r", cql.StreamInt32, func(r *row) *int32 { return &r.R }), cql.StreamColumn("p", cql.StreamBlob, func(r *row) *[]byte { return &r.P }))
var bufferedDecoder = cql.RowInto(func(*row) {}, cql.Column("k", cql.Int32Codec, func(r *row) *int32 { return &r.K }), cql.Column("r", cql.Int32Codec, func(r *row) *int32 { return &r.R }), cql.Column("p", cql.OwnedBlobCodec, func(r *row) *[]byte { return &r.P }))

func connect(hosts []string) (adapter, error) {
	policy, _ := routing.NewPolicy(routing.Options{TokenAware: true, Datacenter: "datacenter1"})
	builder := g.NewConfig(hosts...).
		WithProtocolVersion(g.Protocol4).
		WithConnectionsPerShard(1).
		WithMaxExcessPerShard(0).
		ConfigureDefaultProfile(func(p *g.ProfileBuilder) {
			p.WithConsistency(cql.LocalQuorum).
				NoSerialConsistency().
				WithTimeout(10 * time.Second).
				WithPageTimeout(10 * time.Second).
				WithPageSize(32).
				NoPrefetch().
				WithRetry(retry.Safe{MaxAttempts: 1}).
				WithRouting(policy) // no speculation: the default
		})
	if *metadataRefresh > 0 {
		builder.WithMetadataRefresh(*metadataRefresh)
	} else {
		builder.NoMetadataRefresh()
	}
	c, e := builder.Build()
	if e != nil {
		return nil, e
	}
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
	metadata := g.Options{IncludeMetadata: true}
	boundMetadata, e := s.CompileOptions(metadata)
	if e != nil {
		s.Close()
		return nil, e
	}
	return &v2Adapter{s: s, metadata: metadata, boundMetadata: boundMetadata}, nil
}
func (a *v2Adapter) Close() error { return a.s.Close() }
func (a *v2Adapter) Exec(ctx context.Context, q string) error {
	b, e := a.s.UnpreparedQuery(cql.Statement[cql.EmptyParams, cql.EmptyRow]{Kind: cql.OrdinaryStatement, CQL: q, Params: cql.EmptyParamsEncoder, Rows: cql.EmptyRowDecoder}, g.Options{})
	if e != nil {
		return e
	}
	defer func() { _ = b.Close(ctx) }()
	_, e = b.Exec(ctx, cql.EmptyParams{})
	return e
}
func (a *v2Adapter) Evidence() any {
	routing := []routing.Info{}
	for _, prepared := range a.reads {
		if prepared != nil {
			b, e := prepared.Query(g.Options{})
			if e == nil {
				info, err := b.Routing(int32(0))
				if err == nil {
					routing = append(routing, info)
				}
				_ = b.Close(context.Background())
			}
		}
	}
	keyspace, _ := a.s.Metadata().Keyspace("bench_compare")
	return map[string]any{"keyspace_strategy": keyspace.StrategyOptions, "keyspace_strategy_class": keyspace.StrategyClass, "metrics": a.s.Metrics(), "routing_samples": routing, "hosts": a.s.Metadata().Hosts(), "protocol": 4, "consistency": "LOCAL_QUORUM", "page_size": 32, "prefetch": 0, "retries": 0, "compression": "none", "tls": false, "coalesce_wait_ns": 0, "incremental_receive": *incremental, "metadata_refresh_ns": int64(*metadataRefresh)}
}

var keyParams = cql.Params(cql.Param("k", cql.Int32Codec, func(k int32) int32 { return k }))
var rowParams = cql.Params(cql.Param("k", cql.Int32Codec, func(r row) int32 { return r.K }), cql.Param("r", cql.Int32Codec, func(r row) int32 { return r.R }), cql.Param("p", cql.BytesEncoder, func(r row) []byte { return r.P }))

// prepared prepares, once, the read and write statements of the point or
// paged table and checks that the read is token routed.
func (a *v2Adapter) prepared(paged bool) (*g.Prepared[int32, row], *g.Prepared[row, cql.EmptyRow], error) {
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
		p, e := a.s.Prepare(ctx, cql.Statement[int32, row]{Kind: cql.OrdinaryStatement, CQL: rd, Params: keyParams, Rows: bufferedDecoder}, g.PrepareOptions{})
		if e != nil {
			return nil, nil, e
		}
		q, e := p.Query(g.Options{})
		if e != nil {
			return nil, nil, e
		}
		routing, e := q.Routing(int32(0))
		_ = q.Close(ctx)
		if e != nil {
			return nil, nil, e
		}
		if !routing.Token.Set {
			return nil, nil, fmt.Errorf("prepared read lacks token routing for %s", rd)
		}
		a.reads[idx] = p
	}
	if a.writes[idx] == nil {
		p, e := a.s.Prepare(ctx, cql.Statement[row, cql.EmptyRow]{Kind: cql.OrdinaryStatement, CQL: wr, Params: rowParams, Rows: cql.EmptyRowDecoder}, g.PrepareOptions{})
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
	readQuery, e := read.Query(g.Options{})
	if e != nil {
		return nil, e
	}
	writeQuery, e := write.Query(a.metadata)
	if e != nil {
		_ = readQuery.Close(context.Background())
		return nil, e
	}
	streamQuery, e := read.StreamQuery(streamDecoder, g.StreamOptions{Incremental: *incremental})
	if e != nil {
		_ = readQuery.Close(context.Background())
		_ = writeQuery.Close(context.Background())
		return nil, e
	}
	return &v2Worker{a: a, read: readQuery, write: writeQuery, streamQuery: streamQuery, stream: stream, out: row{P: make([]byte, 0, size)}}, nil
}

// Write executes the acknowledged upsert. After a failure the query may still
// drain an accepted write; its next execution waits for that drain.
func (w *v2Worker) Write(ctx context.Context, k, r int32, p []byte) error {
	_, e := w.write.Exec(ctx, row{K: k, R: r, P: p})
	return e
}
func (w *v2Worker) Read(ctx context.Context, k int32, consume consumer) (resultErr error) {
	if w.stream {
		rs, e := w.streamQuery.Run(ctx, k)
		if e != nil {
			return e
		}
		defer func() { resultErr = errors.Join(resultErr, rs.Close()) }()
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
	_, e := w.read.Each(ctx, k, &w.out, consume)
	return e
}

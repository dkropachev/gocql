// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"encoding/binary"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/binding"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/metrics"
	"github.com/scylladb/gocql/v2/observe"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

// pageObserver records the attempts' starts and ends and the requests'
// events.
type pageObserver struct {
	mu       sync.Mutex
	starts   []observe.AttemptStart
	attempts []routing2.AttemptEvent
	requests []observe.RequestEvent
}

func (o *pageObserver) StartRequest(ctx context.Context, _ observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
	return ctx, func(e observe.RequestEvent) {
		o.mu.Lock()
		defer o.mu.Unlock()
		o.requests = append(o.requests, e)
	}
}

func (o *pageObserver) StartAttempt(_ context.Context, start observe.AttemptStart) func(routing2.AttemptEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.starts = append(o.starts, start)
	return func(e routing2.AttemptEvent) {
		o.mu.Lock()
		defer o.mu.Unlock()
		o.attempts = append(o.attempts, e)
	}
}

// TestPageAccounting checks that each page's attempts are numbered from 1
// within the page, the pages from 1 within the request, on the direct and
// the general path, and that the request and the session's metrics count
// the pages.
func TestPageAccounting(t *testing.T) {
	for _, general := range []bool{false, true} {
		name := "Direct"
		if general {
			name = "General"
		}
		t.Run(name, func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			defer s.Close()
			go func() {
				for _, body := range [][]byte{runtimeRowsBody([]byte("p2"), "a"), runtimeRowsBody([]byte("p3"), "b"), runtimeRowsBody(nil, "c")} {
					stream := typedTestReadRequest(t, conns[0], peers[0])
					if _, err := peers[0].Write(typedTestResponse(t, 4, stream, body)); err != nil {
						return
					}
				}
			}()
			profile := settings.InternalDefaultProfile()
			if general {
				profile = *generalPathProfile()
			}
			profile.Prefetch = 0
			observer := new(pageObserver)
			b := runtimeQuery(t)
			defer b.Close()
			before := s.Metrics()
			rows, err := Run(context.Background(), s, b, testOptions(Options{Profile: &profile, Observer: hooksOf(observer)}))
			if err != nil {
				t.Fatal(err)
			}
			var row cql.Bytes
			n := 0
			for rows.NextInto(&row) {
				n++
			}
			if err := rows.Close(); err != nil || n != 3 {
				t.Fatalf("%d rows, %v", n, err)
			}
			observer.mu.Lock()
			defer observer.mu.Unlock()
			if len(observer.starts) != 3 || len(observer.attempts) != 3 {
				t.Fatalf("%d attempt starts, %d ends, want 3", len(observer.starts), len(observer.attempts))
			}
			for i := range 3 {
				start, end := observer.starts[i], observer.attempts[i]
				if start.Page != i+1 || start.Attempt != 1 || end.Page != i+1 || end.Attempt != 1 {
					t.Errorf("page %d: start page %d attempt %d, end page %d attempt %d", i+1, start.Page, start.Attempt, end.Page, end.Attempt)
				}
			}
			if len(observer.requests) != 1 || observer.requests[0].Pages != 3 || observer.requests[0].Finished.IsZero() {
				t.Fatalf("request events %+v, want one of 3 pages", observer.requests)
			}
			after := s.Metrics()
			if after.Pages-before.Pages != 3 || after.Requests-before.Requests != 1 || after.Attempts-before.Attempts != 3 {
				t.Fatalf("metrics counted %d pages, %d requests, %d attempts; want 3, 1, 3", after.Pages-before.Pages, after.Requests-before.Requests, after.Attempts-before.Attempts)
			}
		})
	}
}

// TestPageLatency checks that the latency percentiles record each page from
// sending it to its response, without the time the caller spends between
// pages reading rows.
func TestPageLatency(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	defer s.Close()
	go func() {
		for _, body := range [][]byte{runtimeRowsBody([]byte("p2"), "a"), runtimeRowsBody(nil, "b")} {
			stream := typedTestReadRequest(t, conns[0], peers[0])
			if _, err := peers[0].Write(typedTestResponse(t, 4, stream, body)); err != nil {
				return
			}
		}
	}()
	profile := settings.InternalDefaultProfile()
	profile.Prefetch = 0
	b := runtimeQuery(t)
	defer b.Close()
	rows, err := Run(context.Background(), s, b, testOptions(Options{Profile: &profile}))
	if err != nil {
		t.Fatal(err)
	}
	const reading = 300 * time.Millisecond
	var row cql.Bytes
	for rows.NextInto(&row) {
		time.Sleep(reading)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	id := InternalRuntimeFor(s).requestID.Load()
	latency, ok := s.InternalLatencySample(id)
	if !ok || latency >= reading {
		t.Fatalf("recorded latency %v (%t), want a page's, under the %v spent reading rows", latency, ok, reading)
	}
}

// TestBoundValuesView checks that RequestStart.Values views the request's
// bound values while the Request hook runs, and that Format redacts.
func TestBoundValuesView(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	defer s.Close()
	void := binary.BigEndian.AppendUint32(nil, 1)
	go serveLoopback(peers[0], func([]byte) []byte { return void }, nil)
	columns := cql.NewColumns(cql.ColumnSpec{Name: "id", Type: cql.IntType}, cql.ColumnSpec{Name: "secret", Type: cql.TextType})
	type params struct {
		id     int32
		secret string
	}
	codec := cql.Params(
		cql.Param("id", cql.Int32Codec, func(p params) int32 { return p.id }),
		cql.Param("secret", cql.StringCodec, func(p params) string { return p.secret }),
	)
	b, err := BindUnpreparedBorrowed(binding.Statement[params, cql.EmptyRow]{Kind: cql.OrdinaryStatement, CQL: "INSERT INTO ks.tbl (id, secret) VALUES (?, ?)", Params: codec}, columns, params{id: 42, secret: "hunter2"})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	var plain, redacted strings.Builder
	var n int
	var start observe.RequestStart
	hooks := observe.Hooks{Request: func(ctx context.Context, s observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
		start = s
		n = s.Values.Len()
		_ = s.Values.Format(&plain, nil)
		_ = s.Values.Format(&redacted, func(c cql.ColumnSpec) bool { return c.Name == "secret" })
		return ctx, nil
	}}
	if _, err := Exec(context.Background(), s, b, testOptions(Options{Observer: hooks, PageSize: 10})); err != nil {
		t.Fatal(err)
	}
	if n != 2 || plain.String() != "id=42, secret='hunter2'" || redacted.String() != "id=42, secret=<redacted>" {
		t.Fatalf("%d values: %q, redacted %q", n, plain.String(), redacted.String())
	}
	if start.PageSize != 10 || start.ID == 0 || start.BatchStatements != nil {
		t.Fatalf("start %+v", start)
	}
}

// TestBatchBoundValues checks that a batch statement's values are read from
// the batch's framed storage, NULL and UNSET included.
func TestBatchBoundValues(t *testing.T) {
	frame := func(values ...[]byte) []byte {
		var out []byte
		for _, v := range values {
			switch {
			case v == nil:
				out = binary.BigEndian.AppendUint32(out, ^uint32(0)) // -1, NULL
			case len(v) == 1 && v[0] == 0xff:
				out = binary.BigEndian.AppendUint32(out, ^uint32(1)) // -2, UNSET
			default:
				out = binary.BigEndian.AppendUint32(out, uint32(len(v)))
				out = append(out, v...)
			}
		}
		return out
	}
	first := frame(binary.BigEndian.AppendUint32(nil, 7), []byte("x"))
	second := frame(nil, []byte{0xff}, []byte(""))
	state := &batchState{
		values: append(append([]byte(nil), first...), second...),
		entries: []batchEntry{
			{statement: "A", columns: cql.NewColumns(cql.ColumnSpec{Name: "a", Type: cql.IntType}, cql.ColumnSpec{Name: "b", Type: cql.TextType}), start: 0, end: len(first), count: 2},
			{statement: "B", columns: cql.NewColumns(cql.ColumnSpec{Type: cql.IntType}, cql.ColumnSpec{Type: cql.IntType}, cql.ColumnSpec{Type: cql.TextType}), start: len(first), end: len(first) + len(second), count: 3},
		},
	}
	statements := state.boundStatements()
	var got []string
	for _, statement := range statements {
		var b strings.Builder
		if err := statement.Values.Format(&b, nil); err != nil {
			t.Fatal(err)
		}
		got = append(got, statement.Statement+": "+b.String())
	}
	if want := []string{"A: a=7, b='x'", "B: 0=null, 1=unset, 2=''"}; strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestUntrackedAndHostMetrics checks that an execution counts in its host's
// metrics, and that an untracked one, as package cqltrace runs, counts in
// neither the session's metrics nor its host's.
func TestUntrackedAndHostMetrics(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	defer s.Close()
	var hostPool *pool.InternalHostConnPool
	for _, p := range implCluster.SessionData(s).InternalPool.InternalHostConnPools {
		hostPool = p
	}
	conns[0].InternalErrorHandler = hostPool
	void := binary.BigEndian.AppendUint32(nil, 1)
	go serveLoopback(peers[0], func([]byte) []byte { return void }, nil)
	b := runtimeQuery(t)
	defer b.Close()
	if _, err := Exec(context.Background(), s, b, CompiledOptions{}); err != nil {
		t.Fatal(err)
	}
	hosts := s.HostMetrics(nil)
	if len(hosts) == 1 && hosts[0].Connections != 1 {
		t.Errorf("host connections %d, want 1", hosts[0].Connections)
	}
	if len(hosts) != 1 || hosts[0].Attempts != 1 || hosts[0].Errors != 0 || hosts[0].HostID != conns[0].InternalHostID() {
		t.Fatalf("hosts %+v, want one host with one attempt", hosts)
	}
	before := s.Metrics()
	var untracked Options
	OptionsData(&untracked).InternalUntracked = true
	if _, err := Exec(context.Background(), s, b, testOptions(untracked)); err != nil {
		t.Fatal(err)
	}
	after := s.Metrics()
	if after.Requests != before.Requests || after.Attempts != before.Attempts || after.Pages != before.Pages || after.InFlight != 0 {
		t.Fatalf("untracked execution counted: before %+v, after %+v", before, after)
	}
	if hosts = s.HostMetrics(hosts[:0]); len(hosts) != 1 || hosts[0].Attempts != 1 {
		t.Fatalf("untracked execution counted on its host: %+v", hosts)
	}
	if got := (*implCluster.Core)(nil).HostMetrics([]metrics.Host{{}}); len(got) != 1 {
		t.Fatalf("a nil session appended %d hosts", len(got)-1)
	}
}

// TestCQLTraceSampler checks that a profile's sampler chooses per request
// whether it is traced, seeing its routing, and that a request whose options
// ask for a trace is traced without asking.
func TestCQLTraceSampler(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	defer s.Close()
	void := binary.BigEndian.AppendUint32(nil, 1)
	go serveLoopback(peers[0], func([]byte) []byte { return void }, nil)
	b := runtimeQuery(t)
	defer b.Close()
	var asked []routing2.Info
	sample := true
	profile := settings.InternalDefaultProfile()
	profile.CQLTraceSampler = func(info routing2.Info) bool {
		asked = append(asked, info)
		return sample
	}
	var traced []bool
	hooks := observe.Hooks{Request: func(ctx context.Context, start observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
		traced = append(traced, start.CQLTrace)
		return ctx, nil
	}}
	for _, options := range []Options{
		{Profile: &profile, Observer: hooks},
		{Profile: &profile, Observer: hooks},
		{Profile: &profile, Observer: hooks, CQLTrace: true},
	} {
		if _, err := Exec(context.Background(), s, b, testOptions(options)); err != nil {
			t.Fatal(err)
		}
		sample = false
	}
	if len(asked) != 2 || asked[0].Consistency != profile.Consistency || len(traced) != 3 || !traced[0] || traced[1] || !traced[2] {
		t.Fatalf("sampler asked %d times (%+v), traced %v", len(asked), asked, traced)
	}
}

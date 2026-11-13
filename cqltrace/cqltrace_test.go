// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltrace

import (
	"context"
	"errors"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
)

func TestNodes(t *testing.T) {
	a, b, c := netip.MustParseAddr("10.0.0.1"), netip.MustParseAddr("10.0.0.2"), netip.MustParseAddr("10.0.0.3")
	info := Info{Coordinator: a, Events: []Event{{Source: b}, {Source: a}, {}, {Source: c}, {Source: b}}}
	if got := info.Nodes(); !slices.Equal(got, []netip.Addr{a, b, c}) {
		t.Fatalf("Nodes() = %v", got)
	}
	if got := (Info{}).Nodes(); len(got) != 0 {
		t.Fatalf("empty Nodes() = %v", got)
	}
}

func TestString(t *testing.T) {
	event := cql.MinTimeUUID(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	info := Info{
		ID:          cql.UUID{15: 1},
		Coordinator: netip.MustParseAddr("10.0.0.1"),
		Duration:    1500 * time.Microsecond,
		Events:      []Event{{ID: event, Activity: "Parsing", Source: netip.MustParseAddr("10.0.0.2"), Elapsed: 42 * time.Microsecond}},
	}
	got := info.String()
	for _, want := range []string{"Tracing session: " + info.ID.String(), "coordinator 10.0.0.1, client -", "duration 1.5ms", " Parsing | 2026-01-02 03:04:05.000000 | 10.0.0.2 | 42\n"} {
		if !strings.Contains(got, want) {
			t.Fatalf("String() lacks %q:\n%s", want, got)
		}
	}
}

func TestSessionStatementColumns(t *testing.T) {
	cassandra, scylla := sessionStatement(false), sessionStatement(true)
	for _, column := range []string{"request_size", "response_size"} {
		if strings.Contains(cassandra.CQL, column) {
			t.Fatalf("Cassandra statement selects %s: %s", column, cassandra.CQL)
		}
		if !strings.Contains(scylla.CQL, column) {
			t.Fatalf("ScyllaDB statement lacks %s: %s", column, scylla.CQL)
		}
	}
	parameters, err := cql.MapType(cql.TextType, cql.TextType)
	if err != nil {
		t.Fatal(err)
	}
	types := map[string]cql.DataType{
		"coordinator": cql.InetType, "client": cql.InetType, "started_at": cql.TimestampType,
		"duration": cql.IntType, "command": cql.TextType, "request": cql.TextType, "parameters": parameters,
		"request_size": cql.IntType, "response_size": cql.IntType,
	}
	for _, statement := range []cql.Statement[cql.UUID, sessionRow]{cassandra, scylla} {
		var columns []cql.ColumnSpec
		for _, name := range strings.Split(strings.TrimSuffix(strings.TrimPrefix(statement.CQL, "SELECT "), " FROM system_traces.sessions WHERE session_id = ?"), ", ") {
			columns = append(columns, cql.ColumnSpec{Name: name, Type: types[name]})
		}
		// Every selected column has a field and every field a selected column.
		if _, err := statement.Rows.Compile(cql.NewColumns(columns...)); err != nil {
			t.Fatalf("%s: %v", statement.CQL, err)
		}
	}
}

func TestFill(t *testing.T) {
	row := sessionRow{
		Coordinator: cql.Nullable[netip.Addr]{Value: netip.MustParseAddr("10.0.0.1"), Valid: true},
		Started:     cql.Nullable[cql.Timestamp]{Value: 1000, Valid: true},
		Duration:    cql.Nullable[int32]{Value: 250, Valid: true},
		Parameters:  cql.Nullable[map[string]string]{Value: map[string]string{"query": "SELECT 1"}, Valid: true},
		RequestSize: cql.Nullable[int32]{Value: 7, Valid: true},
	}
	var info Info
	row.fill(&info)
	if info.Started.UnixMilli() != 1000 || info.Duration != 250*time.Microsecond || info.Parameters["query"] != "SELECT 1" {
		t.Fatalf("fill = %+v", info)
	}
	if !info.RequestSize.Set || info.RequestSize.Value != 7 || info.ResponseSize.Set || info.Client.IsValid() {
		t.Fatalf("fill optional fields = %+v", info)
	}
}

func TestNotReady(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	read := errors.New("read failed")
	if err := notReady(ctx, read); err != read {
		t.Fatalf("live ctx: %v", err)
	}
	cancel()
	err := notReady(ctx, context.Canceled)
	if !errors.Is(err, ErrNotReady) || !errors.Is(err, context.Canceled) {
		t.Fatalf("ended ctx: %v", err)
	}
	if err := wait(ctx, time.Hour); !errors.Is(err, ErrNotReady) || !errors.Is(err, context.Canceled) {
		t.Fatalf("wait: %v", err)
	}
}

func TestSettle(t *testing.T) {
	events := func(n int) []Event { return make([]Event, n) }
	// reads returns each count in turn, then repeats the last one.
	reads := func(counts ...int) (func(context.Context) ([]Event, error), *int) {
		calls := new(int)
		return func(context.Context) ([]Event, error) {
			i := min(*calls, len(counts)-1)
			*calls++
			return events(counts[i]), nil
		}, calls
	}
	fast := Fetcher{Settle: time.Millisecond}
	// bounded keeps a settle loop that never settles from hanging the suite.
	bounded := func(t *testing.T) context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		t.Cleanup(cancel)
		return ctx
	}

	t.Run("Stable", func(t *testing.T) {
		info := Info{Events: events(2)}
		read, calls := reads(2)
		if err := fast.settle(bounded(t), &info, read); err != nil || !info.Settled || len(info.Events) != 2 || *calls != 1 {
			t.Fatalf("err %v, settled %v, events %d, reads %d", err, info.Settled, len(info.Events), *calls)
		}
	})
	t.Run("Growing", func(t *testing.T) {
		info := Info{Events: events(1)}
		read, calls := reads(2, 3, 3)
		if err := fast.settle(bounded(t), &info, read); err != nil || !info.Settled || len(info.Events) != 3 || *calls != 3 {
			t.Fatalf("err %v, settled %v, events %d, reads %d", err, info.Settled, len(info.Events), *calls)
		}
	})
	t.Run("ShrinkingKeepsLargerRead", func(t *testing.T) {
		info := Info{Events: events(3)}
		read, calls := reads(1, 3)
		if err := fast.settle(bounded(t), &info, read); err != nil || !info.Settled || len(info.Events) != 3 || *calls != 2 {
			t.Fatalf("err %v, settled %v, events %d, reads %d", err, info.Settled, len(info.Events), *calls)
		}
	})
	t.Run("Negative", func(t *testing.T) {
		info := Info{Events: events(1)}
		read, calls := reads(5)
		if err := (Fetcher{Settle: -1}).settle(bounded(t), &info, read); err != nil || info.Settled || *calls != 0 {
			t.Fatalf("err %v, settled %v, reads %d", err, info.Settled, *calls)
		}
	})
	t.Run("DefaultWaits200ms", func(t *testing.T) {
		info := Info{Events: events(1)}
		read, _ := reads(1)
		start := time.Now()
		if err := (Fetcher{}).settle(bounded(t), &info, read); err != nil || !info.Settled {
			t.Fatalf("err %v, settled %v", err, info.Settled)
		}
		if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
			t.Fatalf("settled after %v, want at least 200ms", elapsed)
		}
	})
	t.Run("ContextEndsWhileSettling", func(t *testing.T) {
		ctx, cancel := context.WithCancel(bounded(t))
		info := Info{Events: events(1)}
		calls := 0
		read := func(context.Context) ([]Event, error) {
			calls++
			cancel()
			return nil, context.Canceled
		}
		if err := fast.settle(ctx, &info, read); err != nil || info.Settled || len(info.Events) != 1 || calls != 1 {
			t.Fatalf("err %v, settled %v, events %d, reads %d", err, info.Settled, len(info.Events), calls)
		}
		// A context that has already ended stops the wait before any read.
		calls = 0
		if err := fast.settle(ctx, &info, read); err != nil || info.Settled || calls != 0 {
			t.Fatalf("ended ctx: err %v, settled %v, reads %d", err, info.Settled, calls)
		}
	})
	t.Run("ReadError", func(t *testing.T) {
		info := Info{Events: events(1)}
		failure := errors.New("read failed")
		read := func(context.Context) ([]Event, error) { return nil, failure }
		if err := fast.settle(bounded(t), &info, read); err != failure || info.Settled {
			t.Fatalf("err %v, settled %v", err, info.Settled)
		}
	})
}

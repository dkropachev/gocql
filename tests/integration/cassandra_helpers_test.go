//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	gocql "github.com/scylladb/gocql/v2"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/retry"
)

// These helpers accept typed descriptors and explicit values. Retained results
// are cloned before their page is closed.
func cassandraValue(value cql.Value, err error) cql.Value {
	if err != nil {
		panic(err)
	}
	return value
}
func cassandraField(t testing.TB, row cql.DynamicRow, name string) cql.Value {
	t.Helper()
	v, err := row.Get(name)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func cassandraInt(t testing.TB, row cql.DynamicRow, name string) int64 {
	t.Helper()
	v, err := cassandraField(t, row, name).Signed()
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func cassandraText(t testing.TB, row cql.DynamicRow, name string) string {
	t.Helper()
	v, err := cassandraField(t, row, name).Text()
	if err != nil {
		t.Fatal(err)
	}
	return v.String()
}
func cassandraBool(t testing.TB, row cql.DynamicRow, name string) bool {
	t.Helper()
	v, err := cassandraField(t, row, name).Bool()
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func cassandraRead(t testing.TB, s *gocql.Session, stmt string, values ...cql.Value) cql.DynamicRow {
	t.Helper()
	row, err := v2ReadOne(context.Background(), s, stmt, values...)
	if err != nil {
		t.Fatal(err)
	}
	return row
}
func cassandraWrite(t testing.TB, s *gocql.Session, stmt string, values ...cql.Value) gocql.Result {
	t.Helper()
	result, err := v2Exec(context.Background(), s, stmt, values...)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func cassandraDDL(t testing.TB, s *gocql.Session, stmt string) {
	t.Helper()
	if err := v2CreateTable(context.Background(), s, stmt); err != nil {
		t.Fatal(err)
	}
}
func cassandraPrepared[P, R any](t testing.TB, s *gocql.Session, statement cql.Statement[P, R], options gocql.PrepareOptions) *gocql.Prepared[P, R] {
	t.Helper()
	p, err := gocql.Prepare(context.Background(), s, statement, options)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func cassandraBound[P, R any](t testing.TB, p *gocql.Prepared[P, R], value P) *Bound[R] {
	t.Helper()
	b, err := BindOwned(p, value)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		b.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := b.Wait(ctx); err != nil {
			t.Error(err)
		}
	})
	return b
}
func cassandraUnprepared[R any](t testing.TB, stmt string, decoder cql.RowDecoder[R]) *Bound[R] {
	t.Helper()
	b, err := BindUnpreparedOwned(cql.Statement[cql.EmptyParams, R]{CQL: stmt, Params: cql.EmptyParamsEncoder, Rows: decoder}, cql.NewColumns(), cql.EmptyParams{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		b.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := b.Wait(ctx); err != nil {
			t.Error(err)
		}
	})
	return b
}
func cassandraReadBound[R any](t testing.TB, s *gocql.Session, b *Bound[R], options gocql.Options) R {
	t.Helper()
	rows, err := Run(context.Background(), s, b, bindOptions(s, options))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var cur R
	if !rows.NextInto(&cur) {
		t.Fatalf("expected one row: %v", rows.Err())
	}
	row, err := rows.Clone()
	if err != nil {
		t.Fatal(err)
	}
	if rows.NextInto(&cur) {
		t.Fatal("expected exactly one row")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return row
}
func cassandraItem[R any](t testing.TB, b *Bound[R]) batchItem {
	t.Helper()
	item, err := BatchEntry(b)
	if err != nil {
		t.Fatal(err)
	}
	return item
}
func cassandraDynamicBound(t testing.TB, s *gocql.Session, stmt string, values []cql.Value, options gocql.PrepareOptions) *Bound[cql.DynamicRow] {
	t.Helper()
	return cassandraBound(t, cassandraPrepared(t, s, cql.Statement[[]cql.Value, cql.DynamicRow]{CQL: stmt, Params: cql.DynamicParams, Rows: cql.DynamicRows}, options), values)
}
func cassandraWriteBound(t testing.TB, s *gocql.Session, stmt string, values []cql.Value, options gocql.PrepareOptions) *Bound[cql.EmptyRow] {
	t.Helper()
	return cassandraBound(t, cassandraPrepared(t, s, cql.Statement[[]cql.Value, cql.EmptyRow]{CQL: stmt, Params: cql.DynamicParams, Rows: cql.EmptyRowDecoder}, options), values)
}
func cassandraRows(t testing.TB, s *gocql.Session, b *Bound[cql.DynamicRow], options gocql.Options) []cql.DynamicRow {
	t.Helper()
	rows, err := Run(context.Background(), s, b, bindOptions(s, options))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []cql.DynamicRow
	var cur cql.DynamicRow
	for rows.NextInto(&cur) {
		row, err := rows.Clone()
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}
func cassandraBatch(t testing.TB, s *gocql.Session, kind cql.BatchKind, items []batchItem, options gocql.Options) gocql.Result {
	t.Helper()
	r, e := execBatch(context.Background(), s, kind, items, bindOptions(s, options))
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func cassandraKeyspace(s *gocql.Session, name string) (*metadata.Keyspace, error) {
	if name == "" {
		return nil, cqlerr.ErrNoKeyspace
	}
	ks, ok := s.Metadata().Keyspace(name)
	if !ok {
		return nil, cqlerr.ErrKeyspaceDoesNotExist
	}
	return ks, nil
}
func cassandraTable(s *gocql.Session, ks, name string) (*metadata.Table, error) {
	if name == "" {
		return nil, cqlerr.ErrNoTable
	}
	k, err := cassandraKeyspace(s, ks)
	if err != nil {
		return nil, err
	}
	table, ok := k.Tables[name]
	if !ok {
		return nil, cqlerr.ErrNotFound
	}
	return table, nil
}
func cassandraOtherSession(t testing.TB, s *gocql.Session, keyspace string) *gocql.Session {
	t.Helper()
	cassandraDDL(t, s, fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'NetworkTopologyStrategy','replication_factor':%d}", keyspace, *flagRF))
	config := createV2Cluster()
	config.Keyspace = keyspace
	other, err := gocql.Connect(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	return other
}

type cassandraObserver struct {
	mu       sync.Mutex
	requests []gocql.RequestEvent
	attempts []gocql.AttemptEvent
}

func (o *cassandraObserver) ObserveRequest(e gocql.RequestEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.requests = append(o.requests, e)
}
func (o *cassandraObserver) ObserveAttempt(e gocql.AttemptEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.attempts = append(o.attempts, e)
}
func (o *cassandraObserver) reset() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.requests = nil
	o.attempts = nil
}
func (o *cassandraObserver) snapshot() ([]gocql.RequestEvent, []gocql.AttemptEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]gocql.RequestEvent(nil), o.requests...), append([]gocql.AttemptEvent(nil), o.attempts...)
}

type cassandraRetry struct{}

func (cassandraRetry) Decide(a retry.Attempt) retry.Decision {
	if a.Idempotent && a.Number < 6 {
		return retry.Decision{Action: retry.Same}
	}
	return retry.Decision{Action: retry.Stop}
}

// Custom values use explicit codecs; field discovery and reflection are absent.
type fullNameCodec struct{}

func (fullNameCodec) CheckType(t cql.DataType) error { return cql.TextCodec.CheckType(t) }
func (fullNameCodec) Encode(w *cql.ValueWriter, n FullName) error {
	return cql.StringEncoder.Encode(w, n.FirstName+" "+n.LastName)
}
func (fullNameCodec) Decode(r cql.ValueReader) (FullName, error) {
	text, err := cql.TextCodec.Decode(r)
	if err != nil {
		return FullName{}, err
	}
	names := strings.SplitN(text.String(), " ", 2)
	if len(names) != 2 {
		return FullName{}, errors.New("full name requires two components")
	}
	return FullName{FirstName: names[0], LastName: names[1]}, nil
}
func (fullNameCodec) Clone(n FullName) FullName { return n }
func cassandraCASBound[R any](t testing.TB, s *gocql.Session, stmt string, values []cql.Value, decoder cql.RowDecoder[R]) *Bound[cql.CASResult[R]] {
	t.Helper()
	return cassandraBound(t, cassandraPrepared(t, s, cql.Statement[[]cql.Value, cql.CASResult[R]]{CQL: stmt, Params: cql.DynamicParams, Rows: cql.CASRows(decoder)}, gocql.PrepareOptions{}), values)
}

func cassandraUUID(text string) cql.UUID {
	u, err := cql.ParseUUID(text)
	if err != nil {
		panic(err)
	}
	return u
}

type cassandraVarintInt64 struct{}

func (cassandraVarintInt64) CheckType(t cql.DataType) error { return cql.VarintCodec.CheckType(t) }
func (cassandraVarintInt64) Decode(r cql.ValueReader) (int64, error) {
	v, err := cql.VarintCodec.Decode(r)
	if err != nil {
		return 0, err
	}
	n := v.BigInt()
	if !n.IsInt64() {
		return 0, fmt.Errorf("varint is outside int64 range")
	}
	return n.Int64(), nil
}
func (cassandraVarintInt64) Clone(v int64) int64 { return v }

type cassandraReconnect struct{}

func (cassandraReconnect) Delay(attempt int) (time.Duration, bool) { return time.Second, true }

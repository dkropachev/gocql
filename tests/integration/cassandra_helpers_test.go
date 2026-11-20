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
	"github.com/scylladb/gocql/v2/exec"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/observe"
	"github.com/scylladb/gocql/v2/retry"
	"github.com/scylladb/gocql/v2/routing"
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
func cassandraWrite(t testing.TB, s *gocql.Session, stmt string, values ...cql.Value) cql.Result {
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
func cassandraPrepared[P, R any, O fixturePrepareOptions](t testing.TB, s *gocql.Session, statement cql.Statement[P, R], options O) *gocql.Prepared[P, R] {
	t.Helper()
	p, err := prepareFixture(context.Background(), s, statement, options)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// cassandraReadOne runs p with value and returns its only row.
func cassandraReadOne[P, R any, O fixtureOptions](t testing.TB, s *gocql.Session, p *gocql.Prepared[P, R], value P, options O) R {
	t.Helper()
	rows, err := p.Run(context.Background(), value, bindOptions(s, options))
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

// cassandraUnpreparedOne runs stmt without preparing it and returns its only
// row, detached from the response.
func cassandraUnpreparedOne[R any](t testing.TB, s *gocql.Session, stmt string, decoder cql.RowDecoder[R], options exec.Options) R {
	t.Helper()
	rows := cassandraUnpreparedAll(t, s, stmt, decoder, options)
	if len(rows) != 1 {
		t.Fatalf("expected exactly one row, got %d", len(rows))
	}
	return rows[0]
}

// cassandraUnpreparedAll runs stmt without preparing it and returns every
// row across all pages, detached from the responses.
func cassandraUnpreparedAll[R any](t testing.TB, s *gocql.Session, stmt string, decoder cql.RowDecoder[R], options exec.Options) []R {
	t.Helper()
	var result []R
	var cur R
	statement := cql.Statement[cql.EmptyParams, R]{CQL: stmt, Params: cql.EmptyParamsEncoder, Rows: decoder}
	if _, err := s.UnpreparedEach(context.Background(), statement, cql.EmptyParams{}, &cur, func(row *R) error {
		result = append(result, decoder.Clone(*row))
		return nil
	}, options); err != nil {
		t.Fatal(err)
	}
	return result
}

// cassandraAdd appends p with value to batch.
func cassandraAdd[P, R any](t testing.TB, batch *gocql.Batch, p *gocql.Prepared[P, R], value P) {
	t.Helper()
	if err := p.AddToBatch(batch, value); err != nil {
		t.Fatal(err)
	}
}
func cassandraDynamicPrepared[O fixturePrepareOptions](t testing.TB, s *gocql.Session, stmt string, options O) *gocql.Prepared[[]cql.Value, cql.DynamicRow] {
	t.Helper()
	return cassandraPrepared(t, s, cql.Statement[[]cql.Value, cql.DynamicRow]{CQL: stmt, Params: cql.DynamicParams, Rows: cql.DynamicRows}, options)
}
func cassandraWritePrepared[O fixturePrepareOptions](t testing.TB, s *gocql.Session, stmt string, options O) *gocql.Prepared[[]cql.Value, cql.EmptyRow] {
	t.Helper()
	return cassandraPrepared(t, s, cql.Statement[[]cql.Value, cql.EmptyRow]{CQL: stmt, Params: cql.DynamicParams, Rows: cql.EmptyRowDecoder}, options)
}
func cassandraRows[P any, O fixtureOptions](t testing.TB, s *gocql.Session, p *gocql.Prepared[P, cql.DynamicRow], value P, options O) []cql.DynamicRow {
	t.Helper()
	rows, err := p.Run(context.Background(), value, bindOptions(s, options))
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
func cassandraBatch[O fixtureOptions](t testing.TB, s *gocql.Session, batch *gocql.Batch, options O) cql.Result {
	t.Helper()
	r, e := s.ExecBatch(context.Background(), batch, bindOptions(s, options))
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
		return nil, fmt.Errorf("table %s.%s: %w", ks, name, cqlerr.ErrNoTable)
	}
	return table, nil
}

// fixtureCompressed reports whether the suite's sessions compress frames.
func fixtureCompressed() bool {
	return *flagCompressTest == "snappy" || *flagCompressTest == "lz4"
}

// fixturePort is the native transport port createV2Cluster connects to.
func fixturePort() uint16 {
	if *flagRunSslTest && *flagDistribution != "cassandra" {
		return 9142
	}
	return 9042
}

func cassandraOtherSession(t testing.TB, s *gocql.Session, keyspace string) *gocql.Session {
	t.Helper()
	cassandraDDL(t, s, fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'NetworkTopologyStrategy','replication_factor':%d}", keyspace, *flagRF))
	cfg := createV2Cluster().WithKeyspace(keyspace)
	other, err := gocql.Connect(context.Background(), mustBuild(t, cfg), testDialect)
	if err != nil {
		t.Fatal(err)
	}
	return other
}

type cassandraObserver struct {
	mu       sync.Mutex
	requests []observe.RequestEvent
	attempts []routing.AttemptEvent
}

func (o *cassandraObserver) ObserveRequest(e observe.RequestEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.requests = append(o.requests, e)
}
func (o *cassandraObserver) ObserveAttempt(e routing.AttemptEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.attempts = append(o.attempts, e)
}

func (o *cassandraObserver) StartRequest(ctx context.Context, _ observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
	return ctx, o.ObserveRequest
}
func (o *cassandraObserver) StartAttempt(context.Context, routing.AttemptStart) func(routing.AttemptEvent) {
	return o.ObserveAttempt
}
func (o *cassandraObserver) reset() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.requests = nil
	o.attempts = nil
}
func (o *cassandraObserver) snapshot() ([]observe.RequestEvent, []routing.AttemptEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]observe.RequestEvent(nil), o.requests...), append([]routing.AttemptEvent(nil), o.attempts...)
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
	return cql.StringCodec.Encode(w, n.FirstName+" "+n.LastName)
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
func cassandraCASPrepared[R any](t testing.TB, s *gocql.Session, stmt string, decoder cql.RowDecoder[R]) *gocql.Prepared[[]cql.Value, cql.CASResult[R]] {
	t.Helper()
	return cassandraPrepared(t, s, cql.Statement[[]cql.Value, cql.CASResult[R]]{CQL: stmt, Params: cql.DynamicParams, Rows: cql.CASRows(decoder)}, exec.PrepareOptions{})
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

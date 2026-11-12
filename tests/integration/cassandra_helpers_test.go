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
	"github.com/scylladb/gocql/v2/exec"

	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cql/dynamic"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/retry"
)

// These helpers accept typed descriptors and explicit values. Retained results
// are cloned before their page is closed.
func cassandraValue(value dynamic.Value, err error) dynamic.Value {
	if err != nil {
		panic(err)
	}
	return value
}
func cassandraField(t testing.TB, row dynamic.Row, name string) dynamic.Value {
	t.Helper()
	v, err := row.Get(name)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func cassandraInt(t testing.TB, row dynamic.Row, name string) int64 {
	t.Helper()
	v, err := cassandraField(t, row, name).Signed()
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func cassandraText(t testing.TB, row dynamic.Row, name string) string {
	t.Helper()
	v, err := cassandraField(t, row, name).Text()
	if err != nil {
		t.Fatal(err)
	}
	return v.String()
}
func cassandraBool(t testing.TB, row dynamic.Row, name string) bool {
	t.Helper()
	v, err := cassandraField(t, row, name).Bool()
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func cassandraRead(t testing.TB, s *gocql.Session[testDialect], stmt string, values ...dynamic.Value) dynamic.Row {
	t.Helper()
	row, err := v2ReadOne(context.Background(), s, stmt, values...)
	if err != nil {
		t.Fatal(err)
	}
	return row
}
func cassandraWrite(t testing.TB, s *gocql.Session[testDialect], stmt string, values ...dynamic.Value) gocql.Result {
	t.Helper()
	result, err := v2Exec(context.Background(), s, stmt, values...)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func cassandraDDL(t testing.TB, s *gocql.Session[testDialect], stmt string) {
	t.Helper()
	if err := v2CreateTable(context.Background(), s, stmt); err != nil {
		t.Fatal(err)
	}
}
func cassandraPrepared[P, R any, O fixturePrepareOptions](t testing.TB, s *gocql.Session[testDialect], statement cql.Statement[P, R], options O) *gocql.Prepared[P, R] {
	t.Helper()
	p, err := prepareFixture(context.Background(), s, statement, options)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// cassandraReadOne runs p with value and returns its only row.
func cassandraReadOne[P, R any, O fixtureOptions](t testing.TB, s *gocql.Session[testDialect], p *gocql.Prepared[P, R], value P, options O) R {
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
func cassandraUnpreparedOne[R any](t testing.TB, s *gocql.Session[testDialect], stmt string, decoder cql.RowDecoder[R], options exec.Options) R {
	t.Helper()
	rows := cassandraUnpreparedAll(t, s, stmt, decoder, options)
	if len(rows) != 1 {
		t.Fatalf("expected exactly one row, got %d", len(rows))
	}
	return rows[0]
}

// cassandraUnpreparedAll runs stmt without preparing it and returns every
// row across all pages, detached from the responses.
func cassandraUnpreparedAll[R any](t testing.TB, s *gocql.Session[testDialect], stmt string, decoder cql.RowDecoder[R], options exec.Options) []R {
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
func cassandraDynamicPrepared[O fixturePrepareOptions](t testing.TB, s *gocql.Session[testDialect], stmt string, options O) *gocql.Prepared[[]dynamic.Value, dynamic.Row] {
	t.Helper()
	return cassandraPrepared(t, s, cql.Statement[[]dynamic.Value, dynamic.Row]{CQL: stmt, Params: dynamic.Params, Rows: dynamic.Rows}, options)
}
func cassandraWritePrepared[O fixturePrepareOptions](t testing.TB, s *gocql.Session[testDialect], stmt string, options O) *gocql.Prepared[[]dynamic.Value, cql.EmptyRow] {
	t.Helper()
	return cassandraPrepared(t, s, cql.Statement[[]dynamic.Value, cql.EmptyRow]{CQL: stmt, Params: dynamic.Params, Rows: cql.EmptyRowDecoder}, options)
}
func cassandraRows[P any, O fixtureOptions](t testing.TB, s *gocql.Session[testDialect], p *gocql.Prepared[P, dynamic.Row], value P, options O) []dynamic.Row {
	t.Helper()
	rows, err := p.Run(context.Background(), value, bindOptions(s, options))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []dynamic.Row
	var cur dynamic.Row
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
func cassandraBatch[O fixtureOptions](t testing.TB, s *gocql.Session[testDialect], batch *gocql.Batch, options O) gocql.Result {
	t.Helper()
	r, e := s.ExecBatch(context.Background(), batch, bindOptions(s, options))
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func cassandraKeyspace(s *gocql.Session[testDialect], name string) (*metadata.Keyspace, error) {
	if name == "" {
		return nil, cqlerr.ErrNoKeyspace
	}
	ks, ok := s.Metadata().Keyspace(name)
	if !ok {
		return nil, cqlerr.ErrKeyspaceDoesNotExist
	}
	return ks, nil
}
func cassandraTable(s *gocql.Session[testDialect], ks, name string) (*metadata.Table, error) {
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

func cassandraOtherSession(t testing.TB, s *gocql.Session[testDialect], keyspace string) *gocql.Session[testDialect] {
	t.Helper()
	cassandraDDL(t, s, fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'NetworkTopologyStrategy','replication_factor':%d}", keyspace, *flagRF))
	cfg := createV2Cluster().WithKeyspace(keyspace)
	other, err := gocql.Connect[testDialect](context.Background(), mustBuild(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	return other
}

type cassandraObserver struct {
	mu       sync.Mutex
	requests []config.RequestEvent
	attempts []config.AttemptEvent
}

func (o *cassandraObserver) ObserveRequest(e config.RequestEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.requests = append(o.requests, e)
}
func (o *cassandraObserver) ObserveAttempt(e config.AttemptEvent) {
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
func (o *cassandraObserver) snapshot() ([]config.RequestEvent, []config.AttemptEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]config.RequestEvent(nil), o.requests...), append([]config.AttemptEvent(nil), o.attempts...)
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
func cassandraCASPrepared[R any](t testing.TB, s *gocql.Session[testDialect], stmt string, decoder cql.RowDecoder[R]) *gocql.Prepared[[]dynamic.Value, cql.CASResult[R]] {
	t.Helper()
	return cassandraPrepared(t, s, cql.Statement[[]dynamic.Value, cql.CASResult[R]]{CQL: stmt, Params: dynamic.Params, Rows: cql.CASRows(decoder)}, exec.PrepareOptions{})
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

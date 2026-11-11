//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package integration

import (
	"context"
	"fmt"
	"net/netip"
	"reflect"
	"testing"
	"time"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
)

type smAudit struct {
	CreatedAt cql.Timestamp
	UpdatedAt cql.Timestamp
}

// smRecord covers every built-in field type, nullable values, collections and
// an embedded struct.
type smRecord struct {
	Bucket int32
	ID     cql.UUID `cql:"id"`
	Name   string
	ASCII  string `cql:"ascii_name,codec=ASCIIStringCodec"`
	Flag   bool
	Tiny   int8
	Small  int16
	Big    int64
	Float  float32
	Double float64
	Blob   []byte
	Addr   netip.Addr
	TimeID cql.TimeUUID
	Day    cql.Date
	Clock  cql.TimeOfDay
	Span   cql.Duration
	Maybe  cql.Nullable[int32]
	Tags   []string
	Scores []int32                    `cql:"scores,codec=SetCodec"`
	Attrs  []cql.Entry[string, int64] `cql:"attrs,codec=cql.MapCodec(cql.StringCodec, cql.Int64Codec)"`
	smAudit
	Note string `cql:"-"`
}

// smKey selects one record.
type smKey struct {
	Bucket int32
	ID     cql.UUID `cql:"id"`
}

const smColumns = "bucket, id, name, ascii_name, flag, tiny, small, big, float, double, blob, addr, timeid, day, clock, span, maybe, tags, scores, attrs, createdat, updatedat"

var smAttrs = cql.WithCodec[smRecord](cql.MapCodec(cql.StringCodec, cql.Int64Codec))

type smFixture struct {
	session *gocql.Session[testDialect]
	table   string
}

func newSMFixture(t *testing.T) (context.Context, smFixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	session := createV2Session(t)
	t.Cleanup(func() { _ = session.Close() })
	table := testTableName(t)
	if err := v2CreateTable(ctx, session, "CREATE TABLE IF NOT EXISTS "+table+` (
		bucket int, id uuid, name text, ascii_name ascii, flag boolean, tiny tinyint,
		small smallint, big bigint, float float, double double, blob blob, addr inet,
		timeid timeuuid, day date, clock time, span duration, maybe int,
		tags list<text>, scores set<int>, attrs map<text, bigint>,
		createdat timestamp, updatedat timestamp,
		PRIMARY KEY (bucket, id))`); err != nil {
		t.Fatal(err)
	}
	if err := truncateTable(t, session, table); err != nil {
		t.Fatal(err)
	}
	return ctx, smFixture{session: session, table: table}
}

func smRecordAt(bucket int32, i int) smRecord {
	at := time.Date(2026, 11, 6, 12, 0, i, 0, time.UTC)
	return smRecord{
		Bucket: bucket, ID: cql.UUID(cql.TimeUUIDFromTime(at)), Name: "héllo " + string(rune('a'+i)), ASCII: "plain",
		Flag: i%2 == 0, Tiny: int8(-i), Small: int16(1000 + i), Big: 1<<40 + int64(i),
		Float: 1.5, Double: -2.25, Blob: []byte{byte(i), 1, 2, 3},
		Addr: netip.MustParseAddr("10.0.0.1"), TimeID: cql.TimeUUIDFromTime(at),
		Day: cql.Date(20000 + i), Clock: cql.TimeOfDay(int64(i) * int64(time.Second)), Span: cql.Duration{Months: 1, Days: int32(i), Nanoseconds: 3},
		Maybe: cql.Nullable[int32]{Value: int32(i), Valid: true}, Tags: []string{"a", "b"}, Scores: []int32{1, int32(10 + i)},
		Attrs:   []cql.Entry[string, int64]{{Key: "k", Value: int64(i)}},
		smAudit: smAudit{CreatedAt: cql.Timestamp(at.UnixMilli()), UpdatedAt: cql.Timestamp(at.UnixMilli() + 1)},
		Note:    "not stored",
	}
}

// stored is what a record reads back as: unmapped fields are not stored.
func stored(r smRecord) smRecord { r.Note = ""; return r }

// recordError is requireRecord for Each callbacks, which must return rather
// than stop the test goroutine inside the driver.
func recordError(got, want smRecord) error {
	if !reflect.DeepEqual(got, stored(want)) {
		return fmt.Errorf("record mismatch:\ngot  %+v\nwant %+v", got, stored(want))
	}
	return nil
}

func requireRecord(t *testing.T, got, want smRecord) {
	t.Helper()
	if !reflect.DeepEqual(got, stored(want)) {
		t.Fatalf("record mismatch:\ngot  %+v\nwant %+v", got, stored(want))
	}
}

func (f smFixture) insert(t *testing.T, ctx context.Context) *gocql.Prepared[smRecord, cql.EmptyRow] {
	t.Helper()
	insert, err := f.session.Prepare(ctx, cql.Statement[smRecord, cql.EmptyRow]{
		Kind:   cql.OrdinaryStatement,
		CQL:    "INSERT INTO " + f.table + " (" + smColumns + ") VALUES (?" + repeatMarkers(21) + ")",
		Params: cql.MustParamsOf(smAttrs),
	})
	if err != nil {
		t.Fatal(err)
	}
	return insert
}

func (f smFixture) selectBucket(t *testing.T, ctx context.Context, rows cql.RowDecoder[smRecord]) *gocql.Prepared[int32, smRecord] {
	t.Helper()
	read, err := f.session.Prepare(ctx, cql.Statement[int32, smRecord]{
		Kind:   cql.OrdinaryStatement,
		CQL:    "SELECT " + smColumns + " FROM " + f.table + " WHERE bucket = ?",
		Params: cql.OneParam(cql.Int32Codec),
		Rows:   rows,
	})
	if err != nil {
		t.Fatal(err)
	}
	return read
}

func repeatMarkers(n int) string {
	s := ""
	for range n {
		s += ", ?"
	}
	return s
}

// smStream is smRecord's streamed form: text decodes into []byte.
type smStream struct {
	Bucket int32
	ID     cql.UUID `cql:"id"`
	Name   []byte   `cql:"name,stream=StreamText"`
	Blob   []byte
	Big    int64
	Maybe  cql.Nullable[int32]
	Scores []int32 `cql:"scores,stream=StreamSet"`
	Span   cql.Duration
	smAudit
}

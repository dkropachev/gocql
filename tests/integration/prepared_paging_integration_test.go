//go:build integration

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package integration

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
)

type liveDestination struct {
	Value   int32
	Payload cql.Bytes
}
type liveDecoder struct {
	calls *atomic.Int32
	base  cql.RowDecoder[liveDestination]
}

func (d liveDecoder) Compile(c cql.Columns) (cql.RowPlan[liveDestination], error) {
	d.calls.Add(1)
	return d.base.Compile(c)
}
func (d liveDecoder) Clone(v liveDestination) liveDestination {
	return d.base.Clone(v)
}

func TestV2PreparedPagingConcurrentSchemaAndDestinations(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	table := fmt.Sprintf("prepared_rows_%x", time.Now().UnixNano())
	other := table + "_ddl"
	if err := v2CreateTable(ctx, session, "CREATE TABLE "+table+"(id int,val int,payload blob,PRIMARY KEY(id,val))"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = v2CreateTable(context.Background(), session, "DROP TABLE IF EXISTS "+other)
		_ = v2CreateTable(context.Background(), session, "DROP TABLE IF EXISTS "+table)
	}()
	payload := bytes.Repeat([]byte("live-borrowed"), 512)
	for i := int32(0); i < 64; i++ {
		cassandraWrite(t, session, "INSERT INTO "+table+"(id,val,payload) VALUES(?,?,?)", cql.IntValue(1), cql.IntValue(i), cql.BlobValue(payload))
	}
	var calls atomic.Int32
	decoder := liveDecoder{calls: &calls, base: cql.Row(cql.Column("val", cql.Int32, func(r *liveDestination) *int32 { return &r.Value }), cql.Column("payload", cql.BlobCodec, func(r *liveDestination) *cql.Bytes { return &r.Payload }))}
	prepared := codecTestMust(gocql.Prepare(ctx, session, cql.Statement[int32, liveDestination]{CQL: "SELECT val,payload FROM " + table + " WHERE id=?", Params: cql.OneParam(cql.Int32), Rows: decoder}, gocql.PrepareOptions{}))
	bound := codecTestMust(BindBorrowed(prepared, int32(1)))
	defer bound.Close()
	churn := make(chan error, 1)
	go func() {
		for i := 0; i < 8; i++ {
			if err := v2CreateTable(ctx, session, "CREATE TABLE "+other+"(id int PRIMARY KEY)"); err != nil {
				churn <- err
				return
			}
			if err := v2CreateTable(ctx, session, "DROP TABLE "+other); err != nil {
				churn <- err
				return
			}
		}
		churn <- nil
	}()
	rows := codecTestMust(Run(ctx, session, bound, bindOptions(session, gocql.Options{PageSize: cql.Option[int32]{Value: 3, Set: true}})))
	defer rows.Close()
	var dst liveDestination
	var retained *gocql.PageLease
	var first cql.Bytes
	count := 0
	for rows.NextInto(&dst) {
		if dst.Value != int32(count) || !bytes.Equal(dst.Payload.OwnedBytes(), payload) {
			t.Fatal("row mismatch", count, dst.Value)
		}
		if count == 0 {
			retained = codecTestMust(rows.Retain())
			defer retained.Close()
			first = dst.Payload
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 64 || dst.Value != 0 || dst.Payload.Len() != 0 {
		t.Fatal("count/final reset", count, dst)
	}
	if rows.NextInto(nil) || rows.Err() != nil {
		t.Fatal("exhaustion replaced by nil error", rows.Err())
	}
	if calls.Load() != 1 {
		t.Fatal("steady pages recompiled", calls.Load())
	}
	if err := <-churn; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.OwnedBytes(), payload) {
		t.Fatal("retained output lost during DDL")
	}
	bound.Close()
	if err := bound.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}

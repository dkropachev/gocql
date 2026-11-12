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

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/settings"
)

// TestCounterWriteHasNoClientTimestamp checks that with driver timestamps a
// write whose parameters include a counter sends no timestamp, through every
// way of executing a prepared statement, while an ordinary write sends one.
func TestCounterWriteHasNoClientTimestamp(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	defer s.Close()
	InternalRuntimeFor(s).config.Timestamps = settings.DriverTimestamps()
	var mu sync.Mutex
	sent := map[string][]bool{} // statement id -> whether each EXECUTE had a timestamp
	go func() {
		for {
			op, stream, body, err := readRequest(peers[0])
			if err != nil {
				return
			}
			var response []byte
			if op == byte(frm.OpPrepare) {
				cql := string(body[4 : 4+int(binary.BigEndian.Uint32(body))])
				id, typ := "plain", cqltype.BigIntType
				if strings.Contains(cql, "c + ?") {
					id, typ = "counter", cqltype.CounterType
				}
				params := cqltype.NewColumns(cqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "c", Type: typ})
				response = preparedBody([]byte(id), params, cqltype.NewColumns())
			} else {
				n := int(binary.BigEndian.Uint16(body))
				id := string(body[2 : 2+n])
				flags := body[2+n+2]
				mu.Lock()
				sent[id] = append(sent[id], flags&0x20 != 0)
				mu.Unlock()
				response = binary.BigEndian.AppendUint32(nil, frm.ResultKindVoid)
			}
			if _, err := peers[0].Write(typedTestResponse(t, 4, stream, response)); err != nil {
				return
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	check := func(t *testing.T, id string, timestamp bool) {
		t.Helper()
		mu.Lock()
		defer mu.Unlock()
		if got := sent[id]; len(got) != 3 || got[0] != timestamp || got[1] != timestamp || got[2] != timestamp {
			t.Fatalf("timestamps sent %v, want %v for each of pooled, query and bound", got, timestamp)
		}
	}
	t.Run("Counter", func(t *testing.T) {
		counterTimestampWrites(t, ctx, s, "UPDATE ks.tbl SET c = c + ? WHERE k = 1", codec.CounterCodec, 1)
		check(t, "counter", false)
	})
	t.Run("Ordinary", func(t *testing.T) {
		counterTimestampWrites(t, ctx, s, "UPDATE ks.tbl SET c = ? WHERE k = 1", codec.Int64, 1)
		check(t, "plain", true)
	})
	// A counter batch sends no timestamp: its body is a timestamp's eight bytes
	// shorter than the same unlogged batch's.
	t.Run("Batch", func(t *testing.T) {
		s, _, peers := runtimeTestSession(t, 1)
		defer s.Close()
		InternalRuntimeFor(s).config.Timestamps = settings.DriverTimestamps()
		q := batchTestQuery(t, s)
		session := InternalWrapSession(s)
		var batch Batch
		if err := q.AddToBatch(&batch, []byte("v")); err != nil {
			t.Fatal(err)
		}
		size := func(kind cqltype.BatchKind) int {
			t.Helper()
			served := make(chan int, 1)
			go func() {
				_, body := batchTestServe(t, peers[0])
				served <- len(body)
			}()
			batch.Kind = kind
			if _, err := session.ExecBatch(ctx, &batch, CompiledOptions{}); err != nil {
				t.Fatal(err)
			}
			return <-served
		}
		if unlogged, counter := size(cqltype.UnloggedBatch), size(cqltype.CounterBatch); unlogged-counter != 8 {
			t.Fatalf("unlogged batch body %d bytes, counter batch %d: want the counter batch without a timestamp", unlogged, counter)
		}
	})
}

// counterTimestampWrites prepares cql and executes it once each as a
// prepared statement's pooled query, as a query and as a bound statement.
func counterTimestampWrites[P any](t *testing.T, ctx context.Context, s *cluster.Core, cql string, encoder codec.Encoder[P], value P) {
	t.Helper()
	p, err := Prepare(ctx, s, binding.Statement[P, codec.EmptyRecord]{Kind: cqltype.OrdinaryStatement, CQL: cql, Params: codec.OneParam(encoder), Rows: codec.EmptyRowDecoder}, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, value, CompiledOptions{}); err != nil {
		t.Fatal("pooled query:", err)
	}
	q, err := p.Query(Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if _, err := q.Exec(ctx, value); err != nil {
		t.Fatal("query:", err)
	}
	b, err := BindBorrowed(p, value)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if _, err := Exec(ctx, s, b, CompiledOptions{}); err != nil {
		t.Fatal("bound:", err)
	}
}

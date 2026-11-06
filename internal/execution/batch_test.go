// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/protocol"
)

func batchTestQuery(t testing.TB, s *implCluster.Session) *Query[[]byte, EmptyRecord] {
	t.Helper()
	typ := cqltype.BlobType
	q, err := NewUnpreparedQuery(s, Statement[[]byte, EmptyRecord]{Kind: OrdinaryStatement, CQL: "INSERT INTO ks.t(v) VALUES(?)", Params: codec.OneParam(codec.BytesEncoder), Rows: codec.EmptyRowDecoder, BindMarkers: cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: typ})}, OptionValues{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(q.Close)
	return q
}

// batchTestServe answers one protocol v4 request with a void result and
// returns its opcode and body.
func batchTestServe(t testing.TB, server net.Conn) (byte, []byte) {
	t.Helper()
	var header [9]byte
	if _, err := io.ReadFull(server, header[:]); err != nil {
		t.Error(err)
		return 0, nil
	}
	body := make([]byte, binary.BigEndian.Uint32(header[5:]))
	if _, err := io.ReadFull(server, body); err != nil {
		t.Error(err)
		return 0, nil
	}
	stream := int(binary.BigEndian.Uint16(header[2:4]))
	if _, err := server.Write(typedTestResponse(t, 4, stream, binary.BigEndian.AppendUint32(nil, frm.ResultKindVoid))); err != nil {
		t.Error(err)
	}
	return header[4], body
}

func TestBatchExecSendsEntriesAndReusesStorage(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	q := batchTestQuery(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type request struct {
		op   byte
		body []byte
	}
	requests := make(chan request, 3)
	go func() {
		for i := 0; i < 3; i++ {
			op, body := batchTestServe(t, peers[0])
			requests <- request{op, body}
		}
	}()
	var batch Batch
	for _, v := range []string{"first", "second"} {
		if err := q.AddTo(&batch, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if batch.Len() != 2 {
		t.Fatal(batch.Len())
	}
	// A batch may be sent again unchanged.
	for i := 0; i < 2; i++ {
		if _, err := q.ExecBatch(ctx, LoggedBatch, &batch, Options{}); err != nil {
			t.Fatal(err)
		}
		r := <-requests
		if r.op != byte(frm.OpBatch) || !bytes.Contains(r.body, []byte("first")) || !bytes.Contains(r.body, []byte("second")) {
			t.Fatalf("request %d: op %#x body %q", i, r.op, r.body)
		}
	}
	if !batch.drained() || batch.shared {
		t.Fatal("a completed execution did not drain")
	}
	values := &batch.values[0]
	batch.Reset()
	if batch.Len() != 0 {
		t.Fatal(batch.Len())
	}
	if err := q.AddTo(&batch, []byte("third")); err != nil {
		t.Fatal(err)
	}
	if &batch.values[0] != values {
		t.Fatal("Reset did not keep the storage")
	}
	if _, err := q.ExecBatch(ctx, UnloggedBatch, &batch, Options{}); err != nil {
		t.Fatal(err)
	}
	r := <-requests
	if !bytes.Contains(r.body, []byte("third")) || bytes.Contains(r.body, []byte("first")) {
		t.Fatalf("body %q", r.body)
	}
}

func TestBatchResetLeavesStorageToUndrainedExecution(t *testing.T) {
	q := testOwnedQuery(t)
	for _, tc := range []struct {
		name string
		// start models an execution that was cancelled while its writer
		// still reads the batch; it returns the writer's release.
		start func(*Batch) func()
	}{
		{"query", func(batch *Batch) func() {
			sender := testOwnedQuery(t)
			if err := sender.state.begin(context.Background()); err != nil {
				t.Fatal(err)
			}
			var request InternalExecutionRequest
			batch.queryExec(&request, &sender.state, LoggedBatch)
			return sender.state.owner.InternalRelease
		}},
		{"session", func(batch *Batch) func() {
			owner := newInputOwner()
			if err := owner.InternalAdmit(); err != nil {
				t.Fatal(err)
			}
			owner.InternalClose()
			batch.begin(LoggedBatch)
			batch.lastOwner, batch.lastQuery = owner, nil
			return owner.InternalRelease
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var batch Batch
			if err := q.AddTo(&batch, []byte("in flight")); err != nil {
				t.Fatal(err)
			}
			release := tc.start(&batch)
			inflight, entry := batch.values, batch.entries[0]
			batch.Reset()
			if err := q.AddTo(&batch, []byte("next batch")); err != nil {
				t.Fatal(err)
			}
			if got := string(inflight[entry.start+4 : entry.end]); got != "in flight" {
				t.Fatalf("in-flight values overwritten: %q", got)
			}
			// The writer finishing drains the execution; the next Reset
			// reuses the storage again.
			release()
			if !batch.drained() {
				t.Fatal("execution did not drain")
			}
			values := &batch.values[0]
			batch.Reset()
			if err := q.AddTo(&batch, []byte("reused")); err != nil {
				t.Fatal(err)
			}
			if &batch.values[0] != values {
				t.Fatal("Reset did not reuse drained storage")
			}
		})
	}
}

func TestBatchValidation(t *testing.T) {
	s, _, _ := runtimeTestSession(t, 0)
	q := batchTestQuery(t, s)
	other := testOwnedQuery(t)
	ctx := context.Background()
	var batch Batch
	if _, err := q.ExecBatch(ctx, LoggedBatch, &batch, Options{}); err == nil {
		t.Fatal("empty batch sent")
	}
	if err := q.AddTo(nil, []byte("v")); err == nil {
		t.Fatal("nil batch accepted")
	}
	if err := q.AddTo(&batch, []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := other.AddTo(&batch, []byte("v")); err == nil || batch.Len() != 1 {
		t.Fatal("statement of another session added", err)
	}
	if _, err := other.ExecBatch(ctx, LoggedBatch, &batch, Options{}); err == nil {
		t.Fatal("batch sent on another session")
	}
	if _, err := q.ExecBatch(ctx, BatchKind(42), &batch, Options{}); err == nil {
		t.Fatal("unknown batch kind sent")
	}
	if err := q.state.begin(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.AddTo(&batch, []byte("v")); !errors.Is(err, ErrBusy) {
		t.Fatal("busy query appended", err)
	}
	q.state.owner.InternalRelease()
	q.Close()
	if err := q.AddTo(&batch, []byte("v")); !errors.Is(err, ErrClosed) {
		t.Fatal("closed query appended", err)
	}
	if batch.Len() != 1 {
		t.Fatal(batch.Len())
	}
}

func TestBatchAddToDoesNotAllocate(t *testing.T) {
	q := testOwnedQuery(t)
	var batch Batch
	value := []byte("batch entry")
	fill := func() {
		for i := 0; i < 16; i++ {
			if err := q.AddTo(&batch, value); err != nil {
				t.Fatal(err)
			}
		}
		batch.Reset()
	}
	fill() // grow the batch to its working size
	if n := testing.AllocsPerRun(100, fill); n != 0 {
		t.Fatalf("AddTo and Reset allocate %v times per batch", n)
	}
}

func BenchmarkBatchAddTo(b *testing.B) {
	q := testOwnedQuery(b)
	var batch Batch
	value := []byte("batch entry")
	b.ReportAllocs()
	for b.Loop() {
		if err := q.AddTo(&batch, value); err != nil {
			b.Fatal(err)
		}
		if batch.Len() == 64 {
			batch.Reset()
		}
	}
}

// batchBenchSession returns a session whose one connection answers every
// request with a void result.
func batchBenchSession(b testing.TB) *implCluster.Session {
	s, _, peers := runtimeTestSession(b, 1)
	go func() {
		var header [9]byte
		response := typedTestResponse(b, 4, 0, binary.BigEndian.AppendUint32(nil, frm.ResultKindVoid))
		body := make([]byte, 1<<16)
		for {
			if _, err := io.ReadFull(peers[0], header[:]); err != nil {
				return
			}
			n := binary.BigEndian.Uint32(header[5:])
			if _, err := io.ReadFull(peers[0], body[:n]); err != nil {
				return
			}
			response[2], response[3] = header[2], header[3]
			if _, err := peers[0].Write(response); err != nil {
				return
			}
		}
	}()
	return s
}

func BenchmarkBatchExec(b *testing.B) {
	for _, n := range []int{1, 8} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			s := batchBenchSession(b)
			q := batchTestQuery(b, s)
			ctx := context.Background()
			value := []byte("0123456789abcdef0123456789abcdef")
			var batch Batch
			b.ReportAllocs()
			for b.Loop() {
				batch.Reset()
				for i := 0; i < n; i++ {
					if err := q.AddTo(&batch, value); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := q.ExecBatch(ctx, UnloggedBatch, &batch, Options{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestBatchStatementKind(t *testing.T) {
	s, _, _ := runtimeTestSession(t, 0)
	for _, tc := range []struct {
		kinds []StatementKind
		want  StatementKind
	}{
		{[]StatementKind{OrdinaryStatement, OrdinaryStatement}, OrdinaryStatement},
		{[]StatementKind{UnknownStatement}, UnknownStatement},
		{[]StatementKind{OrdinaryStatement, UnknownStatement}, UnknownStatement},
		{[]StatementKind{UnknownStatement, ConditionalStatement, OrdinaryStatement}, ConditionalStatement},
	} {
		var batch Batch
		for _, kind := range tc.kinds {
			if err := batch.add(s, cqlText{}, "UPDATE t SET v=1", &RoutingInfo{Kind: kind}, encodedValues{}); err != nil {
				t.Fatal(err)
			}
		}
		if batch.routing.Kind != tc.want {
			t.Errorf("%v: kind %v, want %v", tc.kinds, batch.routing.Kind, tc.want)
		}
	}
}

func TestBatchAttemptReusesWireBatch(t *testing.T) {
	s, conns, _ := runtimeTestSession(t, 1)
	q := batchTestQuery(t, s)
	var batch Batch
	if err := q.AddTo(&batch, []byte("v")); err != nil {
		t.Fatal(err)
	}
	state := batch.begin(LoggedBatch)
	rt := InternalRuntimeFor(s)
	ctx := context.Background()
	first, err := rt.prepareBatchAttempt(ctx, conns[0], state, nil)
	if err != nil || first != &state.attempt {
		t.Fatal("first attempt did not take the state's wire batch", err)
	}
	// A concurrent, speculative attempt builds its own.
	second, err := rt.prepareBatchAttempt(ctx, conns[0], state, nil)
	if err != nil || second == first || len(second.Items) != 1 {
		t.Fatal("concurrent attempt shared the wire batch", err)
	}
	// A repair refills the attempt's own wire batch.
	if again, err := rt.prepareBatchAttempt(ctx, conns[0], state, first); err != nil || again != first {
		t.Fatal("repair did not refill the wire batch", err)
	}
	state.releaseAttempt(second)
	if !state.attemptBusy.Load() {
		t.Fatal("another attempt's release freed the wire batch")
	}
	state.releaseAttempt(first)
	if next, _ := rt.prepareBatchAttempt(ctx, conns[0], state, nil); next != &state.attempt {
		t.Fatal("released wire batch not reused")
	}
}

// The server names one missing statement per UNPREPARED response, and under
// prepared cache pressure it can have evicted several of a batch's
// statements. The batch is repaired once per missing statement.
func TestBatchRepairsEveryEvictedStatement(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	implCluster.SessionData(s).InternalCfg.Keyspace = "ks"
	typ := cqltype.BlobType
	columns := cqltype.NewColumns(cqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: typ})
	query := func(id string) *Query[[]byte, EmptyRecord] {
		state, hp := preparedState(t, s, cqltype.NewColumns())
		hp.InternalId = []byte(id)
		state.InternalStatement = "INSERT INTO ks.tbl(value) VALUES(?) -- " + id
		state.InternalParameters = columns
		state.InternalInfo.PartitionKeyIndexes = []int{0}
		handle := new(Prepared[[]byte, EmptyRecord])
		PreparedData(handle).InternalState = state
		PreparedData(handle).InternalParams = codecTestMust(codec.OneParam(codec.BytesEncoder).Compile(columns))
		PreparedData(handle).InternalRows = codec.EmptyRowDecoder
		PreparedData(handle).InternalPlans = &InternalResultPlanCache[EmptyRecord]{}
		q := codecTestMust(NewQuery(handle, OptionValues{}))
		t.Cleanup(q.Close)
		return q
	}
	first, second := query("id1"), query("id2")
	var batch Batch
	if err := first.AddTo(&batch, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := second.AddTo(&batch, []byte("b")); err != nil {
		t.Fatal(err)
	}
	serverDone := make(chan error, 1)
	go func() {
		// BATCH -> UNPREPARED id1, PREPARE, BATCH -> UNPREPARED id2, PREPARE, BATCH -> void.
		responses := []func(stream int) []byte{
			func(stream int) []byte { return errorResponse(t, stream, protocol.ErrCodeUnprepared, []byte("id1")) },
			func(stream int) []byte {
				return typedTestResponse(t, 4, stream, preparedBody([]byte("id1"), columns, cqltype.NewColumns(), 0))
			},
			func(stream int) []byte { return errorResponse(t, stream, protocol.ErrCodeUnprepared, []byte("id2")) },
			func(stream int) []byte {
				return typedTestResponse(t, 4, stream, preparedBody([]byte("id2"), columns, cqltype.NewColumns(), 0))
			},
			func(stream int) []byte {
				return typedTestResponse(t, 4, stream, binary.BigEndian.AppendUint32(nil, frm.ResultKindVoid))
			},
		}
		for _, respond := range responses {
			_, stream, _, err := readRequest(peers[0])
			if err == nil {
				_, err = peers[0].Write(respond(stream))
			}
			if err != nil {
				serverDone <- err
				return
			}
		}
		serverDone <- nil
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := first.ExecBatch(ctx, LoggedBatch, &batch, Options{}); err != nil {
		t.Fatal(err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

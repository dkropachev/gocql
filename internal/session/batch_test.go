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

package session

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/model"
	implRuntimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/protocol"
)

func batchTestQuery(t testing.TB, s *implCluster.Core) *Query[[]byte, codec.EmptyRecord] {
	t.Helper()
	typ := cqltype.BlobType
	q, err := unpreparedQuery(s, binding.Statement[[]byte, codec.EmptyRecord]{Kind: cqltype.OrdinaryStatement, CQL: "INSERT INTO ks.t(v) VALUES(?)", Params: codec.OneParam(codec.BytesEncoder), Rows: codec.EmptyRowDecoder, BindMarkers: cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: typ})}, Options{})
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

// batchBenchSession returns a session whose one connection answers every
// request with a void result.
func batchBenchSession(b testing.TB) *implCluster.Core {
	return batchServeSession(b, binary.BigEndian.AppendUint32(nil, frm.ResultKindVoid))
}

// batchServeSession returns a session whose one connection answers every
// request with a protocol v4 RESULT of body.
func batchServeSession(b testing.TB, body []byte) *implCluster.Core {
	s, _, peers := runtimeTestSession(b, 1)
	go func() {
		var header [9]byte
		response := typedTestResponse(b, 4, 0, body)
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

func TestBatch(t *testing.T) {
	t.Run("AddBound", func(t *testing.T) {
		t.Run("CopiesBindings", func(t *testing.T) {
			s, _, _ := runtimeTestSession(t, 0)
			a := runtimeQuery(t)
			b := runtimeQuery(t)
			b.Close()
			var batch Batch
			if err := InternalAddBound(&batch, s, b); !errors.Is(err, implRuntimeerrors.ErrClosed) {
				t.Fatal(err)
			}
			if batch.Len() != 0 {
				t.Fatal("closed binding added")
			}
			if err := InternalAddBound(&batch, s, a); err != nil {
				t.Fatal(err)
			}
			a.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := a.Wait(ctx); err != nil {
				t.Fatal("batch retained its binding", err)
			}
			if batch.Len() != 1 {
				t.Fatal(batch.Len())
			}
		})
	})

	t.Run("AddToBatch", func(t *testing.T) {
		t.Run("DoesNotAllocate", func(t *testing.T) {
			q := testOwnedQuery(t)
			var batch Batch
			value := []byte("batch entry")
			fill := func() {
				for i := 0; i < 16; i++ {
					if err := q.AddToBatch(&batch, value); err != nil {
						t.Fatal(err)
					}
				}
				batch.Reset()
			}
			fill() // grow the batch to its working size
			if n := testing.AllocsPerRun(100, fill); n != 0 {
				t.Fatalf("AddToBatch and Reset allocate %v times per batch", n)
			}
		})

		// TestBatch/AddToBatch/EncodesIntoBatchStorage checks that each entry's
		// values are encoded into the batch's own storage, back to back, and
		// that later input mutation leaves them alone.
		t.Run("EncodesIntoBatchStorage", func(t *testing.T) {
			q := testOwnedQuery(t)
			var batch Batch
			first, second := []byte("first"), []byte("second value")
			for _, v := range [][]byte{first, second} {
				if err := q.AddToBatch(&batch, v); err != nil {
					t.Fatal(err)
				}
			}
			first[0], second[0] = 'X', 'X'
			var want []byte
			for _, v := range []string{"first", "second value"} {
				want = binary.BigEndian.AppendUint32(want, uint32(len(v)))
				want = append(want, v...)
			}
			if !bytes.Equal(batch.values, want) {
				t.Fatalf("batch values %q, want %q", batch.values, want)
			}
			if e := batch.entries[1]; e.start != 9 || e.end != len(want) || e.count != 1 {
				t.Fatalf("second entry %+v", e)
			}
			if n := q.buffer.RetainedBytes(); n != 0 {
				t.Fatalf("AddToBatch used the query's buffer: %d bytes retained", n)
			}
		})

		// TestBatch/AddToBatch/FailedEncodingKeepsBatch checks that an encoding
		// error leaves the batch as it was.
		t.Run("FailedEncodingKeepsBatch", func(t *testing.T) {
			s, _ := attachmentSession()
			q, err := unpreparedQuery(s, binding.Statement[[]byte, codec.EmptyRecord]{Kind: cqltype.OrdinaryStatement, CQL: "INSERT INTO ks.t(v) VALUES(?)", Params: codec.OneParam[[]byte](failingBlobEncoder{}), Rows: codec.EmptyRowDecoder, BindMarkers: cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: cqltype.BlobType})}, Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()
			var batch Batch
			if err := q.AddToBatch(&batch, []byte("kept")); err != nil {
				t.Fatal(err)
			}
			values := bytes.Clone(batch.values)
			// The encoder writes its value before failing.
			if err := q.AddToBatch(&batch, []byte("fail")); !errors.Is(err, errFailingBlob) {
				t.Fatal(err)
			}
			if batch.Len() != 1 || !bytes.Equal(batch.values, values) {
				t.Fatalf("failed append changed the batch: %d entries, %q", batch.Len(), batch.values)
			}
		})

		// TestBatch/AddToBatch/BusyQuery checks that a query that is executing,
		// or holds an open handle, still appends.
		t.Run("BusyQuery", func(t *testing.T) {
			q := testOwnedQuery(t)
			if err := q.state.begin(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer q.state.owner.InternalRelease()
			var batch Batch
			if err := q.AddToBatch(&batch, []byte("v")); err != nil {
				t.Fatal(err)
			}
			if _, err := q.Routing([]byte("v")); err != nil {
				t.Fatal("Routing of a busy query:", err)
			}
			if batch.Len() != 1 {
				t.Fatal(batch.Len())
			}
		})

		// TestBatch/AddToBatch/PreparedTakesNoPooledQuery checks that
		// Prepared.AddToBatch encodes without creating the statement's pool.
		t.Run("PreparedTakesNoPooledQuery", func(t *testing.T) {
			s, _, peers := runtimeTestSession(t, 1)
			defer s.Close()
			defer peers[0].Close()
			p := oneShotPrepare(t, s, peers[0], make(chan oneShotRequest, 1))
			var batch Batch
			for i := range int32(3) {
				if err := p.AddToBatch(&batch, i); err != nil {
					t.Fatal(err)
				}
			}
			if batch.Len() != 3 {
				t.Fatal(batch.Len())
			}
			if p.privateStorage.pool != nil || queryPools(s) != 0 {
				t.Fatal("AddToBatch created a query pool")
			}
			fill := func() {
				batch.Reset()
				for i := range int32(16) {
					if err := p.AddToBatch(&batch, i); err != nil {
						t.Fatal(err)
					}
				}
			}
			fill()
			if n := testing.AllocsPerRun(100, fill); n != 0 {
				t.Fatalf("Prepared.AddToBatch and Reset allocate %v times per batch", n)
			}
		})
	})

	t.Run("Exec", func(t *testing.T) {
		t.Run("AttemptReusesWireBatch", func(t *testing.T) {
			s, conns, _ := runtimeTestSession(t, 1)
			q := batchTestQuery(t, s)
			var batch Batch
			if err := q.AddToBatch(&batch, []byte("v")); err != nil {
				t.Fatal(err)
			}
			state := batch.begin()
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
		})

		// The server names one missing statement per UNPREPARED response, and under
		// prepared cache pressure it can have evicted several of a batch's
		// statements. The batch is repaired once per missing statement.
		t.Run("RepairsEveryEvictedStatement", func(t *testing.T) {
			s, _, peers := runtimeTestSession(t, 1)
			implCluster.SessionData(s).InternalCfg.Keyspace = "ks"
			typ := cqltype.BlobType
			columns := cqltype.NewColumns(cqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: typ})
			query := func(id string) *Query[[]byte, codec.EmptyRecord] {
				state, hp := preparedState(t, s, cqltype.NewColumns())
				hp.InternalId = []byte(id)
				state.InternalStatement = "INSERT INTO ks.tbl(value) VALUES(?) -- " + id
				state.InternalParameters = columns
				state.InternalInfo.PartitionKeyIndexes = []int{0}
				handle := new(Prepared[[]byte, codec.EmptyRecord])
				PreparedData(handle).InternalState = state
				PreparedData(handle).params = codecTestMust(codec.OneParam(codec.BytesEncoder).Compile(columns))
				PreparedData(handle).rows = codec.EmptyRowDecoder
				PreparedData(handle).plans = &InternalResultPlanCache[codec.EmptyRecord]{}
				q := codecTestMust(handle.Query(Options{}))
				t.Cleanup(q.Close)
				return q
			}
			first, second := query("id1"), query("id2")
			var batch Batch
			if err := first.AddToBatch(&batch, []byte("a")); err != nil {
				t.Fatal(err)
			}
			if err := second.AddToBatch(&batch, []byte("b")); err != nil {
				t.Fatal(err)
			}
			serverDone := make(chan error, 1)
			go func() {
				// BATCH -> UNPREPARED id1, PREPARE, BATCH -> UNPREPARED id2, PREPARE, BATCH -> void.
				responses := []func(stream int) []byte{
					func(stream int) []byte { return errorResponse(t, stream, protocol.CodeUnprepared, []byte("id1")) },
					func(stream int) []byte {
						return typedTestResponse(t, 4, stream, preparedBody([]byte("id1"), columns, cqltype.NewColumns(), 0))
					},
					func(stream int) []byte { return errorResponse(t, stream, protocol.CodeUnprepared, []byte("id2")) },
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
			if _, err := InternalWrapSession(s).ExecBatch(ctx, &batch); err != nil {
				t.Fatal(err)
			}
			if err := <-serverDone; err != nil {
				t.Fatal(err)
			}
		})

		t.Run("SendsEntriesAndReusesStorage", func(t *testing.T) {
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
				if err := q.AddToBatch(&batch, []byte(v)); err != nil {
					t.Fatal(err)
				}
			}
			if batch.Len() != 2 {
				t.Fatal(batch.Len())
			}
			// A batch may be sent again unchanged.
			for i := 0; i < 2; i++ {
				if _, err := InternalWrapSession(s).ExecBatch(ctx, &batch); err != nil {
					t.Fatal(err)
				}
				r := <-requests
				if r.op != byte(frm.OpBatch) || r.body[0] != byte(cqltype.LoggedBatch) || !bytes.Contains(r.body, []byte("first")) || !bytes.Contains(r.body, []byte("second")) {
					t.Fatalf("request %d: op %#x body %q", i, r.op, r.body)
				}
			}
			if !batch.drained() || batch.shared {
				t.Fatal("a completed execution did not drain")
			}
			values := &batch.values[0]
			batch.Kind = cqltype.UnloggedBatch
			batch.Reset()
			if batch.Len() != 0 || batch.Kind != cqltype.UnloggedBatch {
				t.Fatal(batch.Len(), batch.Kind)
			}
			if err := q.AddToBatch(&batch, []byte("third")); err != nil {
				t.Fatal(err)
			}
			if &batch.values[0] != values {
				t.Fatal("Reset did not keep the storage")
			}
			if _, err := InternalWrapSession(s).ExecBatch(ctx, &batch); err != nil {
				t.Fatal(err)
			}
			r := <-requests
			if r.body[0] != byte(cqltype.UnloggedBatch) || !bytes.Contains(r.body, []byte("third")) || bytes.Contains(r.body, []byte("first")) {
				t.Fatalf("body %q", r.body)
			}
		})
	})

	t.Run("Reset", func(t *testing.T) {
		t.Run("LeavesStorageToUndrainedExecution", func(t *testing.T) {
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
					batch.queryExec(&request, &sender.state)
					return sender.state.owner.InternalRelease
				}},
				{"session", func(batch *Batch) func() {
					owner := InternalNewInputOwner()
					if err := owner.InternalAdmit(); err != nil {
						t.Fatal(err)
					}
					owner.InternalClose()
					batch.begin()
					batch.lastOwner, batch.lastQuery = owner, nil
					return owner.InternalRelease
				}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var batch Batch
					if err := q.AddToBatch(&batch, []byte("in flight")); err != nil {
						t.Fatal(err)
					}
					release := tc.start(&batch)
					inflight, entry := batch.values, batch.entries[0]
					batch.Reset()
					if err := q.AddToBatch(&batch, []byte("next batch")); err != nil {
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
					if err := q.AddToBatch(&batch, []byte("reused")); err != nil {
						t.Fatal(err)
					}
					if &batch.values[0] != values {
						t.Fatal("Reset did not reuse drained storage")
					}
				})
			}
		})
	})

	t.Run("StatementKind", func(t *testing.T) {
		s, _, _ := runtimeTestSession(t, 0)
		for _, tc := range []struct {
			kinds []cqltype.StatementKind
			want  cqltype.StatementKind
		}{
			{[]cqltype.StatementKind{cqltype.OrdinaryStatement, cqltype.OrdinaryStatement}, cqltype.OrdinaryStatement},
			{[]cqltype.StatementKind{cqltype.UnknownStatement}, cqltype.UnknownStatement},
			{[]cqltype.StatementKind{cqltype.OrdinaryStatement, cqltype.UnknownStatement}, cqltype.UnknownStatement},
			{[]cqltype.StatementKind{cqltype.UnknownStatement, cqltype.ConditionalStatement, cqltype.OrdinaryStatement}, cqltype.ConditionalStatement},
		} {
			var batch Batch
			for _, kind := range tc.kinds {
				if err := batch.add(s, cqlText{}, "UPDATE t SET v=1", &model.RoutingInfo{Kind: kind}, model.EncodedValues{}); err != nil {
					t.Fatal(err)
				}
			}
			if batch.routing.Kind != tc.want {
				t.Errorf("%v: kind %v, want %v", tc.kinds, batch.routing.Kind, tc.want)
			}
		}
	})

	t.Run("Validation", func(t *testing.T) {
		s, _, _ := runtimeTestSession(t, 0)
		q := batchTestQuery(t, s)
		session := InternalWrapSession(s)
		other := testOwnedQuery(t)
		otherSession := InternalWrapSession(other.state.runtime.Core)
		ctx := context.Background()
		var batch Batch
		if _, err := session.ExecBatch(ctx, &batch); err == nil {
			t.Fatal("empty batch sent")
		}
		if err := q.AddToBatch(nil, []byte("v")); err == nil {
			t.Fatal("nil batch accepted")
		}
		if err := q.AddToBatch(&batch, []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := other.AddToBatch(&batch, []byte("v")); err == nil || batch.Len() != 1 {
			t.Fatal("statement of another session added", err)
		}
		if _, err := otherSession.ExecBatch(ctx, &batch); err == nil {
			t.Fatal("batch sent on another session")
		}
		batch.Kind = cqltype.BatchKind(42)
		if _, err := session.ExecBatch(ctx, &batch); err == nil {
			t.Fatal("unknown batch kind sent")
		}
		batch.Kind = cqltype.LoggedBatch
		if err := q.state.begin(ctx); err != nil {
			t.Fatal(err)
		}
		// AddToBatch uses none of the query's execution state.
		if err := q.AddToBatch(&batch, []byte("v")); err != nil {
			t.Fatal("busy query did not append", err)
		}
		q.state.owner.InternalRelease()
		closeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		q.Close()
		if err := q.Wait(closeCtx); err != nil {
			t.Fatal(err)
		}
		if err := q.AddToBatch(&batch, []byte("v")); !errors.Is(err, implRuntimeerrors.ErrClosed) {
			t.Fatal("closed query appended", err)
		}
		if batch.Len() != 2 {
			t.Fatal(batch.Len())
		}
	})
}

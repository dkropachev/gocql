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

package execution

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/binding"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wire"
	"github.com/scylladb/gocql/v2/retry"
)

type countSpeculation struct{ calls atomic.Int32 }

func (p *countSpeculation) Delay(int, routing.MetricsSnapshot) (time.Duration, bool) {
	p.calls.Add(1)
	return 0, false
}

type panicObserver struct{}

func (panicObserver) ObserveRequest(settings.RequestEvent) { panic("request") }
func (panicObserver) ObserveAttempt(routing.AttemptEvent)  { panic("attempt") }

type panicLogger struct{}

func (panicLogger) Print(...any)          { panic("logger") }
func (panicLogger) Printf(string, ...any) { panic("logger") }
func (panicLogger) Println(...any)        { panic("logger") }
func TestExecution(t *testing.T) {
	t.Run("Bind", func(t *testing.T) {
		t.Run("CanonicalAssembly", func(t *testing.T) {
			for _, size := range []int{0, 32, 4096, 500 << 10} {
				for _, proto := range []byte{3, 4, 5} {
					for _, owned := range []bool{false, true} {
						t.Run(fmt.Sprintf("bytes%d/v%d/owned%t", size, proto, owned), func(t *testing.T) {
							typ := cqltype.BlobType
							cols := cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: typ})
							calls := 0
							input := bytes.Repeat([]byte{0x71}, size)
							statement := binding.Statement[[]byte, codec.EmptyRecord]{Kind: cqltype.OrdinaryStatement, CQL: "INSERT INTO ks.tbl(v)VALUES(?)", Params: codec.OneParam[[]byte](countingBlobEncoder{calls: &calls}), Rows: codec.EmptyRowDecoder}
							var bound *Bound[codec.EmptyRecord]
							if owned {
								bound = codecTestMust(BindUnpreparedOwned(statement, cols, input))
							} else {
								bound = codecTestMust(BindUnpreparedBorrowed(statement, cols, input))
							}
							defer bound.Close()
							canonical := BoundData(bound).InternalRequest.InternalValues
							c := &transport.Conn{InternalVersion: proto}
							c.InternalInitFramerCache()
							for _, batch := range []bool{false, true} {
								for attempt := 0; attempt < 4; attempt++ {
									request := &model.AttemptRequest{Statement: statement.CQL, Values: canonical.Framed, ValueCount: 1, Consistency: cqltype.One}
									if batch {
										request.Batch = &model.TypedBatchRequest{Kind: cqltype.LoggedBatch, Items: []model.TypedBatchItem{{Statement: statement.CQL, Values: canonical.Framed, ValueCount: 1}, {Statement: statement.CQL, Values: canonical.Framed, ValueCount: 1}}}
									}
									frame := codecTestMust(c.InternalBuildTypedRequest(request, attempt))
									retained := int64(0)
									frame.Each(func(s wire.Span) error {
										p := s.Bytes()
										if len(p) == 0 {
											return nil
										}
										start := uintptr(unsafe.Pointer(unsafe.SliceData(p)))
										canonical.Cells[0].Body.Each(func(original wire.Span) error {
											o := original.Bytes()
											if len(o) > 0 {
												lo := uintptr(unsafe.Pointer(unsafe.SliceData(o)))
												if start >= lo && start+uintptr(len(p)) <= lo+uintptr(len(o)) {
													retained += int64(len(p))
												}
											}
											return nil
										})
										return nil
									})
									want := int64(size)
									if batch {
										want *= 2
									}
									if retained != want {
										t.Fatalf("attempt=%d batch=%t canonical bytes=%d want=%d", attempt, batch, retained, want)
									}
								}
							}
							if calls != 1 {
								t.Fatal("secondary encode", calls)
							}
							t.Logf("encoder_calls=1 attempt_envelopes=8 payload_backing_reused bytes=%d", size)
						})
					}
				}
			}
		})

		t.Run("KindDeclarationAndServerRefinement", func(t *testing.T) {
			for _, kind := range []cqltype.StatementKind{cqltype.UnknownStatement, cqltype.OrdinaryStatement, cqltype.ConditionalStatement} {
				for _, cql := range []string{"SELECT value FROM ks.tbl", "INSERT INTO t (x) VALUES (1) IF NOT EXISTS", "SELECT 'IF' FROM t"} {
					b, err := BindUnpreparedOwned(binding.Statement[codec.EmptyRecord, codec.EmptyRecord]{CQL: cql, Kind: kind, Params: codec.EmptyParamsEncoder, Rows: codec.EmptyRowDecoder}, cqltype.NewColumns(), codec.EmptyRecord{})
					if err != nil {
						t.Fatal(err)
					}
					if b.Routing().Kind != kind || b.Routing().Conditional != (kind != cqltype.OrdinaryStatement) {
						t.Fatal(cql, b.Routing())
					}
					b.Close()
				}
			}
			s, cs, _ := runtimeTestSession(t, 1)
			for _, test := range []struct {
				negotiated, lwt bool
				want            cqltype.StatementKind
			}{{false, false, cqltype.UnknownStatement}, {true, false, cqltype.OrdinaryStatement}, {true, true, cqltype.ConditionalStatement}, {false, true, cqltype.ConditionalStatement}} {
				cs[0].InternalFramers.InternalDefaults.InternalFlagLWT = 0
				if test.negotiated {
					cs[0].InternalFramers.InternalDefaults.InternalFlagLWT = 1 << 30
				}
				cs[0].InternalRefreshNativeProtocol()
				info, err := InternalRuntimeFor(s).InternalHostPreparedInfo(cs[0], &transport.InternalResponsePage{InternalResponseData: model.ResponseData{Lwt: test.lwt}}, &InternalPreparedState{InternalStatement: "INSERT x IF y"})
				if err != nil || info.Kind != test.want {
					t.Fatal(info, err)
				}
			}
		})
	})

	// TestExecution/CanExecuteDirect checks that executions without a reusable
	// query take the direct path unless an observer, speculation, write
	// coalescing or a stream-observed connection needs the general one.
	t.Run("CanExecuteDirect", func(t *testing.T) {
		rt := &InternalSessionRuntime{}
		request := func() *InternalExecutionRequest {
			return &InternalExecutionRequest{InternalLogical: &InternalLogicalExecution{}, InternalOptions: InternalResolvedOptions{InternalProfile: defaultProfile()}}
		}
		if !canExecuteDirect(rt, request()) {
			t.Fatal("one-shot execution should run direct")
		}
		r := request()
		r.query = &queryState{}
		if !canExecuteDirect(rt, r) {
			t.Fatal("query execution should run direct")
		}
		r = request()
		r.InternalLogical.observer = directTestObserver{}
		if canExecuteDirect(rt, r) {
			t.Fatal("an observer needs the general path")
		}
		r = request()
		r.connection = &Conn{InternalStreamObserver: directTestStreamObserver{}}
		if canExecuteDirect(rt, r) {
			t.Fatal("a stream-observed connection needs the general path")
		}
		r = request()
		r.InternalLogical = nil
		if canExecuteDirect(rt, r) {
			t.Fatal("an unadmitted request needs the general path")
		}
		coalescing := &InternalSessionRuntime{}
		coalescing.InternalConfig.CoalesceWait = time.Millisecond
		if canExecuteDirect(coalescing, request()) {
			t.Fatal("write coalescing needs the general path")
		}
	})

	t.Run("Exec", func(t *testing.T) {
		t.Run("CancellationAfterVoidAck", func(t *testing.T) {
			for _, closeSession := range []bool{false, true} {
				name := "caller"
				if closeSession {
					name = "session"
				}
				t.Run(name, func(t *testing.T) {
					s, conns, peers := runtimeTestSession(t, 1)
					acknowledged := make(chan struct{})
					continueAttempt := make(chan struct{})
					observer := &execVoidObserver{onAttempt: func(event routing.AttemptEvent) {
						if event.Err == nil {
							// The native VOID reply has been parsed, but execution has not
							// accepted the successful attempt's outcome yet.
							close(acknowledged)
							<-continueAttempt
						}
					}}
					go func() {
						stream := typedTestReadRequest(t, conns[0], peers[0])
						_, _ = peers[0].Write(typedTestResponse(t, 4, stream, []byte{0, 0, 0, frm.ResultKindVoid}))
					}()
					b := runtimeQuery(t)
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					done := make(chan error, 1)
					go func() {
						_, err := Exec(ctx, s, b, testOptions(Options{Observer: observer}))
						done <- err
					}()
					select {
					case <-acknowledged:
					case <-time.After(time.Second):
						t.Fatal("VOID acknowledgement was not observed")
					}
					closed := make(chan error, 1)
					if closeSession {
						go func() { closed <- s.Close() }()
					} else {
						cancel()
					}
					select {
					case err := <-done:
						if !errors.Is(err, context.Canceled) && !errors.Is(err, runtimeerrors.ErrClosed) {
							t.Fatal(err)
						}
					case <-time.After(time.Second):
						t.Fatal("Exec ignored cancellation after the acknowledgement")
					}
					close(continueAttempt)
					b.Close()
					joined, stop := context.WithTimeout(context.Background(), time.Second)
					defer stop()
					if err := b.Wait(joined); err != nil {
						t.Fatal(err)
					}
					if closeSession {
						select {
						case err := <-closed:
							if err != nil {
								t.Fatal(err)
							}
						case <-time.After(time.Second):
							t.Fatal("session close did not join the acknowledged attempt")
						}
					}
					if m := s.Metrics(); m.Requests != 1 || m.Attempts != 1 || m.InFlight != 0 || m.Errors != 1 {
						t.Fatal(m)
					}
				})
			}
		})

		t.Run("CancellationDrainsAcceptedInput", func(t *testing.T) {
			for _, closeSession := range []bool{false, true} {
				name := "caller"
				if closeSession {
					name = "session"
				}
				t.Run(name, func(t *testing.T) {
					s, conns, peers := runtimeTestSession(t, 1)
					accepted := make(chan struct{})
					reply := make(chan struct{})
					served := make(chan struct{})
					go func() {
						defer close(served)
						stream := typedTestReadRequest(t, conns[0], peers[0])
						close(accepted)
						<-reply
						_, _ = peers[0].Write(typedTestResponse(t, 4, stream, []byte{0, 0, 0, frm.ResultKindVoid}))
					}()
					b := runtimeQuery(t)
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					done := make(chan error, 1)
					go func() {
						_, err := Exec(ctx, s, b, BoundOptions{})
						done <- err
					}()
					<-accepted
					b.Close()
					short, stop := context.WithTimeout(context.Background(), 5*time.Millisecond)
					if err := b.Wait(short); !errors.Is(err, context.DeadlineExceeded) {
						t.Fatal("accepted input was released before acknowledgement/cancellation", err)
					}
					stop()
					if closeSession {
						if err := s.Close(); err != nil {
							t.Fatal(err)
						}
					} else {
						cancel()
					}
					select {
					case err := <-done:
						if !errors.Is(err, context.Canceled) && !errors.Is(err, runtimeerrors.ErrClosed) {
							t.Fatal(err)
						}
					case <-time.After(time.Second):
						t.Fatal("Exec did not stop after cancellation")
					}
					close(reply)
					joined, stop := context.WithTimeout(context.Background(), time.Second)
					defer stop()
					if err := b.Wait(joined); err != nil {
						t.Fatal(err)
					}
					<-served
					if m := s.Metrics(); m.Requests != 1 || m.InFlight != 0 || m.Errors != 1 {
						t.Fatal(m)
					}
				})
			}
		})

		t.Run("DrainsAndValidatesNonVoidReplies", func(t *testing.T) {
			emptyTrailing := binary.BigEndian.AppendUint32(nil, frm.ResultKindRows)
			emptyTrailing = binary.BigEndian.AppendUint32(emptyTrailing, 0) // flags
			emptyTrailing = binary.BigEndian.AppendUint32(emptyTrailing, 0) // columns
			emptyTrailing = binary.BigEndian.AppendUint32(emptyTrailing, 0) // rows
			emptyTrailing = append(emptyTrailing, 99)
			for _, tc := range []struct {
				name   string
				bodies [][]byte
				rows   int64
				column bool
			}{
				{"empty-columns", [][]byte{runtimeRowsBody(nil)}, 0, true},
				{"paged-columns", [][]byte{runtimeRowsBody([]byte{1}, "first"), runtimeRowsBody(nil, "second")}, 2, true},
				{"malformed-empty-rows", [][]byte{emptyTrailing}, 0, false},
				{"malformed-void", [][]byte{{0, 0, 0, frm.ResultKindVoid, 99}}, 0, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					s, conns, peers := runtimeTestSession(t, 1)
					served := make(chan struct{})
					go func() {
						defer close(served)
						for _, body := range tc.bodies {
							stream := typedTestReadRequest(t, conns[0], peers[0])
							if _, err := peers[0].Write(typedTestResponse(t, 4, stream, body)); err != nil {
								t.Error(err)
								return
							}
						}
					}()
					b := runtimeQuery(t)
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					result, err := Exec(ctx, s, b, BoundOptions{})
					if err == nil || result.Rows != tc.rows {
						t.Fatalf("result = %+v, %v", result, err)
					}
					if tc.column && (!strings.Contains(err.Error(), "result columns require Run") || result.Attempts != len(tc.bodies)) {
						t.Fatalf("paged column rejection changed: %+v, %v", result, err)
					}
					b.Close()
					if err := b.Wait(ctx); err != nil {
						t.Fatal(err)
					}
					<-served
					if m := s.Metrics(); m.Requests != 1 || m.InFlight != 0 || m.Errors != 1 {
						t.Fatal(m)
					}
				})
			}
		})

		t.Run("VoidCompletionAndMetadata", func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			trace := cqltype.UUID{15: 42}
			served := make(chan struct{})
			go func() {
				defer close(served)
				stream := typedTestReadRequest(t, conns[0], peers[0])
				body := append([]byte{}, trace[:]...)
				body = binary.BigEndian.AppendUint16(body, 1)
				body = binary.BigEndian.AppendUint16(body, uint16(len("warning")))
				body = append(body, "warning"...)
				body = binary.BigEndian.AppendUint16(body, 1) // custom payload entries
				body = binary.BigEndian.AppendUint16(body, 3)
				body = append(body, "key"...)
				body = binary.BigEndian.AppendUint32(body, 3)
				body = append(body, "val"...)
				body = binary.BigEndian.AppendUint32(body, frm.ResultKindVoid)
				response := typedTestResponse(t, 4, stream, body)
				response[1] = frm.FlagTracing | frm.FlagWarning | frm.FlagCustomPayload
				if _, err := peers[0].Write(response); err != nil {
					t.Error(err)
				}
			}()
			b := runtimeQuery(t) // Exec must ignore this binding's Bytes row decoder.
			observer := new(execVoidObserver)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			result, err := Exec(ctx, s, b, testOptions(Options{Trace: true, Observer: observer}))
			if err != nil || result.Attempts != 1 || result.Rows != 0 || len(result.TraceIDs()) != 1 || result.TraceIDs()[0] != trace || len(result.Warnings()) != 1 || result.Warnings()[0] != "warning" {
				t.Fatalf("result = %+v, %v", result, err)
			}
			if len(result.CustomPayload()) != 1 || result.CustomPayload()[0].Key != "key" || string(result.CustomPayload()[0].Value.Value.OwnedBytes()) != "val" {
				t.Fatalf("custom payload = %+v", result.CustomPayload())
			}
			b.Close()
			if err := b.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			<-served
			observer.mu.Lock()
			requests, attempts := observer.requests, observer.attempts
			observer.mu.Unlock()
			if len(requests) != 1 || len(attempts) != 1 || requests[0].Err != nil || requests[0].Result.TraceIDs()[0] != trace || requests[0].Result.Warnings()[0] != "warning" {
				t.Fatalf("observer events = %+v / %+v", requests, attempts)
			}
			if len(requests[0].Result.CustomPayload()) != 1 || string(requests[0].Result.CustomPayload()[0].Value.Value.OwnedBytes()) != "val" {
				t.Fatalf("observer custom payload = %+v", requests[0].Result.CustomPayload())
			}
			if m := s.Metrics(); m.Requests != 1 || m.Attempts != 1 || m.InFlight != 0 || m.Errors != 0 {
				t.Fatal(m)
			}
			if ctx.Err() != nil {
				t.Fatal("Exec cancelled its caller context")
			}
		})
	})

	t.Run("Logger", func(t *testing.T) {
		t.Run("ConcurrentCapture", func(t *testing.T) {
			logger := &logging.InternalTestLogger{}
			done := make(chan struct{}, 8)
			for i := 0; i < 8; i++ {
				go func() {
					for j := 0; j < 100; j++ {
						logger.Print("a")
						logger.Printf("%s", "b")
						logger.Println("c")
						_ = logger.String()
					}
					done <- struct{}{}
				}()
			}
			for i := 0; i < 8; i++ {
				<-done
			}
			if len(logger.String()) != 8*100*4 {
				t.Fatal("lost concurrent log data")
			}
		})

		t.Run("ObserverPanicsContained", func(t *testing.T) {
			logger := logging.InternalV2SafeLogger{InternalLogger: panicLogger{}}
			logger.Print("x")
			logger.Printf("%s", "x")
			logger.Println("x")
			s, cs, ps := runtimeTestSession(t, 1)
			implCluster.SessionData(s).InternalLogger = panicLogger{}
			go func() {
				stream := typedTestReadRequest(t, cs[0], ps[0])
				_, _ = ps[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "row")))
			}()
			b := runtimeQuery(t)
			defer b.Close()
			rows, err := Run(context.Background(), s, b, testOptions(Options{Observer: panicObserver{}}))
			if err != nil {
				t.Fatal(err)
			}
			var row cqltype.Bytes
			if !rows.NextInto(&row) {
				t.Fatal(rows.Err())
			}
			rows.Close()
			if m := s.Metrics(); m.Requests != 1 || m.Attempts != 1 || m.InFlight != 0 {
				t.Fatal(m)
			}
		})
	})

	t.Run("QualifiedForKeyspace", func(t *testing.T) {
		for _, test := range []struct {
			cql, keyspace string
			want          bool
		}{
			{"SELECT value FROM ks.tbl", "ks", true}, {"INSERT INTO ks.tbl(k)VALUES('ks.tbl')", "ks", true}, {"UPDATE ks.tbl SET value=1 WHERE k=2", "ks", true}, {"DELETE FROM ks.tbl WHERE k=2", "ks", true}, {"TRUNCATE TABLE ks.tbl", "ks", true}, {"CREATE TABLE IF NOT EXISTS ks.tbl(k int PRIMARY KEY)", "ks", true}, {"SELECT * FROM \"Ks\".\"Tbl\"", "Ks", true}, {"SELECT * FROM tbl WHERE value='ks.tbl'", "ks", false}, {"SELECT * FROM other.tbl -- ks.tbl", "ks", false}, {"SELECT * FROM ks.tbl /* unfinished", "ks", false},
		} {
			if got := InternalQualifiedForKeyspace(test.cql, test.keyspace); got != test.want {
				t.Fatal(test.cql, got, test.want)
			}
		}
	})

	t.Run("Retry", func(t *testing.T) {
		t.Run("Delayed", func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			profile := settings.InternalDefaultProfile()
			profile.Retry = runtimeDelayedRetry{delay: time.Millisecond}
			handle := new(profile)
			done := make(chan struct{})
			go func() {
				defer close(done)
				for attempt := 0; attempt < 3; attempt++ {
					stream := typedTestReadRequest(t, conns[0], peers[0])
					response := errorResponse(t, stream, protocol.ErrCodeOverloaded, nil)
					if attempt == 2 {
						response = typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "retried"))
					}
					if _, err := peers[0].Write(response); err != nil {
						t.Error(err)
						return
					}
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			bound := runtimeQuery(t)
			page, err := RunPage(ctx, s, bound, model.StartPage(), testOptions(Options{Profile: handle}))
			if err != nil {
				t.Fatal(err)
			}
			var row cqltype.Bytes
			if !page.NextInto(&row) || string(row.OwnedBytes()) != "retried" {
				t.Fatal("delayed retry result", page.Err())
			}
			page.Close()
			bound.Close()
			if err := bound.Wait(ctx); err != nil {
				t.Fatal("delayed retries did not drain", err)
			}
			<-done
			if got := s.Metrics(); got.Attempts != 3 || got.Retries != 2 {
				t.Fatalf("delayed retry accounting: %+v", got)
			}
		})

		t.Run("DelayedCancellationDrains", func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			scheduled := make(chan struct{}, 1)
			profile := settings.InternalDefaultProfile()
			profile.Retry = runtimeDelayedRetry{delay: time.Hour, scheduled: scheduled}
			handle := new(profile)
			go func() {
				stream := typedTestReadRequest(t, conns[0], peers[0])
				_, _ = peers[0].Write(errorResponse(t, stream, protocol.ErrCodeOverloaded, nil))
			}()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			bound := runtimeQuery(t)
			finished := make(chan error, 1)
			go func() {
				page, err := RunPage(ctx, s, bound, model.StartPage(), testOptions(Options{Profile: handle}))
				if page != nil {
					page.Close()
				}
				finished <- err
			}()
			deadline, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			select {
			case <-scheduled:
			case <-deadline.Done():
				t.Fatal("retry was not scheduled")
			}
			cancel()
			select {
			case err := <-finished:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled retry: %v", err)
				}
			case <-deadline.Done():
				t.Fatal("cancelled delayed retry did not return")
			}
			bound.Close()
			if err := bound.Wait(deadline); err != nil {
				t.Fatal("cancelled delayed retry retained input", err)
			}
			if got := s.Metrics(); got.Attempts != 1 || got.Retries != 0 {
				t.Fatalf("cancelled delay launched an attempt: %+v", got)
			}
		})

		t.Run("SafeBytesWritten", func(t *testing.T) {
			for _, written := range []bool{false, true} {
				err := runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: errors.New("write"), InternalPotentiallyExecuted: written})
				status := runtimeerrors.ExecutionStatusOf(err)
				decision := (retry.Safe{}).Decide(retry.Attempt{Err: err, Number: 1, BytesWritten: InternalAttemptBytesWritten(err), ExecutionStatus: status})
				if (decision.Action == retry.Next) == written {
					t.Fatal(written, decision)
				}
			}
			if !InternalAttemptBytesWritten(errors.New("unknown completion")) {
				t.Fatal("unproven write classified safe")
			}
		})
	})

	t.Run("Speculation", func(t *testing.T) {
		t.Run("LoserWriterKeepsInput", func(t *testing.T) {
			for _, pageTimeout := range []time.Duration{0, time.Second} {
				t.Run(pageTimeout.String(), func(t *testing.T) {
					runtimeSpeculativeLoserWriterKeepsInput(t, pageTimeout)
				})
			}
		})

		t.Run("UnknownConditionalNever", func(t *testing.T) {
			for _, kind := range []cqltype.StatementKind{cqltype.UnknownStatement, cqltype.OrdinaryStatement, cqltype.ConditionalStatement} {
				t.Run(map[cqltype.StatementKind]string{cqltype.UnknownStatement: "unknown", cqltype.OrdinaryStatement: "ordinary", cqltype.ConditionalStatement: "conditional"}[kind], func(t *testing.T) {
					s, cs, ps := runtimeTestSession(t, 1)
					spec := &countSpeculation{}
					profile := settings.InternalDefaultProfile()
					profile.Speculation = spec
					profile.Prefetch = 0
					handle := new(profile)
					seen := make(chan struct{})
					resume := make(chan struct{})
					done := make(chan struct{})
					go func() {
						defer close(done)
						stream := typedTestReadRequest(t, cs[0], ps[0])
						close(seen)
						<-resume
						_, _ = ps[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "result")))
					}()
					b := runtimeQuery(t)
					BoundData(b).InternalRequest.InternalRouting.Kind = kind
					BoundData(b).InternalRequest.InternalRouting.Conditional = kind != cqltype.OrdinaryStatement
					result := make(chan error, 1)
					go func() {
						page, err := RunPage(context.Background(), s, b, model.StartPage(), testOptions(Options{Profile: handle, Idempotent: true}))
						if page != nil {
							page.Close()
						}
						result <- err
					}()
					<-seen
					close(resume)
					if err := <-result; err != nil {
						t.Fatal(err)
					}
					want := int32(0)
					if kind == cqltype.OrdinaryStatement {
						want = 1
					}
					if spec.calls.Load() != want {
						t.Fatal("speculation classification", kind, spec.calls.Load())
					}
					b.Close()
					<-done
				})
			}
		})
	})
}

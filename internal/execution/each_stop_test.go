// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/codec"
)

func TestEachStopIteration(t *testing.T) {
	callbackFailure := errors.New("processing failed")
	for _, tc := range []struct {
		name      string
		err       error
		stopAt    int
		wantError bool
	}{
		{name: "FirstRow", err: StopIteration, stopAt: 1},
		{name: "LaterPageWrapped", err: fmt.Errorf("found enough: %w", StopIteration), stopAt: 3},
		{name: "CallbackFailure", err: callbackFailure, stopAt: 1, wantError: true},
		{name: "JoinedFailure", err: errors.Join(StopIteration, callbackFailure), stopAt: 1, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, serve := eachTestQuery(t)
			observer := new(execVoidObserver)
			rt := q.state.runtime
			rt.InternalConfig.Observer = observer
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			pages := [][]byte{runtimeRowsBody([]byte{1}, "a", "b")}
			if tc.stopAt > 2 {
				pages = append(pages, runtimeRowsBody([]byte{2}, "c", "d"))
			}
			served := serve(pages...)
			var dst []byte
			calls := 0
			result, err := q.Each(ctx, codec.EmptyRecord{}, &dst, func(*[]byte) error {
				calls++
				if calls == tc.stopAt {
					return tc.err
				}
				return nil
			})
			<-served
			if calls != tc.stopAt {
				t.Fatalf("callbacks %d, want %d", calls, tc.stopAt)
			}
			if tc.wantError {
				if !errors.Is(err, tc.err) {
					t.Fatalf("Each error %v, want %v", err, tc.err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			// Row accounting describes fetched pages, including rows not visited.
			if result.Rows != int64(len(pages)*2) {
				t.Fatalf("fetched rows %d", result.Rows)
			}
			observer.mu.Lock()
			if len(observer.requests) != 1 {
				t.Fatalf("request events %d", len(observer.requests))
			}
			eventErr := observer.requests[0].Err
			observer.mu.Unlock()
			wantErrors := uint64(0)
			if tc.wantError {
				wantErrors = 1
				if !errors.Is(eventErr, tc.err) {
					t.Fatalf("observer error %v", eventErr)
				}
			} else if eventErr != nil {
				t.Fatalf("successful stop observed as %v", eventErr)
			}
			if got := rt.errors.Load(); got != wantErrors {
				t.Fatalf("error metric %d, want %d", got, wantErrors)
			}
			if rt.cancelled.Load() != 0 || rt.inflight.Load() != 0 {
				t.Fatal("stop left cancelled or inflight execution")
			}
			// The server expects the next request to be a new execution, not a
			// continuation; successful stop must release the query's admission.
			served = serve(runtimeRowsBody(nil, "next"))
			found, _, err := q.One(ctx, codec.EmptyRecord{}, &dst)
			<-served
			if err != nil || !found || string(dst) != "next" {
				t.Fatalf("query reuse: %t %q %v", found, dst, err)
			}
		})
	}
}

// An error from decoding is never successful callback control flow, even if it
// happens to be the same sentinel.
type stopDecodeRows struct{}

func (d stopDecodeRows) Compile(Columns) (RowPlan[[]byte], error) { return d, nil }
func (d stopDecodeRows) Clone(v []byte) []byte                    { return append([]byte(nil), v...) }
func (stopDecodeRows) Decode(*RowReader) ([]byte, error)          { return nil, StopIteration }
func (stopDecodeRows) DecodeInto(*RowReader, *[]byte) error       { return StopIteration }

func TestEachStopIterationOnlyFromCallback(t *testing.T) {
	q, serve := eachTestQuery(t)
	q.bound.privateStorage.InternalRows = stopDecodeRows{}
	q.bound.privateStorage.plans = &InternalResultPlanCache[[]byte]{}
	served := serve(runtimeRowsBody(nil, "a"))
	var dst []byte
	calls := 0
	_, err := q.Each(context.Background(), codec.EmptyRecord{}, &dst, func(*[]byte) error { calls++; return nil })
	<-served
	if !errors.Is(err, StopIteration) || calls != 0 || q.state.runtime.errors.Load() != 1 {
		t.Fatalf("decoder error was swallowed: calls=%d err=%v metric=%d", calls, err, q.state.runtime.errors.Load())
	}
}

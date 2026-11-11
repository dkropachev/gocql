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
	"errors"
	"fmt"
	"hash/maphash"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	implCqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	implModel "github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/paging"
	implPaging "github.com/scylladb/gocql/v2/internal/paging"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/settings"
)

func TestPaging(t *testing.T) {
	t.Run("History", func(t *testing.T) {
		t.Run("CyclesLimitsAndTokenIdentity", func(t *testing.T) {
			h := implPaging.InternalPagingHistory{InternalMaxCount: 4, InternalMaxBytes: 4}
			a, b := implModel.PageStateFromToken([]byte("a")), implModel.PageStateFromToken([]byte("b"))
			if h.InternalAdd(a) != nil || h.InternalAdd(b) != nil || !errors.Is(h.InternalAdd(a), runtimeerrors.ErrPagingNoProgress) {
				t.Fatal("nonconsecutive cycle")
			}
			if h.InternalCount != 2 || h.InternalBytes != 2 {
				t.Fatal("cycle stored twice")
			}
			entry := h.InternalEntries
			found := false
			for _, v := range entry {
				if bytes.Equal(model.PageToken(v.InternalState), model.PageToken(a)) {
					found = &model.PageToken(v.InternalState)[0] == &model.PageToken(a)[0]
				}
			}
			if !found {
				t.Fatal("history copied payload")
			}
			if h.InternalAdd(implModel.PageStateFromToken(nil)) != nil || !errors.Is(h.InternalAdd(implModel.PageStateFromToken([]byte{})), runtimeerrors.ErrPagingNoProgress) {
				t.Fatal("empty cycle")
			}
			if !errors.Is(h.InternalAdd(implModel.PageStateFromToken([]byte("long"))), runtimeerrors.ErrPagingStateLimit) || h.InternalCount != 3 {
				t.Fatal("byte limit")
			}
			h = implPaging.InternalPagingHistory{InternalMaxCount: 1, InternalMaxBytes: 100}
			if h.InternalAdd(a) != nil || !errors.Is(h.InternalAdd(b), runtimeerrors.ErrPagingStateLimit) {
				t.Fatal("count limit")
			}
		})

		t.Run("HashCollisionComparesFullBytes", func(t *testing.T) {
			a, b := implModel.PageStateFromToken([]byte("a")), implModel.PageStateFromToken([]byte("b"))
			h := implPaging.InternalPagingHistory{InternalMaxCount: 4, InternalMaxBytes: 8}
			_ = h.InternalAdd(b)
			h.InternalSpill()
			hash := maphash.Bytes(h.InternalSeed, model.PageToken(b))
			h.InternalEntries[hash] = implPaging.InternalPagingHistoryEntry{InternalState: a}
			h.InternalCount = 1
			h.InternalBytes = 1
			if err := h.InternalAdd(b); err != nil {
				t.Fatal("hash collision mistaken for equality", err)
			}
			if h.InternalCount != 2 || h.InternalEntries[hash].InternalNext == nil {
				t.Fatal("collision entry lost")
			}
			if !errors.Is(h.InternalAdd(b), runtimeerrors.ErrPagingNoProgress) {
				t.Fatal("exact duplicate lost")
			}
		})

		// The inline history and its spill into the hashed map detect the same
		// repeats and enforce the same limits on either side of the spill.
		t.Run("InlineSpill", func(t *testing.T) {
			token := func(i int) implModel.PageState { return implModel.PageStateFromToken([]byte{byte(i), 0xff}) }
			inline := new([paging.InternalInlineHistory]implModel.PageState)
			h := implPaging.InternalPagingHistory{InternalMaxCount: 100, InternalMaxBytes: 1000, InternalInline: inline}
			for i := 0; i < 12; i++ {
				if err := h.InternalAdd(token(i)); err != nil {
					t.Fatal(i, err)
				}
				if (h.InternalEntries != nil) != (i >= paging.InternalInlineHistory) {
					t.Fatal("spill at wrong entry", i)
				}
				for j := 0; j <= i; j++ {
					if !errors.Is(h.InternalAdd(token(j)), runtimeerrors.ErrPagingNoProgress) {
						t.Fatal("repeat lost", i, j)
					}
				}
			}
			if h.InternalCount != 12 || h.InternalBytes != 24 {
				t.Fatal(h.InternalCount, h.InternalBytes)
			}
			for _, held := range inline {
				if model.PageToken(held) != nil {
					t.Fatal("spilled history kept an inline token")
				}
			}
			for _, limit := range []int{3, paging.InternalInlineHistory, paging.InternalInlineHistory + 2} {
				h = implPaging.InternalPagingHistory{InternalMaxCount: limit, InternalMaxBytes: 1000, InternalInline: new([paging.InternalInlineHistory]implModel.PageState)}
				for i := 0; i < limit; i++ {
					if err := h.InternalAdd(token(i)); err != nil {
						t.Fatal(limit, i, err)
					}
				}
				if !errors.Is(h.InternalAdd(token(limit)), runtimeerrors.ErrPagingStateLimit) || !errors.Is(h.InternalAdd(token(0)), runtimeerrors.ErrPagingNoProgress) {
					t.Fatal("count limit", limit)
				}
				h = implPaging.InternalPagingHistory{InternalMaxCount: 100, InternalMaxBytes: int64(2 * limit), InternalInline: new([paging.InternalInlineHistory]implModel.PageState)}
				for i := 0; i < limit; i++ {
					if err := h.InternalAdd(token(i)); err != nil {
						t.Fatal(limit, i, err)
					}
				}
				if !errors.Is(h.InternalAdd(token(limit)), runtimeerrors.ErrPagingStateLimit) {
					t.Fatal("byte limit", limit)
				}
			}
			h = implPaging.InternalPagingHistory{InternalMaxCount: 100, InternalMaxBytes: 1000, InternalInline: inline}
			_ = h.InternalAdd(token(0))
			h.InternalClear()
			if h.InternalCount != 0 || h.InternalEntries != nil || model.PageToken(inline[0]) != nil || h.InternalAdd(token(0)) != nil {
				t.Fatal("clear")
			}
			// Without inline storage every token goes to the map.
			h = implPaging.InternalPagingHistory{InternalMaxCount: 100, InternalMaxBytes: 1000}
			if h.InternalAdd(token(0)) != nil || h.InternalEntries == nil || !errors.Is(h.InternalAdd(token(0)), runtimeerrors.ErrPagingNoProgress) {
				t.Fatal("history without inline storage")
			}
		})
	})

	t.Run("Limits", func(t *testing.T) {
		t.Run("ConfigurationAndCompleteState", func(t *testing.T) {
			c := settings.DefaultConfig("localhost")
			c.Limits.MaxPagingStates = 0
			if c.Validate() == nil {
				t.Fatal("zero state budget")
			}
			c = settings.DefaultConfig("localhost")
			c.Limits.MaxPagingStateBytes = -1
			if c.Validate() == nil {
				t.Fatal("negative byte budget")
			}
			if _, err := RunPage[codec.EmptyRecord](nil, nil, nil, implModel.CompletePageState(), CompiledOptions{}); !errors.Is(err, runtimeerrors.ErrPagingComplete) {
				t.Fatal("complete admitted", err)
			}
		})
	})

	t.Run("Rows", func(t *testing.T) {
		t.Run("ActiveNilDoesNotPage", func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			go func() {
				stream := typedTestReadRequest(t, conns[0], peers[0])
				_, _ = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody([]byte("more"))))
			}()
			b := runtimeQuery(t)
			defer b.Close()
			rows := codecTestMust(Run(context.Background(), s, b, CompiledOptions{}))
			defer rows.Close()
			if rows.NextInto(nil) {
				t.Fatal("nil accepted")
			}
			if _, ok := errors.AsType[*implCqltype.ValidationError](rows.Err()); !ok {
				t.Fatal(rows.Err())
			}
			if PageData(RowsData(rows).page).index != 0 {
				t.Fatal("nil consumed row")
			}
		})

		t.Run("CycleAndBudgetBeforePrefetch", func(t *testing.T) {
			for _, prefetch := range []int{0, 1} {
				for _, mode := range []string{"cycle", "budget"} {
					t.Run(fmt.Sprintf("prefetch=%v/%s", prefetch, mode), func(t *testing.T) {
						s, conns, peers := runtimeTestSession(t, 1)
						rt := InternalRuntimeFor(s)
						rt.config.Limits.MaxPagingStates = 2
						rt.config.Limits.MaxPagingStateBytes = 2
						profile := settings.InternalDefaultProfile()
						profile.Prefetch = prefetch
						handle := new(profile)
						tokens := []string{"a", "b", "a"}
						if mode == "budget" {
							tokens = tokens[:2]
						}
						serverDone := make(chan struct{})
						go func() {
							defer close(serverDone)
							for _, token := range tokens {
								stream := typedTestReadRequest(t, conns[0], peers[0])
								_, _ = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody([]byte(token), token)))
							}
						}()
						b := runtimeQuery(t)
						defer b.Close()
						rows := codecTestMust(Run(context.Background(), s, b, testOptions(Options{Profile: handle})))
						defer rows.Close()
						if mode == "budget" {
							RowsData(rows).history.InternalMaxCount = 1
						}
						count := 0
						var row implCqltype.Bytes
						for rows.NextInto(&row) {
							count++
						}
						want := runtimeerrors.ErrPagingNoProgress
						if mode == "budget" {
							want = runtimeerrors.ErrPagingStateLimit
						}
						if !errors.Is(rows.Err(), want) {
							t.Fatal(count, rows.Err())
						}
						<-serverDone
					})
				}
			}
		})

		t.Run("NextIntoPagingAndCloseInterrupt", func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			nextRequested := make(chan struct{})
			stopped := make(chan struct{})
			go func() {
				defer close(stopped)
				for i, body := range [][]byte{runtimeRowsBody([]byte{}), runtimeRowsBody([]byte("next"), "one")} {
					stream := typedTestReadRequest(t, conns[0], peers[0])
					if _, err := peers[0].Write(typedTestResponse(t, 4, stream, body)); err != nil {
						t.Error(err)
						return
					}
					_ = i
				}
				typedTestReadRequest(t, conns[0], peers[0])
				close(nextRequested)
			}()
			b := runtimeQuery(t)
			defer b.Close()
			rows := codecTestMust(Run(context.Background(), s, b, CompiledOptions{}))
			defer rows.Close()
			var dst implCqltype.Bytes
			if !rows.NextInto(&dst) || string(dst.OwnedBytes()) != "one" {
				t.Fatal(rows.Err())
			}
			retained := codecTestMust(rows.Retain())
			defer retained.Close()
			copyView := dst
			done := make(chan bool)
			go func() { done <- rows.NextInto(&dst) }()
			<-nextRequested
			closed := make(chan error)
			go func() { closed <- rows.Close() }()
			select {
			case ok := <-done:
				if ok || dst.Len() != 0 {
					t.Fatal("cancelled NextInto succeeded")
				}
			case <-time.After(time.Second):
				t.Fatal("close did not interrupt next page")
			}
			if err := <-closed; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if string(copyView.OwnedBytes()) != "one" {
				t.Fatal("retained view lost")
			}
			<-stopped
			if rows.NextInto(nil) || !errors.Is(rows.Err(), context.Canceled) {
				t.Fatal("terminal error changed")
			}
		})
	})

	t.Run("RunPage", func(t *testing.T) {
		// A page deadline cancels accepted attempts and drains their input without
		// cancelling the caller's context or releasing an orphan's stream too early.
		t.Run("DeadlinePreservesCaller", func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			requestRead := make(chan int, 1)
			go func() {
				requestRead <- typedTestReadRequest(t, conns[0], peers[0])
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			bound := runtimeQuery(t)
			page, err := RunPage(ctx, s, bound, model.StartPage(), testOptions(Options{
				PageTimeout: cqltype.Option[time.Duration]{Set: true, Value: 20 * time.Millisecond},
			}))
			if page != nil {
				page.Close()
				t.Fatal("page returned without a response")
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("page deadline: %v", err)
			}
			if ctx.Err() != nil {
				t.Fatalf("page deadline cancelled caller: %v", ctx.Err())
			}
			stream := <-requestRead
			bound.Close()
			go func() {
				_, _ = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "late")))
			}()
			if err := bound.Wait(ctx); err != nil {
				t.Fatalf("accepted attempt did not drain: %v", err)
			}
		})

		t.Run("EmptyContinuationAndProfileSnapshot", func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				for _, body := range [][]byte{runtimeRowsBody([]byte{}), runtimeRowsBody([]byte{1}, "row"), runtimeRowsBody(nil)} {
					stream := typedTestReadRequest(t, conns[0], peers[0])
					if _, err := peers[0].Write(typedTestResponse(t, 4, stream, body)); err != nil {
						t.Error(err)
						return
					}
				}
			}()
			p := settings.InternalDefaultProfile()
			p.PageSize = 7
			profile := new(p)
			bound := runtimeQuery(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			rows, err := Run(ctx, s, bound, testOptions(Options{Profile: profile}))
			if err != nil {
				t.Fatal(err)
			}
			profile.PageSize = 11
			if RowsData(rows).request.InternalOptions.InternalProfile.PageSize != 7 {
				t.Fatal("profile changed after admission")
			}
			bound.Close()
			var view cqltype.Bytes
			if !rows.NextInto(&view) || string(view.OwnedBytes()) != "row" {
				t.Fatal(rows.Err())
			}
			retained, err := rows.Retain()
			if err != nil {
				t.Fatal(err)
			}
			var tail cqltype.Bytes
			if rows.NextInto(&tail) {
				t.Fatal("unexpected row")
			}
			if err = rows.Err(); err != nil {
				t.Fatal(err)
			}
			if err = bound.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			if string(view.OwnedBytes()) != "row" {
				t.Fatal("retained page lost")
			}
			retained.Close()
			<-done
		})

		t.Run("RepeatedState", func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			go func() {
				for _, value := range []string{"a", "b"} {
					stream := typedTestReadRequest(t, conns[0], peers[0])
					_, _ = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody([]byte{1}, value)))
				}
			}()
			bound := runtimeQuery(t)
			defer bound.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			rows, err := Run(ctx, s, bound, CompiledOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			count := 0
			var row cqltype.Bytes
			for rows.NextInto(&row) {
				count++
			}
			if count != 2 || !errors.Is(rows.Err(), runtimeerrors.ErrPagingNoProgress) {
				t.Fatal(count, rows.Err())
			}
		})
	})
}

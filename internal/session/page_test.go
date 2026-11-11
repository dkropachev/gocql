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
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	implCqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	implModel "github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	implRuntimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestPage(t *testing.T) {
	t.Run("Close", func(t *testing.T) {
		t.Run("CompletionCanInspect", func(t *testing.T) {
			p := testPage(t)
			pageObserved := false
			PageData(p).release = func() { _ = p.Err(); _ = p.Close(); pageObserved = true }
			p.Close()
			if !pageObserved {
				t.Fatal("page completion missing")
			}
			rows := func() Rows[implCqltype.Bytes] {
				opaqueStorageValuexxxxxxxx := newRows[implCqltype.Bytes]()
				RowsData(opaqueStorageValuexxxxxxxx).page = testPage(t)
				RowsData(opaqueStorageValuexxxxxxxx).ctx = context.Background()
				return opaqueStorageValuexxxxxxxx
			}()
			rowsObserved := false
			RowsData(rows).release = func() { _ = rows.Err(); _ = rows.Info(); _ = rows.Close(); rowsObserved = true }
			rows.Close()
			if !rowsObserved {
				t.Fatal("rows completion missing")
			}
		})

		t.Run("ReleasesExactlyOnce", func(t *testing.T) {
			p := testPage(t, "value")
			var releases atomic.Int32
			PageData(p).release = func() { releases.Add(1); _ = p.Err(); _ = p.Close() }
			// A Page is not safe for concurrent use: Close is idempotent, also from
			// its own completion callback.
			for i := 0; i < 32; i++ {
				_ = p.Close()
			}
			if releases.Load() != 1 {
				t.Fatal("page release count", releases.Load())
			}
			var wg sync.WaitGroup
			rows := func() Rows[implCqltype.Bytes] {
				opaqueStorageValuexxxxxxxxx := newRows[implCqltype.Bytes]()
				RowsData(opaqueStorageValuexxxxxxxxx).page = testPage(t)
				RowsData(opaqueStorageValuexxxxxxxxx).ctx = context.Background()
				return opaqueStorageValuexxxxxxxxx
			}()
			releases.Store(0)
			RowsData(rows).release = func() { releases.Add(1); _ = rows.Info(); _ = rows.Close() }
			for i := 0; i < 32; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); _ = rows.Close() }()
			}
			wg.Wait()
			if releases.Load() != 1 {
				t.Fatal("rows release count", releases.Load())
			}
		})
	})

	t.Run("NextInto", func(t *testing.T) {
		t.Run("MixedCurrentAndRetention", func(t *testing.T) {
			p := testPage(t, "one", "two", "three")
			defer p.Close()
			var first, second implCqltype.Bytes
			if !p.NextInto(&first) || string(first.OwnedBytes()) != "one" {
				t.Fatal(p.Err())
			}
			retained := codecTestMust(p.Retain())
			defer retained.Close()
			original := first
			first = implCqltype.BorrowBytes([]byte("edited"))
			if string(codecTestMust(p.Clone()).OwnedBytes()) != "edited" {
				t.Fatal("current destination not observed")
			}
			if !p.NextInto(&second) || string(second.OwnedBytes()) != "two" || string(first.OwnedBytes()) != "edited" {
				t.Fatal("NextInto modified old destination")
			}
			if !p.NextInto(&second) || string(second.OwnedBytes()) != "three" {
				t.Fatal(p.Err())
			}
			if p.NextInto(&second) || second.Len() != 0 || p.Err() != nil {
				t.Fatal("exhaustion", p.Err())
			}
			if p.NextInto(nil) || p.Err() != nil {
				t.Fatal("terminal nil replaced exhaustion", p.Err())
			}
			if string(original.OwnedBytes()) != "one" {
				t.Fatal("retention lost")
			}
			p.Close()
			if string(original.OwnedBytes()) != "one" {
				t.Fatal("close lost retained page")
			}
			retained.Close()
			if _, err := original.At(0); !errors.Is(err, implCqltype.ErrReleased) {
				t.Fatal(err)
			}
		})

		t.Run("PartialErrorClears", func(t *testing.T) {
			integer := codecTestMust(implCqltype.ScalarType(implCqltype.TypeInt))
			blob := codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))
			type row struct {
				Number int32
				Value  implCqltype.Bytes
				Unused int
			}
			cols := implCqltype.NewColumns(implCqltype.ColumnSpec{Name: "n", Type: integer}, implCqltype.ColumnSpec{Name: "v", Type: blob})
			body := typedTestChain(t, []byte{0, 0, 0, 4, 0, 0, 0, 7, 0, 0, 0, 5, 1})
			decoder := codec.Row(codec.Column("n", codec.Int32, func(r *row) *int32 { return &r.Number }), codec.Column("v", codec.BlobCodec, func(r *row) *implCqltype.Bytes { return &r.Value }))
			p := codecTestMust(InternalNewPage(&transport.InternalResponsePage{ResponseData: implModel.ResponseData{Columns: cols, Rows: body, RowCount: 1, Owner: wire.NewLease(body, nil)}}, decoder, nil))
			defer p.Close()
			dst := row{Unused: 8}
			if p.NextInto(&dst) || dst.Number != 0 || dst.Unused != 0 || p.Err() == nil {
				t.Fatal("partial row published", dst, p.Err())
			}
		})

		t.Run("TerminalClearing", func(t *testing.T) {
			for _, mode := range []string{"empty", "nil", "closed", "malformed", "cancelled"} {
				t.Run(mode, func(t *testing.T) {
					p := testPage(t, "one")
					defer p.Close()
					dst := implCqltype.BorrowBytes([]byte("stale"))
					switch mode {
					case "empty":
						p = testPage(t)
						defer p.Close()
					case "nil":
						if p.NextInto(nil) {
							t.Fatal("nil accepted")
						}
						if _, ok := errors.AsType[*implCqltype.ValidationError](p.Err()); !ok || PageData(p).index != 0 {
							t.Fatal(p.Err())
						}
					case "closed":
						p.Close()
					case "malformed":
						PageData(p).response.Rows = typedTestChain(t, []byte{0, 0, 0, 5, 1})
						PageData(p).InternalCursor = wire.CursorOver(PageData(p).response.Rows)
					case "cancelled":
						ctx, cancel := context.WithCancel(context.Background())
						cancel()
						PageData(p).ctx = ctx
					}
					if p.NextInto(&dst) || dst.Len() != 0 {
						t.Fatal("false did not clear destination")
					}
					previous := p.Err()
					if p.NextInto(nil) || p.Err() != previous {
						t.Fatal("terminal error changed", p.Err())
					}
				})
			}
		})
	})

	t.Run("SessionClose", func(t *testing.T) {
		t.Run("DeliveredSurvivesUntilAdvance", func(t *testing.T) {
			for _, manual := range []bool{false, true} {
				t.Run(fmt.Sprint(manual), func(t *testing.T) {
					s, conns, peers := runtimeTestSession(t, 1)
					serverDone := make(chan struct{})
					go func() {
						defer close(serverDone)
						stream := typedTestReadRequest(t, conns[0], peers[0])
						_, _ = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody([]byte("more"), "delivered")))
					}()
					b := runtimeQuery(t)
					defer b.Close()
					var dst implCqltype.Bytes
					if manual {
						p := codecTestMust(RunPage(context.Background(), s, b, implModel.StartPage(), CompiledOptions{}))
						defer p.Close()
						if !p.NextInto(&dst) {
							t.Fatal(p.Err())
						}
						before := dst
						_ = s.Close()
						if string(before.OwnedBytes()) != "delivered" {
							t.Fatal("shutdown recycled delivered page")
						}
						if p.NextInto(&dst) || dst.Len() != 0 || !errors.Is(p.Err(), runtimeerrors.ErrClosed) {
							t.Fatal("closed page advance", p.Err())
						}
					} else {
						r := codecTestMust(Run(context.Background(), s, b, CompiledOptions{}))
						defer r.Close()
						if !r.NextInto(&dst) {
							t.Fatal(r.Err())
						}
						before := dst
						_ = s.Close()
						if string(before.OwnedBytes()) != "delivered" {
							t.Fatal("shutdown recycled delivered rows")
						}
						if r.NextInto(&dst) || dst.Len() != 0 || !errors.Is(r.Err(), runtimeerrors.ErrClosed) {
							t.Fatal("closed rows advance", r.Err())
						}
					}
					<-serverDone
				})
			}
		})

		t.Run("PreservesDeliveredAndDrains", func(t *testing.T) {
			for _, automatic := range []bool{false, true} {
				t.Run(map[bool]string{false: "page", true: "rows"}[automatic], func(t *testing.T) {
					s, cs, ps := runtimeTestSession(t, 1)
					implCluster.SessionData(s).InternalCtx, implCluster.SessionData(s).InternalCancel = context.WithCancel(context.Background())
					go func() {
						stream := typedTestReadRequest(t, cs[0], ps[0])
						_, _ = ps[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "retained")))
					}()
					b := runtimeQuery(t)
					var view cqltype.Bytes
					var advance func() bool
					var closeRows func() error
					var retain func() (*PageLease, error)
					if automatic {
						r, err := Run(context.Background(), s, b, CompiledOptions{})
						if err != nil {
							t.Fatal(err)
						}
						if !r.NextInto(&view) {
							t.Fatal(r.Err())
						}
						advance = func() bool { var next cqltype.Bytes; return r.NextInto(&next) }
						closeRows = r.Close
						retain = r.Retain
					} else {
						p, err := RunPage(context.Background(), s, b, model.StartPage(), CompiledOptions{})
						if err != nil {
							t.Fatal(err)
						}
						if !p.NextInto(&view) {
							t.Fatal(p.Err())
						}
						advance = func() bool { var next cqltype.Bytes; return p.NextInto(&next) }
						closeRows = p.Close
						retain = p.Retain
					}
					lease, err := retain()
					if err != nil {
						t.Fatal(err)
					}
					defer lease.Close()
					b.Close()
					var wg sync.WaitGroup
					for i := 0; i < 4; i++ {
						wg.Add(1)
						go func() { defer wg.Done(); s.Close() }()
					}
					wg.Wait()
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					if err := b.Wait(ctx); err != nil {
						t.Fatal(err)
					}
					if string(view.OwnedBytes()) != "retained" {
						t.Fatal("shutdown invalidated current row")
					}
					if advance() {
						t.Fatal("advance after shutdown")
					}
					closeRows()
					if string(view.OwnedBytes()) != "retained" {
						t.Fatal("retained lease lost")
					}
					if _, err := Run(context.Background(), s, b, CompiledOptions{}); !errors.Is(err, implRuntimeerrors.ErrClosed) {
						t.Fatal(err)
					}
				})
			}
		})
	})

	t.Run("State", func(t *testing.T) {
		t.Run("EmptyContinuationSurvivesClose", func(t *testing.T) {
			p := testPage(t)
			PageData(p).response.State = implModel.PageStateFromToken(nil)
			state := p.State()
			p.Close()
			if !state.More() || state.Complete() || state.Token() == nil || len(state.Token()) != 0 {
				t.Fatal("empty continuation collapsed")
			}
			if !p.State().More() {
				t.Fatal("close lost state")
			}
		})
	})
}

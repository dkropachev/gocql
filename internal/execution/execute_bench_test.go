/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package execution

import (
	"context"

	"fmt"

	"github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/model"

	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wire"

	"runtime"

	"testing"
	"time"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	implProtocol "github.com/scylladb/gocql/v2/internal/protocol"
)

func BenchmarkSingleConn(b *testing.B) {
	srv := NewTestServer(b, 3, context.Background())
	defer srv.Stop()

	cluster := testCluster(implProtocol.ProtoVersion3, srv.Address)
	// Set the timeout arbitrarily low so that the query hits the timeout in a
	// timely manner.
	cluster.Timeout = 500 * time.Millisecond
	cluster.NumConns = 1
	db, err := implCluster.InternalClusterSettingsOpen(cluster)
	if err != nil {
		b.Fatalf("newClusterSettings: %v", err)
	}
	defer db.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := execTestStatement(context.Background(), db, "void", Options{})
			if err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkSharedAdmission(b *testing.B) {
	for _, parallelism := range []int{1, 4, 16} {
		b.Run(fmt.Sprint(parallelism), func(b *testing.B) {
			s := func() *cluster.Session {
				opaqueStorageValue := new(cluster.Session)
				return opaqueStorageValue
			}()

			bound := runtimeQuery(b)
			b.ReportAllocs()
			old := runtime.GOMAXPROCS(parallelism)
			defer runtime.GOMAXPROCS(old)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					ctx, cancel, request, err := InternalAdmitExecution(context.Background(), s, bound, Options{})
					if err != nil {
						b.Error(err)
						return
					}
					_ = ctx
					request.InternalLogical.InternalFinish(model.Result{}, nil)
					cancel()
					request.InternalLogical.InternalReleaseInput()
				}
			})
			b.StopTimer()
			bound.Close()
			bound.Wait(context.Background())
		})
	}
}

func scalarReader(t testing.TB, kind cqltype.Type, state cqltype.CellState, chunks ...[]byte) codec.ValueReader {
	t.Helper()
	spans := make([]wire.Span, len(chunks))
	for i, b := range chunks {
		spans[i] = wire.Borrow(b)
	}
	chain, err := wire.NewChain(spans...)
	if err != nil {
		t.Fatal(err)
	}
	return codec.NewValueReader(foundationScalar(t, kind), state, chain, nil)
}

func BenchmarkV2WarmPreparedSelection(b *testing.B) {
	s := func() *cluster.Session {
		opaqueStorageValue := new(cluster.Session)
		implCluster.SessionData(opaqueStorageValue).InternalLogger = logging.InternalNopLogger{}
		return opaqueStorageValue
	}()
	columns := cqltype.NewColumns(cqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: cqltype.BlobType})
	tracker := InternalRuntimeFor(s).InternalSchemaTracker()
	deps := tracker.InternalDependencies("ks", cqltype.NewColumns(), columns)
	hp := &InternalHostPrepared{InternalEpoch: deps.InternalSnapshot()}
	id := cqltype.UUID{15: 1}
	state := &InternalPreparedState{InternalHosts: map[cqltype.UUID]*InternalHostPrepared{id: hp}, InternalDependencies: deps}
	conn := &transport.Conn{InternalHost: func() *endpoint.HostInfo {
		opaqueStorageValuex := new(endpoint.HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuex).InternalHostId = id
		return opaqueStorageValuex
	}()}
	rt := InternalRuntimeFor(s)
	rt.InternalMu.Lock()
	defer rt.InternalMu.Unlock()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := InternalRuntimeFor(s).InternalPrepareOnConn(context.Background(), conn, state, model.EncodedValues{}, false)
		if err != nil || got != hp {
			b.Fatal(err)
		}
	}
}

// This isolates admission/capture from transport, decoding, and application
// hashing. Canonical comparisons remain necessary for throughput decisions.
func BenchmarkReusableQueryAdmission(b *testing.B) {
	q := testOwnedQuery(b)
	ctx := context.Background()
	value := []byte("owned parameter")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := q.capture(ctx, value); err != nil {
			b.Fatal(err)
		}
		_, cancel, request, err := InternalAdmitExecution(ctx, q.state.runtime.Session, &q.bound, Options{})
		if err != nil {
			b.Fatal(err)
		}
		cancel()
		request.InternalLogical.InternalFinish(Result{}, nil)
		request.InternalLogical.InternalReleaseInput()
		q.state.owner.InternalRelease()
	}
}

func BenchmarkDurationDecode(b *testing.B) {
	reader := scalarReader(b, cqltype.TypeDuration, cqltype.Present, []byte{2, 4, 6})
	b.ReportAllocs()
	for b.Loop() {
		if _, err := codec.DurationCodec.Decode(reader); err != nil {
			b.Fatal(err)
		}
	}
}

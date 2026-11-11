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

package session

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/routing"
)

func TestSchemaAgreement(t *testing.T) {
	t.Run("Await", func(t *testing.T) {
		// TestSchemaAgreement/Await/BoundedByMaxWait verifies that a stalled
		// system.peers/system.local query cannot make awaitSchemaAgreement block past
		// MaxWaitSchemaAgreement, even when MetadataSchemaRequestTimeout is much larger.
		t.Run("BoundedByMaxWait", func(t *testing.T) {
			srv := NewTestServer(t, defaultProto, context.Background())
			defer srv.Stop()

			const maxWaitSchemaAgreement = 200 * time.Millisecond
			const metadataSchemaRequestTimeout = 5 * time.Second

			cluster := testCluster(defaultProto, srv.Address)
			cluster.MaxWaitSchemaAgreement = maxWaitSchemaAgreement
			cluster.MetadataSchemaRequestTimeout = metadataSchemaRequestTimeout

			session, err := cluster.Open()
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer session.Close()

			conn := session.GetConn()
			if conn == nil {
				t.Fatal("unable to get a connection")
			}

			atomic.StoreInt32(&srv.stallSystemQueries, 1)

			start := time.Now()
			err = session.AwaitV2SchemaAgreementOn(context.Background(), conn)
			elapsed := time.Since(start)

			if err == nil {
				t.Fatal("expected an error from a stalled schema agreement query, got nil")
			}
			if elapsed >= metadataSchemaRequestTimeout {
				t.Fatalf("awaitSchemaAgreement took %v, expected to return near MaxWaitSchemaAgreement (%v), not wait for MetadataSchemaRequestTimeout (%v)",
					elapsed, maxWaitSchemaAgreement, metadataSchemaRequestTimeout)
			}
		})

		t.Run("ChecksEnabledDownHosts", func(t *testing.T) {
			for _, success := range []bool{false, true} {
				t.Run(fmt.Sprint(success), func(t *testing.T) {
					s, id, missing := agreementSession(t)
					hosts := []model.HostSnapshot{{ID: missing, Enabled: true, Up: false}}
					if success {
						c, _ := agreementConn(t, s, id, cqltype.MustRandomUUID(), 0)
						agreementPool(s, c)
						hosts = append(hosts, model.HostSnapshot{ID: id, Enabled: true, Up: true})
					}
					s.Peripheral().InternalSnapshot.Store(routing.NewMetadataSnapshot(routing.SnapshotState{InternalHosts: hosts}))
					if err := s.AwaitSchemaAgreement(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
						t.Fatal("missing enabled host/zero successes counted as agreement", err)
					}
				})
			}
		})

		t.Run("RetainsExcludedCoordinator", func(t *testing.T) {
			s, id, coordinatorID := agreementSession(t)
			version := cqltype.MustRandomUUID()
			c, _ := agreementConn(t, s, id, version, 0)
			agreementPool(s, c)
			coordinator, count := agreementConn(t, s, coordinatorID, version, 0)
			s.Peripheral().InternalSnapshot.Store(routing.NewMetadataSnapshot(routing.SnapshotState{InternalHosts: []model.HostSnapshot{{ID: id, Enabled: true}, {ID: cqltype.MustRandomUUID(), Enabled: false}}}))
			if err := s.AwaitV2SchemaAgreementOn(context.Background(), coordinator); err != nil {
				t.Fatal(err)
			}
			if count.Load() != 1 {
				t.Fatal("excluded exact coordinator not queried", count.Load())
			}
			different, _ := agreementConn(t, s, coordinatorID, cqltype.MustRandomUUID(), 0)
			if err := s.AwaitV2SchemaAgreementOn(context.Background(), different); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("older topology agreement ignored coordinator schema", err)
			}
		})

		t.Run("TerminalAuthorization", func(t *testing.T) {
			s, id, _ := agreementSession(t)
			c, count := agreementConn(t, s, id, cqltype.MustRandomUUID(), protocol.CodeUnauthorized)
			agreementPool(s, c)
			s.Peripheral().InternalSnapshot.Store(routing.NewMetadataSnapshot(routing.SnapshotState{InternalHosts: []model.HostSnapshot{{ID: id, Enabled: true}}}))
			err := s.AwaitSchemaAgreement(context.Background())
			if request, ok := errors.AsType[protocol.ServerError](err); !ok || request.Code() != protocol.CodeUnauthorized {
				t.Fatal(err)
			}
			if count.Load() != 1 {
				t.Fatal("authorization retried", count.Load())
			}
		})
	})

	t.Run("DDL", func(t *testing.T) {
		t.Run("ErrorNeverRetriesStatement", func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			conn := conns[0]
			config := InternalRuntimeFor(s).config
			config.Metadata.AutomaticAgreement = true
			config.Metadata.RefreshAfterAgreement = false
			config.Metadata.AgreementTimeout = 100 * time.Millisecond
			InternalRuntimeFor(s).config = config
			s.ConfigureV2Peripheral(config)
			metadata := s.Metadata()
			metadataCopy := *metadata
			routing.SnapshotData(&metadataCopy).InternalHosts = append([]model.HostSnapshot(nil), routing.SnapshotData(metadata).InternalHosts...)
			routing.SnapshotData(&metadataCopy).InternalHosts = append(routing.SnapshotData(&metadataCopy).InternalHosts, model.HostSnapshot{ID: cqltype.MustRandomUUID(), Enabled: true})
			s.Peripheral().InternalSnapshot.Store(&metadataCopy)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			var ddl atomic.Int32
			done := make(chan struct{})
			go func() {
				defer close(done)
				for {
					var header [9]byte
					if _, err := io.ReadFull(peers[0], header[:]); err != nil {
						return
					}
					size := binary.BigEndian.Uint32(header[5:])
					body := make([]byte, size)
					if _, err := io.ReadFull(peers[0], body); err != nil {
						return
					}
					stream := int(binary.BigEndian.Uint16(header[2:4]))
					query := ""
					if len(body) >= 4 {
						n := int(binary.BigEndian.Uint32(body))
						if n <= len(body)-4 {
							query = string(body[4 : 4+n])
						}
					}
					if strings.HasPrefix(query, "CREATE") {
						ddl.Add(1)
						response := binary.BigEndian.AppendUint32(nil, frm.ResultKindSchemaChanged)
						for _, v := range []string{"CREATED", "KEYSPACE", "schema_events"} {
							response = binary.BigEndian.AppendUint16(response, uint16(len(v)))
							response = append(response, v...)
						}
						if _, err := peers[0].Write(typedTestResponse(t, 4, stream, response)); err != nil {
							return
						}
					} else {
						if _, err := peers[0].Write(errorResponse(t, stream, protocol.CodeUnauthorized, nil)); err != nil {
							return
						}
					}
				}
			}()
			b := codecTestMust(BindUnpreparedOwned(binding.Statement[codec.EmptyRecord, codec.EmptyRecord]{CQL: "CREATE KEYSPACE schema_events", Params: codec.EmptyParamsEncoder, Rows: codec.EmptyRowDecoder}, cqltype.NewColumns(), codec.EmptyRecord{}))
			defer b.Close()
			_, err := Exec(ctx, s, b, testOptions(*func() *Options {
				opaqueStorageValuexxxxxx := new(Options)
				OptionsData(opaqueStorageValuexxxxxx).InternalConnection = conn
				opaqueStorageValuexxxxxx.Idempotent = true
				opaqueStorageValuexxxxxx.Retry = retryOnce{}
				return opaqueStorageValuexxxxxx
			}()))
			if err == nil || !strings.Contains(err.Error(), "DDL succeeded") {
				t.Fatal("successful DDL failure not distinguished", err)
			}
			conn.Close()
			peers[0].Close()
			<-done
			if ddl.Load() != 1 {
				t.Fatal("successful DDL replayed", ddl.Load())
			}
		})

		t.Run("StatusSurvivesAgreementCancellation", func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			conn := conns[0]
			config := InternalRuntimeFor(s).config
			config.Metadata.AutomaticAgreement = true
			config.Metadata.RefreshAfterAgreement = false
			InternalRuntimeFor(s).config = config
			s.ConfigureV2Peripheral(config)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered, serverDone := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(serverDone)
				stream := typedTestReadRequest(t, conn, peers[0])
				body := binary.BigEndian.AppendUint32(nil, frm.ResultKindSchemaChanged)
				for _, value := range []string{"CREATED", "KEYSPACE", "schema_events"} {
					body = binary.BigEndian.AppendUint16(body, uint16(len(value)))
					body = append(body, value...)
				}
				if _, err := peers[0].Write(typedTestResponse(t, 4, stream, body)); err != nil {
					return
				}
				typedTestReadRequest(t, conn, peers[0])
				close(entered)
				<-ctx.Done()
			}()
			bound := codecTestMust(BindUnpreparedOwned(binding.Statement[codec.EmptyRecord, codec.EmptyRecord]{CQL: "CREATE KEYSPACE schema_events", Params: codec.EmptyParamsEncoder, Rows: codec.EmptyRowDecoder}, cqltype.NewColumns(), codec.EmptyRecord{}))
			defer bound.Close()
			result := make(chan error, 1)
			go func() {
				_, err := Exec(ctx, s, bound, testOptions(*func() *Options {
					opaqueStorageValuexxxxxxxxxxxxx := new(Options)
					OptionsData(opaqueStorageValuexxxxxxxxxxxxx).InternalConnection = conn
					return opaqueStorageValuexxxxxxxxxxxxx
				}()))
				result <- err
			}()
			<-entered
			cancel()
			err := <-result
			if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "DDL succeeded") {
				t.Fatal("canceled post-DDL agreement lost acknowledged success", err)
			}
			conn.Close()
			peers[0].Close()
			<-serverDone
		})
	})
}

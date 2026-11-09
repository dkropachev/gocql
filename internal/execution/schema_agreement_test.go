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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/scylladb/gocql/v2/internal/binding"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/transport"
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

			session, err := implCluster.InternalClusterSettingsOpen(cluster)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer session.Close()

			conn := implCluster.InternalSessionGetConn(session)
			if conn == nil {
				t.Fatal("unable to get a connection")
			}

			atomic.StoreInt32(&srv.stallSystemQueries, 1)

			start := time.Now()
			err = implCluster.InternalSessionAwaitV2SchemaAgreementOn(session, context.Background(), conn)
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
					implCluster.InternalSessionPeripheral(s).InternalSnapshot.Store(routing.NewMetadataSnapshot(routing.SnapshotState{InternalHosts: hosts}))
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
			implCluster.InternalSessionPeripheral(s).InternalSnapshot.Store(routing.NewMetadataSnapshot(routing.SnapshotState{InternalHosts: []model.HostSnapshot{{ID: id, Enabled: true}, {ID: cqltype.MustRandomUUID(), Enabled: false}}}))
			if err := implCluster.InternalSessionAwaitV2SchemaAgreementOn(s, context.Background(), coordinator); err != nil {
				t.Fatal(err)
			}
			if count.Load() != 1 {
				t.Fatal("excluded exact coordinator not queried", count.Load())
			}
			different, _ := agreementConn(t, s, coordinatorID, cqltype.MustRandomUUID(), 0)
			if err := implCluster.InternalSessionAwaitV2SchemaAgreementOn(s, context.Background(), different); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("older topology agreement ignored coordinator schema", err)
			}
		})

		t.Run("TerminalAuthorization", func(t *testing.T) {
			s, id, _ := agreementSession(t)
			c, count := agreementConn(t, s, id, cqltype.MustRandomUUID(), protocol.ErrCodeUnauthorized)
			agreementPool(s, c)
			implCluster.InternalSessionPeripheral(s).InternalSnapshot.Store(routing.NewMetadataSnapshot(routing.SnapshotState{InternalHosts: []model.HostSnapshot{{ID: id, Enabled: true}}}))
			err := s.AwaitSchemaAgreement(context.Background())
			if request, ok := errors.AsType[protocol.RequestError](err); !ok || request.GetCode() != protocol.ErrCodeUnauthorized {
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
			config := InternalRuntimeFor(s).InternalConfig
			config.Metadata.AutomaticAgreement = true
			config.Metadata.RefreshAfterAgreement = false
			config.Metadata.AgreementTimeout = 100 * time.Millisecond
			InternalRuntimeFor(s).InternalConfig = config
			implCluster.InternalSessionConfigureV2Peripheral(s, config)
			metadata := s.Metadata()
			metadataCopy := *metadata
			routing.SnapshotData(&metadataCopy).InternalHosts = append([]model.HostSnapshot(nil), routing.SnapshotData(metadata).InternalHosts...)
			routing.SnapshotData(&metadataCopy).InternalHosts = append(routing.SnapshotData(&metadataCopy).InternalHosts, model.HostSnapshot{ID: cqltype.MustRandomUUID(), Enabled: true})
			implCluster.InternalSessionPeripheral(s).InternalSnapshot.Store(&metadataCopy)
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
						if _, err := peers[0].Write(errorResponse(t, stream, protocol.ErrCodeUnauthorized, nil)); err != nil {
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
			config := InternalRuntimeFor(s).InternalConfig
			config.Metadata.AutomaticAgreement = true
			config.Metadata.RefreshAfterAgreement = false
			InternalRuntimeFor(s).InternalConfig = config
			implCluster.InternalSessionConfigureV2Peripheral(s, config)
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

	t.Run("Get", func(t *testing.T) {
		schema_version1 := cqltype.ParseUUIDMust("af810386-a694-11ef-81fa-3aea73156247")
		peersRows := []transport.InternalSchemaAgreementHost{
			{
				DataCenter:    "datacenter1",
				HostID:        cqltype.ParseUUIDMust("b2035fd9-e0ca-4857-8c45-e63c00fb7c43"),
				Rack:          "rack1",
				RPCAddress:    "127.0.0.3",
				SchemaVersion: schema_version1,
			},
			{
				DataCenter:    "datacenter1",
				HostID:        cqltype.ParseUUIDMust("4b21ee4c-acea-4267-8e20-aaed5361a0dd"),
				Rack:          "rack1",
				RPCAddress:    "127.0.0.2",
				SchemaVersion: schema_version1,
			},
			{
				DataCenter:    "datacenter2",
				HostID:        cqltype.ParseUUIDMust("dfef4a22-b8d8-47e9-aee5-8c19d4b7a9e3"),
				Rack:          "rack1",
				RPCAddress:    "127.0.0.5",
				SchemaVersion: cqltype.ParseUUIDMust("875a938a-a695-11ef-4314-85c8ef0ebaa2"),
			},
		}

		var logger logging.StdLogger

		t.Run("SchemaNotConsistent", func(t *testing.T) {
			err := transport.InternalGetSchemaAgreement(
				"875a938a-a695-11ef-4314-85c8ef0ebaa2",
				peersRows,
				logger,
			)

			assert.Error(t, err, "error expected when local schema is different then others")
		})

		t.Run("ZeroTokenNodeSchemaNotConsistent", func(t *testing.T) {
			err := transport.InternalGetSchemaAgreement(
				"af810386-a694-11ef-81fa-3aea73156247",
				peersRows,
				logger,
			)

			assert.Error(t, err, "expected error when zero-token node has different schema")
		})

		t.Run("SchemaConsistent", func(t *testing.T) {
			peersRows[2].SchemaVersion = schema_version1
			err := transport.InternalGetSchemaAgreement(
				"af810386-a694-11ef-81fa-3aea73156247",
				peersRows,
				logger,
			)

			assert.NoError(t, err, "expected no error when all nodes have the same schema")
		})

		t.Run("EmptyLocalSchemaVersion", func(t *testing.T) {
			err := transport.InternalGetSchemaAgreement(
				"",
				[]transport.InternalSchemaAgreementHost{
					{
						DataCenter:    "datacenter1",
						HostID:        cqltype.ParseUUIDMust("b2035fd9-e0ca-4857-8c45-e63c00fb7c43"),
						Rack:          "rack1",
						RPCAddress:    "127.0.0.3",
						SchemaVersion: schema_version1,
					},
				},
				logger,
			)

			assert.NoError(t, err, "expected no error when local version is empty and peers are consistent")
		})

		t.Run("EmptyLocalWithNoPeers", func(t *testing.T) {
			err := transport.InternalGetSchemaAgreement("", nil, logger)

			assert.NoError(t, err, "expected no error when both local and peers are empty")
		})

		t.Run("EmptyLocalWithInconsistentPeers", func(t *testing.T) {
			err := transport.InternalGetSchemaAgreement(
				"",
				[]transport.InternalSchemaAgreementHost{
					{
						DataCenter:    "datacenter1",
						HostID:        cqltype.ParseUUIDMust("b2035fd9-e0ca-4857-8c45-e63c00fb7c43"),
						Rack:          "rack1",
						RPCAddress:    "127.0.0.3",
						SchemaVersion: schema_version1,
					},
					{
						DataCenter:    "datacenter2",
						HostID:        cqltype.ParseUUIDMust("dfef4a22-b8d8-47e9-aee5-8c19d4b7a9e3"),
						Rack:          "rack1",
						RPCAddress:    "127.0.0.5",
						SchemaVersion: cqltype.ParseUUIDMust("875a938a-a695-11ef-4314-85c8ef0ebaa2"),
					},
				},
				logger,
			)

			assert.Error(t, err, "expected error when peers have different schemas")
		})
	})
}

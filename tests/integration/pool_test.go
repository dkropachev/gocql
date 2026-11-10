//go:build integration

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
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package integration

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/endpoint"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
	"github.com/scylladb/gocql/v2/routing"
)

func TestPool(t *testing.T) {
	// The control connection completes real startup, then the data dial fails. The
	// public conviction hook must receive the discovered host, not a mutable core.
	t.Run("Conviction", func(t *testing.T) {
		if *flagProto != 4 || *flagRunSslTest {
			t.Skip("raw v4 callback lane")
		}
		for _, verdict := range []bool{false, true} {
			t.Run(fmt.Sprint(verdict), func(t *testing.T) {
				cause := errors.New("injected data connection failure")
				policy := &testConviction{cause: cause, verdict: verdict}
				var dials atomic.Int32
				config := newFixtureConfig(getClusterHosts()[:1], 0, func(c *gocql.ConfigBuilder) {
					c.NoDiscovery().
						WithConnectionsPerHost(1).
						NoAdvancedShardAwareness().
						WithReconnect(noReconnect{}).
						WithConviction(policy).
						NoHeartbeat().
						NoMetadataRefresh()
				})
				config.WithDialer(endpoint.DialerFunc(func(ctx context.Context, target endpoint.DialTarget) (net.Conn, error) {
					if dials.Add(1) > 1 {
						policy.mu.Lock()
						policy.targets = append(policy.targets, target.Host)
						policy.mu.Unlock()
						return nil, cause
					}
					return (endpoint.DefaultDialer{}).DialContext(ctx, target)
				}))
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				s, err := gocql.Connect(ctx, mustBuild(t, config))
				if s != nil {
					s.Close()
				}
				if err == nil {
					t.Fatal("failed data connection unexpectedly connected")
				}
				policy.mu.Lock()
				hosts := append([]routing.HostSnapshot(nil), policy.hosts...)
				targets := append([]routing.HostSnapshot(nil), policy.targets...)
				resets := policy.resets
				policy.mu.Unlock()
				if len(hosts) == 0 || len(targets) == 0 {
					t.Fatal("custom policy not called", err)
				}
				for _, host := range hosts {
					if !reflect.DeepEqual(host, targets[0]) {
						t.Fatalf("conviction lost dial host context: got=%+v want=%+v", host, targets[0])
					}
					if host.ID.IsEmpty() || host.Address.Addr().String() != getClusterHosts()[0] || host.Datacenter == "" || host.Rack == "" || !host.Enabled || host.Connected {
						t.Fatalf("incomplete host context: %+v", host)
					}
				}
				if resets != 0 {
					t.Fatal("driver introduced a new reset call")
				}
				policy.Reset(hosts[0])
				policy.mu.Lock()
				resets = policy.resets
				policy.mu.Unlock()
				if resets != 1 {
					t.Fatal("public reset callback not usable")
				}
				t.Log("PUBLIC CONVICTION CALLBACK", verdict, hosts[0].ID, hosts[0].Address, len(hosts[0].Tokens))
			})
		}
	})

	t.Run("ExclusiveTransport", func(t *testing.T) {
		// v3/v4 frame compression is intentionally unsupported for streaming; this
		// lane explicitly negotiates raw, while v5 retains its raw/LZ4 selection.
		compression := *flagCompressTest
		var skip fixtureParts
		if *flagProto < 5 {
			skip = fixtureCompression
			compression = "no-compression"
		}
		config := createV2ClusterWithout(skip)
		config.WithConnectionsPerHost(1).NoHeartbeat()
		if *flagProto < 5 {
			config.WithCompression(gocql.NoCompression)
		}
		s := createV2SessionFromConfig(config, t, false)
		defer s.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		conn := codecTestMust(implCluster.InternalSessionSelectTypedConn(implExecution.InternalCore((*implExecution.Session)(s)), ctx, routing.Info{}, nil))
		table := fmt.Sprintf("pool_stream_%x", time.Now().UnixNano())
		qualified := (fixtureKeyspace("") + ".") + table
		ddl := func(stmt string) {
			b := codecTestMust(BindUnpreparedOwned(cql.Command(stmt), cql.NewColumns(), cql.EmptyParams{}))
			defer b.Close()
			if _, err := Exec(ctx, s, b, bindOptions(s, *func() *implExecution.Options {
				opaqueStorageValue := new(implExecution.Options)
				implExecution.OptionsData(opaqueStorageValue).InternalConnection = conn
				return opaqueStorageValue
			}())); err != nil {
				t.Fatal(err)
			}
			if err := implCluster.InternalSessionAwaitV2SchemaAgreementOn(implExecution.InternalCore((*implExecution.Session)(s)), ctx, conn); err != nil {
				t.Fatal(err)
			}
		}
		ddl("CREATE TABLE " + qualified + "(id int PRIMARY KEY,value blob)")
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			b, err := BindUnpreparedOwned(cql.Command("DROP TABLE IF EXISTS "+qualified), cql.NewColumns(), cql.EmptyParams{})
			if err == nil {
				defer b.Close()
				_, err = Exec(cleanup, s, b, gocql.BoundOptions{})
			}
			if err != nil {
				t.Errorf("cleanup unique stream table: %v", err)
			}
		}()
		insert := codecTestMust(s.Prepare(ctx, cql.Statement[cql.Pair[int32, []byte], cql.EmptyRow]{CQL: "INSERT INTO " + qualified + "(id,value) VALUES(?,?)", Params: cql.PairParams(1, cql.OneParam(cql.Int32), cql.OneParam(cql.BytesEncoder)), Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{}))
		size := 400 << 10
		for i := 0; i < 4; i++ {
			payload := bytes.Repeat([]byte{byte(i + 1)}, size)
			bound := codecTestMust(BindOwned(insert, cql.Pair[int32, []byte]{First: int32(i), Second: payload}))
			_, err := Exec(ctx, s, bound, gocql.BoundOptions{})
			bound.Close()
			if err != nil {
				t.Fatal(err)
			}
		}
		pool, ok := implExecution.InternalSessionData((*implExecution.Session)(s)).InternalPool.InternalGetPool(conn.InternalHost)
		if !ok {
			t.Fatal("pool missing")
		}
		r, err := pool.InternalReserveStream(ctx, routing.Target{HostID: implEndpoint.InternalHostInfoHostUUID(conn.InternalHost)}, routing.Info{})
		if err != nil {
			t.Fatal(err)
		}
		defer r.InternalClose(false)
		decoder := cql.StreamRow(func(*cql.Pair[int32, []byte]) {}, cql.StreamColumn("id", cql.StreamInt32, func(r *cql.Pair[int32, []byte]) *int32 { return &r.First }), cql.StreamColumn("value", cql.StreamBlob, func(r *cql.Pair[int32, []byte]) *[]byte { return &r.Second }))
		request := &attemptRequest{Statement: "SELECT id,value FROM " + qualified, Consistency: cql.Quorum, PageSize: 2}
		seen := map[int32]bool{}
		pages := 0
		for {
			response, err := r.InternalExchange(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			pages++
			if err = response.InternalInstallColumns(response.InternalColumns); err != nil {
				t.Fatal(err)
			}
			plan := codecTestMust(decoder.Compile(response.InternalColumns))
			dst := cql.Pair[int32, []byte]{Second: make([]byte, 0, size)}
			for {
				ok, err := decodeStreamRow(response, plan, &dst)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				if dst.First < 0 || dst.First >= 4 || seen[dst.First] || len(dst.Second) != size || !bytes.Equal(dst.Second, bytes.Repeat([]byte{byte(dst.First + 1)}, size)) {
					t.Fatal("invalid streamed live row", dst.First, len(dst.Second))
				}
				seen[dst.First] = true
			}
			next := response.InternalMetadata.PagingState
			response.InternalReleaseMetadata()
			if next == nil {
				break
			}
			request.PagingState = next
		}
		if len(seen) != 4 || pages < 2 {
			t.Fatal("live paging incomplete", len(seen), pages)
		}
		if r.InternalUnits != nil {
			t.Logf("bounded validated-unit storage=%d bytes copies=%d", r.InternalUnits.InternalUnits.StorageBytes(), r.InternalUnits.InternalCopies)
			if r.InternalUnits.InternalUnits.StorageBytes() > r.InternalLimits.MaxTransformBytes {
				t.Fatal("unit budget exceeded")
			}
		}
		if err = r.InternalClose(true); err != nil {
			t.Fatal(err)
		}
		// Early abandonment discards the reserved slot, and ordinary bounded pool
		// repair restores healthy shared execution without growing its target.
		r, err = pool.InternalReserveStream(ctx, routing.Target{HostID: implEndpoint.InternalHostInfoHostUUID(conn.InternalHost)}, routing.Info{})
		if err != nil {
			t.Fatal(err)
		}
		response, err := r.InternalExchange(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		response.InternalReleaseMetadata()
		if err = r.InternalClose(false); err != nil {
			t.Fatal(err)
		}
		deadline := time.After(15 * time.Second)
		for {
			pool.InternalMu.RLock()
			count, missing := pool.InternalConnPicker.Size()
			pool.InternalMu.RUnlock()
			if count == 1 && missing == 0 {
				break
			}
			select {
			case <-deadline:
				t.Fatal("pool did not recover reserved slot")
			case <-time.After(10 * time.Millisecond):
			}
		}
		query := codecTestMust(BindUnpreparedOwned(cql.Statement[cql.EmptyParams, int32]{CQL: "SELECT id FROM " + qualified + " LIMIT 1", Params: cql.EmptyParamsEncoder, Rows: cql.OneColumn(cql.Int32)}, cql.NewColumns(), cql.EmptyParams{}))
		defer query.Close()
		rows := codecTestMust(Run(ctx, s, query, gocql.BoundOptions{}))
		defer rows.Close()
		var id int32
		if !rows.NextInto(&id) {
			t.Fatal("shared execution after repair", rows.Err())
		}
		t.Logf("live streaming proto=%d TLS=%v compression=%s rows=%d pages=%d", *flagProto, *flagRunSslTest, compression, len(seen), pages)
	})

	t.Run("ShardAwarePort", func(t *testing.T) {
		t.Parallel()

		t.Run("MaliciousNAT", func(t *testing.T) {
			t.Parallel()

			testShardAwarePortMaliciousNAT(t, makeIntegrationShardAwareCluster)
		})
		t.Run("NoReconnections", func(t *testing.T) {
			t.Parallel()

			testShardAwarePortNoReconnections(t, makeIntegrationShardAwareCluster)
		})
		t.Run("Unreachable", func(t *testing.T) {
			t.Parallel()

			testShardAwarePortUnreachable(t, makeIntegrationShardAwareCluster)
		})
		t.Run("UnusedIfNotEnabled", func(t *testing.T) {
			t.Parallel()

			testShardAwarePortUnusedIfNotEnabled(t, makeIntegrationShardAwareCluster)
		})
	})

	t.Run("Sizing", func(t *testing.T) {
		for _, mode := range []string{"host", "shard"} {
			t.Run(mode, func(t *testing.T) {
				config := createV2ClusterWithout(fixtureTLS)
				config.NoHeartbeat()
				if *flagRunSslTest {
					config.WithCoalesceWrites(100 * time.Microsecond)
				}
				if mode == "host" {
					config.WithConnectionsPerHost(2)
				} else {
					config.WithConnectionsPerShard(2)
				}
				// The fixture enables the shard-aware port by default.
				const shardAwarePort = true
				var tlsCalls atomic.Int32
				if *flagRunSslTest {
					// Every handshake runs VerifyConnection once.
					base := fixtureClientTLS()
					base.VerifyConnection = func(tls.ConnectionState) error {
						tlsCalls.Add(1)
						return nil
					}
					config.WithTLS(gocql.StaticTLS(fixtureTLSConfig(base)))
				}
				s := createV2SessionFromConfig(config, t, false)
				defer s.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				for _, host := range implExecution.InternalSessionData((*implExecution.Session)(s)).InternalHostSource.InternalGetHostsList() {
					pool, ok := implExecution.InternalSessionData((*implExecution.Session)(s)).InternalPool.InternalGetPool(host)
					if !ok {
						t.Fatal("missing host pool")
					}
					p, ok := pool.InternalConnPicker.(*typedConnPicker)
					if !ok {
						t.Fatalf("v2 pool used legacy picker %T", pool.InternalConnPicker)
					}
					for {
						p.InternalMu.RLock()
						count, missing := p.InternalCount(), max(0, p.InternalTarget()-p.InternalCount())
						p.InternalMu.RUnlock()
						if missing == 0 {
							t.Logf("mode=%s host=%s total=%d shards=%d", mode, implEndpoint.InternalHostInfoHostUUID(host), count, p.GetShardCount())
							break
						}
						select {
						case <-ctx.Done():
							t.Fatalf("pool target not reached mode=%s count=%d missing=%d", mode, count, missing)
						case <-time.After(time.Millisecond):
						}
					}
					p.InternalMu.RLock()
					targeted := 0
					for shard, connections := range p.InternalConns {
						if mode == "shard" && len(connections) != 2 {
							p.InternalMu.RUnlock()
							t.Fatalf("shard%d has%d connections", shard, len(connections))
						}
						for _, conn := range connections {
							if conn.InternalRequestedShard.Set {
								targeted++
								if int(conn.InternalRequestedShard.Value) != implEndpoint.ScyllaConnectionFeaturesData(&conn.InternalScyllaSupported).InternalShard {
									p.InternalMu.RUnlock()
									t.Fatal("wrong requested shard")
								}
							}
							if *flagRunSslTest {
								if _, ok := conn.InternalR.(*connReader).InternalConn.(*tls.Conn); !ok {
									p.InternalMu.RUnlock()
									t.Fatal("TLS host path became plaintext")
								}
							}
						}
					}
					p.InternalMu.RUnlock()
					if mode == "host" && targeted != 0 {
						t.Fatal("per-host override performed targeted fills")
					}
					if mode == "shard" && p.InternalNrShards > 1 && shardAwarePort && targeted == 0 {
						t.Fatal("shard mode never used source-port routing")
					}
					if pool.InternalTransportState() != poolReady {
						t.Fatal("full pool not ready")
					}
				}
				if *flagRunSslTest && tlsCalls.Load() == 0 {
					t.Fatal("per-host TLS hook not used")
				}
			})
		}
	})
}

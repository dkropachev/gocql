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
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	implCql "github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/protocol"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/retry"
)

type runtimeBlockingDNS struct{ done chan struct{} }

func (d runtimeBlockingDNS) LookupIP(string) ([]net.IP, error) { <-d.done; return nil, nil }

func TestConfig(t *testing.T) {
	t.Run("Defaults", func(t *testing.T) {
		t.Run("Transport", func(t *testing.T) {
			c := settings.DefaultConfig("localhost")
			if c.ConnectTimeout != 5*time.Second || c.DNSTimeout != 5*time.Second || c.ReadTimeout != 0 || c.WriteTimeout != 0 || c.SocketKeepalive != 0 || c.CoalesceWait != 0 || c.KeepaliveInterval != 30*time.Second || c.KeepaliveTimeout != 30*time.Second || c.Pool.SourcePortStart != 49152 || c.Pool.SourcePortEnd != 65535 {
				t.Fatal("transport defaults", c)
			}
			for _, change := range []func(*settings.Config){func(c *settings.Config) { c.Pool.ConnectionsPerShard = 0 }, func(c *settings.Config) { c.Pool.MaxRequestsPerConnection = 0 }, func(c *settings.Config) { c.Pool.MaxOrphanedStreams = -1 }, func(c *settings.Config) { c.Pool.SourcePortStart = 0 }, func(c *settings.Config) { c.Pool.SourcePortStart = 65535; c.Pool.SourcePortEnd = 49152 }, func(c *settings.Config) { c.Linger = cqltype.Option[time.Duration]{Value: time.Millisecond, Set: true} }} {
				invalid := settings.DefaultConfig("localhost")
				change(&invalid)
				if err := invalid.Validate(); err == nil {
					t.Fatal("invalid transport setting accepted")
				}
			}
			for _, capacity := range []int{1, 2, 63, 64, 65, 32768} {
				gen := streams.NewLimited(capacity)
				seen := make(map[int]bool)
				for i := 0; i < capacity; i++ {
					id, ok := gen.GetStream()
					if !ok || id < 0 || id >= capacity || seen[id] {
						t.Fatal("inexact capacity", capacity, id, ok)
					}
					seen[id] = true
				}
				if _, ok := gen.GetStream(); ok || gen.Available() != 0 || !seen[0] {
					t.Fatal("capacity/stream zero", capacity)
				}
			}
		})

		t.Run("ZeroSemantics", func(t *testing.T) {
			c := settings.DefaultConfig("localhost")
			p := *c.DefaultProfile
			if c.Metadata.PeriodicRefresh != time.Minute || c.Metadata.RequestTimeout != 31*time.Second || p.Timeout != 30*time.Second || p.PageTimeout != 0 || p.Prefetch != 1 || p.PageSize != 5000 || p.Consistency != cqltype.LocalQuorum {
				t.Fatal(c.Metadata, p)
			}
			for _, edit := range []func(*settings.Config){func(c *settings.Config) { c.Metadata.AgreementTimeout = 0 }, func(c *settings.Config) { c.Metadata.AgreementPoll = 0 }, func(c *settings.Config) {
				c.Metadata.ServerTimeout = implCql.Option[time.Duration]{Set: true, Value: time.Nanosecond}
			}} {
				invalid := settings.DefaultConfig("localhost")
				edit(&invalid)
				if invalid.Validate() == nil {
					t.Fatal("invalid explicit setting accepted")
				}
			}
			c.ConnectTimeout = 0
			c.DNSTimeout = 0
			c.Metadata.PeriodicRefresh = 0
			c.Metadata.RequestTimeout = 0
			c.Metadata.ServerTimeout = implCql.Option[time.Duration]{Set: true, Value: 0}
			if err := c.Validate(); err != nil {
				t.Fatal(err)
			}
			s := func() *cluster.Core {
				opaqueStorageValue := new(cluster.Core)
				return opaqueStorageValue
			}()

			InternalRuntimeFor(s).InternalSetProfile(c.DefaultProfile)
			options := testOptions(Options{Timeout: implCql.Option[time.Duration]{Set: true}, PageTimeout: implCql.Option[time.Duration]{Set: true}, NoSerialConsistency: true, Prefetch: implCql.Option[int]{Set: true}, Timestamp: implCql.Option[int64]{Set: true}, NowInSeconds: implCql.Option[int32]{Set: true}})
			resolved, err := resolveOptions(s, options)
			if err != nil {
				t.Fatal(err)
			}
			if resolved.InternalProfile.Timeout != 0 || resolved.InternalProfile.PageTimeout != 0 || resolved.InternalProfile.Prefetch != 0 || resolved.InternalProfile.SerialConsistency.Set || !resolved.InternalOptions.Timestamp.Set || !resolved.InternalOptions.NowInSeconds.Set {
				t.Fatal(resolved)
			}
		})
	})

	t.Run("IncludeMetadata", func(t *testing.T) {
		t.Run("ChangesExecuteWireFlag", func(t *testing.T) {
			s, _, _ := runtimeTestSession(t, 0)
			defer s.Close()
			c, peer := typedTestConn(t, 4, nil)
			id := cqltype.UUID{15: 1}
			host := new(implEndpoint.HostInfo)
			implEndpoint.HostInfoData(host).InternalHostId = id
			implEndpoint.HostInfoData(host).InternalState.Store(implEndpoint.NodeUp)
			c.InternalHost = host
			c.InternalHooks = s.NewConnectionHooks()
			implCluster.SessionData(s).InternalHostSource.InternalHosts[id.String()] = host
			picker := pool.InternalNewDefaultConnPicker(1)
			picker.Put(c)
			implCluster.SessionData(s).InternalPool.InternalHostConnPools[id] = &pool.InternalHostConnPool{InternalHost: host, InternalConnPicker: picker}
			c.InternalFramers.InternalDefaults.InternalScyllaUseMetadataID = true
			c.InternalRefreshNativeProtocol()
			go c.InternalServe(c.InternalCtx)
			columns := cqltype.NewColumns(cqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: cqltype.BlobType})
			state, _ := preparedState(t, s, columns)
			handle := new(Prepared[codec.EmptyRecord, cqltype.Bytes])
			storage := PreparedData(handle)
			storage.InternalState = state
			storage.params = codecTestMust(codec.EmptyParamsEncoder.Compile(cqltype.NewColumns()))
			storage.rows = codec.OneColumn(codec.BlobCodec)
			storage.plans = &InternalResultPlanCache[cqltype.Bytes]{}
			bound := codecTestMust(BindBorrowed(handle, codec.EmptyRecord{}))
			defer bound.Close()
			done := make(chan error, 1)
			go func() {
				for _, include := range []bool{false, true, false} {
					opcode, stream, body, err := readRequest(peer)
					if err != nil {
						done <- err
						return
					}
					if opcode != 10 {
						done <- fmt.Errorf("wanted EXECUTE got%d", opcode)
						return
					}
					at := 0
					at += 2 + int(binary.BigEndian.Uint16(body[at:]))
					at += 2 + int(binary.BigEndian.Uint16(body[at:]))
					at += 2
					skip := body[at]&2 != 0
					if skip == include {
						done <- fmt.Errorf("IncludeMetadata=%t produced skip=%t", include, skip)
						return
					}
					if _, err = peer.Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "value"))); err != nil {
						done <- err
						return
					}
				}
				done <- nil
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			for _, include := range []bool{false, true, false} {
				page := codecTestMust(RunPage(ctx, s, bound, model.StartPage(), testOptions(Options{IncludeMetadata: include, PageSize: cqltype.Some(cqltype.Unpaged)})))
				var row cqltype.Bytes
				if !page.NextInto(&row) || string(row.OwnedBytes()) != "value" {
					t.Fatal(page.Err())
				}
				if err := page.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			bound.Close()
			if err := bound.Wait(ctx); err != nil {
				t.Fatal(err)
			}
		})
	})

	t.Run("PagingSettings", func(t *testing.T) {
		t.Run("ManualAndBatchExplicit", func(t *testing.T) {
			s, _, _ := runtimeTestSession(t, 0)
			b := runtimeQuery(t)
			defer b.Close()
			if _, err := InternalCompileOptions(s, Options{PageSize: cqltype.Some(cqltype.Unpaged), Prefetch: implCql.Option[int]{Set: true, Value: 1}}); !hasErrorType[*settings.UnsupportedSettingError](err) {
				t.Fatal("unpaged with prefetch", err)
			}
			_, err := RunPage(context.Background(), s, b, model.StartPage(), testOptions(Options{Prefetch: implCql.Option[int]{Set: true, Value: 1}}))
			if !hasErrorType[*settings.UnsupportedSettingError](err) {
				t.Fatal(err)
			}
			var batch Batch
			if err := InternalAddBound(&batch, s, b); err != nil {
				t.Fatal(err)
			}
			_, err = InternalRunBatch(context.Background(), s, cqltype.LoggedBatch, &batch, codec.EmptyRowDecoder, testOptions(Options{PageSize: implCql.Option[int32]{Set: true, Value: 1}}))
			if !hasErrorType[*settings.UnsupportedSettingError](err) {
				t.Fatal(err)
			}
		})
	})

	t.Run("Report", func(t *testing.T) {
		t.Run("CurrentTypedProfile", func(t *testing.T) {
			config := settings.DefaultConfig("127.0.0.1")
			profile := *config.DefaultProfile
			routing, err := implRouting.NewRoutingPolicy(implRouting.RoutingOptions{TokenAware: true, Datacenter: "dc", Rack: "rack", AllowRemote: true, Latency: implRouting.LatencyOptions{Enabled: true}})
			if err != nil {
				t.Fatal(err)
			}
			profile.Routing = routing
			profile.Retry = retry.Safe{MaxAttempts: 7}
			profile.PageSize = 23
			config.DefaultProfile = &profile
			s := func() *cluster.Core {
				opaqueStorageValue := new(cluster.Core)
				implCluster.SessionData(opaqueStorageValue).InternalCfg = *cluster.InternalNewClusterSettings()
				implCluster.SessionData(opaqueStorageValue).InternalTypedState = &InternalSessionRuntime{InternalProfile: config.DefaultProfile, config: config}
				return opaqueStorageValue
			}()
			report := s.BuildQueryReport()
			if report.Defaults.Page.Size != 23 || report.Retry.Policy.(cluster.InternalRetryPolicySimpleReport).MaxRetries != 6 {
				t.Fatalf("typed defaults: %+v", report)
			}
			location := report.LoadBalancing.NodePreference.(cluster.InternalNodeLocationRackReport)
			if location.LocalDC != "dc" || location.LocalRack != "rack" {
				t.Fatalf("typed locality: %+v", location)
			}
			policy := report.LoadBalancing.Policy.(cluster.InternalLoadBalancingTokenAwareReport)
			if !policy.FallbackToNonPreferredNodes || policy.AdaptiveOrdering.Signals[0] != "latency" {
				t.Fatalf("typed routing: %+v", policy)
			}
			profile.PageSize = 37
			profile.Retry = retry.Safe{MaxAttempts: 1}
			config.DefaultProfile = &profile
			next := s.BuildQueryReport()
			if next.Defaults.Page.Size != 37 || next.Retry.Policy.(cluster.InternalRetryPolicySimpleReport).MaxRetries != 0 {
				t.Fatalf("replaced profile: %+v", next)
			}
		})

		t.Run("Retry", func(t *testing.T) {
			for _, tc := range []struct{ attempts, want int }{{-1, 2}, {0, 2}, {1, 0}, {3, 2}, {7, 6}} {
				got := cluster.InternalBuildRetryPolicyReport(retry.Safe{MaxAttempts: tc.attempts})
				want := cluster.InternalRetryPolicySimpleReport{Type: "simple", MaxRetries: tc.want}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("attempts %d: got %#v want %#v", tc.attempts, got, want)
				}
			}
			for _, policy := range []retry.Policy{(*customRetry)(nil), &customRetry{}} {
				got := cluster.InternalBuildRetryPolicyReport(policy).(cluster.InternalRetryPolicyCustomReport)
				if got.Name != "customRetry" || got.Type != "custom" {
					t.Fatalf("custom report: %+v", got)
				}
			}
		})
	})

	t.Run("Snapshot", func(t *testing.T) {
		config := settings.DefaultConfig("localhost")
		config.TLS = &tls.Config{ServerName: "original"}
		config.Metadata.Keyspaces = []string{"ks"}
		copy := config.Snapshot()
		config.ContactPoints[0] = "changed"
		config.Metadata.Keyspaces[0] = "changed"
		config.TLS.ServerName = "changed"
		delete(config.Partitioners, "Murmur3Partitioner")
		if copy.ContactPoints[0] != "localhost" || copy.Metadata.Keyspaces[0] != "ks" || copy.TLS.ServerName != "original" || copy.Partitioners["Murmur3Partitioner"] == nil {
			t.Fatal("configuration aliases mutable input")
		}
		for _, edit := range []func(*settings.Config){func(c *settings.Config) { c.Protocol = 6 }, func(c *settings.Config) { c.Metadata.AgreementPoll = -1 }, func(c *settings.Config) { c.Limits.MaxMetadataDepth = 129 }, func(c *settings.Config) { c.Pool.MaxRequestsPerConnection = 32769 }, func(c *settings.Config) { c.Metadata.Keyspaces = []string{"a", "a"} }} {
			invalid := settings.DefaultConfig("localhost")
			edit(&invalid)
			err := invalid.Validate()
			if _, ok := errors.AsType[*cqltype.ValidationError](err); !ok {
				t.Fatalf("validation error %v", err)
			}
		}
	})

	t.Run("SocketBuffers", func(t *testing.T) {
		t.Run("ReachActualSocket", func(t *testing.T) {
			listener := codecTestMust(net.Listen("tcp", "127.0.0.1:0"))
			defer listener.Close()
			accepted := make(chan net.Conn, 1)
			go func() {
				c, e := listener.Accept()
				if e == nil {
					accepted <- c
				}
			}()
			raw := codecTestMust(net.Dial("tcp", listener.Addr().String()))
			defer raw.Close()
			peer := <-accepted
			defer peer.Close()
			wrapped := &socketOptions{TCPConn: raw.(*net.TCPConn)}
			config := settings.DefaultConfig("localhost")
			config.ReceiveBuffer = cqltype.Option[int]{Set: true, Value: 4096}
			config.SendBuffer = cqltype.Option[int]{Set: true, Value: 8192}
			config.Linger = cqltype.Option[time.Duration]{Set: true, Value: 2 * time.Second}
			config.SocketKeepalive = 7 * time.Second
			if err := applyV2SocketOptions(wrapped, config, "tcp"); err != nil {
				t.Fatal(err)
			}
			if wrapped.read != 4096 || wrapped.write != 8192 || wrapped.linger != 2 || wrapped.period != 7*time.Second {
				t.Fatal("socket settings were not applied", wrapped)
			}
			zero := config
			zero.ReceiveBuffer = cqltype.Option[int]{Set: true}
			zero.SendBuffer = cqltype.Option[int]{Set: true}
			zero.Linger = cqltype.Option[time.Duration]{Set: true}
			if err := applyV2SocketOptions(wrapped, zero, "tcp"); err != nil {
				t.Fatal(err)
			}
			if wrapped.read != 0 || wrapped.write != 0 || wrapped.linger != 0 || wrapped.readCalls != 2 || wrapped.writeCalls != 2 || wrapped.lingerCalls != 2 {
				t.Fatal("explicit zero socket settings were omitted", wrapped)
			}
			// Pipe has no TCP preflight controls, so each requested option reaches its
			// own unsupported-setting branch rather than failing earlier at NODELAY.
			for _, field := range []string{"receive", "send", "linger"} {
				a, b := net.Pipe()
				c := settings.DefaultConfig("localhost")
				switch field {
				case "receive":
					c.ReceiveBuffer = config.ReceiveBuffer
				case "send":
					c.SendBuffer = config.SendBuffer
				case "linger":
					c.Linger = config.Linger
				}
				e := applyV2SocketOptions(a, c, "tcp")
				a.Close()
				b.Close()
				if unsupported, ok := errors.AsType[*settings.UnsupportedSettingError](e); !ok || unsupported.Setting != map[string]string{"receive": "receive buffer", "send": "send buffer", "linger": "linger"}[field] {
					t.Fatal("wrong unsupported setting", field, e)
				}
			}
		})

		t.Run("UnsupportedKeepTypedError", func(t *testing.T) {
			a, b := net.Pipe()
			defer a.Close()
			defer b.Close()
			config := settings.DefaultConfig("localhost")
			config.ReceiveBuffer = cqltype.Option[int]{Value: 4096, Set: true}
			if err := applyV2SocketOptions(a, config, "tcp"); !hasErrorType[*settings.UnsupportedSettingError](err) {
				t.Fatal("unsupported backend setting", err)
			}
		})
	})

	t.Run("TimestampGenerator", func(t *testing.T) {
		t.Run("Concurrent", func(t *testing.T) {
			generator := settings.InternalNewTimestampGenerator()
			values := make(chan int64, 1000)
			for i := 0; i < 10; i++ {
				go func() {
					for j := 0; j < 100; j++ {
						values <- generator()
					}
				}()
			}
			seen := make(map[int64]bool)
			for i := 0; i < 1000; i++ {
				value := <-values
				if seen[value] {
					t.Fatal("duplicate timestamp")
				}
				seen[value] = true
			}
		})

		t.Run("OnceAcrossRetryAndPages", func(t *testing.T) {
			s, _, peers := runtimeTestSession(t, 1)
			defer s.Close()
			var calls atomic.Int32
			InternalRuntimeFor(s).config.TimestampGenerator = func() int64 { calls.Add(1); return 987654321 }
			done := make(chan error, 1)
			go func() {
				for step := 0; step < 5; step++ {
					_, stream, body, err := readRequest(peers[0])
					if err != nil {
						done <- err
						return
					}
					at := 4 + int(binary.BigEndian.Uint32(body[:4])) + 2
					hasTimestamp := body[at]&0x20 != 0
					if step < 3 {
						if !hasTimestamp || int64(binary.BigEndian.Uint64(body[len(body)-8:])) != 987654321 {
							done <- errors.New("generated timestamp changed across attempts/pages")
							return
						}
					}
					if step == 3 && (!hasTimestamp || binary.BigEndian.Uint64(body[len(body)-8:]) != 0) {
						done <- errors.New("explicit zero timestamp lost")
						return
					}
					if step == 4 && hasTimestamp {
						done <- errors.New("conditional operation generated timestamp")
						return
					}
					var response []byte
					if step == 0 {
						response = errorResponse(t, stream, protocol.ErrCodeServer, nil)
					} else if step == 1 {
						response = typedTestResponse(t, 4, stream, runtimeRowsBody([]byte("page"), "a"))
					} else {
						response = typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "b"))
					}
					if _, err = peers[0].Write(response); err != nil {
						done <- err
						return
					}
				}
				done <- nil
			}()
			profile := settings.InternalDefaultProfile()
			profile.Prefetch = 0
			profile.Retry = retryOnce{}
			options := testOptions(Options{Profile: new(profile)})
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			b := runtimeQuery(t)
			rows := codecTestMust(Run(ctx, s, b, options))
			var count int
			var row cqltype.Bytes
			for rows.NextInto(&row) {
				count++
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			b.Close()
			if err := b.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			if count != 2 || calls.Load() != 1 {
				t.Fatal("generator not one per logical execution", count, calls.Load())
			}
			explicit := runtimeQuery(t)
			values := options.Values()
			values.Timestamp = cqltype.Option[int64]{Set: true, Value: 0}
			values.PageSize = cqltype.Some(cqltype.Unpaged)
			opts := testOptions(values)
			page := codecTestMust(RunPage(ctx, s, explicit, model.StartPage(), opts))
			if err := page.Close(); err != nil {
				t.Fatal(err)
			}
			explicit.Close()
			if err := explicit.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			conditional := codecTestMust(BindUnpreparedBorrowed(binding.Statement[codec.EmptyRecord, cqltype.Bytes]{Kind: cqltype.ConditionalStatement, CQL: "SELECT value FROM ks.tbl", Params: codec.EmptyParamsEncoder, Rows: codec.OneColumn(codec.BlobCodec)}, cqltype.NewColumns(), codec.EmptyRecord{}))
			unpaged := options.Values()
			unpaged.PageSize = cqltype.Some(cqltype.Unpaged)
			page = codecTestMust(RunPage(ctx, s, conditional, model.StartPage(), testOptions(unpaged)))
			if err := page.Close(); err != nil {
				t.Fatal(err)
			}
			conditional.Close()
			if err := conditional.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 1 {
				t.Fatal("explicit/conditional execution invoked generator", calls.Load())
			}
		})
	})
}

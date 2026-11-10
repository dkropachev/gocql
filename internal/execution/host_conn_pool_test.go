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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync/atomic"
	"testing"
	"time"

	implCql "github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	implCqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/model"
	implModel "github.com/scylladb/gocql/v2/internal/model"
	implPool "github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/routing"
	implRuntimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func TestHostConnPool(t *testing.T) {
	t.Run("Connected", func(t *testing.T) {
		t.Run("LateCannotUndoDownOrRemoval", func(t *testing.T) {
			f := newNodeEventFixture(t)
			h := f.addHost(t, "127.0.10.1")
			old, _ := implCluster.SessionData(f.session).InternalPool.InternalGetPool(h)
			f.session.HandleNodeDown(h.ConnectAddress(), h.Port())
			f.session.HandlePoolConnected(old)
			if h.IsUp() {
				t.Fatal("delayed fill callback undid down")
			}
			implCluster.SessionData(f.session).InternalHostSource.InternalRemoveHost(h.HostID())
			f.session.HandlePoolConnected(old)
			if h.IsUp() {
				t.Fatal("delayed callback resurrected removed host")
			}
		})

		t.Run("PausedAcrossNewerDown", func(t *testing.T) {
			f := newNodeEventFixture(t)
			host := f.addHost(t, "127.0.10.1")
			conn, _ := typedTestConn(t, 4, nil)
			conn.InternalHost = host
			pool := &implPool.InternalHostConnPool{InternalHost: host, InternalServices: f.session.NewPoolServices(), InternalConnPicker: &implPool.InternalDefaultConnPicker{InternalConns: []*transport.Conn{conn}}}
			implCluster.SessionData(f.session).InternalPool.InternalMu.Lock()
			implCluster.SessionData(f.session).InternalPool.InternalHostConnPools[implEndpoint.InternalHostInfoHostUUID(host)] = pool
			implCluster.SessionData(f.session).InternalPool.InternalMu.Unlock()
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			p := f.session.Peripheral()
			p.InternalConfig.LifecycleObserver = lifecycleFunc(func(e settings.LifecycleEvent) {
				if e.Stage == "connected" {
					close(entered)
					<-release
				}
			})
			go func() { defer close(done); f.session.HandlePoolConnected(pool) }()
			<-entered
			f.session.HandleNodeDown(host.ConnectAddress(), host.Port())
			close(release)
			<-done
			if host.IsUp() || len(f.policy.hostUpCalls()) != 0 {
				t.Fatal("delayed callback reintroduced host after DOWN", f.policy.hostUpCalls())
			}
		})
	})

	t.Run("Opening", func(t *testing.T) {
		t.Run("EndpointGenerationAndClosedFence", func(t *testing.T) {
			for _, mode := range []string{"endpoint", "closed"} {
				t.Run(mode, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					client, server := net.Pipe()
					defer server.Close()
					selected, release := make(chan struct{}), make(chan struct{})
					config := settings.DefaultConfig("localhost")
					config.Pool.ConnectionsPerHost = 1
					config.KeepaliveInterval = 0
					config.DialHost = func(ctx context.Context, target endpoint.DialTarget) (net.Conn, error) {
						close(selected)
						select {
						case <-release:
							return client, nil
						case <-ctx.Done():
							client.Close()
							return nil, ctx.Err()
						}
					}
					host := func() *endpoint.HostInfo {
						opaqueStorageValuexxxxxxxxxxxxx := new(endpoint.HostInfo)
						implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxxxxxx).InternalConnectAddress = net.ParseIP("127.0.0.1")
						implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxxxxxx).InternalPort = 9042
						implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxxxxxx).InternalHostId = cqltype.UUID{15: 1}
						return opaqueStorageValuexxxxxxxxxxxxx
					}()
					s := func() *cluster.Core {
						opaqueStorageValuexxxxxxxxxxxxxx := new(cluster.Core)
						implCluster.SessionData(opaqueStorageValuexxxxxxxxxxxxxx).InternalCtx = ctx
						implCluster.SessionData(opaqueStorageValuexxxxxxxxxxxxxx).InternalLogger = logging.InternalNopLogger{}
						return opaqueStorageValuexxxxxxxxxxxxxx
					}()
					implCluster.SessionData(s).InternalCfg.ReconnectionPolicy = &implPool.NoReconnectionPolicy{}
					implCluster.SessionData(s).InternalConnCfg = &transport.ConnConfig{TypedConfig: config.ConnectivityPointer(), ProtoVersion: 4, CQLVersion: "3.0.0", ConnectTimeout: time.Second, HostDialer: &transport.InternalV2TLSHostDialer{InternalConfig: config.Connectivity()}, Logger: logging.InternalNopLogger{}}
					pool := s.NewHostConnPool(host, 1, "")
					defer pool.Close()
					serverDone := make(chan struct{})
					go func() {
						defer close(serverDone)
						for {
							var h [9]byte
							if _, err := io.ReadFull(server, h[:]); err != nil {
								return
							}
							if _, err := io.CopyN(io.Discard, server, int64(binary.BigEndian.Uint32(h[5:]))); err != nil {
								return
							}
							body := []byte{}
							op := frm.OpReady
							if h[4] == byte(frm.OpOptions) {
								op = frm.OpSupported
								body = []byte{0, 0}
							}
							response := []byte{0x84, 0, h[2], h[3], byte(op)}
							response = binary.BigEndian.AppendUint32(response, uint32(len(body)))
							response = append(response, body...)
							if _, err := server.Write(response); err != nil {
								return
							}
						}
					}()
					done := make(chan error, 1)
					go func() { done <- pool.InternalConnect() }()
					<-selected
					if mode == "endpoint" {
						implEndpoint.InternalHostInfoSetTranslatedConnectionInfo(host, endpoint.InternalTranslatedAddresses{CQL: endpoint.AddressPort{Address: net.ParseIP("127.0.0.2"), Port: 9042}})
						close(release)
					} else {
						pool.Close()
					}
					select {
					case err := <-done:
						if err == nil && mode == "endpoint" {
							t.Fatal("late endpoint opening accepted")
						}
					case <-time.After(2 * time.Second):
						t.Fatal("opening did not stop")
					}
					if pool.Size() != 0 {
						t.Fatal("late opening entered pool")
					}
					if mode == "closed" && pool.InternalTransportState() != implPool.InternalPoolClosed {
						t.Fatal("closed pool resurrected")
					}
					client.Close()
					<-serverDone
				})
			}
		})
	})

	t.Run("Repair", func(t *testing.T) {
		t.Run("PolicyAndClosedState", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			config := settings.DefaultConfig("localhost")
			policy := &testReconnect{calls: make(chan int, 4), again: true}
			config.Reconnect = policy
			s := func() *cluster.Core {
				opaqueStorageValuexxxxxxxxx := new(cluster.Core)
				implCluster.SessionData(opaqueStorageValuexxxxxxxxx).InternalCtx = ctx
				implCluster.SessionData(opaqueStorageValuexxxxxxxxx).InternalLogger = logging.InternalNopLogger{}
				implCluster.SessionData(opaqueStorageValuexxxxxxxxx).InternalConnCfg = &transport.ConnConfig{TypedConfig: config.ConnectivityPointer()}
				return opaqueStorageValuexxxxxxxxx
			}()
			pool := s.NewHostConnPool(func() *endpoint.HostInfo {
				opaqueStorageValuexxxxxxxxxx := new(endpoint.HostInfo)
				return opaqueStorageValuexxxxxxxxxx
			}(), 1, "")
			policy.pool = pool
			// A full empty test picker avoids network work but leaves the actual repair
			// scheduling/backoff state machine active.
			pool.InternalConnPicker = &implPool.InternalDefaultConnPicker{InternalSize: 0}
			pool.InternalScheduleTypedRepair()
			if attempt := <-policy.calls; attempt != 0 {
				t.Fatal(attempt)
			}
			deadline := time.After(time.Second)
			for {
				pool.InternalMu.RLock()
				pending := pool.InternalRepairScheduled
				pool.InternalMu.RUnlock()
				if !pending {
					break
				}
				select {
				case <-deadline:
					t.Fatal("repair timer did not finish")
				default:
					time.Sleep(time.Millisecond)
				}
			}
			policy.again = false
			pool.InternalScheduleTypedRepair()
			if attempt := <-policy.calls; attempt != 1 {
				t.Fatal("repair attempt not advanced", attempt)
			}
			pool.Close()
			if pool.InternalTransportState() != implPool.InternalPoolClosed {
				t.Fatal("pool not terminal")
			}
			pool.InternalScheduleTypedRepair()
			select {
			case <-policy.calls:
				t.Fatal("closed pool scheduled repair")
			default:
			}
		})

		t.Run("TriggersHonorPolicy", func(t *testing.T) {
			for _, again := range []bool{true, false} {
				t.Run(fmt.Sprint(again), func(t *testing.T) {
					c, _ := typedTestConn(t, 4, nil)
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					config := settings.DefaultConfig("localhost")
					config.Pool.ConnectionsPerHost = 1
					policy := &longRepairPolicy{again: again}
					config.Reconnect = policy
					var dials atomic.Int32
					s := func() *cluster.Core {
						opaqueStorageValuexxxxxxxxxxx := new(cluster.Core)
						implCluster.SessionData(opaqueStorageValuexxxxxxxxxxx).InternalCtx = ctx
						implCluster.SessionData(opaqueStorageValuexxxxxxxxxxx).InternalLogger = logging.InternalNopLogger{}
						implCluster.SessionData(opaqueStorageValuexxxxxxxxxxx).InternalConnCfg = &transport.ConnConfig{TypedConfig: config.ConnectivityPointer(), HostDialer: countingHostDialer{&dials}}
						return opaqueStorageValuexxxxxxxxxxx
					}()
					implCluster.SessionData(s).InternalCfg.ReconnectionPolicy = &implPool.NoReconnectionPolicy{}
					h := func() *endpoint.HostInfo {
						opaqueStorageValuexxxxxxxxxxxx := new(endpoint.HostInfo)
						implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxxxxx).InternalHostId = cqltype.UUID{15: 1}
						implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxxxxx).InternalState.Store(endpoint.NodeUp)
						return opaqueStorageValuexxxxxxxxxxxx
					}()
					c.InternalHost = h
					c.InternalHooks = s.NewConnectionHooks()
					pool := s.NewHostConnPool(h, 1, "")
					defer pool.Close()
					pool.InternalStarted = true
					pool.InternalState = implPool.InternalPoolReady
					pool.InternalConnPicker = codecTestMust(implPool.InternalNewTypedConnPicker(c, config.Pool))
					pool.InternalConnPicker.Put(c)
					implCluster.SessionData(s).InternalHostSource = &cluster.InternalRingDescriber{InternalHosts: map[string]*endpoint.HostInfo{h.HostID(): h}}
					implCluster.SessionData(s).InternalPool = &implPool.InternalPolicyConnPool{InternalHostConnPools: map[cqltype.UUID]*implPool.InternalHostConnPool{implEndpoint.InternalHostInfoHostUUID(h): pool}}
					pool.HandleError(c, io.EOF, true)
					for i := 0; i < 10; i++ {
						pool.Pick(nil)
						s.PickTypedPool(pool, model.Target{}, model.RoutingInfo{})
						pool.InternalFill()
					}
					wait, cancelWait := context.WithTimeout(context.Background(), 10*time.Millisecond)
					defer cancelWait()
					if r, err := pool.InternalReserveStream(wait, model.Target{HostID: implEndpoint.InternalHostInfoHostUUID(h)}, model.RoutingInfo{}); err == nil {
						r.InternalClose(false)
						t.Fatal("broken pool manufactured reservation")
					}
					if dials.Load() != 0 || policy.calls.Load() != 1 {
						t.Fatal("refill trigger bypassed active delay/stop", again, dials.Load(), policy.calls.Load())
					}
					pool.Close()
					pool.HandleError(c, io.EOF, true)
					pool.InternalFill()
					if dials.Load() != 0 || policy.calls.Load() != 1 {
						t.Fatal("closed pool restarted repair")
					}
				})
			}
		})
	})

	t.Run("Select", func(t *testing.T) {
		t.Run("HostPinSurvivesCustomPolicy", func(t *testing.T) {
			s, cs, _ := runtimeTestSession(t, 2)
			pinned := implEndpoint.InternalHostInfoHostUUID(cs[0].InternalHost)
			other := implEndpoint.InternalHostInfoHostUUID(cs[1].InternalHost)
			plan := &routing.InternalTargetPlan{InternalTargets: []model.Target{{HostID: other}}}
			if conn, err := s.SelectTypedConn(context.Background(), model.RoutingInfo{HostID: implCql.Option[cqltype.UUID]{Set: true, Value: pinned}}, plan); conn != nil || !errors.Is(err, implRuntimeerrors.ErrNoConnections) {
				t.Fatal("host pin escaped", conn, err)
			}
		})

		t.Run("PickerStrictAndAdvisory", func(t *testing.T) {
			options := settings.DefaultConfig("localhost").Pool
			makeConn := func(shard int) *transport.Conn {
				return &transport.Conn{InternalScyllaSupported: *func() *endpoint.ScyllaConnectionFeatures {
					opaqueStorageValuexx := new(endpoint.ScyllaConnectionFeatures)
					opaqueStorageValuexx.ScyllaHostFeatures = *func() *endpoint.ScyllaHostFeatures {
						opaqueStorageValuex := new(endpoint.ScyllaHostFeatures)
						implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuex).InternalNrShards = 2
						return opaqueStorageValuex
					}()
					implEndpoint.ScyllaConnectionFeaturesData(opaqueStorageValuexx).InternalShard = shard
					return opaqueStorageValuexx
				}(), InternalStreams: streams.New()}
			}
			a, b := makeConn(0), makeConn(1)
			p := codecTestMust(implPool.InternalNewTypedConnPicker(a, options))
			_ = p.Put(a)
			_ = p.Put(b)
			a.InternalExclusiveFlag.Store(true)
			if got := p.InternalPickTarget(implModel.Target{ShardID: implCqltype.Option[uint32]{Value: 0, Set: true}}, implModel.RoutingInfo{}); got != b {
				t.Fatal("advisory hint could not use free capacity")
			}
			if got := p.InternalPickTarget(implModel.Target{StrictShard: true, ShardID: implCqltype.Option[uint32]{Value: 0, Set: true}}, implModel.RoutingInfo{}); got != a {
				t.Fatal("strict target relaxed while reserved")
			}
			if got := p.InternalPickTarget(implModel.Target{StrictShard: true, ShardID: implCqltype.Option[uint32]{Value: math.MaxUint32, Set: true}}, implModel.RoutingInfo{}); got != nil {
				t.Fatal("invalid strict target relaxed")
			}
		})

		t.Run("ReservedShardFreeAlternative", func(t *testing.T) {
			makeConn := func(shard int) *transport.Conn {
				return &transport.Conn{InternalScyllaSupported: *func() *endpoint.ScyllaConnectionFeatures {
					opaqueStorageValuexxxxxxxxxxxxxxxx := new(endpoint.ScyllaConnectionFeatures)
					opaqueStorageValuexxxxxxxxxxxxxxxx.ScyllaHostFeatures = *func() *endpoint.ScyllaHostFeatures {
						opaqueStorageValuexxxxxxxxxxxxxxx := new(endpoint.ScyllaHostFeatures)
						implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuexxxxxxxxxxxxxxx).InternalNrShards = 2
						return opaqueStorageValuexxxxxxxxxxxxxxx
					}()
					implEndpoint.ScyllaConnectionFeaturesData(opaqueStorageValuexxxxxxxxxxxxxxxx).InternalShard = shard
					return opaqueStorageValuexxxxxxxxxxxxxxxx
				}(), InternalStreams: streams.New()}
			}
			first, second := makeConn(0), makeConn(1)
			p := codecTestMust(implPool.InternalNewTypedConnPicker(first, settings.DefaultConfig("localhost").Pool))
			p.Put(first)
			p.Put(second)
			first.InternalExclusiveFlag.Store(true)
			routing := model.RoutingInfo{Token: cqltype.Option[int64]{Value: math.MinInt64, Set: true}}
			if got := p.InternalPickTarget(model.Target{}, routing); got != second {
				t.Fatal("non-strict selection waited despite idle eligible connection")
			}
			if got := p.InternalPickTarget(model.Target{StrictShard: true, ShardID: cqltype.Option[uint32]{Value: 0, Set: true}}, routing); got != first {
				t.Fatal("strict shard was not preserved")
			}
		})

		t.Run("StrictStreamingReservation", func(t *testing.T) {
			s, conns, _ := runtimeTestSession(t, 1)
			c := conns[0]
			pool := implCluster.SessionData(s).InternalPool.InternalHostConnPools[implEndpoint.InternalHostInfoHostUUID(c.InternalHost)]
			config := settings.DefaultConfig("localhost")
			config.Pool.ConnectionsPerHost = 1
			implCluster.SessionData(s).InternalConnCfg = &transport.ConnConfig{TypedConfig: config.ConnectivityPointer()}
			c.InternalCfg = implCluster.SessionData(s).InternalConnCfg
			pool.InternalServices = s.NewPoolServices()
			pool.InternalRepairCtx, pool.InternalRepairCancel = context.WithCancel(c.InternalCtx)
			defer pool.InternalRepairCancel()
			pool.InternalStarted = true
			pool.InternalState = implPool.InternalPoolReady
			pool.InternalConnPicker = codecTestMust(implPool.InternalNewTypedConnPicker(c, config.Pool))
			pool.InternalConnPicker.Put(c)
			c.InternalErrorHandler = pool
			target := implModel.Target{HostID: implEndpoint.InternalHostInfoHostUUID(c.InternalHost), ShardID: implCqltype.Option[uint32]{Value: 0, Set: true}, StrictShard: true}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			first := codecTestMust(pool.InternalReserveStream(ctx, target, implModel.RoutingInfo{}))
			done := make(chan *transport.InternalExclusiveConn, 1)
			errs := make(chan error, 1)
			go func() {
				next, err := pool.InternalReserveStream(ctx, target, implModel.RoutingInfo{})
				if err != nil {
					errs <- err
					return
				}
				done <- next
			}()
			select {
			case r := <-done:
				r.InternalClose(false)
				t.Fatal("strict reserved target created side capacity")
			case err := <-errs:
				t.Fatal(err)
			case <-time.After(10 * time.Millisecond):
			}
			if pool.Size() != 1 {
				t.Fatal("reservation changed configured capacity")
			}
			if err := first.InternalClose(true); err != nil {
				t.Fatal(err)
			}
			select {
			case next := <-done:
				if next.InternalConn != c {
					t.Fatal("strict reservation switched connection slot")
				}
				next.InternalClose(true)
			case err := <-errs:
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})

		t.Run("WaitsForReservedCapacity", func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			c := conns[0]
			pool := implCluster.SessionData(s).InternalPool.InternalHostConnPools[implEndpoint.InternalHostInfoHostUUID(c.InternalHost)]
			config := settings.DefaultConfig("localhost")
			config.Pool.ConnectionsPerHost = 1
			implCluster.SessionData(s).InternalConnCfg = &transport.ConnConfig{TypedConfig: config.ConnectivityPointer()}
			c.InternalCfg = &transport.ConnConfig{TypedConfig: config.ConnectivityPointer()}
			pool.InternalServices = s.NewPoolServices()
			pool.InternalRepairCtx, pool.InternalRepairCancel = context.WithCancel(c.InternalCtx)
			defer pool.InternalRepairCancel()
			pool.InternalStarted = true
			pool.InternalState = implPool.InternalPoolReady
			pool.InternalConnPicker = codecTestMust(implPool.InternalNewTypedConnPicker(c, config.Pool))
			pool.InternalConnPicker.Put(c)
			c.InternalErrorHandler = pool
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			r := codecTestMust(pool.InternalReserveStream(ctx, model.Target{HostID: implEndpoint.InternalHostInfoHostUUID(c.InternalHost)}, model.RoutingInfo{}))
			bound := runtimeQuery(t)
			defer bound.Close()
			done := make(chan error, 1)
			go func() {
				rows, err := Run(ctx, s, bound, testOptions(Options{HostID: cqltype.Option[cqltype.UUID]{Value: implEndpoint.InternalHostInfoHostUUID(c.InternalHost), Set: true}}))
				rows.Close()
				done <- err
			}()
			select {
			case err := <-done:
				t.Fatal("normal typed selection failed instead of waiting for reserved slot", err)
			case <-time.After(10 * time.Millisecond):
			}
			if n := pool.Size(); n != 1 {
				t.Fatal("waiting created extra pool slots", n)
			}
			go func() {
				stream := typedTestReadRequest(t, c, peers[0])
				writeResponse(t, c, peers[0], stream, typedTestRowsBody([]byte("resumed")))
			}()
			if err := r.InternalClose(true); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal("waiter failed after release", err)
				}
			case <-ctx.Done():
				t.Fatal("waiter did not resume")
			}
			r = codecTestMust(pool.InternalReserveStream(ctx, model.Target{HostID: implEndpoint.InternalHostInfoHostUUID(c.InternalHost)}, model.RoutingInfo{}))
			defer r.InternalClose(true)
			wait, cancelWait := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancelWait()
			_, err := Run(wait, s, bound, testOptions(Options{HostID: cqltype.Option[cqltype.UUID]{Value: implEndpoint.InternalHostInfoHostUUID(c.InternalHost), Set: true}}))
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("waiting ignored caller cancellation", err)
			}
		})
	})

	t.Run("Shards", func(t *testing.T) {
		t.Run("ExcessAndLateSharderFence", func(t *testing.T) {
			makeConn := func(shard, shards int) *transport.Conn {
				return &transport.Conn{InternalScyllaSupported: *func() *endpoint.ScyllaConnectionFeatures {
					opaqueStorageValuexxxxx := new(endpoint.ScyllaConnectionFeatures)
					opaqueStorageValuexxxxx.ScyllaHostFeatures = *func() *endpoint.ScyllaHostFeatures {
						opaqueStorageValuexxxx := new(endpoint.ScyllaHostFeatures)
						implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuexxxx).InternalNrShards = shards
						return opaqueStorageValuexxxx
					}()
					implEndpoint.ScyllaConnectionFeaturesData(opaqueStorageValuexxxxx).InternalShard = shard
					return opaqueStorageValuexxxxx
				}(), InternalStreams: streams.New()}
			}
			opts := settings.DefaultConfig("localhost").Pool
			opts.MaxExcessPerShard = 1
			c := makeConn(0, 3)
			p := codecTestMust(implPool.InternalNewTypedConnPicker(c, opts))
			if p.Put(c) != nil || p.Put(makeConn(0, 3)) != nil {
				t.Fatal("initial/exact excess rejected")
			}
			if p.Put(makeConn(0, 3)) == nil {
				t.Fatal("one shard exceeded excess cap")
			}
			opts.MaxExcessPerShard = 0
			p = codecTestMust(implPool.InternalNewTypedConnPicker(c, opts))
			p.Put(c)
			if p.Put(makeConn(0, 3)) == nil || p.GetExcessConnectionCount() != 0 {
				t.Fatal("zero excess ignored")
			}
			generation := p.InternalOpeningGeneration()
			if err := p.InternalPut(makeConn(0, 2), generation, true); err != nil {
				t.Fatal(err)
			}
			if err := p.InternalPut(makeConn(1, 3), generation, true); err == nil || p.GetShardCount() != 2 {
				t.Fatal("late old sharder reinstalled")
			}
		})
	})
}

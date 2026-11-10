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

package cluster

import (
	"context"
	"math"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gocql/v2/internal/debounce"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	implPool "github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func TestHostConnPool(t *testing.T) {
	// TestHostConnPool/Close verifies that hostConnPool.Close() does not
	// self-deadlock when defaultConnPicker closes connections that trigger
	// HandleError callbacks.
	//
	// Deadlock chain (before fix):
	//
	//	Close() -> connPicker.Close() -> conn.Close() -> HandleError() -> pool.mu.Lock() (DEADLOCK)
	t.Run("Close", func(t *testing.T) {
		t.Parallel()

		host := func() *endpoint.HostInfo {
			opaqueStorageValue := new(endpoint.HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValue).InternalConnectAddress = net.ParseIP("127.0.0.1")
			implEndpoint.HostInfoData(opaqueStorageValue).InternalPort = 9042
			return opaqueStorageValue
		}()
		session := func() *Core {
			opaqueStorageValuex := new(Core)
			SessionData(opaqueStorageValuex).InternalCfg = InternalClusterSettings{
				NumConns:         2,
				ConvictionPolicy: &settings.SimpleConvictionPolicy{},
			}
			SessionData(opaqueStorageValuex).InternalLogger = logging.InternalNopLogger{}
			return opaqueStorageValuex
		}()

		pool := &implPool.InternalHostConnPool{
			InternalServices:   session.NewPoolServices(),
			InternalHost:       host,
			InternalSize:       2,
			InternalKeyspace:   "test",
			InternalConnPicker: implPool.InternalNopConnPicker{},
			InternalLogger:     logging.InternalNopLogger{},
			InternalDebouncer:  debounce.NewSimpleDebouncer(),
		}

		// Build a defaultConnPicker with Conns that trigger HandleError on Close.
		picker := implPool.InternalNewDefaultConnPicker(2)
		for i := 0; i < 2; i++ {
			ctx, cancel := context.WithCancel(context.Background())
			conn := &transport.Conn{
				InternalR:            errorConn{},
				InternalErrorHandler: pool,
				InternalCancel:       cancel,
				InternalCtx:          ctx,
				InternalLogger:       logging.InternalNopLogger{},
			}
			_ = picker.Put(conn)
		}
		pool.InternalConnPicker = picker

		// Close must return before timeout; otherwise the pool is deadlocked.
		done := make(chan struct{})
		go func() {
			pool.Close()
			close(done)
		}()

		select {
		case <-done:
			// Success — Close returned without deadlocking.
		case <-time.After(5 * time.Second):
			t.Fatal("hostConnPool.Close() deadlocked: timed out after 5 seconds")
		}
	})

	t.Run("Connect", func(t *testing.T) {
		t.Parallel()

		// TestHostConnPool/Connect/ClosedPoolNoDeadlock verifies that connect's
		// already-closed-pool path does not close a connection while holding pool.mu.
		t.Run("ClosedPoolNoDeadlock", func(t *testing.T) {
			t.Parallel()

			host := func() *endpoint.HostInfo {
				opaqueStorageValuexx := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuexx).InternalConnectAddress = net.ParseIP("127.0.0.1")
				implEndpoint.HostInfoData(opaqueStorageValuexx).InternalPort = 9042
				return opaqueStorageValuexx
			}()
			session := func() *Core {
				opaqueStorageValuexxx := new(Core)
				SessionData(opaqueStorageValuexxx).InternalCfg = InternalClusterSettings{
					NumConns:         1,
					ConvictionPolicy: &settings.SimpleConvictionPolicy{},
				}
				SessionData(opaqueStorageValuexxx).InternalLogger = logging.InternalNopLogger{}
				return opaqueStorageValuexxx
			}()

			pool := &implPool.InternalHostConnPool{
				InternalServices:   session.NewPoolServices(),
				InternalHost:       host,
				InternalSize:       1,
				InternalKeyspace:   "test",
				InternalConnPicker: implPool.InternalNopConnPicker{},
				InternalLogger:     logging.InternalNopLogger{},
				InternalDebouncer:  debounce.NewSimpleDebouncer(),
				InternalClosed:     true,
			}

			ctx, cancel := context.WithCancel(context.Background())
			conn := &transport.Conn{
				InternalR:            errorConn{},
				InternalErrorHandler: pool,
				InternalCancel:       cancel,
				InternalCtx:          ctx,
				InternalLogger:       logging.InternalNopLogger{},
			}

			done := make(chan struct{})
			go func() {
				pool.InternalMu.Lock()
				if pool.InternalClosed {
					pool.InternalMu.Unlock()
					conn.Close()
				}
				close(done)
			}()

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("closed-pool connect cleanup deadlocked: timed out after 5 seconds")
			}
		})

		// TestHostConnPool/Connect/NoAttempts covers a ReconnectionPolicy that allows no
		// attempts after Validate passed: connect must return an error, not hand a nil
		// conn to initConnPicker.
		t.Run("NoAttempts", func(t *testing.T) {
			t.Parallel()

			for _, retries := range []int{0, -1} {
				t.Run(strconv.Itoa(retries), func(t *testing.T) {
					session := func() *Core {
						opaqueStorageValuexxxxxxxx := new(Core)
						SessionData(opaqueStorageValuexxxxxxxx).InternalCfg = InternalClusterSettings{ReconnectionPolicy: &implPool.ConstantReconnectionPolicy{MaxRetries: retries}}
						SessionData(opaqueStorageValuexxxxxxxx).InternalLogger = logging.InternalNopLogger{}
						return opaqueStorageValuexxxxxxxx
					}()

					pool := newUndialedHostPool(t, session, "127.0.0.1")

					err := pool.InternalConnect()
					if err == nil || !strings.Contains(err.Error(), "no connection attempted") {
						t.Fatalf("connect() = %v, want the no-attempts error", err)
					}
				})
			}
		})
	})

	t.Run("ConnectMany", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		config := settings.DefaultConfig("localhost")
		config.Pool.ConnectionsPerHost = math.MaxInt
		dialer := &boundedOpeningDialer{entered: make(chan struct{}, 64), release: make(chan struct{})}
		s := func() *Core {
			opaqueStorageValuexxxxxx := new(Core)
			SessionData(opaqueStorageValuexxxxxx).InternalCtx = ctx
			SessionData(opaqueStorageValuexxxxxx).InternalLogger = logging.InternalNopLogger{}
			SessionData(opaqueStorageValuexxxxxx).InternalConnCfg = &transport.ConnConfig{TypedConfig: config.ConnectivityConfigPointer(), ProtoVersion: 4, HostDialer: dialer, Logger: logging.InternalNopLogger{}}
			return opaqueStorageValuexxxxxx
		}()
		SessionData(s).InternalCfg.ReconnectionPolicy = &implPool.NoReconnectionPolicy{}
		pool := s.NewHostConnPool(func() *endpoint.HostInfo {
			opaqueStorageValuexxxxxxx := new(endpoint.HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValuexxxxxxx).InternalConnectAddress = net.ParseIP("127.0.0.1")
			implEndpoint.HostInfoData(opaqueStorageValuexxxxxxx).InternalPort = 9042
			return opaqueStorageValuexxxxxxx
		}(), math.MaxInt, "")
		defer pool.Close()
		done := make(chan error, 1)
		go func() { done <- pool.InternalConnectMany(math.MaxInt) }()
		for i := 0; i < 32; i++ {
			select {
			case <-dialer.entered:
			case <-time.After(3 * time.Second):
				close(dialer.release)
				t.Fatal("bounded workers not admitted", i)
			}
		}
		close(dialer.release)
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("failed batch reported success")
			}
		case <-time.After(time.Second):
			t.Fatal("huge target batch did not stop after failure")
		}
		if dialer.max.Load() > 32 || dialer.calls.Load() != 32 {
			t.Fatal("unbounded dials/goroutines for huge target", dialer.max.Load(), dialer.calls.Load())
		}
	})

	t.Run("InitConnPicker", func(t *testing.T) {
		t.Parallel()

		conn := mockConn(0)
		conn.InternalScyllaSupported = endpoint.InternalParseSupported(map[string][]string{
			endpoint.InternalLwtAddMetadataMarkKey: {"LWT_OPTIMIZATION_META_BIT_MASK=8"},
		}, &logging.InternalTestLogger{})
		require.True(t, conn.InternalIsScyllaConn())
		require.Zero(t, implEndpoint.ScyllaHostFeaturesValueData(conn.InternalGetScyllaSupported().ScyllaHostFeatures).InternalNrShards)

		pool := &implPool.InternalHostConnPool{
			InternalSize:       1,
			InternalConnPicker: implPool.InternalNopConnPicker{},
			InternalLogger:     &logging.InternalTestLogger{},
		}
		pool.InternalInitConnPicker(conn)

		_, isScyllaPicker := pool.InternalConnPicker.(*implPool.InternalScyllaConnPicker)
		assert.False(t, isScyllaPicker, "must not build scyllaConnPicker without shards")
	})

	// TestHostConnPool/PickConn is a regression
	// guard: a SelectedHost carrying an unboxed int64Token must still reach the
	// shard-aware picker. If the raw token were dropped, scyllaConnPicker would
	// fall back to leastBusyConn() and shard-aware routing would silently degrade.
	t.Run("PickConn", func(t *testing.T) {
		t.Parallel()

		picker := implPool.InternalScyllaConnPicker{InternalNrShards: 4, InternalMsbIgnore: 12, InternalNrConns: 4}
		picker.InternalConns = make([]*transport.Conn, 4)
		for i := range picker.InternalConns {
			picker.InternalConns[i] = &transport.Conn{InternalStreams: streams.New()}
		}

		tok := endpoint.InternalInt64Token(math.MinInt64)
		shard := picker.InternalShardOf(tok)
		want := picker.InternalConns[shard]

		pool := &implPool.InternalHostConnPool{InternalConnPicker: &picker}
		host := int64SelectedHost{info: func() *endpoint.HostInfo {
			opaqueStorageValue := new(endpoint.HostInfo)
			return opaqueStorageValue
		}(), tokenCasted: tok}

		got := pool.PickConn(host)
		if got != want {
			t.Fatalf("PickConn returned %p (least-busy fallback?); want shard-aware conn %p for shard %d",
				got, want, shard)
		}
	})
}

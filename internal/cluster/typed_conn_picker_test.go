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

package cluster

import (
	"context"
	"math"
	"strconv"
	"testing"

	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/pool"
	implPool "github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func TestTypedConnPicker(t *testing.T) {
	t.Run("Pick", func(t *testing.T) {
		options := settings.DefaultConfig("localhost").Pool
		options.ConnectionsPerShard = 3
		c := &transport.Conn{}
		p := codecTestMust(implPool.InternalNewTypedConnPicker(c, options))
		p.InternalConns[0] = []*transport.Conn{{InternalStreams: streams.New()}, {InternalStreams: streams.New()}, {InternalStreams: streams.New()}}
		p.InternalPos.Store(math.MaxInt64)
		if p.InternalPickTarget(model.Target{}, model.RoutingInfo{}) == nil {
			t.Fatal("wrapped position lost eligible connection")
		}
	})

	t.Run("PutInitial", func(t *testing.T) {
		makeConn := func(shard, shards int) *transport.Conn {
			return &transport.Conn{InternalScyllaSupported: *func() *endpoint.ScyllaConnectionFeatures {
				opaqueStorageValuexxxxxx := new(endpoint.ScyllaConnectionFeatures)
				opaqueStorageValuexxxxxx.ScyllaHostFeatures = *func() *endpoint.ScyllaHostFeatures {
					opaqueStorageValuexxxxx := new(endpoint.ScyllaHostFeatures)
					implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuexxxxx).InternalNrShards = shards
					return opaqueStorageValuexxxxx
				}()
				implEndpoint.ScyllaConnectionFeaturesData(opaqueStorageValuexxxxxx).InternalShard = shard
				return opaqueStorageValuexxxxxx
			}(), InternalStreams: streams.New()}
		}
		config := settings.DefaultConfig("localhost").Pool
		first := makeConn(0, 3)
		p := codecTestMust(pool.InternalNewTypedConnPicker(first, config))
		if err := p.InternalPutInitial(first); err != nil {
			t.Fatal(err)
		}
		if err := p.InternalPutInitial(makeConn(1, 3)); err != nil {
			t.Fatal("compatible initial opening rejected", err)
		}
		if err := p.InternalPutInitial(makeConn(0, 2)); err == nil || p.GetShardCount() != 3 {
			t.Fatal("late initial opening replaced established sharder")
		}
	})

	t.Run("ShardOf", func(t *testing.T) {
		features := endpoint.InternalParseSupported(validSupported(), logging.InternalNopLogger{})
		options := settings.DefaultConfig("localhost").Pool
		p := codecTestMust(implPool.InternalNewTypedConnPicker(&transport.Conn{InternalScyllaSupported: features}, options))
		if p.InternalShardOf(math.MinInt64) != 0 || p.InternalShardOf(math.MaxInt64) != 11 {
			t.Fatal("zero MSB semantics incorrect")
		}
		// The full32-bit LWT mask remains a wire bitset on both native architectures.
		values := validSupported()
		values[endpoint.InternalLwtAddMetadataMarkKey] = []string{"LWT_OPTIMIZATION_META_BIT_MASK=2147483648"}
		if err := transport.InternalValidateV2Supported(values, options); err != nil {
			t.Fatal(err)
		}
		ext := endpoint.InternalNewLwtAddMetaMarkExt(values, logging.InternalNopLogger{})
		if ext == nil || ext.InternalSerialize()[endpoint.InternalLwtAddMetadataMarkKey] != "LWT_OPTIMIZATION_META_BIT_MASK="+strconv.FormatUint(1<<31, 10) {
			t.Fatal("high LWT bit did not roundtrip")
		}
	})

	t.Run("Sizing", func(t *testing.T) {
		makeConn := func(shard int) *transport.Conn {
			return &transport.Conn{InternalScyllaSupported: *func() *endpoint.ScyllaConnectionFeatures {
				opaqueStorageValuex := new(endpoint.ScyllaConnectionFeatures)
				opaqueStorageValuex.ScyllaHostFeatures = *func() *endpoint.ScyllaHostFeatures {
					opaqueStorageValue := new(endpoint.ScyllaHostFeatures)
					implEndpoint.ScyllaHostFeaturesData(opaqueStorageValue).InternalNrShards = 3
					return opaqueStorageValue
				}()
				implEndpoint.ScyllaConnectionFeaturesData(opaqueStorageValuex).InternalShard = shard
				return opaqueStorageValuex
			}(), InternalStreams: streams.New(), InternalCtx: context.Background()}
		}
		c := makeConn(0)
		opts := settings.DefaultConfig("localhost").Pool
		opts.ConnectionsPerHost = 2
		p := codecTestMust(pool.InternalNewTypedConnPicker(c, opts))
		if err := p.Put(c); err != nil {
			t.Fatal(err)
		}
		if a, b := p.NextShard(); a != 0 || b != 0 {
			t.Fatal("host override targeted shard")
		}
		if err := p.Put(makeConn(0)); err != nil {
			t.Fatal(err)
		}
		if err := p.Put(makeConn(2)); err == nil {
			t.Fatal("host budget exceeded")
		}
		if n, m := p.Size(); n != 2 || m != 0 {
			t.Fatal(n, m)
		}
		opts.ConnectionsPerHost = 0
		opts.ConnectionsPerShard = 2
		p = codecTestMust(pool.InternalNewTypedConnPicker(c, opts))
		for i := 0; i < 6; i++ {
			shard, shards := p.NextShard()
			if shards != 3 {
				t.Fatal("no shard target", i)
			}
			if err := p.Put(makeConn(shard)); err != nil {
				t.Fatal(err)
			}
			p.InternalDialFinished(shard, shards)
		}
		if n, m := p.Size(); n != 6 || m != 0 {
			t.Fatal(n, m)
		}
	})

	t.Run("Target", func(t *testing.T) {
		options := settings.DefaultConfig("localhost").Pool
		for _, bad := range []endpoint.ScyllaConnectionFeatures{{ScyllaHostFeatures: *func() *endpoint.ScyllaHostFeatures {
			opaqueStorageValue := new(endpoint.ScyllaHostFeatures)
			implEndpoint.ScyllaHostFeaturesData(opaqueStorageValue).InternalNrShards = 65536
			return opaqueStorageValue
		}()}, {ScyllaHostFeatures: *func() *endpoint.ScyllaHostFeatures {
			opaqueStorageValuex := new(endpoint.ScyllaHostFeatures)
			implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuex).InternalNrShards = -1
			return opaqueStorageValuex
		}()}, {ScyllaHostFeatures: *func() *endpoint.ScyllaHostFeatures {
			opaqueStorageValuexx := new(endpoint.ScyllaHostFeatures)
			implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuexx).InternalNrShards = 2
			implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuexx).InternalMsbIgnore = 64
			return opaqueStorageValuexx
		}()}, *func() *endpoint.ScyllaConnectionFeatures {
			opaqueStorageValuexxxx := new(endpoint.ScyllaConnectionFeatures)
			opaqueStorageValuexxxx.ScyllaHostFeatures = *func() *endpoint.ScyllaHostFeatures {
				opaqueStorageValuexxx := new(endpoint.ScyllaHostFeatures)
				implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuexxx).InternalNrShards = 2
				return opaqueStorageValuexxx
			}()
			implEndpoint.ScyllaConnectionFeaturesData(opaqueStorageValuexxxx).InternalShard = 2
			return opaqueStorageValuexxxx
		}()} {
			if _, err := implPool.InternalNewTypedConnPicker(&transport.Conn{InternalScyllaSupported: bad}, options); err == nil {
				t.Fatal("invalid sharder reached allocation", bad)
			}
		}
		options.ConnectionsPerShard = math.MaxInt
		if _, err := transport.InternalCheckedV2PoolBudget(options, 2); err == nil {
			t.Fatal("target multiplication overflow")
		}
		options.ConnectionsPerShard = 1
		options.MaxExcessPerShard = math.MaxInt
		if _, err := transport.InternalCheckedV2PoolBudget(options, 2); err == nil {
			t.Fatal("excess multiplication overflow")
		}
		if _, err := transport.InternalCheckedV2PoolBudget(options, 1); err == nil {
			t.Fatal("target plus excess overflow")
		}
		options.MaxExcessPerShard = 0
		options.ConnectionsPerShard = math.MaxInt
		if target, err := transport.InternalCheckedV2PoolBudget(options, 1); err != nil || target != math.MaxInt {
			t.Fatal("valid representable large target capped", target, err)
		}
		options.ConnectionsPerHost = math.MaxInt
		options.MaxExcessPerShard = math.MaxInt
		if target, err := transport.InternalCheckedV2PoolBudget(options, 65535); err != nil || target != math.MaxInt {
			t.Fatal("inactive shard sizing overrode host target", target, err)
		}
		options = settings.DefaultConfig("localhost").Pool
		options.MaxExcessPerShard = 0
		c := &transport.Conn{InternalScyllaSupported: endpoint.ScyllaConnectionFeatures{ScyllaHostFeatures: *func() *endpoint.ScyllaHostFeatures {
			opaqueStorageValuexxxxx := new(endpoint.ScyllaHostFeatures)
			implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuexxxxx).InternalNrShards = 65535
			return opaqueStorageValuexxxxx
		}()}}
		p, err := implPool.InternalNewTypedConnPicker(c, options)
		if err != nil || len(p.InternalConns) != 65535 || p.InternalTarget() != 65535 {
			t.Fatal("valid largest sharder rejected", err)
		}
	})
}

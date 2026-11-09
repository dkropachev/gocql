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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func TestScyllaSupported(t *testing.T) {
	t.Run("IsScyllaConn", func(t *testing.T) {
		t.Parallel()

		makeConn := func(supported map[string][]string) *transport.Conn {
			conn := mockConn(0)
			conn.InternalSupported = supported
			conn.InternalScyllaSupported = endpoint.InternalParseSupported(supported, &logging.InternalTestLogger{})
			return conn
		}

		t.Run("Scylla with sharding", func(t *testing.T) {
			t.Parallel()
			conn := makeConn(map[string][]string{
				"SCYLLA_NR_SHARDS":                     {"12"},
				"SCYLLA_PARTITIONER":                   {"org.apache.cassandra.dht.Murmur3Partitioner"},
				"SCYLLA_SHARDING_ALGORITHM":            {"biased-token-round-robin"},
				"SCYLLA_SHARDING_IGNORE_MSB":           {"12"},
				endpoint.InternalLwtAddMetadataMarkKey: {"LWT_OPTIMIZATION_META_BIT_MASK=8"},
			})
			assert.True(t, conn.InternalIsScyllaConn())
		})

		t.Run("Scylla without sharding (allow_shard_aware_drivers: false)", func(t *testing.T) {
			t.Parallel()
			conn := makeConn(map[string][]string{
				endpoint.InternalLwtAddMetadataMarkKey: {"LWT_OPTIMIZATION_META_BIT_MASK=8"},
				endpoint.InternalRateLimitError:        {"ERROR_CODE=42"},
			})
			assert.True(t, conn.InternalIsScyllaConn())
		})

		t.Run("pure Cassandra", func(t *testing.T) {
			t.Parallel()
			conn := makeConn(map[string][]string{"CQL_VERSION": {"3.0.0"}})
			assert.False(t, conn.InternalIsScyllaConn())
		})
	})

	t.Run("LWTExt", func(t *testing.T) {
		t.Parallel()

		t.Run("init framer without cql extensions", func(t *testing.T) {
			// mock connection without cql extensions, expected not to have
			// the `flagLWT` field being set in the framer created out of it
			f := framerForExts(nil)
			if f.FlagLWT != 0 {
				t.Error("expected to have LWT flag uninitialized after framer init")
			}
		})

		t.Run("init framer with cql extensions", func(t *testing.T) {
			// create a mock connection, add `lwt` cql protocol extension to it,
			// ensure that framer recognizes this extension and adjusts appropriately
			framerWithLwtExt := framerForExts([]endpoint.InternalCqlProtocolExtension{
				&endpoint.InternalLwtAddMetadataMarkExt{
					InternalLwtOptMetaBitMask: 1,
				},
			})
			if framerWithLwtExt.FlagLWT == 0 {
				t.Error("expected to have LWT flag to be set after framer init")
			}
		})
	})

	t.Run("ParseSupported", func(t *testing.T) {
		t.Parallel()

		const (
			lwtMask        = 8
			rateLimitCode  = 42
			shardAwarePort = 19042
		)

		validSharding := map[string][]string{
			"SCYLLA_SHARD":                         {"3"},
			"SCYLLA_NR_SHARDS":                     {"12"},
			"SCYLLA_PARTITIONER":                   {"org.apache.cassandra.dht.Murmur3Partitioner"},
			"SCYLLA_SHARDING_ALGORITHM":            {"biased-token-round-robin"},
			"SCYLLA_SHARDING_IGNORE_MSB":           {"12"},
			"SCYLLA_SHARD_AWARE_PORT":              {fmt.Sprintf("%d", shardAwarePort)},
			endpoint.InternalLwtAddMetadataMarkKey: {fmt.Sprintf("LWT_OPTIMIZATION_META_BIT_MASK=%d", lwtMask)},
			endpoint.InternalRateLimitError:        {fmt.Sprintf("ERROR_CODE=%d", rateLimitCode)},
		}

		tests := []struct {
			name                  string
			supported             map[string][]string
			nrShards              int
			msbIgnore             uint64
			shardingAlgorithm     string
			partitioner           string
			lwtFlagMask           int
			rateLimitErrorCode    int
			shardAwarePort        uint16
			isMetadataIDSupported bool
			isScylla              bool
		}{
			{
				name:               "full valid Scylla with sharding",
				supported:          validSharding,
				nrShards:           12,
				msbIgnore:          12,
				shardingAlgorithm:  "biased-token-round-robin",
				partitioner:        "org.apache.cassandra.dht.Murmur3Partitioner",
				lwtFlagMask:        lwtMask,
				rateLimitErrorCode: rateLimitCode,
				shardAwarePort:     shardAwarePort,
				isScylla:           true,
			},
			{
				name: "Scylla without sharding (allow_shard_aware_drivers: false)",
				supported: map[string][]string{
					endpoint.InternalLwtAddMetadataMarkKey: {fmt.Sprintf("LWT_OPTIMIZATION_META_BIT_MASK=%d", lwtMask)},
					endpoint.InternalRateLimitError:        {fmt.Sprintf("ERROR_CODE=%d", rateLimitCode)},
					"SCYLLA_SHARD_AWARE_PORT":              {fmt.Sprintf("%d", shardAwarePort)},
				},
				nrShards:           0,
				msbIgnore:          0,
				shardingAlgorithm:  "",
				lwtFlagMask:        lwtMask,
				rateLimitErrorCode: rateLimitCode,
				shardAwarePort:     shardAwarePort,
				isScylla:           true,
			},
			{
				name: "invalid sharding (msbIgnore missing) with ports and LWT",
				supported: map[string][]string{
					"SCYLLA_NR_SHARDS":                     {"12"},
					"SCYLLA_PARTITIONER":                   {"org.apache.cassandra.dht.Murmur3Partitioner"},
					"SCYLLA_SHARDING_ALGORITHM":            {"biased-token-round-robin"},
					"SCYLLA_SHARD_AWARE_PORT":              {fmt.Sprintf("%d", shardAwarePort)},
					endpoint.InternalLwtAddMetadataMarkKey: {fmt.Sprintf("LWT_OPTIMIZATION_META_BIT_MASK=%d", lwtMask)},
				},
				nrShards:          0,
				msbIgnore:         0,
				shardingAlgorithm: "",
				partitioner:       "org.apache.cassandra.dht.Murmur3Partitioner",
				lwtFlagMask:       lwtMask,
				shardAwarePort:    shardAwarePort,
				isScylla:          true,
			},
			{
				name:                  "Scylla detected via SCYLLA_USE_METADATA_ID only",
				supported:             map[string][]string{"SCYLLA_USE_METADATA_ID": {""}},
				isMetadataIDSupported: true,
				isScylla:              true,
			},
			{
				name:           "shard-aware port only, no detection keys",
				supported:      map[string][]string{"SCYLLA_SHARD_AWARE_PORT": {fmt.Sprintf("%d", shardAwarePort)}},
				shardAwarePort: shardAwarePort,
				isScylla:       false,
			},
			{
				name:      "pure Cassandra (no SCYLLA keys)",
				supported: map[string][]string{"CQL_VERSION": {"3.0.0"}},
				isScylla:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				got := endpoint.InternalParseSupported(tt.supported, &logging.InternalTestLogger{})
				assert.Equal(t, tt.nrShards, implEndpoint.ScyllaHostFeaturesData(&got.ScyllaHostFeatures).InternalNrShards, "nrShards")
				assert.Equal(t, tt.msbIgnore, implEndpoint.ScyllaHostFeaturesData(&got.ScyllaHostFeatures).InternalMsbIgnore, "msbIgnore")
				assert.Equal(t, tt.shardingAlgorithm, implEndpoint.ScyllaHostFeaturesData(&got.ScyllaHostFeatures).InternalShardingAlgorithm, "shardingAlgorithm")
				assert.Equal(t, tt.partitioner, implEndpoint.ScyllaHostFeaturesData(&got.ScyllaHostFeatures).InternalPartitioner, "partitioner")
				assert.Equal(t, tt.lwtFlagMask, implEndpoint.ScyllaHostFeaturesData(&got.ScyllaHostFeatures).InternalLwtFlagMask, "lwtFlagMask")
				assert.Equal(t, tt.rateLimitErrorCode, implEndpoint.ScyllaHostFeaturesData(&got.ScyllaHostFeatures).InternalRateLimitErrorCode, "rateLimitErrorCode")
				assert.Equal(t, tt.shardAwarePort, implEndpoint.ScyllaHostFeaturesData(&got.ScyllaHostFeatures).InternalShardAwarePort, "shardAwarePort")
				assert.Equal(t, tt.isMetadataIDSupported, implEndpoint.ScyllaHostFeaturesData(&got.ScyllaHostFeatures).InternalIsMetadataIDSupported, "isMetadataIDSupported")
				assert.Equal(t, tt.isScylla, implEndpoint.ScyllaHostFeaturesData(&got.ScyllaHostFeatures).InternalIsScylla, "isScylla")
			})
		}
	})

	t.Run("RateLimitingExt", func(t *testing.T) {
		t.Parallel()

		t.Run("init framer without cql extensions", func(t *testing.T) {
			// mock connection without cql extensions, expected to have the `rateLimitingErrorCode`
			// field set to 0 (default, signifying no code)
			f := framerForExts(nil)
			if f.RateLimitingErrorCode != 0 {
				t.Error("expected to have rateLimitingErrorCode set to 0 (no code) after framer init")
			}
		})

		const mockCode = 42
		t.Run("init framer with cql extensions", func(t *testing.T) {
			// create a mock connection, add `lwt` cql protocol extension to it,
			// ensure that framer recognizes this extension and adjusts appropriately
			framerWithRateLimitExt := framerForExts([]endpoint.InternalCqlProtocolExtension{
				&endpoint.InternalRateLimitExt{
					InternalRateLimitErrorCode: mockCode,
				},
			})
			if framerWithRateLimitExt.RateLimitingErrorCode != mockCode {
				t.Error("expected to have rateLimitingErrorCode set to mockCode after framer init")
			}
		})
	})

	t.Run("UseMetadataIDExt", func(t *testing.T) {
		t.Parallel()

		t.Run("newScyllaUseMetadataIDExt detects the advertised key", func(t *testing.T) {
			if ext := endpoint.InternalNewScyllaUseMetadataIDExt(map[string][]string{endpoint.InternalScyllaUseMetadataID: {}}, protocol.ProtoVersion4); ext == nil {
				t.Error("expected a non-nil extension when SCYLLA_USE_METADATA_ID is advertised")
			}
			if ext := endpoint.InternalNewScyllaUseMetadataIDExt(map[string][]string{}, protocol.ProtoVersion4); ext != nil {
				t.Error("expected a nil extension when SCYLLA_USE_METADATA_ID is not advertised")
			}
		})

		// The extension backports the v5 result metadata id to v4, and opting in changes
		// what EXECUTE and RESULT/Prepared carry. A server advertising it says nothing
		// about the version this connection speaks: clusterSettings.ProtoVersion is pinned
		// without going through discoverProtocol, and newFramer accepts v3 through v5.
		t.Run("newScyllaUseMetadataIDExt opts in on v4 only", func(t *testing.T) {
			advertised := map[string][]string{endpoint.InternalScyllaUseMetadataID: {}}
			for _, version := range []byte{protocol.ProtoVersion3, protocol.ProtoVersion5} {
				if ext := endpoint.InternalNewScyllaUseMetadataIDExt(advertised, version); ext != nil {
					t.Errorf("protocol v%d: expected a nil extension, got %+v", version, ext)
				}
			}
			// The gate reads through protoVersionMask, as connFramers.initCache does with
			// the same byte, so a version carrying the high bit still resolves to v4.
			if ext := endpoint.InternalNewScyllaUseMetadataIDExt(advertised, protocol.ProtoVersion4|protocol.ProtoDirectionMask); ext == nil {
				t.Error("expected a non-nil extension for v4 with the direction bit set")
			}
		})

		t.Run("name and serialize", func(t *testing.T) {
			ext := &endpoint.InternalScyllaUseMetadataIDExt{}
			if ext.InternalName() != endpoint.InternalScyllaUseMetadataID {
				t.Errorf("name() = %q, want %q", ext.InternalName(), endpoint.InternalScyllaUseMetadataID)
			}
			ser := ext.InternalSerialize()
			if v, ok := ser[endpoint.InternalScyllaUseMetadataID]; !ok || len(ser) != 1 || v != "" {
				t.Errorf("serialize() = %v, want a single %q key mapping to \"\"", ser, endpoint.InternalScyllaUseMetadataID)
			}
		})

		t.Run("parseCQLProtocolExtensions registers the extension only when advertised", func(t *testing.T) {
			exts := endpoint.InternalParseCQLProtocolExtensions(map[string][]string{endpoint.InternalScyllaUseMetadataID: {}}, protocol.ProtoVersion4, &logging.InternalTestLogger{})
			if endpoint.InternalFindCQLProtoExtByName(exts, endpoint.InternalScyllaUseMetadataID) == nil {
				t.Error("expected parseCQLProtocolExtensions to register SCYLLA_USE_METADATA_ID when advertised")
			}

			extsAbsent := endpoint.InternalParseCQLProtocolExtensions(map[string][]string{}, protocol.ProtoVersion4, &logging.InternalTestLogger{})
			if endpoint.InternalFindCQLProtoExtByName(extsAbsent, endpoint.InternalScyllaUseMetadataID) != nil {
				t.Error("did not expect SCYLLA_USE_METADATA_ID to be registered when not advertised")
			}
		})

		// The list parseCQLProtocolExtensions returns is what startupCoordinator.startup
		// serializes into STARTUP, so keeping the extension out of it on v3 is what keeps
		// the opt-in off the wire — the framer config follows from the same list.
		t.Run("parseCQLProtocolExtensions leaves the extension out below v4", func(t *testing.T) {
			advertised := map[string][]string{endpoint.InternalScyllaUseMetadataID: {}, endpoint.InternalTabletsRoutingV1: {}}
			exts := endpoint.InternalParseCQLProtocolExtensions(advertised, protocol.ProtoVersion3, &logging.InternalTestLogger{})
			if endpoint.InternalFindCQLProtoExtByName(exts, endpoint.InternalScyllaUseMetadataID) != nil {
				t.Error("did not expect SCYLLA_USE_METADATA_ID to be registered on protocol v3")
			}
			// Version-independent extensions are unaffected by the gate.
			if endpoint.InternalFindCQLProtoExtByName(exts, endpoint.InternalTabletsRoutingV1) == nil {
				t.Error("expected TABLETS_ROUTING_V1 to still be registered on protocol v3")
			}
		})

		// The framer side of the extension is covered by internal/execution's
		// TestConnStartup/InitFramerCache/ScyllaUseMetadataID and its negative counterpart, which go
		// through the same connFramers.initCache path and additionally pin
		// driverConn.usesMetadataID against it.
	})

	t.Run("ValidateV2Supported", func(t *testing.T) {
		options := settings.DefaultConfig("localhost").Pool
		for _, change := range []func(map[string][]string){
			func(v map[string][]string) { v["SCYLLA_SHARD"] = nil }, func(v map[string][]string) { delete(v, "SCYLLA_SHARD") }, func(v map[string][]string) { v["SCYLLA_NR_SHARDS"] = []string{} }, func(v map[string][]string) { v["SCYLLA_NR_SHARDS"] = []string{"0"} }, func(v map[string][]string) { v["SCYLLA_NR_SHARDS"] = []string{"65536"} }, func(v map[string][]string) { v["SCYLLA_NR_SHARDS"] = []string{"9223372036854775807"} }, func(v map[string][]string) { v["SCYLLA_NR_SHARDS"] = []string{"12", "13"} }, func(v map[string][]string) { v["SCYLLA_SHARD"] = []string{"12"} }, func(v map[string][]string) { v["SCYLLA_SHARD"] = []string{"-1"} }, func(v map[string][]string) { v["SCYLLA_SHARDING_IGNORE_MSB"] = []string{"64"} }, func(v map[string][]string) { v["SCYLLA_SHARDING_IGNORE_MSB"] = []string{"-1"} }, func(v map[string][]string) { v["SCYLLA_PARTITIONER"] = nil }, func(v map[string][]string) { v["SCYLLA_SHARD_AWARE_PORT"] = nil }, func(v map[string][]string) { v[endpoint.InternalRateLimitError] = []string{"ERROR_CODE"} }, func(v map[string][]string) {
				v[endpoint.InternalLwtAddMetadataMarkKey] = []string{"LWT_OPTIMIZATION_META_BIT_MASK"}
			},
		} {
			values := validSupported()
			change(values)
			err := transport.InternalValidateV2Supported(values, options)
			if _, ok := errors.AsType[*cqltype.ProtocolError](err); !ok {
				t.Fatal("malformed SUPPORTED not rejected before parse", values, err)
			}
			// Legacy callers are also panic-free, even if they do not request typed setup.
			endpoint.InternalParseSupported(values, logging.InternalNopLogger{})
		}
		for _, values := range []map[string][]string{nil, {}, {endpoint.InternalScyllaUseMetadataID: nil}, {"SCYLLA_SHARD_AWARE_PORT": {"19042"}, endpoint.InternalLwtAddMetadataMarkKey: {"LWT_OPTIMIZATION_META_BIT_MASK=536870912"}}, validSupported()} {
			if err := transport.InternalValidateV2Supported(values, options); err != nil {
				t.Fatal("legitimate absent/optional capability rejected", values, err)
			}
		}
		values := validSupported()
		if got := endpoint.InternalParseSupported(values, logging.InternalNopLogger{}); implEndpoint.ScyllaHostFeaturesData(&got.ScyllaHostFeatures).InternalNrShards != 12 || implEndpoint.ScyllaHostFeaturesData(&got.ScyllaHostFeatures).InternalMsbIgnore != 0 {
			t.Fatal("explicit zero MSB disabled valid sharding", got)
		}
		values["SCYLLA_NR_SHARDS"] = []string{"65535"}
		values["SCYLLA_SHARD"] = []string{"65534"}
		values["SCYLLA_SHARDING_IGNORE_MSB"] = []string{"63"}
		if err := transport.InternalValidateV2Supported(values, options); err != nil {
			t.Fatal("valid16-bit maximum rejected", err)
		}
		maximum := endpoint.InternalParseSupported(values, logging.InternalNopLogger{})
		if implEndpoint.ScyllaHostFeaturesData(&maximum.ScyllaHostFeatures).InternalNrShards != 65535 || implEndpoint.ScyllaConnectionFeaturesData(&maximum).InternalShard != 65534 || implEndpoint.ScyllaHostFeaturesData(&maximum.ScyllaHostFeatures).InternalMsbIgnore != 63 {
			t.Fatal(maximum)
		}
	})
}

//go:build unit
// +build unit

package cluster

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	implPool "github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func TestScyllaConnPickerPickNilToken(t *testing.T) {
	t.Parallel()

	s := implPool.InternalScyllaConnPicker{
		InternalNrShards:  4,
		InternalMsbIgnore: 12,
	}

	t.Run("no conns", func(t *testing.T) {
		s.InternalConns = []*transport.Conn{{
			InternalStreams: streams.New(),
		}}
		if s.Pick(endpoint.Token(nil)) != s.InternalConns[0] {
			t.Fatal("expected connection")
		}
	})

	t.Run("one shard", func(t *testing.T) {
		s.InternalConns = []*transport.Conn{{
			InternalStreams: streams.New(),
		}}
		if s.Pick(endpoint.Token(nil)) != s.InternalConns[0] {
			t.Fatal("expected connection")
		}
	})

	t.Run("multiple shards", func(t *testing.T) {
		s.InternalConns = []*transport.Conn{nil, {
			InternalStreams: streams.New(),
		}}
		if s.Pick(endpoint.Token(nil)) != s.InternalConns[1] {
			t.Fatal("expected connection")
		}
		if s.Pick(endpoint.Token(nil)) != s.InternalConns[1] {
			t.Fatal("expected connection")
		}
	})

	t.Run("multiple shards no conns", func(t *testing.T) {
		s.InternalConns = []*transport.Conn{nil, nil}
		if s.Pick(endpoint.Token(nil)) != nil {
			t.Fatal("expected nil")
		}
		if s.Pick(endpoint.Token(nil)) != nil {
			t.Fatal("expected nil")
		}
	})
}

// TestScyllaConnPickerPickInt64MatchesPick verifies the raw-int64 fast path
// PickInt64 selects exactly the same shard-aware connection as the boxed Pick.
func TestScyllaConnPickerPickInt64MatchesPick(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		nrShards  int
		msbIgnore uint64
		tokens    []int64
	}{
		{"four-shards", 4, 12, []int64{math.MinInt64, -123456789, 0, 123456789, math.MaxInt64}},
		{"eight-shards", 8, 0, []int64{-100000, 42, 999999999999}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := implPool.InternalScyllaConnPicker{InternalNrShards: tc.nrShards, InternalMsbIgnore: tc.msbIgnore}
			s.InternalConns = make([]*transport.Conn, tc.nrShards)
			for i := range s.InternalConns {
				s.InternalConns[i] = &transport.Conn{InternalStreams: streams.New()}
			}

			for _, tok := range tc.tokens {
				tt := endpoint.InternalInt64Token(tok)
				want := s.Pick(tt)
				got := s.PickInt64(tok)
				if got != want {
					t.Fatalf("token %d: PickInt64=%p, want %p (Pick)", tok, got, want)
				}
				if shard := s.InternalShardOf(tt); got != s.InternalConns[shard] {
					t.Fatalf("token %d: shard %d conn %p, got %p", tok, shard, s.InternalConns[shard], got)
				}
			}
		})
	}
}

// TestHostConnPoolPickConnRoutesInt64TokenToShardAwarePicker is a regression
// guard: a SelectedHost carrying an unboxed int64Token must still reach the
// shard-aware picker. If the raw token were dropped, scyllaConnPicker would
// fall back to leastBusyConn() and shard-aware routing would silently degrade.
func TestHostConnPoolPickConnRoutesInt64TokenToShardAwarePicker(t *testing.T) {
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
	host := InternalInt64SelectedHost{InternalInfo: func() *endpoint.HostInfo {
		opaqueStorageValue := new(endpoint.HostInfo)
		return opaqueStorageValue
	}(), InternalTokenCasted: tok}

	got := pool.PickConn(host)
	if got != want {
		t.Fatalf("PickConn returned %p (least-busy fallback?); want shard-aware conn %p for shard %d",
			got, want, shard)
	}
}

func hammerConnPicker(t *testing.T, wg *sync.WaitGroup, s *implPool.InternalScyllaConnPicker, loops int) {
	t.Helper()
	for i := 0; i < loops; i++ {
		if c := s.Pick(nil); c == nil {
			t.Error("unexpected nil")
		}
	}
	wg.Done()
}

func TestScyllaConnPickerHammerPickNilToken(t *testing.T) {
	t.Parallel()

	s := implPool.InternalScyllaConnPicker{
		InternalNrShards:  4,
		InternalMsbIgnore: 12,
	}
	s.InternalConns = make([]*transport.Conn, 100)
	for i := range s.InternalConns {
		if i%7 == 0 {
			continue
		}
		s.InternalConns[i] = &transport.Conn{
			InternalStreams: streams.New(),
		}
	}

	n := runtime.GOMAXPROCS(0)
	loops := 10000 / n

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go hammerConnPicker(t, &wg, &s, loops)
	}
	wg.Wait()
}

func TestScyllaConnPickerRemove(t *testing.T) {
	t.Parallel()

	s := implPool.InternalScyllaConnPicker{
		InternalNrShards:  4,
		InternalMsbIgnore: 12,
	}

	conn := mockConn(0)
	s.Put(conn)
	s.Put(mockConn(1))

	if s.InternalNrConns != 2 {
		t.Error("added 2 connections, expected connection count to be 2")
	}

	s.Remove(conn)
	if s.InternalNrConns != 1 {
		t.Errorf("removed 1 connection, expected connection count to be 1 but was %d", s.InternalNrConns)
	}

	if s.InternalConns[0] != nil {
		t.Errorf("Expected %v to be removed from it's position", conn)
	}
}

func TestScyllaConnPickerShardOf(t *testing.T) {
	t.Parallel()

	s := implPool.InternalScyllaConnPicker{
		InternalNrShards:  4,
		InternalMsbIgnore: 12,
	}
	for _, test := range scyllaShardOfTests {
		if shard := s.InternalShardOf(endpoint.InternalInt64Token(test.token)); shard != test.shard {
			t.Errorf("wrong scylla shard calculated for token %d, expected %d, got %d", test.token, test.shard, shard)
		}
	}
}

func TestScyllaRandomConnPicker(t *testing.T) {
	t.Parallel()

	t.Run("max iterations", func(t *testing.T) {
		s := &implPool.InternalScyllaConnPicker{
			InternalNrShards:  4,
			InternalMsbIgnore: 12,
			InternalPos:       math.MaxUint64,
			InternalConns:     []*transport.Conn{nil, mockConn(1)},
		}

		if s.Pick(endpoint.Token(nil)) == nil {
			t.Fatal("expected connection")
		}
	})

	t.Run("async access of max iterations", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		s := &implPool.InternalScyllaConnPicker{
			InternalNrShards:  4,
			InternalMsbIgnore: 12,
			InternalPos:       math.MaxUint64,
			InternalConns:     []*transport.Conn{nil, mockConn(1)},
		}

		var wg sync.WaitGroup
		connCh := make(chan *transport.Conn, 9)
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 3; i++ {
					select {
					case connCh <- s.Pick(endpoint.Token(nil)):
					case <-ctx.Done():
					}
				}
			}()
		}
		wg.Wait()
		close(connCh)

		if s.InternalPos != 8 {
			t.Fatalf("expected position to be 8 | actual %d", s.InternalPos)
		}
		if len(connCh) != 9 {
			t.Fatalf("expected 9 connection picks, got %d", len(connCh))
		}
		for conn := range connCh {
			if conn == nil {
				t.Fatal("expected connection, got nil")
			}
		}
	})
}

func TestScyllaRateLimitingExtParsing(t *testing.T) {
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
}

func TestScyllaLWTExtParsing(t *testing.T) {
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
}

// framerForExts returns a pooled write framer from a mock driverConn that negotiated
// exts, i.e. through the same connFramers.initCache path production uses. There is
// no standalone framer-with-extensions constructor to test against: the per-driverConn
// pool is the only place the negotiated extension list is turned into framer state.
func framerForExts(exts []endpoint.InternalCqlProtocolExtension) *protocol.Framer {
	conn := mockConn(0)
	conn.InternalVersion = protocol.ProtoVersion4
	conn.InternalLogger = &logging.InternalTestLogger{}
	conn.InternalCqlProtoExts = exts
	conn.InternalInitFramerCache()
	return conn.InternalGetWriteFramer()
}

func TestScyllaUseMetadataIDExtParsing(t *testing.T) {
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

	// The framer side of the extension is covered by conn_test.go's
	// TestInitFramerCacheScyllaUseMetadataID and its negative counterpart, which go
	// through the same connFramers.initCache path and additionally pin
	// driverConn.usesMetadataID against it.
}

func TestParseSupported(t *testing.T) {
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
}

func TestIsScyllaConn(t *testing.T) {
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
}

func TestInitConnPickerNonShardAwareScylla(t *testing.T) {
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
}

func TestScyllaPortIterator(t *testing.T) {
	t.Parallel()

	for _shardCount := 1; _shardCount <= 64; _shardCount++ {
		shardCount := _shardCount
		t.Run(fmt.Sprintf("shard count %d", shardCount), func(t *testing.T) {
			for shardID := 0; shardID < shardCount; shardID++ {
				// Count by brute force ports that can be used to connect to requested shard
				expectedPortCount := 0
				for i := transport.InternalScyllaPortBasedBalancingMin; i <= transport.InternalScyllaPortBasedBalancingMax; i++ {
					if i%shardCount == shardID {
						expectedPortCount++
					}
				}

				// Enumerate all ports using the port iterator and assert various things
				iterator := transport.InternalNewScyllaPortIterator(shardID, shardCount)
				actualPortCount := 0
				previousPort := 0

				for {
					portU16, ok := iterator.Next()
					if !ok {
						break
					}

					port := int(portU16)

					if port < transport.InternalScyllaPortBasedBalancingMin || port > transport.InternalScyllaPortBasedBalancingMax {
						t.Errorf("expected port %d generated from iterator to be in range [%d..%d]",
							port, transport.InternalScyllaPortBasedBalancingMin, transport.InternalScyllaPortBasedBalancingMax)
					}

					if port <= previousPort {
						t.Errorf("expected port %d generated from iterator to be larger than the previous generated port %d",
							port, previousPort)
					}

					actualShardOfPort := transport.InternalScyllaShardForSourcePort(portU16, shardCount)
					if actualShardOfPort != shardID {
						t.Errorf("expected port %d returned from iterator to belong to shard %d, but belongs to %d",
							port, shardID, actualShardOfPort)
					}

					previousPort = port
					actualPortCount++
				}

				if expectedPortCount != actualPortCount {
					t.Errorf("expected port iterator to generate %d ports, but got %d",
						expectedPortCount, actualPortCount)
				}
			}
		})
	}
}

func TestScyllaConnPickerHandleShardCountChange(t *testing.T) {
	tests := []struct {
		name             string
		initialShards    int
		newShards        int
		initialConns     []int // shard IDs of initial connections
		expectedMigrated int
		expectedClosed   int
	}{
		{
			name:             "shard increase from 4 to 8",
			initialShards:    4,
			newShards:        8,
			initialConns:     []int{0, 2, 3},
			expectedMigrated: 3, // All initial connections survive
			expectedClosed:   0,
		},
		{
			name:             "shard decrease from 8 to 4",
			initialShards:    8,
			newShards:        4,
			initialConns:     []int{0, 2, 5, 7},
			expectedMigrated: 2, // Only shards 0, 2 survive
			expectedClosed:   2, // Shards 5, 7 get closed
		},
		{
			name:             "no change same count",
			initialShards:    8,
			newShards:        8,
			initialConns:     []int{1, 3, 5},
			expectedMigrated: 4, // All initial connections survive + new one
			expectedClosed:   0,
		},
		{
			name:             "massive decrease from 16 to 2",
			initialShards:    16,
			newShards:        2,
			initialConns:     []int{0, 1, 5, 8, 12, 15},
			expectedMigrated: 2, // Only shards 0, 1 survive
			expectedClosed:   4, // Shards 5, 8, 12, 15 get closed
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			logger := &logging.InternalTestLogger{}
			picker := &implPool.InternalScyllaConnPicker{
				InternalLogger:                     logger,
				InternalDisableShardAwarePortUntil: new(atomic.Pointer[time.Time]),
				InternalHostId:                     tUUID(99),
				InternalAddress:                    "192.168.1.1:9042", // Regular port
				InternalConns:                      make([]*transport.Conn, tt.initialShards),
				InternalExcessConns:                make([]*transport.Conn, 0),
				InternalNrShards:                   tt.initialShards,
				InternalMsbIgnore:                  12,
				InternalNrConns:                    0,
				InternalPos:                        0,
				InternalLastAttemptedShard:         0,
				InternalShardAwarePortDisabled:     false,
				InternalExcessConnsLimitRate:       0.1,
			}

			var connectionsToCheck []*transport.Conn

			// Add initial connections
			for _, shardID := range tt.initialConns {
				conn := mockConnForPicker(shardID, tt.initialShards)
				err := picker.Put(conn)
				require.NoError(t, err)

				if shardID >= tt.newShards {
					connectionsToCheck = append(connectionsToCheck, conn)
				}
			}

			// Verify initial state
			assert.Equal(t, tt.initialShards, picker.InternalNrShards)
			assert.Equal(t, len(tt.initialConns), picker.InternalNrConns)

			// Execute topology change
			newConn := mockConnForPicker(0, tt.newShards)
			err := picker.Put(newConn)
			require.NoError(t, err)

			// Allow background goroutine to complete
			time.Sleep(50 * time.Millisecond)

			// Verify new topology
			assert.Equal(t, tt.newShards, picker.InternalNrShards)
			assert.Equal(t, len(picker.InternalConns), tt.newShards)

			// Count migrated connections
			migratedCount := 0
			for _, conn := range picker.InternalConns {
				if conn != nil {
					migratedCount++
				}
			}

			assert.Equal(t, tt.expectedMigrated, migratedCount)

			// Verify connections that should be closed are actually closed
			closedCount := 0
			for _, conn := range connectionsToCheck {
				if conn.Closed() {
					closedCount++
				}
			}

			assert.Equal(t, tt.expectedClosed, closedCount,
				"Expected %d connections to be closed, but %d were closed",
				tt.expectedClosed, closedCount)
		})
	}
}

func mockConn(shard int) *transport.Conn {
	return &transport.Conn{
		InternalStreams: streams.New(),
		InternalScyllaSupported: *func() *endpoint.ScyllaConnectionFeatures {
			opaqueStorageValuexx := new(endpoint.ScyllaConnectionFeatures)
			implEndpoint.ScyllaConnectionFeaturesData(opaqueStorageValuexx).InternalShard = shard
			opaqueStorageValuexx.ScyllaHostFeatures = *func() *endpoint.ScyllaHostFeatures {
				opaqueStorageValuex := new(endpoint.ScyllaHostFeatures)
				implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuex).InternalNrShards = 4
				implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuex).InternalMsbIgnore = 12
				implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuex).InternalPartitioner = "org.apache.cassandra.dht.Murmur3Partitioner"
				implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuex).InternalShardingAlgorithm = "biased-token-round-robin"
				return opaqueStorageValuex
			}()
			return opaqueStorageValuexx
		}(),
	}
}

func mockConnForPicker(shard, nrShards int) *transport.Conn {
	ctx, cancel := context.WithCancel(context.Background())

	conn1, conn2 := net.Pipe()
	_ = conn2.Close()

	return &transport.Conn{
		InternalScyllaSupported: *func() *endpoint.ScyllaConnectionFeatures {
			opaqueStorageValuexxxx := new(endpoint.ScyllaConnectionFeatures)
			implEndpoint.ScyllaConnectionFeaturesData(opaqueStorageValuexxxx).InternalShard = shard
			opaqueStorageValuexxxx.ScyllaHostFeatures = *func() *endpoint.ScyllaHostFeatures {
				opaqueStorageValuexxx := new(endpoint.ScyllaHostFeatures)
				implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuexxx).InternalNrShards = nrShards
				implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuexxx).InternalMsbIgnore = 12
				return opaqueStorageValuexxx
			}()
			return opaqueStorageValuexxxx
		}(),

		InternalR:       &transport.InternalConnReader{InternalConn: conn1, InternalR: bufio.NewReader(conn1)},
		InternalAddr:    fmt.Sprintf("192.168.1.%d:9042", shard+1),
		InternalClosed:  false,
		InternalMu:      sync.Mutex{},
		InternalLogger:  &logging.InternalTestLogger{},
		InternalCtx:     ctx,
		InternalCancel:  cancel,
		InternalCalls:   newCallTable(),
		InternalStreams: streams.New(),
	}
}

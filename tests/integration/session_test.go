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
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/logging"
	implSession "github.com/scylladb/gocql/v2/internal/session"

	"github.com/stretchr/testify/require"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/auth"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/endpoint"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	clustertestkit "github.com/scylladb/gocql/v2/internal/testkit/cluster"
	"github.com/scylladb/gocql/v2/internal/wire"
	"github.com/scylladb/gocql/v2/retry"
)

// Typed request helpers keep parameter construction explicit and bind inputs once.
func sessionTestBind(t testing.TB, session *gocql.Session, stmt string, values ...cql.Value) *Bound[cql.EmptyRow] {
	t.Helper()
	prepared, err := session.Prepare(context.Background(), cql.Statement[[]cql.Value, cql.EmptyRow]{CQL: stmt, Params: cql.DynamicParams, Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bound, err := BindOwned(prepared, values)
	if err != nil {
		t.Fatal(err)
	}
	return bound
}
func sessionTestItem[R any](t testing.TB, bound *Bound[R]) batchItem {
	t.Helper()
	item, err := BatchEntry(bound)
	if err != nil {
		t.Fatal(err)
	}
	return item
}
func sessionTestInt(t testing.TB, row cql.DynamicRow, name string) int64 {
	t.Helper()
	value, err := row.Get(name)
	if err != nil {
		t.Fatal(err)
	}
	integer, err := value.Signed()
	if err != nil {
		t.Fatal(err)
	}
	return integer
}

type sessionTestObserver struct {
	mu       sync.Mutex
	requests []gocql.RequestEvent
	attempts []gocql.AttemptEvent
}

func (o *sessionTestObserver) ObserveRequest(e gocql.RequestEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.requests = append(o.requests, e)
}
func (o *sessionTestObserver) ObserveAttempt(e gocql.AttemptEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.attempts = append(o.attempts, e)
}
func (o *sessionTestObserver) snapshot() ([]gocql.RequestEvent, []gocql.AttemptEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]gocql.RequestEvent(nil), o.requests...), append([]gocql.AttemptEvent(nil), o.attempts...)
}

type sessionTestRetry struct {
	action      retry.Action
	maxAttempts int
}

func (p sessionTestRetry) Decide(e retry.Attempt) retry.Decision {
	if e.Number >= p.maxAttempts {
		return retry.Decision{Action: retry.Stop}
	}
	return retry.Decision{Action: p.action}
}

type sessionCache struct {
	orig       tls.ClientSessionCache
	values     map[string][][]byte
	caches     map[string][]int64
	valuesLock sync.Mutex
}

func (c *sessionCache) Get(sessionKey string) (session *tls.ClientSessionState, ok bool) {
	return c.orig.Get(sessionKey)
}

func (c *sessionCache) Put(sessionKey string, cs *tls.ClientSessionState) {
	if cs == nil {
		c.orig.Put(sessionKey, nil)
		return
	}
	ticket, _, err := cs.ResumptionState()
	if err != nil {
		panic(err)
	}
	if len(ticket) == 0 {
		panic("ticket should not be empty")
	}
	c.valuesLock.Lock()
	c.values[sessionKey] = append(c.values[sessionKey], ticket)
	c.valuesLock.Unlock()
	c.orig.Put(sessionKey, cs)
}

func (c *sessionCache) NumberOfTickets() int {
	c.valuesLock.Lock()
	defer c.valuesLock.Unlock()
	total := 0
	for _, tickets := range c.values {
		total += len(tickets)
	}
	return total
}

func newSessionCache() *sessionCache {
	return &sessionCache{
		orig:       tls.NewLRUClientSessionCache(1024),
		values:     make(map[string][][]byte),
		caches:     make(map[string][]int64),
		valuesLock: sync.Mutex{},
	}
}

// withSessionCache returns the fixture client TLS with cache installed, wrapped
// for the fixture's per-host verification. verify, when set, runs after the
// fixture's chain and identity checks.
func withSessionCache(cache tls.ClientSessionCache, verify func(tls.ConnectionState) error) func(*gocql.ConfigBuilder) {
	return func(b *gocql.ConfigBuilder) {
		// Preserve fixture roots, mutual TLS identity, and verifier callbacks.
		base := fixtureClientTLS()
		base.ClientSessionCache = cache
		base.VerifyConnection = verify
		b.WithTLS(gocql.StaticTLS(fixtureTLSConfig(base)))
	}
}

func TestSession(t *testing.T) {
	t.Run("AwaitSchemaAgreement", func(t *testing.T) {
		t.Parallel()

		t.Run("Agrees", func(t *testing.T) {
			t.Parallel()

			session := createV2Session(t)
			defer session.Close()

			if err := session.AwaitSchemaAgreement(context.Background()); err != nil {
				t.Fatalf("expected session.AwaitSchemaAgreement to not return an error but got '%v'", err)
			}
		})
		t.Run("ContextCanceled", func(t *testing.T) {
			t.Parallel()

			session := createV2Session(t)
			defer session.Close()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			if err := session.AwaitSchemaAgreement(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("expected session.AwaitSchemaAgreement to return 'context canceled' but got '%v'", err)
			}

		})
		t.Run("SessionClosed", func(t *testing.T) {
			t.Parallel()

			session := createV2Session(t)
			session.Close()

			if err := session.AwaitSchemaAgreement(context.Background()); !errors.Is(err, cqlerr.ErrClosed) {
				t.Fatalf("expected session.AwaitSchemaAgreement to return ErrClosed but got '%v'", err)
			}

		})
	})

	t.Run("Close", func(t *testing.T) {
		// Real socket/error/transform owners use the session cache only after every
		// page alias is released. Session close drains idle storage without invalidating
		// retained delivered pages, and a late lease cannot repopulate the closed cache.
		t.Run("PageCacheOwnership", func(t *testing.T) {
			config := mustBuild(t, createV2Cluster(func(c *gocql.ConfigBuilder) {
				c.WithConnectionsPerHost(1).NoHeartbeat().NoMetadataRefresh()
			}))
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			s, e := gocql.Connect(ctx, config)
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			ks := testKeyspaceName(t, fmt.Sprintf("%x", time.Now().UnixNano()))
			created := false
			exec := func(stmt string) {
				t.Helper()
				b := codecTestMust(BindUnpreparedOwned(cql.Statement[cql.EmptyParams, cql.EmptyRow]{CQL: stmt, Kind: cql.OrdinaryStatement, Params: cql.EmptyParamsEncoder, Rows: cql.EmptyRowDecoder}, cql.NewColumns(), cql.EmptyParams{}))
				defer b.Close()
				if _, e := Exec(ctx, s, b, gocql.CompiledOptions{}); e != nil {
					t.Fatal(e)
				}
			}
			exec("CREATE KEYSPACE " + ks + " WITH replication={'class':'NetworkTopologyStrategy','replication_factor':1}")
			created = true
			defer func() {
				if !created {
					return
				}
				cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
				defer stop()
				cleanupSession, e := gocql.Connect(cleanup, config)
				if e != nil {
					t.Error(e)
					return
				}
				defer cleanupSession.Close()
				b, e := BindUnpreparedOwned(cql.Statement[cql.EmptyParams, cql.EmptyRow]{CQL: "DROP KEYSPACE " + ks, Kind: cql.OrdinaryStatement, Params: cql.EmptyParamsEncoder, Rows: cql.EmptyRowDecoder}, cql.NewColumns(), cql.EmptyParams{})
				if e == nil {
					defer b.Close()
					_, e = Exec(cleanup, cleanupSession, b, gocql.CompiledOptions{})
				}
				if e != nil {
					t.Error(e)
				}
			}()
			exec("CREATE TABLE " + ks + ".records(id int PRIMARY KEY,value blob)")
			insert := codecTestMust(s.Prepare(ctx, cql.Statement[[]byte, cql.EmptyRow]{CQL: "INSERT INTO " + ks + ".records(id,value)VALUES(1,?)", Kind: cql.OrdinaryStatement, Params: cql.OneParam(cql.BytesEncoder), Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{}))
			write := func(value byte) {
				t.Helper()
				payload := bytes.Repeat([]byte{value}, 1<<20)
				bound := codecTestMust(BindBorrowed(insert, payload))
				_, e := Exec(ctx, s, bound, gocql.CompiledOptions{})
				bound.Close()
				if e != nil {
					t.Fatal(e)
				}
				if e = bound.Wait(ctx); e != nil {
					t.Fatal(e)
				}
			}
			write(0x71)
			prepared := codecTestMust(s.Prepare(ctx, cql.Statement[cql.EmptyParams, cql.Bytes]{CQL: "SELECT value FROM " + ks + ".records WHERE id=1", Kind: cql.OrdinaryStatement, Params: cql.EmptyParamsEncoder, Rows: cql.OneColumn(cql.BlobCodec)}, gocql.PrepareOptions{}))
			bound := codecTestMust(BindOwned(prepared, cql.EmptyParams{}))
			defer bound.Close()
			rows := codecTestMust(Run(ctx, s, bound, bindOptions(s, gocql.Options{Prefetch: cql.Option[int]{Set: true, Value: 0}})))
			var old cql.Bytes
			if !rows.NextInto(&old) {
				t.Fatal(rows.Err())
			}
			lease := codecTestMust(rows.Retain())
			defer lease.Close()
			owner := cqltype.BytesLease(old)
			if owner.IsZero() {
				t.Fatal("missing physical owner")
			}
			rows.Close()
			firstAddress := uintptr(0)
			cqltype.BytesChain(old).Each(func(span wire.Span) error {
				if firstAddress == 0 && span.Len() > 0 {
					firstAddress = uintptr(unsafe.Pointer(unsafe.SliceData(span.Bytes())))
				}
				return nil
			})
			for generation := 0; generation < 8; generation++ {
				write(byte(0x72 + generation))
				freshRows := codecTestMust(Run(ctx, s, bound, bindOptions(s, gocql.Options{Prefetch: cql.Option[int]{Set: true, Value: 0}})))
				var view cql.Bytes
				if !freshRows.NextInto(&view) {
					t.Fatal(freshRows.Err())
				}
				cqltype.BytesChain(view).Each(func(span wire.Span) error {
					if span.Len() > 0 && uintptr(unsafe.Pointer(unsafe.SliceData(span.Bytes()))) == firstAddress {
						t.Fatal("live retained backing reused")
					}
					return nil
				})
				if got := codecTestMust(view.At(0)); got != byte(0x72+generation) {
					t.Fatal(got)
				}
				freshRows.Close()
				if got := codecTestMust(old.At(0)); got != 0x71 {
					t.Fatal("retained old page changed", got)
				}
			}
			clone := old.Clone()
			cache := (*implSession.Session)(s).Data().InternalConnCfg.InternalReceiveCache
			if n, c := cache.Storage(); n > wire.ReceiveCacheMaxBytes || c > wire.ReceiveCacheMaxObjects {
				t.Fatal("idle cache bound", n, c)
			}
			if e = s.Close(); e != nil {
				t.Fatal(e)
			}
			if !owner.Alive() || codecTestMust(old.At(0)) != 0x71 {
				t.Fatal("session close invalidated delivered owner")
			}
			if n, c := cache.Storage(); n != 0 || c != 0 {
				t.Fatal("session cache did not drain", n, c)
			}
			lease.Close()
			if owner.Alive() {
				t.Fatal("owner final release did not finish")
			}
			if _, e = old.At(0); !errors.Is(e, cqlerr.ErrReleased) {
				t.Fatal("old backing remained accessible", e)
			}
			if n, c := cache.Storage(); n != 0 || c != 0 {
				t.Fatal("late lease repopulated closed cache", n, c)
			}
			if codecTestMust(clone.At(0)) != 0x71 {
				t.Fatal("Clone lost independent bytes")
			}
			t.Log("8 overwritten real responses preserve old retained backing; Close drains idle cache, late final lease cannot repopulate; Clone independent")
		})
		t.Run("Unconnected", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			// The default profile already prefetches one page.
			profile, err := gocql.NewProfile().WithConsistency(cql.All).WithPageSize(100).Build()
			require.NoError(t, err)
			// Session setters are replaced by atomic profile snapshots. Fractional
			// prefetch became a bounded count of zero or one physical page.
			session := func() *gocql.Session {
				opaqueStorageValue := (*gocql.Session)(implSession.InternalWrapSession(new(implCluster.Core)))
				(*implSession.Session)(opaqueStorageValue).Data().InternalLogger = logging.Nop()
				return opaqueStorageValue
			}()
			profileOptions, err := resolveOptions(session, gocql.Options{Profile: &profile})
			require.NoError(t, err)
			runtimeFor(session).InternalSetProfile(&profileOptions.InternalProfile)
			defer session.Close()
			resolved, err := resolveOptions(session, gocql.Options{Trace: true})
			require.NoError(t, err)
			require.Equal(t, cql.All, resolved.InternalProfile.Consistency)
			require.Equal(t, int32(100), resolved.InternalProfile.PageSize)
			require.Equal(t, 1, resolved.InternalProfile.Prefetch)
			require.True(t, resolved.InternalOptions.Trace)

			intType := cql.IntType
			var encoded int32
			statement := cql.Statement[int32, cql.EmptyRow]{CQL: "INSERT INTO test(id) VALUES (?)", Params: cql.Params(cql.ParamAt(0, cql.Int32Codec, func(n int32) int32 { encoded = n; return n })), Rows: cql.EmptyRowDecoder}
			bound, err := BindUnpreparedOwned(statement, cql.NewColumns(cql.ColumnSpec{Name: "id", Type: intType}), int32(1))
			require.NoError(t, err)
			defer bound.Close()
			require.Equal(t, int32(1), encoded, "typed parameter accessor replaces callback binding")
			// An incomplete session now reports ErrNoConnections from the typed path.
			// It does not construct the old mutable Query/Iter error holder.
			_, err = Run(ctx, session, bound, gocql.CompiledOptions{})
			require.ErrorIs(t, err, cqlerr.ErrNoConnections)
			batch := []batchItem{sessionTestItem(t, bound)}
			_, err = execBatch(ctx, session, cql.LoggedBatch, batch, gocql.CompiledOptions{})
			require.ErrorIs(t, err, cqlerr.ErrNoConnections)
			require.NoError(t, session.Close())
			require.True(t, session.Closed())
			require.NoError(t, session.Close())
			_, err = execBatch(ctx, session, cql.LoggedBatch, batch, gocql.CompiledOptions{})
			require.ErrorIs(t, err, cqlerr.ErrClosed)
			_, err = Exec(ctx, session, bound, gocql.CompiledOptions{})
			require.ErrorIs(t, err, cqlerr.ErrClosed)
		})
	})

	t.Run("Connect", func(t *testing.T) {
		t.Parallel()

		// Verifies that gocql will work with a host configured to only accept authenticated connections
		t.Run("Authentication", func(t *testing.T) {
			t.Parallel()

			if !*flagRunAuthTest {
				t.Skip("Authentication is not configured in the target cluster")
			}

			cluster := createV2ClusterWithout(fixtureAuth).WithAuth(auth.Password("cassandra", "cassandra"))
			session, err := gocql.Connect(context.Background(), mustBuild(t, cluster))

			if err != nil {
				t.Fatalf("Authentication error: %s", err)
			}

			session.Close()
			invalid := createV2ClusterWithout(fixtureAuth).WithAuth(auth.Password("cassandra", "invalid-password"))
			rejected, err := gocql.Connect(context.Background(), mustBuild(t, invalid))
			if err == nil {
				rejected.Close()
				t.Fatal("server accepted rejected credentials")
			}
			if authErr, ok := errors.AsType[cqlerr.RequestError](err); !ok || authErr.GetCode() != cqlerr.ErrCodeCredentials {
				t.Fatalf("expected authentication rejection, got %T: %v", err, err)
			}
		})
		t.Run("DNS", func(t *testing.T) {
			t.Parallel()

			checkIfSessionWorking := func(t *testing.T, cluster gocql.Config, hosts []string) {
				s, err := gocql.Connect(context.Background(), cluster)
				if err != nil {
					t.Fatalf("failed to create session: %v", err)
				}
				defer s.Close()

				err = s.RefreshMetadata(context.Background())
				if err != nil {
					t.Fatalf("failed to refresh ring: %v", err)
				}

				rows, err := v2Query(context.Background(), s, "select * from system.peers")
				if err == nil {
					var row cql.DynamicRow
					for rows.NextInto(&row) {
					}
					err = rows.Err()
					rows.Close()
				}
				if err != nil {
					t.Fatalf("failed to execute query: %v", err)
				}
				ringHosts := s.Metadata().Hosts()
				if len(ringHosts) != len(hosts) {
					t.Fatalf("wrong number of hosts: got %d, want %d", len(ringHosts), len(hosts))
				}
			}

			// A mod rewrites the contact points the previous one produced and
			// returns the option that makes them resolvable.
			type clusterMod func(contactPoints []string) ([]string, func(*gocql.ConfigBuilder))

			OneDNSPerNode := func(contactPoints []string) ([]string, func(*gocql.ConfigBuilder)) {
				r := newMockDNSResolver()
				var dnsRecords []string
				for id, host := range contactPoints {
					dns := fmt.Sprintf("node%d.cluster.local", id+1)
					dnsRecords = append(dnsRecords, dns)
					r.Update(dns, MustIP(host))
				}
				return dnsRecords, func(c *gocql.ConfigBuilder) { c.WithResolver(r) }
			}

			OneDNSPerCluster := func(contactPoints []string) ([]string, func(*gocql.ConfigBuilder)) {
				r := newMockDNSResolver()
				var hostIPs []net.IP
				for _, host := range contactPoints {
					hostIPs = append(hostIPs, MustIP(host))
				}
				r.Update("cluster.local", hostIPs...)
				return []string{"cluster.local"}, func(c *gocql.ConfigBuilder) { c.WithResolver(r) }
			}

			OneDNSPerClusterFirstBroken := func(contactPoints []string) ([]string, func(*gocql.ConfigBuilder)) {
				r := newMockDNSResolver()
				var hostIPs []net.IP
				for _, host := range contactPoints {
					hostIPs = append(hostIPs, MustIP(host))
				}
				hostIPs[0] = MustIP("0.0.0.0")
				r.Update("cluster.local", hostIPs...)
				return []string{"cluster.local"}, func(c *gocql.ConfigBuilder) { c.WithResolver(r) }
			}

			WithAddressTranslator := func(contactPoints []string) ([]string, func(*gocql.ConfigBuilder)) {
				var toAddresses []net.IP
				var fromAddresses []net.IP
				var clusterHosts []string
				for _, host := range contactPoints {
					ip := MustIP(host)

					var fromAddress net.IP
					if ip.To4().String() == ip.String() {
						ip = ip.To4()
						fromAddress = net.IPv4(ip[0], ip[1], ip[2]+1, ip[3])
					} else {
						fromAddress = net.IP{ip[0], ip[1], ip[2], ip[3], ip[4], ip[5], ip[6], ip[7], ip[8], ip[9], ip[10], ip[11], ip[12] + 1, ip[13], ip[14], ip[15]}
					}
					toAddresses = append(toAddresses, ip)
					fromAddresses = append(fromAddresses, fromAddress)
					clusterHosts = append(clusterHosts, fromAddress.String())
				}

				translator := endpoint.TranslatorFunc(func(_ endpoint.TranslatorHostInfo, addr endpoint.AddressPort) (endpoint.AddressPort, error) {
					for id, host := range fromAddresses {
						if host.Equal(addr.Address) {
							return endpoint.AddressPort{Address: toAddresses[id], Port: addr.Port}, nil
						}
					}
					for _, host := range toAddresses {
						if host.Equal(addr.Address) {
							return addr, nil
						}
					}
					panic("failed to translate address")
				})
				return clusterHosts, func(c *gocql.ConfigBuilder) { c.WithAddressTranslator(translator) }
			}

			testCases := []struct {
				name        string
				clusterMods []clusterMod
			}{
				{
					name:        "OneDNSPerNode",
					clusterMods: []clusterMod{OneDNSPerNode},
				},
				{
					name:        "OneDNSPerCluster",
					clusterMods: []clusterMod{OneDNSPerCluster},
				},
				{
					name:        "AddressTranslator+OneDNSPerNode",
					clusterMods: []clusterMod{WithAddressTranslator, OneDNSPerNode},
				},
				{
					name:        "AddressTranslator+OneDNSPerCluster",
					clusterMods: []clusterMod{WithAddressTranslator, OneDNSPerCluster},
				},
				{
					name:        "OneDNSPerClusterFirstBroken",
					clusterMods: []clusterMod{OneDNSPerClusterFirstBroken},
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					contactPoints := getClusterHosts()
					var opts []func(*gocql.ConfigBuilder)
					for _, mod := range tc.clusterMods {
						var opt func(*gocql.ConfigBuilder)
						contactPoints, opt = mod(contactPoints)
						opts = append(opts, opt)
					}
					cluster := mustBuild(t, newFixtureConfig(contactPoints, 0, opts...))
					checkIfSessionWorking(t, cluster, getClusterHosts())
				})
			}
		})
		t.Run("DiscoverProtocol", func(t *testing.T) {
			t.Parallel()
			config := createV2ClusterWithout(fixtureProtocol | fixtureCompression).WithProtocolVersion(gocql.ProtocolAuto).WithCompression(gocql.NoCompression)
			session, err := gocql.Connect(context.Background(), mustBuild(t, config))
			require.NoError(t, err)
			defer session.Close()
			require.NotZero(t, (*implSession.Session)(session).Data().InternalCfg.ProtoVersion)
		})
		t.Run("EmptyHosts", func(t *testing.T) {
			t.Parallel()
			// Empty contact points are now rejected when the config is built.
			if _, err := newFixtureConfig(nil, 0).Build(); err == nil {
				t.Fatal("expected error for empty contact points")
			}
		})
		// Checks that an invalid keyspace will return promptly and without a flood of connections
		t.Run("InvalidKeyspace", func(t *testing.T) {
			t.Parallel()
			config := createV2Cluster().WithKeyspace("invalidKeyspace")
			session, err := gocql.Connect(context.Background(), mustBuild(t, config))
			if err == nil {
				session.Close()
				t.Fatal("expected invalid keyspace to fail")
			}
			require.Error(t, err)
		})
		t.Run("LowTimeout", func(t *testing.T) {
			t.Parallel()

			// Point of these tests to make sure that with low timeout connection creation will gracefully fail

			type TestExpectation int
			const (
				DontRun TestExpectation = iota
				Fail    TestExpectation = iota
				Pass    TestExpectation = iota
				CanPass TestExpectation = iota
			)

			match := func(t *testing.T, e TestExpectation, result error) {
				t.Helper()

				switch e {
				case DontRun:
					t.Fatal("should not be run")
				case Fail:
					if result == nil {
						t.Fatal("should return an error")
					}
				case Pass:
					if result != nil {
						t.Fatalf("should pass, but returned an error: %s", result.Error())
					}
				case CanPass:
					if result == nil {
						t.Log("test passed due to high timeout")
					}
				default:
					panic(fmt.Sprintf("unknown test expectation: %v", e))
				}
			}

			for _, lowTimeout := range []time.Duration{1 * time.Nanosecond, 10 * time.Nanosecond, 100 * time.Nanosecond} {
				// The timeout arms a timer, and a fast query can finish before it
				// fires. That has been seen at 10ns and 100ns (scylladb/gocql#973,
				// #1135), so those may pass; 1ns stays a required failure.
				canPassOnHighTimeout := Fail
				if lowTimeout > time.Nanosecond {
					canPassOnHighTimeout = CanPass
				}
				t.Run(lowTimeout.String(), func(t *testing.T) {
					for _, tcase := range []struct {
						name                       string
						getCluster                 func() gocql.Config
						connect                    TestExpectation
						regularQuery               TestExpectation
						controlQuery               TestExpectation
						controlQueryAfterReconnect TestExpectation
					}{
						{
							name: "Timeout",
							getCluster: func() gocql.Config {
								return mustBuild(t, createV2ClusterWithout(fixtureTimeout).ConfigureDefaultProfile(func(p *gocql.ProfileBuilder) { p.WithTimeout(lowTimeout) }))
							},
							connect: Pass,
							// Above 1ns the query can complete before the deadline fires,
							// the same race already tolerated below for WriteTimeout/ReadTimeout.
							regularQuery:               canPassOnHighTimeout,
							controlQuery:               Pass,
							controlQueryAfterReconnect: Pass,
						},
						{
							name: "MetadataSchemaRequestTimeout",
							getCluster: func() gocql.Config {
								return mustBuild(t, createV2Cluster().WithMetadataTimeout(lowTimeout))
							},
							connect:                    Pass,
							regularQuery:               Pass,
							controlQuery:               CanPass,
							controlQueryAfterReconnect: CanPass,
						},
						{
							name: "WriteTimeout",
							getCluster: func() gocql.Config {
								return mustBuild(t, createV2Cluster().WithWriteTimeout(lowTimeout))
							},
							connect:      Pass,
							regularQuery: canPassOnHighTimeout,
							controlQuery: canPassOnHighTimeout,
							// It breaks control connection, then it can start reconnecting in any moment
							// As result test is not stable
							controlQueryAfterReconnect: canPassOnHighTimeout,
						},
						{
							name: "ReadTimeout",
							getCluster: func() gocql.Config {
								return mustBuild(t, createV2Cluster().WithReadTimeout(lowTimeout))
							},
							connect: Pass,
							// When data is available immediately reading from socket is not failing,
							// despite that deadline is in the past
							// Because of that even with low read timeout it can pass
							regularQuery: CanPass,
							controlQuery: CanPass,
							// It breaks control connection, then it can start reconnecting in any moment
							// As result test is not stable
							controlQueryAfterReconnect: CanPass,
						},
						{
							name: "AllTimeouts",
							getCluster: func() gocql.Config {
								return mustBuild(t, createV2ClusterWithout(fixtureTimeout).
									ConfigureDefaultProfile(func(p *gocql.ProfileBuilder) { p.WithTimeout(lowTimeout) }).
									WithReadTimeout(lowTimeout).
									WithWriteTimeout(lowTimeout).
									WithMetadataTimeout(lowTimeout))
							},
							connect:                    Pass,
							regularQuery:               Fail,
							controlQuery:               Fail,
							controlQueryAfterReconnect: Fail,
						},
					} {
						t.Run(tcase.name, func(t *testing.T) {
							var (
								s   *gocql.Session
								err error
							)

							t.Run("Connect", func(t *testing.T) {
								s, err = gocql.Connect(context.Background(), tcase.getCluster())
								match(t, tcase.connect, err)
								if err != nil {
									t.Fatal("failed to create session", err.Error())
								}
							})
							if s != nil {
								defer s.Close()
							} else {
								if tcase.connect == Fail {
									t.FailNow()
								} else {
									t.Fatal("session was not created")
								}
							}

							if tcase.regularQuery != DontRun {
								t.Run("Regular Query", func(t *testing.T) {
									_, err = v2ReadOne(context.Background(), s, "SELECT key FROM system.local WHERE key='local'")
									match(t, tcase.regularQuery, err)
								})
							}

							if tcase.controlQuery != DontRun {
								t.Run("Query from control connection", func(t *testing.T) {
									err = queryV2ControlKey(context.Background(), s)
									match(t, tcase.controlQuery, err)
								})
							}

							if tcase.controlQueryAfterReconnect != DontRun {
								t.Run("Query from control connection after reconnect", func(t *testing.T) {
									s, err = gocql.Connect(context.Background(), tcase.getCluster())
									if err != nil {
										t.Fatal("failed to create session", err.Error())
									}
									defer s.Close()
									err = (*implSession.Session)(s).Data().InternalControl.InternalReconnect()
									if err != nil {
										t.Fatalf("failed to reconnect to control connection: %v", err)
									}
									err = queryV2ControlKey(context.Background(), s)
									match(t, tcase.controlQueryAfterReconnect, err)
								})
							}
						})
					}
				})
			}
		})
		t.Run("ReportsProtocolError", func(t *testing.T) {
			t.Parallel()
			// An unsupported version is now rejected when the config is built.
			_, err := createV2ClusterWithout(fixtureProtocol).WithProtocolVersion(gocql.ProtocolVersion(0xff)).Build()
			if err == nil {
				t.Fatal("expected unsupported protocol error")
			}
			require.Contains(t, err.Error(), "WithProtocolVersion")
		})
		t.Run("TLSTicketResumption", func(t *testing.T) {
			t.Parallel()

			if !*flagRunSslTest || *flagDistribution != "scylla" {
				t.Skip("requires a ScyllaDB TLS fixture")
			}
			if flagCassVersion.Major < 2025 || (flagCassVersion.Major == 2025 && flagCassVersion.Minor < 2) {
				t.Skip("TLS ticket resumption requires ScyllaDB 2025.2 or later")
			}

			c := newSessionCache()
			var handshakes, resumed atomic.Int64
			session := createV2SessionWithout(t, fixtureTLS, withSessionCache(c, func(state tls.ConnectionState) error {
				handshakes.Add(1)
				if state.DidResume {
					resumed.Add(1)
				}
				return nil
			}))
			defer session.Close()

			waitAllConnectionsOpened := func() error {
				endtime := time.Now().UTC().Add(time.Second * 10)
				for {
					if time.Now().UTC().After(endtime) {
						return fmt.Errorf("timed out waiting for all connections opened")
					}
					missing, err := clustertestkit.MissingConnections((*implSession.Session)(session).Core())
					if err != nil {
						return fmt.Errorf("failed to get missing connections count: %w", err)
					}
					if missing == 0 {
						return nil
					}
					time.Sleep(time.Millisecond * 100)
				}
			}

			if err := waitAllConnectionsOpened(); err != nil {
				t.Fatal(err)
			}
			tickets := c.NumberOfTickets()
			if tickets == 0 {
				t.Fatal("no tickets learned, which means that server does not support TLS tickets")
			}

			for round := 0; round < 2; round++ {
				previousHandshakes, previousResumed := handshakes.Load(), resumed.Load()
				clustertestkit.CloseAllConnections((*implSession.Session)(session).Core())
				if err := waitAllConnectionsOpened(); err != nil {
					t.Fatal(err)
				}
				if handshakes.Load() <= previousHandshakes {
					t.Fatal("pool reconnection did not establish new TLS connections")
				}
				if resumed.Load() <= previousResumed {
					t.Fatal("none of the replacement TLS connections resumed a session")
				}
			}
		})
		// Tests to make sure the CreateSession function timeouts out correctly
		// and prevents an infinite loop of connection retries.
		t.Run("Timeout", func(t *testing.T) {
			t.Parallel()
			config := mustBuild(t, newFixtureConfig([]string{"127.0.0.1:1"}, 0))
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			start := time.Now()
			session, err := gocql.Connect(ctx, config)
			if err == nil {
				session.Close()
				t.Fatal("expected startup failure")
			}
			require.Less(t, time.Since(start), 2*time.Second)
		})
		t.Run("UnixSocket", func(t *testing.T) {
			t.Parallel()

			socketFiles := getClusterSocketFile()
			if len(socketFiles) == 0 {
				t.Skip("this test needs path to socket file provided into -cluster-socket cli option")
			}

			const connectTimeout = 5 * time.Second
			// The maintenance socket carries plaintext CQL even when TCP clients use
			// TLS. A builder cannot drop TLS once set, so this starts from NewConfig
			// rather than createV2Cluster and copies the fixture's non-TLS settings.
			c := gocql.NewConfig("unix:"+socketFiles[0]).
				WithCQLVersion(*flagCQL).
				WithSchemaAgreement(2*time.Minute, 200*time.Millisecond).
				ConfigureDefaultProfile(func(p *gocql.ProfileBuilder) {
					p.WithConsistency(cql.Quorum).WithTimeout(*flagTimeout).WithRetry(retry.Safe{MaxAttempts: max(1, *flagRetry+1)})
				}).
				WithConnectionsPerHost(1).
				NoDiscovery().
				WithProtocolVersion(gocql.Protocol3).
				WithReconnect(unixSocketNoReconnect{}).
				WithIgnoreEvents(gocql.StatusEvent, gocql.TopologyEvent, gocql.SchemaEvent).
				WithConnectTimeout(connectTimeout).
				WithDialer(endpoint.DialerFunc(func(ctx context.Context, target endpoint.DialTarget) (net.Conn, error) {
					return (&net.Dialer{Timeout: connectTimeout}).DialContext(ctx, target.Network, target.Address)
				}))
			if *flagRunAuthTest {
				c.WithAuth(auth.Password("cassandra", "cassandra"))
			}

			sess, err := gocql.Connect(context.Background(), mustBuild(t, c))
			if err != nil {
				t.Fatalf("unable to create session: %v", err)
			}

			defer sess.Close()

			keyspace := testKeyspaceName(t)

			err = v2CreateTable(context.Background(), sess, `DROP KEYSPACE IF EXISTS `+keyspace)
			if err != nil {
				t.Fatal("unable to drop keyspace if exists:", err)
			}

			err = v2CreateTable(context.Background(), sess, fmt.Sprintf(`CREATE KEYSPACE %s
	WITH replication = {
		'class' : 'NetworkTopologyStrategy',
		'replication_factor' : 1
	}`, keyspace))
			if err != nil {
				t.Fatal("unable to create keyspace:", err)
			}
		})
	})

	t.Run("OpenSingleHost", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		config := mustBuild(t, createV2Cluster())
		// The standalone executor is replaced by a typed session restricted to a
		// discovered host identity, rather than a second query execution engine.
		discovery, err := gocql.Connect(ctx, config)
		require.NoError(t, err)
		hosts := discovery.Metadata().Hosts()
		require.NotEmpty(t, hosts)
		hostID := hosts[0].ID
		require.NoError(t, discovery.Close())
		session, err := gocql.OpenSingleHost(ctx, config, hostID)
		require.NoError(t, err)
		defer session.Close()
		bound, err := BindUnpreparedOwned(cql.Statement[cql.EmptyParams, cql.TimeUUID]{CQL: "SELECT now() FROM system.local", Params: cql.EmptyParamsEncoder, Rows: cql.OneColumn(cql.TimeUUIDCodec)}, cql.NewColumns(), cql.EmptyParams{})
		require.NoError(t, err)
		defer bound.Close()
		rows, err := Run(ctx, session, bound, gocql.CompiledOptions{})
		require.NoError(t, err)
		defer rows.Close()
		var now cql.TimeUUID
		require.True(t, rows.NextInto(&now))
		require.False(t, cql.UUID(now).IsEmpty())
		require.False(t, rows.NextInto(&now))
		require.NoError(t, rows.Err())
		require.Equal(t, hostID, rows.Info().HostID)
		for _, host := range session.Metadata().Hosts() {
			if host.Connected {
				require.Equal(t, hostID, host.ID, "single-host session connected to another host")
			}
		}
	})

	// Verifies that a node marked down is eventually reconnected.
	// WARNING: This test must NOT use t.Parallel(). It calls session.handleNodeDown()
	// which mutates shared HostInfo state visible to all concurrent sessions.
	//
	//nolint:paralleltest // mutates shared HostInfo state via handleNodeDown()
	t.Run("Reconnect", func(t *testing.T) {
		config := createV2Cluster(func(c *gocql.ConfigBuilder) { c.WithReconnect(cassandraReconnect{}) })
		session := createV2SessionFromConfig(config, t, false)
		defer session.Close()
		host := (*implSession.Session)(session).Data().InternalHostSource.InternalGetHostsList()[0]
		(*implSession.Session)(session).Core().HandleNodeDown(host.ConnectAddress(), host.Port())
		defer implEndpoint.InternalHostInfoSetState(host, NodeUp)
		require.Equal(t, NodeDown, host.State())
		require.True(t, pollUntil(31*time.Second, func() bool { return host.State() == NodeUp }), "host did not reconnect")
	})

	t.Run("Smoke", func(t *testing.T) {
		proto, _ := strconv.Atoi(v2SmokeOption("proto", "4"))
		config := gocql.NewConfig(strings.Split(v2SmokeOption("cluster", "127.0.20.1,127.0.20.2,127.0.20.3"), ",")...).
			WithProtocolVersion(gocql.ProtocolVersion(proto)).
			NoConfigReport().
			ConfigureDefaultProfile(func(p *gocql.ProfileBuilder) {
				p.WithTimeout(30 * time.Second).WithConsistency(cql.LocalQuorum)
			})
		switch v2SmokeOption("compressor", "no-compression") {
		case "lz4":
			config.WithCompression(gocql.LZ4Compression)
		case "snappy":
			config.WithCompression(gocql.SnappyCompression)
		}
		if v2SmokeOption("runauth", "false") == "true" {
			config.WithAuth(auth.Password("cassandra", "cassandra"))
		}
		if v2SmokeOption("runssl", "false") == "true" {
			if v2SmokeOption("distribution", "scylla") != "cassandra" {
				config.WithPort(9142)
			}
			cert, err := tls.LoadX509KeyPair("../../testdata/pki/gocql.crt", "../../testdata/pki/gocql.key")
			if err != nil {
				t.Fatal(err)
			}
			ca, err := os.ReadFile("../../testdata/pki/ca.crt")
			if err != nil {
				t.Fatal(err)
			}
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(ca) {
				t.Fatal("invalid root CA")
			}
			config.WithTLS(gocql.StaticTLS(&tls.Config{RootCAs: roots, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12, InsecureSkipVerify: true, VerifyConnection: func(state tls.ConnectionState) error {
				if len(state.PeerCertificates) == 0 {
					return fmt.Errorf("missing fixture server certificate")
				}
				intermediates := x509.NewCertPool()
				for _, certificate := range state.PeerCertificates[1:] {
					intermediates.AddCert(certificate)
				}
				if _, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
					return err
				}
				for _, identity := range state.PeerCertificates[0].URIs {
					if identity.String() == "spiffe://test.cassandra.apache.org/cassandra-gocql-driver/integrationTest/cassandra" {
						return nil
					}
				}
				return fmt.Errorf("unexpected fixture server identity")
			}}))
			// The existing fixture identifies nodes with a verified SPIFFE URI rather than IP SANs.
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		session, err := gocql.Connect(ctx, mustBuild(t, config))
		if err != nil {
			t.Fatal(err)
		}
		defer session.Close()
		keyspace := fmt.Sprintf("v2_runtime_%x", time.Now().UnixNano())
		exec := func(stmt string) {
			t.Helper()
			bound, err := BindUnpreparedOwned(cql.Command(stmt), cql.NewColumns(), cql.EmptyParams{})
			if err != nil {
				t.Fatal(err)
			}
			defer bound.Close()
			if _, err = Exec(ctx, session, bound, gocql.CompiledOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		exec("CREATE KEYSPACE " + keyspace + " WITH replication = {'class':'NetworkTopologyStrategy','replication_factor':1}")
		defer func() {
			bound, err := BindUnpreparedOwned(cql.Command("DROP KEYSPACE "+keyspace), cql.NewColumns(), cql.EmptyParams{})
			if err == nil {
				defer bound.Close()
				if _, err = Exec(context.Background(), session, bound, gocql.CompiledOptions{}); err != nil {
					t.Error(err)
				}
			}
		}()
		exec("CREATE TABLE " + keyspace + ".values (id int PRIMARY KEY, value blob)")
		type input struct {
			ID    int32
			Value []byte
		}
		type output struct {
			ID    int32
			Value cql.Bytes
		}
		inputs := cql.Params(cql.Param("id", cql.Int32Codec, func(v input) int32 { return v.ID }), cql.Param("value", cql.BytesEncoder, func(v input) []byte { return v.Value }))
		rows := cql.Row(cql.Column("id", cql.Int32Codec, func(v *output) *int32 { return &v.ID }), cql.Column("value", cql.BlobCodec, func(v *output) *cql.Bytes { return &v.Value }))
		prepared, err := session.Prepare(ctx, cql.Statement[input, cql.EmptyRow]{CQL: "INSERT INTO " + keyspace + ".values (id,value) VALUES (?,?)", Params: inputs, Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
		if err != nil {
			t.Fatal(err)
		}
		payload := bytes.Repeat([]byte("typed-large-segment-payload"), 24000)
		for id := int32(1); id <= 3; id++ {
			bound, err := BindBorrowed(prepared, input{ID: id, Value: payload})
			if err != nil {
				t.Fatal(err)
			}
			if !bound.Routing().Token.Set {
				t.Fatal("prepared partition key had no routing token")
			}
			if _, err = Exec(ctx, session, bound, gocql.CompiledOptions{}); err != nil {
				t.Fatal(err)
			}
			bound.Close()
			if err = bound.Wait(ctx); err != nil {
				t.Fatal(err)
			}
		}
		selectBound, err := BindUnpreparedOwned(cql.Statement[cql.EmptyParams, output]{CQL: "SELECT id,value FROM " + keyspace + ".values", Params: cql.EmptyParamsEncoder, Rows: rows}, cql.NewColumns(), cql.EmptyParams{})
		if err != nil {
			t.Fatal(err)
		}
		result, err := Run(ctx, session, selectBound, bindOptions(session, gocql.Options{PageSize: cql.Option[int32]{Value: 1, Set: true}}))
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		var retained *gocql.PageLease
		var view cql.Bytes
		var detached output
		var row output
		for result.NextInto(&row) {
			if !bytes.Equal(row.Value.OwnedBytes(), payload) {
				t.Fatal("large payload changed")
			}
			if count == 0 {
				retained, err = result.Retain()
				if err != nil {
					t.Fatal(err)
				}
				view = row.Value
				detached, err = result.Clone()
				if err != nil {
					t.Fatal(err)
				}
			}
			count++
		}
		if err = result.Err(); err != nil {
			t.Fatal(err)
		}
		if count != 3 {
			t.Fatalf("rows %d", count)
		}
		if result.Info().Rows != 3 {
			t.Fatalf("reported rows %d", result.Info().Rows)
		}
		selectBound.Close()
		if err = selectBound.Wait(ctx); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(view.OwnedBytes(), payload) {
			t.Fatal("retained page changed")
		}
		retained.Close()
		if !bytes.Equal(detached.Value.OwnedBytes(), payload) {
			t.Fatal("clone changed")
		}
	})

	t.Run("Startup", func(t *testing.T) {
		t.Parallel()

		t.Run("ApplicationInfo", func(t *testing.T) {
			t.Parallel()

			cluster := mustBuild(t, createV2Cluster())
			s, err := gocql.Connect(context.Background(), cluster)
			if err != nil {
				t.Fatalf("ApplicationInformation error: %s", err)
			}
			defer s.Close()
			var clientsTableName string
			for _, tableName := range []string{"system_views.clients", "system.clients"} {
				_, err = readV2ClientOptions(context.Background(), s, "select client_options from "+tableName)
				if err == nil {
					clientsTableName = tableName
					break
				}
			}

			if clientsTableName == "" {
				t.Skip("Skipping because server does have `client_options` in clients table")
			}

			tcases := []struct {
				testName string
				name     string
				version  string
				clientID string
			}{
				{
					testName: "full",
					name:     "my-application",
					version:  "1.0.0",
					clientID: "my-client-id",
				},
				{
					testName: "empty",
				},
				{
					testName: "name-only",
					name:     "my-application",
				},
				{
					testName: "version-only",
					version:  "1.0.0",
				},
				{
					testName: "client-id-only",
					clientID: "my-client-id",
				},
			}
			for _, tcase := range tcases {
				t.Run(tcase.testName, func(t *testing.T) {
					// Unset identity fields leave the setters uncalled; the
					// builder rejects an empty name or client id.
					cluster := createV2Cluster()
					if tcase.name != "" || tcase.version != "" {
						cluster = cluster.WithApplication(tcase.name, tcase.version)
					}
					if tcase.clientID != "" {
						cluster = cluster.WithClientID(tcase.clientID)
					}
					if tcase.name == "" && tcase.version != "" {
						if _, err := cluster.Build(); err == nil {
							t.Fatal("application version without a name accepted")
						}
						return
					}
					s, err := gocql.Connect(context.Background(), mustBuild(t, cluster))
					if err != nil {
						t.Fatalf("failed to connect to the cluster: %s", err)
					}
					defer s.Close()

					rows, err := readV2ClientOptions(context.Background(), s, "select client_options from "+clientsTableName)
					if err != nil {
						t.Fatalf("failed to execute query: %v", err)
					}
					found := false
					for _, row := range rows {
						if row[sessionIDStartupKey] != s.ID() {
							continue
						}
						if tcase.name != "" {
							if row["APPLICATION_NAME"] != tcase.name {
								continue
							}
						} else {
							if _, ok := row["APPLICATION_NAME"]; ok {
								continue
							}
						}
						if tcase.version != "" {
							if row["APPLICATION_VERSION"] != tcase.version {
								continue
							}
						} else {
							if _, ok := row["APPLICATION_VERSION"]; ok {
								continue
							}
						}
						if tcase.clientID != "" {
							if row["CLIENT_ID"] != tcase.clientID {
								continue
							}
						} else {
							if _, ok := row["CLIENT_ID"]; ok {
								continue
							}
						}
						found = true
						break
					}
					if !found {
						t.Fatalf("failed to find the application info row")
					}
				})
			}

		})
		// Verifies end to end that a session reports one
		// SESSION_ID shared by all of its connections, and that DRIVER_CONFIG is
		// reported exactly once, by the control connection, mirroring the existing
		// TestSession/Startup/ApplicationInfo precedent.
		t.Run("DriverConfig", func(t *testing.T) {
			t.Parallel()

			const agreementTimeout = 2 * time.Minute
			cluster := createV2ClusterWithout(fixtureAgreement).WithSchemaAgreement(agreementTimeout, 200*time.Millisecond)
			s, err := gocql.Connect(context.Background(), mustBuild(t, cluster))
			if err != nil {
				t.Fatalf("failed to connect to the cluster: %s", err)
			}
			defer s.Close()

			// Read the id through the exported accessor rather than the field: an
			// application correlating its own logs with system.clients has only the
			// accessor, so that is the path worth exercising end to end.
			wantSessionID := s.ID()
			if wantSessionID == "" {
				t.Fatal("expected the session to have a non-empty id")
			}

			var clientsTableName string
			for _, tableName := range []string{"system_views.clients", "system.clients"} {
				if _, err := readV2ClientOptions(context.Background(), s, "select client_options from "+tableName); err == nil {
					clientsTableName = tableName
					break
				}
			}
			if clientsTableName == "" {
				t.Skip("Skipping because server does not have `client_options` in clients table")
			}

			// Wait until the pools have finished filling so that the number of
			// connections this session owns stops moving. Without this the poll below
			// could reach its exit condition while connections are still being opened.
			if err := waitUntilPoolsStopFilling(context.Background(), s, 10*time.Second); err != nil {
				t.Fatalf("failed to wait for the connection pools to fill: %s", err)
			}

			// The clients table is node-local: every node lists only the connections
			// made to it. Integration runs use a three node cluster, so a load-balanced
			// query round-robins across the cluster and most polls would land on a node
			// that the control connection was never made to, and therefore never see
			// DRIVER_CONFIG at all. Run every poll on the control connection itself so
			// the rows always come from the one node whose client list is being
			// reasoned about.
			//
			// Stopping at the first row carrying DRIVER_CONFIG would also make the
			// "exactly once" assertion below vacuous: the control connection is
			// established before the pool ones, so its row is the first to appear, and a
			// regression that also reported DRIVER_CONFIG on pool connections would go
			// unnoticed because their rows are not in the table yet. Wait for every
			// connection to that node to be accounted for instead.
			var configs []string
			deadline := time.After(10 * time.Second)
			for {
				// Re-read the control connection's host on every pass: it can move if
				// the control connection reconnects, and the pool size it is compared
				// against has to belong to the same node. Re-reading the pool size also
				// means that should a pool start filling again, the bar rises with it
				// instead of leaving the loop free to exit on a stale count.
				ch := (*implSession.Session)(s).Data().InternalControl.InternalGetConn()
				if ch == nil {
					t.Fatal("expected the session to have a control connection")
				}
				controlHost := ch.InternalHost

				// Connections this session has to the control connection's node: its
				// pool for that host, plus the control connection itself, which is
				// dialed outside the pool.
				wantConns := 1
				if hostPool, ok := (*implSession.Session)(s).Data().InternalPool.InternalGetPool(controlHost); ok {
					wantConns += hostPool.Size()
				}

				configs = nil
				conns := 0

				rows, err := readV2ClientOptions(context.Background(), s, "select client_options from "+clientsTableName)
				if err != nil {
					t.Fatalf("failed to execute query: %v", err)
				}
				for _, row := range rows {
					if row[sessionIDStartupKey] != wantSessionID {
						continue
					}
					conns++
					if config, ok := row[driverConfigStartupKey]; ok {
						configs = append(configs, config)
					}
				}

				// More rows than connections is fine: a connection that has already been
				// closed can linger in the clients table. Those rows cannot hide a
				// regression, they can only reveal one, since a second DRIVER_CONFIG is
				// a failure no matter which connection produced it.
				if conns >= wantConns && len(configs) > 0 {
					break
				}

				select {
				case <-deadline:
					t.Fatalf("expected all %d connections with SESSION_ID %q to node %s to be listed in %s with at least one reporting DRIVER_CONFIG within 10s, saw %d connections and %d DRIVER_CONFIG",
						wantConns, wantSessionID, controlHost.ConnectAddress(), clientsTableName, conns, len(configs))
				case <-time.After(250 * time.Millisecond):
				}
			}

			if len(configs) != 1 {
				t.Errorf("expected exactly one connection with SESSION_ID %q to report DRIVER_CONFIG, got %d", wantSessionID, len(configs))
			}
			// Assert on the decoded report rather than a byte-exact string: the payload
			// is a full configuration description whose exact serialization is the unit
			// tests' business, while what only a live server can confirm is that it
			// round-trips intact and that the ScyllaDB-gated key is decided correctly.
			for _, config := range configs {
				// STARTUP retains the versioned driver-report wire schema even though the
				// application now configures the session with Config.
				if strings.Contains(strings.ToLower(config), `"password"`) || strings.Contains(strings.ToLower(config), `"username"`) {
					t.Error("driver configuration exposes authentication credentials")
				}
				var report driverConfigReport
				if err := json.Unmarshal([]byte(config), &report); err != nil {
					t.Errorf("expected %s to be valid JSON, got %q: %v", driverConfigStartupKey, config, err)
					continue
				}
				if report.Version != driverConfigVersion {
					t.Errorf("expected %s version %d, got %d", driverConfigStartupKey, driverConfigVersion, report.Version)
				}
				if got, want := report.ControlPlane.Schema.Agreement.TimeoutMs, agreementTimeout.Milliseconds(); got != want {
					t.Errorf("expected schema agreement timeout-ms %d, got %d", want, got)
				}
				// server-side-ms describes the USING TIMEOUT clause, which only ScyllaDB
				// understands. The fake server the unit tests run against cannot
				// exercise either side of that gate.
				wantServerSide := false
				if ch := (*implSession.Session)(s).Data().InternalControl.InternalGetConn(); ch != nil {
					wantServerSide = ch.InternalConn.InternalIsScyllaConn()
				}
				if got := report.ControlPlane.Queries.System.Timeout.ServerSideMs != nil; got != wantServerSide {
					t.Errorf("expected server-side-ms present=%v against this server, got %v", wantServerSide, got)
				}
			}
		})
	})
}

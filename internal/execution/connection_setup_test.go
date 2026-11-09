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
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/logging"
	implPool "github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func TestConnectionSetup(t *testing.T) {
	t.Run("Connect", func(t *testing.T) {
		t.Run("CancelledBeforeSetup", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
			defer cancel()
			<-ctx.Done()
			if _, err := Connect(ctx, settings.DefaultConfig("unresolved")); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
		})

		t.Run("PreservesCause", func(t *testing.T) {
			for _, cause := range []error{errors.New("fixture dial rejected"), &frm.ErrorFrame{Code: protocol.ErrCodeCredentials, Message: "rejected"}} {
				config := settings.DefaultConfig("127.0.20.1")
				config.Protocol = 4
				config.Logger = logging.InternalNopLogger{}
				config.Dialer = endpoint.DialerFunc(func(context.Context, endpoint.DialTarget) (net.Conn, error) { return nil, cause })
				session, err := Connect(context.Background(), config)
				if session != nil {
					session.Close()
				}
				if !errors.Is(err, cause) {
					t.Fatalf("bootstrap discarded dial cause: %v", err)
				}
				if request, ok := cause.(protocol.RequestError); ok {
					if got, ok := errors.AsType[protocol.RequestError](err); !ok || got.GetCode() != request.GetCode() {
						t.Fatal("bootstrap discarded server error", err)
					}
				}
			}
		})
	})

	t.Run("DNS", func(t *testing.T) {
		t.Run("ContextAndDialHost", func(t *testing.T) {
			resolver := &endpoint.InternalV2DNSResolver{InternalBase: runtimeBlockingDNS{done: make(chan struct{})}}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			resolver.InternalStartup.Store(&endpoint.InternalContextBox{InternalCtx: ctx})
			if _, err := resolver.LookupIP("ignored"); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			close(resolver.InternalBase.(runtimeBlockingDNS).done)
			config := settings.DefaultConfig("localhost")
			var calls atomic.Int32
			client, server := net.Pipe()
			defer server.Close()
			config.DialHost = func(ctx context.Context, target endpoint.DialTarget) (net.Conn, error) {
				host := target.Host
				calls.Add(1)
				if host.Datacenter != "dc" || host.Address != netip.MustParseAddrPort("127.0.0.1:9042") {
					t.Fatalf("host snapshot %+v", host)
				}
				return client, nil
			}
			host := func() *endpoint.HostInfo {
				opaqueStorageValue := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValue).InternalConnectAddress = net.IPv4(127, 0, 0, 1)
				implEndpoint.HostInfoData(opaqueStorageValue).InternalPort = 9042
				implEndpoint.HostInfoData(opaqueStorageValue).InternalDataCenter = "dc"
				return opaqueStorageValue
			}()
			dialer := transport.InternalV2TLSHostDialer{InternalConfig: settings.InternalConnectivityConfig(config)}
			connection, err := dialer.DialHost(context.Background(), host)
			if err != nil {
				t.Fatal(err)
			}
			connection.Conn.Close()
			if calls.Load() != 1 {
				t.Fatal("host hook not called")
			}
		})
	})

	t.Run("Deadline", func(t *testing.T) {
		t.Run("IncludesDNSAndTranslation", func(t *testing.T) {
			config := settings.DefaultConfig("setup.test")
			config.ConnectTimeout = 10 * time.Second
			config.DNSTimeout = 20 * time.Second
			resolver := &endpoint.InternalV2DNSResolver{InternalBase: testDNS(func(string) ([]net.IP, error) {
				return []net.IP{net.ParseIP("127.0.0.1")}, nil
			}), InternalTimeout: config.DNSTimeout, InternalSetupTimeout: config.ConnectTimeout}
			resolver.InternalStartup.Store(&endpoint.InternalContextBox{InternalCtx: context.Background()})
			defer resolver.InternalClearStartup()
			hosts := codecTestMust(endpoint.InternalResolveInitialEndpoint(resolver, "setup.test:9042", 9042))
			host := hosts[0]
			expected := implEndpoint.HostInfoData(host).InternalInitialSetupDeadline
			var observed time.Time
			entered := make(chan struct{})
			config.DialHost = func(ctx context.Context, target endpoint.DialTarget) (net.Conn, error) {
				observed, _ = ctx.Deadline()
				close(entered)
				<-ctx.Done()
				return nil, ctx.Err()
			}
			session := func() *cluster.Core {
				opaqueStorageValuexxxxxxxx := new(cluster.Core)
				implCluster.SessionData(opaqueStorageValuexxxxxxxx).InternalLogger = logging.InternalNopLogger{}
				implCluster.SessionData(opaqueStorageValuexxxxxxxx).InternalAddressTranslator = endpoint.AddressTranslatorFunc(func(ip net.IP, port int) (net.IP, int) { time.Sleep(20 * time.Millisecond); return ip, port })
				return opaqueStorageValuexxxxxxxx
			}()
			implCluster.SessionData(session).InternalCfg.DNSResolver = resolver
			cfg := &transport.ConnConfig{ProtoVersion: 4, ConnectTimeout: config.ConnectTimeout, HostDialer: &transport.InternalV2TLSHostDialer{InternalConfig: settings.InternalConnectivityConfig(config)}, TypedConfig: settings.InternalConnectivityConfigPointer(config)}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := implCluster.InternalSessionDialShard(session, ctx, host, cfg, transport.InternalConnErrorHandlerFn(func(*transport.Conn, error, bool) {}), 0, 0)
				done <- err
			}()
			select {
			case <-entered:
			case err := <-done:
				t.Fatal("setup ended before staged hook", err)
			case <-time.After(time.Second):
				t.Fatal("staged hook not entered")
			}
			cancel()
			err := <-done
			if !errors.Is(err, context.Canceled) || !observed.Equal(expected) {
				t.Fatal("setup deadline restarted after DNS/translation", observed, expected, err)
			}

			if !implEndpoint.InternalHostInfoClaimSetupDeadline(host).IsZero() {
				t.Fatal("warm connection inherited consumed setup deadline")
			}

		})

		t.Run("IndependentPerContact", func(t *testing.T) {
			resolver := &endpoint.InternalV2DNSResolver{InternalBase: testDNS(func(string) ([]net.IP, error) { return []net.IP{net.ParseIP("127.0.0.1")}, nil }), InternalSetupTimeout: time.Minute}
			first := codecTestMust(endpoint.InternalResolveInitialEndpoint(resolver, "first.test:9042", 9042))[0]
			second := codecTestMust(endpoint.InternalResolveInitialEndpoint(resolver, "second.test:9142", 9042))[0]
			implEndpoint.
				// Distinct sentinel deadlines make identity isolation deterministic even when
				// both lookups complete in one clock tick and resolve to the same IP.
				HostInfoData(first).InternalInitialSetupDeadline = time.Unix(100, 1)
			implEndpoint.HostInfoData(second).InternalInitialSetupDeadline = time.Unix(200, 2)
			if d := implEndpoint.InternalHostInfoClaimSetupDeadline(first); !d.Equal(time.Unix(100, 1)) {
				t.Fatal("first opening stole second deadline", d)
			}
			if d := implEndpoint.InternalHostInfoClaimSetupDeadline(first); !d.IsZero() {
				t.Fatal("opening reused spent deadline", d)
			}
			if d := implEndpoint.InternalHostInfoClaimSetupDeadline(second); !d.Equal(time.Unix(200, 2)) {
				t.Fatal("second contact deadline lost", d)
			}
			if implEndpoint.HostInfoData(first).InternalHostname == implEndpoint.HostInfoData(second).InternalHostname || first.Port() == second.Port() {
				t.Fatal("contact identity merged prematurely")
			}
		})

		t.Run("OpeningValidation", func(t *testing.T) {
			expected := time.Now().Add(10 * time.Second)
			entered := make(chan struct{})
			var observed time.Time
			c := &transport.Conn{InternalOpeningDeadline: expected, InternalValidateTarget: func(ctx context.Context) error {
				observed, _ = ctx.Deadline()
				close(entered)
				<-ctx.Done()
				return ctx.Err()
			}}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- c.InternalValidateOpeningTarget(ctx) }()
			<-entered
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) || !observed.Equal(expected) {
				t.Fatal("validation got new setup budget", observed, expected, err)
			}
			c.InternalOpeningDeadline = time.Now().Add(-time.Second)
			c.InternalValidateTarget = func(context.Context) error { t.Fatal("expired setup reached publication hook"); return nil }
			if err := c.InternalValidateOpeningTarget(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("expired original deadline ignored", err)
			}
			entered = make(chan struct{})
			c.InternalOpeningDeadline = time.Time{}
			c.InternalValidateTarget = func(ctx context.Context) error {
				if _, ok := ctx.Deadline(); ok {
					t.Error("zero local timeout added deadline")
				}
				close(entered)
				<-ctx.Done()
				return ctx.Err()
			}
			ctx, cancel = context.WithCancel(context.Background())
			go func() { done <- c.InternalValidateOpeningTarget(ctx) }()
			<-entered
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatal("caller cancellation lost", err)
			}
		})
	})

	t.Run("ShardAwarePort", func(t *testing.T) {
		t.Run("ExhaustionAndNATFallback", func(t *testing.T) {
			host := func() *endpoint.HostInfo {
				opaqueStorageValuex := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuex).InternalConnectAddress = net.ParseIP("127.0.0.1")
				implEndpoint.HostInfoData(opaqueStorageValuex).InternalPort = 9042
				implEndpoint.HostInfoData(opaqueStorageValuex).InternalHostId = cqltype.UUID{15: 1}
				return opaqueStorageValuex
			}()
			implEndpoint.InternalHostInfoSetTranslatedConnectionInfo(host, endpoint.InternalTranslatedAddresses{CQL: endpoint.AddressPort{Address: net.ParseIP("127.0.0.1"), Port: 9042}, ShardAware: endpoint.AddressPort{Address: net.ParseIP("127.0.0.1"), Port: 19042}})
			config := settings.DefaultConfig("localhost")
			config.Pool.SourcePortStart = 49152
			config.Pool.SourcePortEnd = 49155
			var targeted, ordinary int
			config.DialHost = func(ctx context.Context, target endpoint.DialTarget) (net.Conn, error) {
				if target.ShardID.Set {
					targeted++
					return nil, transport.ErrScyllaSourcePortAlreadyInUse
				}
				ordinary++
				a, b := net.Pipe()
				b.Close()
				return a, nil
			}
			result, err := (&transport.InternalV2TLSHostDialer{InternalConfig: settings.InternalConnectivityConfig(config)}).DialShard(context.Background(), host, 1, 2)
			if err != nil {
				t.Fatal(err)
			}
			result.Conn.Close()
			if targeted != 2 || ordinary != 1 || implEndpoint.DialedHostData(result).InternalShardTarget.Set {
				t.Fatal("range exhaustion fallback", targeted, ordinary)
			}
			c := &transport.Conn{InternalScyllaSupported: *func() *endpoint.ScyllaConnectionFeatures {
				opaqueStorageValuexxx := new(endpoint.ScyllaConnectionFeatures)
				opaqueStorageValuexxx.ScyllaHostFeatures = *func() *endpoint.ScyllaHostFeatures {
					opaqueStorageValuexx := new(endpoint.ScyllaHostFeatures)
					implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuexx).InternalNrShards = 2
					return opaqueStorageValuexx
				}()
				implEndpoint.ScyllaConnectionFeaturesData(opaqueStorageValuexxx).InternalShard = 0
				return opaqueStorageValuexxx
			}(), InternalRequestedShard: cqltype.Option[uint32]{Value: 1, Set: true}, InternalStreams: streams.New()}
			p := codecTestMust(implPool.InternalNewTypedConnPicker(c, config.Pool))
			if p.Put(c) == nil {
				t.Fatal("NAT wrong shard accepted")
			}
			if _, shards := p.NextShard(); shards != 0 {
				t.Fatal("NAT ordinary-port cooldown absent")
			}
		})
	})

	t.Run("TLS", func(t *testing.T) {
		t.Run("Detach", func(t *testing.T) {
			cert, tlsConfig := testTLS(t)
			tlsConfig.Certificates = []tls.Certificate{cert}
			tlsConfig.NextProtos = []string{"cql"}
			tlsConfig.ClientCAs = tlsConfig.RootCAs
			cloned := settings.InternalCloneV2TLSConfig(tlsConfig)
			originalDER := append([]byte(nil), cloned.Certificates[0].Certificate[0]...)
			tlsConfig.Certificates[0].Certificate[0][0] ^= 1
			tlsConfig.Certificates[0].OCSPStaple[0] = 'X'
			tlsConfig.Certificates[0].SignedCertificateTimestamps[0][0] = 'X'
			tlsConfig.NextProtos[0] = "mutated"
			if !bytes.Equal(cloned.Certificates[0].Certificate[0], originalDER) || string(cloned.Certificates[0].OCSPStaple) != "ocsp" || string(cloned.Certificates[0].SignedCertificateTimestamps[0]) != "sct" || cloned.NextProtos[0] != "cql" || cloned.RootCAs == tlsConfig.RootCAs || cloned.ClientCAs == tlsConfig.ClientCAs {
				t.Fatal("TLS mutable containers alias")
			}
		})

		t.Run("LogicalHostIdentity", func(t *testing.T) {
			cert, configTLS := testTLS(t)
			configTLS.ServerName = ""
			var name string
			configTLS.VerifyConnection = func(state tls.ConnectionState) error { name = state.ServerName; return nil }
			listener := codecTestMust(net.Listen("tcp", "127.0.0.1:0"))
			defer listener.Close()
			host := testHost(t, listener.Addr())
			implEndpoint.HostInfoData(host).InternalHostname = "transport.test"
			implEndpoint.HostInfoData(host).InternalConnectAddress = net.ParseIP("192.0.2.12")
			done := make(chan error, 1)
			go func() {
				raw, err := listener.Accept()
				if err != nil {
					done <- err
					return
				}
				defer raw.Close()
				done <- tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{cert}}).Handshake()
			}()
			config := settings.DefaultConfig("transport.test")
			config.TLS = configTLS
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			conn, err := (&transport.InternalV2TLSHostDialer{InternalConfig: settings.InternalConnectivityConfig(config)}).DialHost(ctx, host)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Conn.Close()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if name != "transport.test" {
				t.Fatal("proxy/translated address replaced logical TLS identity", name)
			}
		})

		t.Run("Source", func(t *testing.T) {
			serverCert, clientTLS := testTLS(t)
			listener := codecTestMust(net.Listen("tcp", "127.0.0.1:0"))
			defer listener.Close()
			peers := make(chan []byte, 64)
			go func() {
				for {
					raw, err := listener.Accept()
					if err != nil {
						return
					}
					go func() {
						defer raw.Close()
						server := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.RequireAnyClientCert})
						if server.Handshake() == nil {
							peers <- server.ConnectionState().PeerCertificates[0].Raw
						}
					}()
				}
			}()
			withClientCert := func() *tls.Config {
				clientCert, _ := testTLS(t)
				c := clientTLS.Clone()
				c.Certificates = []tls.Certificate{clientCert}
				return c
			}
			first, second := withClientCert(), withClientCert()
			live := &testTLSSource{}
			live.Store(first)
			config := settings.DefaultConfig("localhost")
			config.TLS = first
			config.TLSSource = live
			config.Pool.ShardAwarePort = false
			dialer := &transport.InternalV2TLSHostDialer{InternalConfig: settings.InternalConnectivityConfig(config)}
			host := testHost(t, listener.Addr())
			dial := func() error {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				connection, err := dialer.DialHost(ctx, host)
				if err == nil {
					connection.Conn.Close()
				}
				return err
			}
			if err := dial(); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(<-peers, first.Certificates[0].Certificate[0]) {
				t.Fatal("first dial did not present the initial certificate")
			}
			live.Store(second)
			if err := dial(); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(<-peers, second.Certificates[0].Certificate[0]) {
				t.Fatal("dial after the swap presented the old certificate")
			}

			_, otherCA := testTLS(t)
			untrusting := second.Clone()
			untrusting.RootCAs = otherCA.RootCAs
			live.Store(untrusting)
			if err := dial(); err == nil {
				t.Fatal("dial after swapping in an unrelated CA trusted the old server")
			}

			live.Store(first)
			var wg sync.WaitGroup
			for i := range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if i%2 == 0 {
						live.Store([]*tls.Config{first, second}[i/2%2])
						return
					}
					if err := dial(); err != nil {
						t.Error(err)
					}
				}()
			}
			wg.Wait()

			live.Store(nil)
			if err := dial(); err == nil || !strings.Contains(err.Error(), "TLS source") {
				t.Fatal("a source with no config did not fail the dial:", err)
			}
		})

		t.Run("ShardDialSource", func(t *testing.T) {
			cert, tlsConfig := testTLS(t)
			listener := codecTestMust(net.Listen("tcp", "127.0.0.1:0"))
			defer listener.Close()
			host := testHost(t, listener.Addr())
			accepted := make(chan error, 1)
			go func() {
				raw, err := listener.Accept()
				if err != nil {
					accepted <- err
					return
				}
				defer raw.Close()
				accepted <- tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{cert}}).Handshake()
			}()
			config := settings.DefaultConfig("localhost")
			config.TLS = tlsConfig
			var selected endpoint.DialTarget
			config.DialHost = func(ctx context.Context, target endpoint.DialTarget) (net.Conn, error) {
				selected = target
				return (endpoint.DefaultDialer{}).DialContext(ctx, target)
			}
			d := transport.InternalV2TLSHostDialer{InternalConfig: settings.InternalConnectivityConfig(config)}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			conn, err := d.DialShard(ctx, host, 1, 2)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Conn.Close()
			if err = <-accepted; err != nil {
				t.Fatal(err)
			}
			if !selected.ShardID.Set || selected.ShardID.Value != 1 || !selected.LocalAddress.IsValid() || selected.LocalAddress.Port()%2 != 1 || selected.LocalAddress.Port() < config.Pool.SourcePortStart || !implEndpoint.DialedHostData(conn).InternalShardTarget.Set {
				t.Fatal("shard target/source lost", selected)
			}
		})
	})

	t.Run("Unix", func(t *testing.T) {
		t.Run("TargetAndUnsupportedSettings", func(t *testing.T) {
			config := settings.DefaultConfig("unix:" + t.TempDir() + "/maintenance")
			config.Protocol = 3
			config.Metadata.InitialDiscovery = false
			targetSeen := false
			config.Dialer = endpoint.DialerFunc(func(ctx context.Context, target endpoint.DialTarget) (net.Conn, error) {
				targetSeen = true
				if target.Network != "unix" || target.Address != config.ContactPoints[0][5:] || target.Host.Address.IsValid() || target.LocalAddress.IsValid() || target.ShardID.Set {
					t.Fatal("Unix target leaked TCP placeholder", target)
				}
				a, b := net.Pipe()
				b.Close()
				return a, nil
			})
			h := func() *endpoint.HostInfo {
				opaqueStorageValuexxxxxx := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxx).InternalConnectAddress = net.ParseIP("127.0.0.1")
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxx).InternalPort = 9042
				return opaqueStorageValuexxxxxx
			}()
			c, err := (&transport.InternalV2TLSHostDialer{InternalConfig: settings.InternalConnectivityConfig(config)}).DialHost(context.Background(), h)
			if err != nil {
				t.Fatal(err)
			}
			c.Conn.Close()
			if !targetSeen {
				t.Fatal("dialer bypassed")
			}
			for _, change := range []func(*settings.Config){func(c *settings.Config) { c.Protocol = 4 }, func(c *settings.Config) { c.TLS = &tls.Config{} }, func(c *settings.Config) { c.LocalAddress = netip.MustParseAddr("127.0.0.1") }} {
				invalid := config
				change(&invalid)
				err := settings.InternalValidateConfig(invalid)
				if _, ok := errors.AsType[*settings.UnsupportedSettingError](err); !ok {
					t.Fatal("Unix unsupported", err)
				}
			}
		})
	})
}

// testTLSSource serves whatever config was stored last.
type testTLSSource struct{ atomic.Pointer[tls.Config] }

func (s *testTLSSource) TLSConfig() *tls.Config { return s.Load() }

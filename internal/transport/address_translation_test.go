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

package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	endpoint2 "github.com/scylladb/gocql/v2/endpoint"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/tests"
)

func TestAddressTranslation(t *testing.T) {
	ctx := context.Background()
	t.Run("TranslateHostAddresses", func(t *testing.T) {
		t.Run("NoScyllaPorts", func(t *testing.T) {
			t.Parallel()

			translator := endpoint2.TranslatorFunc(func(context.Context, endpoint2.Peer, netip.AddrPort) (endpoint2.Address, error) {
				return endpoint2.Address{Host: "10.10.10.10", Port: 9142}, nil
			})
			host := endpoint.HostInfoBuilder{
				ConnectAddress: net.ParseIP("10.0.0.1"),
				Port:           9042,
			}.Build()

			translated, err := InternalTranslateHostAddresses(ctx, translator, nil, &host, 0, nil, nil)

			tests.AssertNil(t, "should return no error", err)
			tests.AssertEqual(t, "translated CQL", netip.MustParseAddrPort("10.10.10.10:9142"), translated.CQL)
			tests.AssertEqual(t, "shard aware empty", netip.AddrPort{}, translated.ShardAware)
			tests.AssertEqual(t, "shard aware tls empty", netip.AddrPort{}, translated.ShardAwareTLS)
		})
		t.Run("InvalidPort", func(t *testing.T) {
			for _, port := range []int{-1, 0, 65536} {
				t.Run(fmt.Sprint(port), func(t *testing.T) {
					host := endpoint.HostInfoBuilder{ConnectAddress: net.ParseIP("10.0.0.1"), Port: port}.Build()
					if _, err := InternalTranslateHostAddresses(ctx, nil, nil, &host, 0, nil, nil); err == nil {
						t.Fatalf("expected invalid port %d to be rejected", port)
					}
				})
			}
		})
		t.Run("WithScyllaPorts", func(t *testing.T) {
			t.Parallel()

			translatedIP := netip.MustParseAddr("192.0.2.10")
			wantPeer := endpoint2.Peer{
				HostID:            cql.MustParseUUID("a0000000-0000-0000-0000-000000000001"),
				Datacenter:        "dc1",
				Rack:              "r1",
				RPCAddress:        netip.MustParseAddr("10.0.1.1"),
				BroadcastAddress:  netip.MustParseAddr("10.0.2.1"),
				Port:              9042,
				ShardAwarePort:    19042,
				ShardAwarePortTLS: 19043,
			}
			translator := endpoint2.TranslatorFunc(func(_ context.Context, peer endpoint2.Peer, addr netip.AddrPort) (endpoint2.Address, error) {
				if peer != wantPeer {
					t.Errorf("peer %+v, want %+v", peer, wantPeer)
				}
				if addr.Addr() != netip.MustParseAddr("10.0.0.1") {
					t.Errorf("untranslated addr %v", addr)
				}
				return endpoint2.AddressOf(netip.AddrPortFrom(translatedIP, addr.Port()+1)), nil
			})
			host := endpoint.HostInfoBuilder{
				ConnectAddress:   net.ParseIP("10.0.0.1"),
				RpcAddress:       net.ParseIP("10.0.1.1"),
				BroadcastAddress: net.ParseIP("10.0.2.1"),
				DataCenter:       "dc1",
				Rack:             "r1",
				Port:             9042,
				HostId:           "a0000000-0000-0000-0000-000000000001",
			}.Build()
			implEndpoint.InternalHostInfoSetScyllaFeatures(&host, *func() *endpoint.ScyllaHostFeatures {
				opaqueStorageValue := new(endpoint.ScyllaHostFeatures)
				implEndpoint.ScyllaHostFeaturesData(opaqueStorageValue).InternalShardAwarePort = 19042
				implEndpoint.ScyllaHostFeaturesData(opaqueStorageValue).InternalShardAwarePortTLS = 19043
				return opaqueStorageValue
			}(),
			)

			translated, err := InternalTranslateHostAddresses(ctx, translator, nil, &host, 0, nil, nil)

			tests.AssertNil(t, "should return no error", err)
			tests.AssertEqual(t, "translated CQL", netip.AddrPortFrom(translatedIP, 9043), translated.CQL)
			tests.AssertEqual(t, "translated shard aware", netip.AddrPortFrom(translatedIP, 19043), translated.ShardAware)
			tests.AssertEqual(t, "translated shard aware tls", netip.AddrPortFrom(translatedIP, 19044), translated.ShardAwareTLS)
		})
	})
	t.Run("TranslateAddressPort", func(t *testing.T) {
		addr := netip.MustParseAddrPort("10.0.0.1:9042")
		to := func(result endpoint2.Address) endpoint2.Translator {
			return endpoint2.TranslatorFunc(func(context.Context, endpoint2.Peer, netip.AddrPort) (endpoint2.Address, error) {
				return result, nil
			})
		}
		for name, result := range map[string]endpoint2.Address{
			"ZeroPortRejected":            {Host: "10.0.0.1"},
			"NoHostRejected":              {Port: 9042},
			"UnspecifiedAddrRejected":     {Host: "0.0.0.0", Port: 9042},
			"UnspecifiedIPv6AddrRejected": {Host: "::", Port: 9042},
		} {
			t.Run(name, func(t *testing.T) {
				if _, _, err := endpoint.InternalTranslateAddressPort(ctx, to(result), nil, endpoint2.Peer{}, addr, netip.AddrPort{}, nil); err == nil {
					t.Fatalf("expected translated %v to be rejected", result)
				}
			})
		}
		t.Run("IPHasNoName", func(t *testing.T) {
			for _, host := range []string{"::ffff:192.0.2.1", "[::ffff:192.0.2.1]"} {
				got, name, err := endpoint.InternalTranslateAddressPort(ctx, to(endpoint2.Address{Host: host, Port: 9142}), failingResolver{t}, endpoint2.Peer{}, addr, netip.AddrPort{}, nil)
				if err != nil || got != netip.MustParseAddrPort("192.0.2.1:9142") || name != "" {
					t.Fatalf("%s = %v, %q, %v; want 192.0.2.1:9142 unmapped, no name and no lookup", host, got, name, err)
				}
			}
		})
		t.Run("NameResolved", func(t *testing.T) {
			resolver := &staticResolver{addrs: []netip.Addr{netip.MustParseAddr("0.0.0.0"), netip.MustParseAddr("::ffff:192.0.2.7"), netip.MustParseAddr("192.0.2.8")}}
			translator := to(endpoint2.Address{Host: "lb.example.com", Port: 9142})
			got, name, err := endpoint.InternalTranslateAddressPort(ctx, translator, resolver, endpoint2.Peer{}, addr, netip.AddrPort{}, nil)
			if err != nil || got != netip.MustParseAddrPort("192.0.2.7:9142") || name != "lb.example.com" || resolver.host != "lb.example.com" {
				t.Fatalf("= %v, %q, %v (looked up %q); want the first usable address of lb.example.com", got, name, err, resolver.host)
			}
			// The address the node was dialed on is kept while the name
			// still resolves to it.
			prefer := netip.MustParseAddrPort("192.0.2.8:9142")
			if got, _, err := endpoint.InternalTranslateAddressPort(ctx, translator, resolver, endpoint2.Peer{}, addr, prefer, nil); err != nil || got != prefer {
				t.Fatalf("with a current address = %v, %v; want %v", got, err, prefer)
			}
			resolver.addrs = []netip.Addr{netip.MustParseAddr("0.0.0.0")}
			if _, _, err := endpoint.InternalTranslateAddressPort(ctx, translator, resolver, endpoint2.Peer{}, addr, prefer, nil); err == nil {
				t.Fatal("a name without usable addresses was accepted")
			}
			resolver.err = errors.New("no such host")
			if _, _, err := endpoint.InternalTranslateAddressPort(ctx, translator, resolver, endpoint2.Peer{}, addr, prefer, nil); !errors.Is(err, resolver.err) {
				t.Fatalf("lookup failure = %v, want it wrapped", err)
			}
		})
		t.Run("TLSName", func(t *testing.T) {
			resolver := &staticResolver{addrs: []netip.Addr{netip.MustParseAddr("192.0.2.7")}}
			translator := endpoint2.TranslatorFunc(func(_ context.Context, _ endpoint2.Peer, a netip.AddrPort) (endpoint2.Address, error) {
				if a.Port() == 9142 {
					return endpoint2.Address{Host: "tls.example.com", Port: 9142}, nil
				}
				return endpoint2.Address{Host: "plain.example.com", Port: a.Port()}, nil
			})
			host := endpoint.HostInfoBuilder{ConnectAddress: net.ParseIP("10.0.0.1"), Port: 9042}.Build()
			translated, err := InternalTranslateHostAddresses(ctx, translator, resolver, &host, 0, nil, nil)
			if err != nil || translated.TLSName != "plain.example.com" || translated.CQL != netip.MustParseAddrPort("192.0.2.7:9042") {
				t.Fatalf("= %+v, %v; want the CQL endpoint's name", translated, err)
			}
			translated, err = InternalTranslateHostAddresses(ctx, translator, resolver, &host, 9142, nil, nil)
			if err != nil || translated.TLSName != "tls.example.com" {
				t.Fatalf("with a TLS port = %+v, %v; want the TLS endpoint's name", translated, err)
			}
		})
		t.Run("ContextPassed", func(t *testing.T) {
			type key struct{}
			ctx := context.WithValue(ctx, key{}, true)
			translator := endpoint2.TranslatorFunc(func(ctx context.Context, _ endpoint2.Peer, a netip.AddrPort) (endpoint2.Address, error) {
				if ctx.Value(key{}) == nil {
					t.Error("translator did not receive the setup context")
				}
				return endpoint2.AddressOf(a), nil
			})
			if _, _, err := endpoint.InternalTranslateAddressPort(ctx, translator, nil, endpoint2.Peer{}, addr, netip.AddrPort{}, nil); err != nil {
				t.Fatal(err)
			}
		})
	})
}

// staticResolver answers every lookup with addrs or err, and records the
// host it was asked for.
type staticResolver struct {
	err   error
	host  string
	addrs []netip.Addr
}

func (r *staticResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	r.host = host
	return r.addrs, r.err
}

// failingResolver fails the test on any lookup.
type failingResolver struct{ t *testing.T }

func (r failingResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	r.t.Error("unexpected lookup")
	return nil, errors.New("unexpected lookup")
}

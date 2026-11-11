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
	"fmt"
	"net"
	"net/netip"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/tests"
)

func TestAddressTranslation(t *testing.T) {
	ctx := context.Background()
	t.Run("IdentityTranslator", func(t *testing.T) {
		t.Run("ZeroAddr", func(t *testing.T) {
			t.Parallel()

			var tr endpoint.AddressTranslator = endpoint.IdentityTranslator()
			got, err := tr.Translate(ctx, endpoint.Peer{}, netip.AddrPort{})
			tests.AssertNil(t, "should return no error", err)
			tests.AssertEqual(t, "translated addr", netip.AddrPort{}, got)
		})
		t.Run("HostProvided", func(t *testing.T) {
			t.Parallel()

			var tr endpoint.AddressTranslator = endpoint.IdentityTranslator()
			addr := netip.MustParseAddrPort("10.1.2.3:9042")
			got, err := tr.Translate(ctx, endpoint.Peer{}, addr)
			tests.AssertNil(t, "should return no error", err)
			tests.AssertEqual(t, "translated addr", addr, got)
		})
	})

	t.Run("TranslateHostAddresses", func(t *testing.T) {
		t.Run("NoScyllaPorts", func(t *testing.T) {
			t.Parallel()

			translator := endpoint.AddressTranslatorFunc(func(context.Context, endpoint.Peer, netip.AddrPort) (netip.AddrPort, error) {
				return netip.MustParseAddrPort("10.10.10.10:9142"), nil
			})
			host := endpoint.HostInfoBuilder{
				ConnectAddress: net.ParseIP("10.0.0.1"),
				Port:           9042,
			}.Build()

			translated, err := InternalTranslateHostAddresses(ctx, translator, &host, nil)

			tests.AssertNil(t, "should return no error", err)
			tests.AssertEqual(t, "translated CQL", netip.MustParseAddrPort("10.10.10.10:9142"), translated.CQL)
			tests.AssertEqual(t, "shard aware empty", netip.AddrPort{}, translated.ShardAware)
			tests.AssertEqual(t, "shard aware tls empty", netip.AddrPort{}, translated.ShardAwareTLS)
		})
		t.Run("InvalidPort", func(t *testing.T) {
			for _, port := range []int{-1, 0, 65536} {
				t.Run(fmt.Sprint(port), func(t *testing.T) {
					host := endpoint.HostInfoBuilder{ConnectAddress: net.ParseIP("10.0.0.1"), Port: port}.Build()
					if _, err := InternalTranslateHostAddresses(ctx, nil, &host, nil); err == nil {
						t.Fatalf("expected invalid port %d to be rejected", port)
					}
				})
			}
		})
		t.Run("WithScyllaPorts", func(t *testing.T) {
			t.Parallel()

			translatedIP := netip.MustParseAddr("192.0.2.10")
			wantPeer := endpoint.Peer{
				HostID:            cqltype.MustParseUUID("a0000000-0000-0000-0000-000000000001"),
				Datacenter:        "dc1",
				Rack:              "r1",
				RPCAddress:        netip.MustParseAddr("10.0.1.1"),
				BroadcastAddress:  netip.MustParseAddr("10.0.2.1"),
				Port:              9042,
				ShardAwarePort:    19042,
				ShardAwarePortTLS: 19043,
			}
			translator := endpoint.AddressTranslatorFunc(func(_ context.Context, peer endpoint.Peer, addr netip.AddrPort) (netip.AddrPort, error) {
				if peer != wantPeer {
					t.Errorf("peer %+v, want %+v", peer, wantPeer)
				}
				if addr.Addr() != netip.MustParseAddr("10.0.0.1") {
					t.Errorf("untranslated addr %v", addr)
				}
				return netip.AddrPortFrom(translatedIP, addr.Port()+1), nil
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

			translated, err := InternalTranslateHostAddresses(ctx, translator, &host, nil)

			tests.AssertNil(t, "should return no error", err)
			tests.AssertEqual(t, "translated CQL", netip.AddrPortFrom(translatedIP, 9043), translated.CQL)
			tests.AssertEqual(t, "translated shard aware", netip.AddrPortFrom(translatedIP, 19043), translated.ShardAware)
			tests.AssertEqual(t, "translated shard aware tls", netip.AddrPortFrom(translatedIP, 19044), translated.ShardAwareTLS)
		})
	})
	t.Run("TranslateAddressPort", func(t *testing.T) {
		addr := netip.MustParseAddrPort("10.0.0.1:9042")
		for name, result := range map[string]netip.AddrPort{
			"ZeroPortRejected":            netip.AddrPortFrom(addr.Addr(), 0),
			"InvalidAddrRejected":         netip.AddrPortFrom(netip.Addr{}, 9042),
			"UnspecifiedAddrRejected":     netip.MustParseAddrPort("0.0.0.0:9042"),
			"UnspecifiedIPv6AddrRejected": netip.MustParseAddrPort("[::]:9042"),
		} {
			t.Run(name, func(t *testing.T) {
				translator := endpoint.AddressTranslatorFunc(func(context.Context, endpoint.Peer, netip.AddrPort) (netip.AddrPort, error) {
					return result, nil
				})
				if _, err := endpoint.InternalTranslateAddressPort(ctx, translator, endpoint.Peer{}, addr, nil); err == nil {
					t.Fatalf("expected translated %v to be rejected", result)
				}
			})
		}
		t.Run("ContextPassed", func(t *testing.T) {
			type key struct{}
			ctx := context.WithValue(ctx, key{}, true)
			translator := endpoint.AddressTranslatorFunc(func(ctx context.Context, _ endpoint.Peer, a netip.AddrPort) (netip.AddrPort, error) {
				if ctx.Value(key{}) == nil {
					t.Error("translator did not receive the setup context")
				}
				return a, nil
			})
			if _, err := endpoint.InternalTranslateAddressPort(ctx, translator, endpoint.Peer{}, addr, nil); err != nil {
				t.Fatal(err)
			}
		})
	})
}

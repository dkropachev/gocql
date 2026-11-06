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
	"fmt"
	"net"
	"testing"

	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/tests"
)

func TestAddressTranslation(t *testing.T) {
	t.Run("IdentityTranslator", func(t *testing.T) {
		t.Run("NilAddrAndZeroPort", func(t *testing.T) {
			t.Parallel()

			var tr endpoint.AddressTranslator = endpoint.IdentityTranslator()
			hostIP := net.ParseIP("")
			if hostIP != nil {
				t.Errorf("expected host ip to be (nil) but was (%+v) instead", hostIP)
			}

			addr, port := tr.Translate(hostIP, 0)
			if addr != nil {
				t.Errorf("expected translated host to be (nil) but was (%+v) instead", addr)
			}
			tests.AssertEqual(t, "translated port", 0, port)
		})
		t.Run("HostProvided", func(t *testing.T) {
			t.Parallel()

			var tr endpoint.AddressTranslator = endpoint.IdentityTranslator()
			hostIP := net.ParseIP("10.1.2.3")
			if hostIP == nil {
				t.Error("expected host ip not to be (nil)")
			}

			addr, port := tr.Translate(hostIP, 9042)
			if !hostIP.Equal(addr) {
				t.Errorf("expected translated addr to be (%+v) but was (%+v) instead", hostIP, addr)
			}
			tests.AssertEqual(t, "translated port", 9042, port)
		})
	})

	t.Run("TranslateHostAddresses", func(t *testing.T) {
		t.Run("NoScyllaPorts", func(t *testing.T) {
			t.Parallel()

			translator := endpoint.AddressTranslatorFunc(func(addr net.IP, port int) (net.IP, int) {
				return net.ParseIP("10.10.10.10"), 9142
			})
			host := endpoint.HostInfoBuilder{
				ConnectAddress: net.ParseIP("10.0.0.1"),
				Port:           9042,
			}.Build()

			translated, err := InternalTranslateHostAddresses(translator, &host, nil)

			tests.AssertNil(t, "should return no error", err)
			tests.AssertTrue(t, "translated CQL address", net.ParseIP("10.10.10.10").Equal(translated.CQL.Address))
			tests.AssertEqual(t, "translated CQL port", uint16(9142), translated.CQL.Port)
			tests.AssertTrue(t, "shard aware empty address", len(translated.ShardAware.Address) == 0)
			tests.AssertEqual(t, "shard aware empty port", uint16(0), translated.ShardAware.Port)
			tests.AssertTrue(t, "shard aware tls empty address", len(translated.ShardAwareTLS.Address) == 0)
			tests.AssertEqual(t, "shard aware tls empty port", uint16(0), translated.ShardAwareTLS.Port)
		})
		t.Run("InvalidPort", func(t *testing.T) {
			for _, port := range []int{-1, 0, 65536} {
				t.Run(fmt.Sprint(port), func(t *testing.T) {
					host := endpoint.HostInfoBuilder{ConnectAddress: net.ParseIP("10.0.0.1"), Port: port}.Build()
					if _, err := InternalTranslateHostAddresses(nil, &host, nil); err == nil {
						t.Fatalf("expected invalid port %d to be rejected", port)
					}
				})
			}
		})
		t.Run("WithScyllaPorts", func(t *testing.T) {
			t.Parallel()

			translatedIP := net.ParseIP("192.0.2.10")
			translator := endpoint.AddressTranslatorFuncV2(func(hostID string, addr endpoint.AddressPort) endpoint.AddressPort {
				if hostID != "a0000000-0000-0000-0000-000000000001" {
					t.Errorf("expected host id %q, got %q", "a0000000-0000-0000-0000-000000000001", hostID)
				}
				return endpoint.AddressPort{
					Address: translatedIP,
					Port:    addr.Port + 1,
				}
			})
			host := endpoint.HostInfoBuilder{
				ConnectAddress: net.ParseIP("10.0.0.1"),
				Port:           9042,
				HostId:         "a0000000-0000-0000-0000-000000000001",
			}.Build()
			implEndpoint.InternalHostInfoSetScyllaFeatures(&host, *func() *endpoint.ScyllaHostFeatures {
				opaqueStorageValue := new(endpoint.ScyllaHostFeatures)
				implEndpoint.ScyllaHostFeaturesData(opaqueStorageValue).InternalShardAwarePort = 19042
				implEndpoint.ScyllaHostFeaturesData(opaqueStorageValue).InternalShardAwarePortTLS = 19043
				return opaqueStorageValue
			}(),
			)

			translated, err := InternalTranslateHostAddresses(translator, &host, nil)

			tests.AssertNil(t, "should return no error", err)
			tests.AssertTrue(t, "translated CQL address", translatedIP.Equal(translated.CQL.Address))
			tests.AssertEqual(t, "translated CQL port", uint16(9043), translated.CQL.Port)
			tests.AssertTrue(t, "translated shard aware address", translatedIP.Equal(translated.ShardAware.Address))
			tests.AssertEqual(t, "translated shard aware port", uint16(19043), translated.ShardAware.Port)
			tests.AssertTrue(t, "translated shard aware tls address", translatedIP.Equal(translated.ShardAwareTLS.Address))
			tests.AssertEqual(t, "translated shard aware tls port", uint16(19044), translated.ShardAwareTLS.Port)
		})
	})
	t.Run("TranslateAddressPort", func(t *testing.T) {
		host := endpoint.HostInfoBuilder{ConnectAddress: net.ParseIP("10.0.0.1"), Port: 9042}.Build()
		addr := endpoint.AddressPort{Address: host.UntranslatedConnectAddress(), Port: 9042}
		for _, port := range []int{-1, 0, 65536} {
			t.Run(fmt.Sprint(port), func(t *testing.T) {
				translator := endpoint.AddressTranslatorFunc(func(ip net.IP, originalPort int) (net.IP, int) {
					return ip, port
				})
				if _, err := endpoint.InternalTranslateAddressPort(translator, &host, addr, nil); err == nil {
					t.Fatalf("expected translated port %d to be rejected", port)
				}
			})
		}
	})
}

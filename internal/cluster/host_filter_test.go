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
	"net"
	"testing"

	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
)

func TestHostFilter(t *testing.T) {
	t.Run("AcceptAll", func(t *testing.T) {
		t.Parallel()

		f := AcceptAllFilter()
		tests := [...]struct {
			addr   net.IP
			accept bool
		}{
			{net.ParseIP("127.0.0.1"), true},
			{net.ParseIP("127.0.0.2"), true},
			{net.ParseIP("127.0.0.3"), true},
		}

		for i, test := range tests {
			if f.Accept(func() *endpoint.HostInfo {
				opaqueStorageValuex := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuex).InternalConnectAddress = test.addr
				return opaqueStorageValuex
			}()) {
				if !test.accept {
					t.Errorf("%d: should not have been accepted but was", i)
				}
			} else if test.accept {
				t.Errorf("%d: should have been accepted but wasn't", i)
			}
		}
	})

	t.Run("DataCenter", func(t *testing.T) {
		// TestHostFilter/DataCenter/AcceptsReportedDC guards the behaviour the introspectable
		// type replaced: it is reported by reading the datacenter back off the filter,
		// which is only sound if the filter still accepts exactly that datacenter.
		t.Run("AcceptsReportedDC", func(t *testing.T) {
			filter := DataCenterHostFilter("dc1")
			for _, tt := range []struct {
				dc   string
				want bool
			}{{"dc1", true}, {"dc2", false}, {"", false}} {
				if got := filter.Accept(func() *endpoint.HostInfo {
					opaqueStorageValuexx := new(endpoint.HostInfo)
					implEndpoint.HostInfoData(opaqueStorageValuexx).InternalDataCenter = tt.dc
					return opaqueStorageValuexx
				}()); got != tt.want {
					t.Errorf("Accept(dataCenter=%q) = %v, want %v", tt.dc, got, tt.want)
				}
			}
		})

		t.Run("Basic", func(t *testing.T) {
			t.Parallel()

			f := DataCenterHostFilter("dc1")
			fDeprecated := DataCenterHostFilter("dc1")

			tests := [...]struct {
				dc     string
				accept bool
			}{
				{"dc1", true},
				{"dc2", false},
			}

			for i, test := range tests {
				if f.Accept(func() *endpoint.HostInfo {
					opaqueStorageValuexxxxxxxxxxx := new(endpoint.HostInfo)
					implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxxxx).InternalDataCenter = test.dc
					return opaqueStorageValuexxxxxxxxxxx
				}()) {
					if !test.accept {
						t.Errorf("%d: should not have been accepted but was", i)
					}
				} else if test.accept {
					t.Errorf("%d: should have been accepted but wasn't", i)
				}

				if f.Accept(func() *endpoint.HostInfo {
					opaqueStorageValuexxxxxxxxxxxx := new(endpoint.HostInfo)
					implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxxxxx).InternalDataCenter = test.dc
					return opaqueStorageValuexxxxxxxxxxxx
				}()) != fDeprecated.Accept(func() *endpoint.HostInfo {
					opaqueStorageValuexxxxxxxxxxxxx := new(endpoint.HostInfo)
					implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxxxxxx).InternalDataCenter = test.dc
					return opaqueStorageValuexxxxxxxxxxxxx
				}()) {
					t.Errorf("%d: DataCenterHostFilter and DataCentreHostFilter should be the same", i)
				}
			}
		})
	})

	t.Run("DenyAll", func(t *testing.T) {
		t.Parallel()

		f := DenyAllFilter()
		tests := [...]struct {
			addr   net.IP
			accept bool
		}{
			{net.ParseIP("127.0.0.1"), false},
			{net.ParseIP("127.0.0.2"), false},
			{net.ParseIP("127.0.0.3"), false},
		}

		for i, test := range tests {
			if f.Accept(func() *endpoint.HostInfo {
				opaqueStorageValuexx := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuexx).InternalConnectAddress = test.addr
				return opaqueStorageValuexx
			}()) {
				if !test.accept {
					t.Errorf("%d: should not have been accepted but was", i)
				}
			} else if test.accept {
				t.Errorf("%d: should have been accepted but wasn't", i)
			}
		}
	})

	t.Run("WhiteList", func(t *testing.T) {
		t.Parallel()

		t.Run("Basic", func(t *testing.T) {
			t.Parallel()

			f := WhiteListHostFilter("127.0.0.1", "127.0.0.2")
			tests := [...]struct {
				addr   net.IP
				accept bool
			}{
				{net.ParseIP("127.0.0.1"), true},
				{net.ParseIP("127.0.0.2"), true},
				{net.ParseIP("127.0.0.3"), false},
			}

			for i, test := range tests {
				if f.Accept(func() *endpoint.HostInfo {
					opaqueStorageValue := new(endpoint.HostInfo)
					implEndpoint.HostInfoData(opaqueStorageValue).InternalRpcAddress = test.addr
					return opaqueStorageValue
				}()) {
					if !test.accept {
						t.Errorf("%d: should not have been accepted but was", i)
					}
				} else if test.accept {
					t.Errorf("%d: should have been accepted but wasn't", i)
				}
			}
		})

		t.Run("EmptyHost", func(t *testing.T) {
			t.Parallel()

			f := WhiteListHostFilter("127.0.0.1")

			host := func() *endpoint.HostInfo {
				opaqueStorageValuexxxxxxxxxx := new(endpoint.HostInfo)
				return opaqueStorageValuexxxxxxxxxx
			}()

			if f.Accept(host) {
				t.Error("empty host should not have been accepted")
			}
		})

		t.Run("MatchesBroadcastAddress", func(t *testing.T) {
			t.Parallel()

			f := WhiteListHostFilter("127.0.0.1")

			host := func() *endpoint.HostInfo {
				opaqueStorageValuexxxx := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuexxxx).InternalConnectAddress = net.ParseIP("10.0.0.1")
				implEndpoint.HostInfoData(opaqueStorageValuexxxx).InternalBroadcastAddress = net.ParseIP("127.0.0.1")
				return opaqueStorageValuexxxx
			}()

			if !f.Accept(host) {
				t.Error("should have been accepted via broadcastAddress but wasn't")
			}
		})

		t.Run("MatchesListenAddress", func(t *testing.T) {
			t.Parallel()

			f := WhiteListHostFilter("127.0.0.1")

			host := func() *endpoint.HostInfo {
				opaqueStorageValuexxxxx := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuexxxxx).InternalConnectAddress = net.ParseIP("10.0.0.1")
				implEndpoint.HostInfoData(opaqueStorageValuexxxxx).InternalListenAddress = net.ParseIP("127.0.0.1")
				return opaqueStorageValuexxxxx
			}()

			if !f.Accept(host) {
				t.Error("should have been accepted via listenAddress but wasn't")
			}
		})

		t.Run("MatchesPeer", func(t *testing.T) {
			t.Parallel()

			f := WhiteListHostFilter("127.0.0.1")

			host := func() *endpoint.HostInfo {
				opaqueStorageValuexxxxxx := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxx).InternalConnectAddress = net.ParseIP("10.0.0.1")
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxx).InternalPeer = net.ParseIP("127.0.0.1")
				return opaqueStorageValuexxxxxx
			}()

			if !f.Accept(host) {
				t.Error("should have been accepted via peer but wasn't")
			}
		})

		t.Run("MatchesPreferredIP", func(t *testing.T) {
			t.Parallel()

			f := WhiteListHostFilter("127.0.0.1")

			host := func() *endpoint.HostInfo {
				opaqueStorageValuexxxxxxx := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxx).InternalConnectAddress = net.ParseIP("10.0.0.1")
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxx).InternalPreferredIP = net.ParseIP("127.0.0.1")
				return opaqueStorageValuexxxxxxx
			}()

			if !f.Accept(host) {
				t.Error("should have been accepted via preferredIP but wasn't")
			}
		})

		t.Run("MatchesRPCAddress", func(t *testing.T) {
			t.Parallel()

			f := WhiteListHostFilter("127.0.0.1")

			host := func() *endpoint.HostInfo {
				opaqueStorageValuexxx := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuexxx).InternalConnectAddress = net.ParseIP("10.0.0.1")
				implEndpoint.HostInfoData(opaqueStorageValuexxx).InternalRpcAddress = net.ParseIP("127.0.0.1")
				return opaqueStorageValuexxx
			}()

			if !f.Accept(host) {
				t.Error("should have been accepted via rpcAddress but wasn't")
			}
		})

		t.Run("MatchesTranslatedAddress", func(t *testing.T) {
			t.Parallel()

			f := WhiteListHostFilter("127.0.0.1")

			host := func() *endpoint.HostInfo {
				opaqueStorageValuexxxxxxxx := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxx).InternalConnectAddress = net.ParseIP("10.0.0.1")
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxx).InternalTranslatedAddresses = &endpoint.InternalTranslatedAddresses{
					CQL: endpoint.AddressPort{Address: net.ParseIP("127.0.0.1"), Port: 9042},
				}
				return opaqueStorageValuexxxxxxxx
			}()

			if !f.Accept(host) {
				t.Error("should have been accepted via translatedAddresses but wasn't")
			}
		})

		t.Run("NoMatch", func(t *testing.T) {
			t.Parallel()

			f := WhiteListHostFilter("127.0.0.1")

			host := func() *endpoint.HostInfo {
				opaqueStorageValuexxxxxxxxx := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxx).InternalConnectAddress = net.ParseIP("10.0.0.1")
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxx).InternalRpcAddress = net.ParseIP("10.0.0.2")
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxx).InternalBroadcastAddress = net.ParseIP("10.0.0.3")
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxx).InternalListenAddress = net.ParseIP("10.0.0.4")
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxx).InternalPeer = net.ParseIP("10.0.0.5")
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxx).InternalPreferredIP = net.ParseIP("10.0.0.6")
				return opaqueStorageValuexxxxxxxxx
			}()

			if f.Accept(host) {
				t.Error("should not have been accepted but was")
			}
		})
	})
}

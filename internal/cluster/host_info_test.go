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
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/tests/mock"
)

func TestHostInfo(t *testing.T) {
	t.Parallel()

	t.Run("AddressPort", func(t *testing.T) {
		t.Parallel()

		t.Run("IsValid", func(t *testing.T) {
			t.Parallel()

			tests := []struct {
				name     string
				addr     endpoint.AddressPort
				expected bool
			}{
				{
					name:     "valid IPv4 address with port",
					addr:     endpoint.AddressPort{Address: net.IPv4(127, 0, 0, 1), Port: 9042},
					expected: true,
				},
				{
					name:     "valid IPv6 address with port",
					addr:     endpoint.AddressPort{Address: net.ParseIP("::1"), Port: 9042},
					expected: true,
				},
				{
					name:     "nil address",
					addr:     endpoint.AddressPort{Address: nil, Port: 9042},
					expected: false,
				},
				{
					name:     "unspecified IPv4 address",
					addr:     endpoint.AddressPort{Address: net.IPv4zero, Port: 9042},
					expected: false,
				},
				{
					name:     "unspecified IPv6 address",
					addr:     endpoint.AddressPort{Address: net.IPv6unspecified, Port: 9042},
					expected: false,
				},
				{
					name:     "zero port",
					addr:     endpoint.AddressPort{Address: net.IPv4(127, 0, 0, 1), Port: 0},
					expected: false,
				},
				{
					name:     "nil address and zero port",
					addr:     endpoint.AddressPort{Address: nil, Port: 0},
					expected: false,
				},
				{
					name:     "empty AddressPort",
					addr:     endpoint.AddressPort{},
					expected: false,
				},
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					result := test.addr.IsValid()
					if result != test.expected {
						t.Errorf("IsValid() = %v, expected %v for %+v", result, test.expected, test.addr)
					}
				})
			}
		})

		t.Run("Equal", func(t *testing.T) {
			t.Parallel()

			addr1 := endpoint.AddressPort{Address: net.IPv4(127, 0, 0, 1), Port: 9042}
			addr2 := endpoint.AddressPort{Address: net.IPv4(127, 0, 0, 1), Port: 9042}
			addr3 := endpoint.AddressPort{Address: net.IPv4(192, 168, 1, 1), Port: 9042}
			addr4 := endpoint.AddressPort{Address: net.IPv4(127, 0, 0, 1), Port: 9043}

			tests := []struct {
				name     string
				a        endpoint.AddressPort
				b        endpoint.AddressPort
				expected bool
			}{
				{
					name:     "equal addresses and ports",
					a:        addr1,
					b:        addr2,
					expected: true,
				},
				{
					name:     "different addresses, same port",
					a:        addr1,
					b:        addr3,
					expected: false,
				},
				{
					name:     "same address, different ports",
					a:        addr1,
					b:        addr4,
					expected: false,
				},
				{
					name:     "IPv6 addresses equal",
					a:        endpoint.AddressPort{Address: net.ParseIP("::1"), Port: 9042},
					b:        endpoint.AddressPort{Address: net.ParseIP("::1"), Port: 9042},
					expected: true,
				},
				{
					name:     "empty",
					a:        endpoint.AddressPort{},
					b:        endpoint.AddressPort{},
					expected: true,
				},
				{
					name:     "empty, non-empty",
					a:        endpoint.AddressPort{},
					b:        endpoint.AddressPort{Address: net.ParseIP("::1"), Port: 9042},
					expected: false,
				},
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					result := test.a.Equal(test.b)
					if result != test.expected {
						t.Errorf("Equal() = %v, expected %v for a=%+v, b=%+v", result, test.expected, test.a, test.b)
					}
				})
			}
		})

		t.Run("String", func(t *testing.T) {
			t.Parallel()

			tests := []struct {
				name     string
				addr     endpoint.AddressPort
				expected string
			}{
				{
					name:     "IPv4 address",
					addr:     endpoint.AddressPort{Address: net.IPv4(127, 0, 0, 1), Port: 9042},
					expected: "127.0.0.1:9042",
				},
				{
					name:     "IPv6 address",
					addr:     endpoint.AddressPort{Address: net.ParseIP("::1"), Port: 9042},
					expected: "::1:9042",
				},
				{
					name:     "different port",
					addr:     endpoint.AddressPort{Address: net.IPv4(192, 168, 1, 1), Port: 8080},
					expected: "192.168.1.1:8080",
				},
				{
					name:     "nil address",
					addr:     endpoint.AddressPort{Address: nil, Port: 9042},
					expected: "<nil>:9042",
				},
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					result := test.addr.String()
					if result != test.expected {
						t.Errorf("String() = %q, expected %q", result, test.expected)
					}
				})
			}
		})

		t.Run("ToNetAddr", func(t *testing.T) {
			t.Parallel()

			tests := []struct {
				name     string
				addr     endpoint.AddressPort
				expected string
			}{
				{
					name:     "IPv4 address",
					addr:     endpoint.AddressPort{Address: net.IPv4(127, 0, 0, 1), Port: 9042},
					expected: "127.0.0.1:9042",
				},
				{
					name:     "IPv6 address",
					addr:     endpoint.AddressPort{Address: net.ParseIP("::1"), Port: 9042},
					expected: "[::1]:9042",
				},
				{
					name:     "IPv6 address with zone",
					addr:     endpoint.AddressPort{Address: net.ParseIP("fe80::1"), Port: 9043},
					expected: "[fe80::1]:9043",
				},
				{
					name:     "different port",
					addr:     endpoint.AddressPort{Address: net.IPv4(192, 168, 1, 1), Port: 8080},
					expected: "192.168.1.1:8080",
				},
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					result := test.addr.ToNetAddr()
					if result != test.expected {
						t.Errorf("ToNetAddr() = %q, expected %q", result, test.expected)
					}
				})
			}
		})
	})

	t.Run("Builder", func(t *testing.T) {
		t.Parallel()

		t.Run("Build", func(t *testing.T) {
			t.Run("basic fields", func(t *testing.T) {
				t.Parallel()

				builder := endpoint.HostInfoBuilder{
					HostId:        "a0000000-0000-0000-0000-000000000123",
					DataCenter:    "dc1",
					Rack:          "rack1",
					Tokens:        []string{"token1", "token2"},
					Port:          9042,
					Workload:      "Analytics",
					DseVersion:    "6.8.0",
					ClusterName:   "test-cluster",
					Partitioner:   "Murmur3Partitioner",
					Hostname:      "node1.example.com",
					SchemaVersion: "schema-v1",
				}

				host := builder.Build()

				if host.HostID() != builder.HostId {
					t.Errorf("HostID() = %q, expected %q", host.HostID(), builder.HostId)
				}
				if host.DataCenter() != builder.DataCenter {
					t.Errorf("DataCenter() = %q, expected %q", host.DataCenter(), builder.DataCenter)
				}
				if host.Rack() != builder.Rack {
					t.Errorf("Rack() = %q, expected %q", host.Rack(), builder.Rack)
				}
				if host.Port() != builder.Port {
					t.Errorf("Port() = %d, expected %d", host.Port(), builder.Port)
				}
				if host.WorkLoad() != builder.Workload {
					t.Errorf("WorkLoad() = %q, expected %q", host.WorkLoad(), builder.Workload)
				}
				if host.DSEVersion() != builder.DseVersion {
					t.Errorf("DSEVersion() = %q, expected %q", host.DSEVersion(), builder.DseVersion)
				}
				if host.ClusterName() != builder.ClusterName {
					t.Errorf("ClusterName() = %q, expected %q", host.ClusterName(), builder.ClusterName)
				}
				if host.Partitioner() != builder.Partitioner {
					t.Errorf("Partitioner() = %q, expected %q", host.Partitioner(), builder.Partitioner)
				}
				if len(host.Tokens()) != len(builder.Tokens) {
					t.Errorf("len(Tokens()) = %d, expected %d", len(host.Tokens()), len(builder.Tokens))
				}
				for i, token := range host.Tokens() {
					if token != builder.Tokens[i] {
						t.Errorf("Tokens()[%d] = %q, expected %q", i, token, builder.Tokens[i])
					}
				}
			})

			t.Run("IP addresses", func(t *testing.T) {
				t.Parallel()

				connectAddr := net.IPv4(192, 168, 1, 1)
				broadcastAddr := net.IPv4(192, 168, 1, 2)
				preferredIP := net.IPv4(192, 168, 1, 3)
				rpcAddr := net.IPv4(192, 168, 1, 4)
				peer := net.IPv4(192, 168, 1, 5)
				listenAddr := net.IPv4(192, 168, 1, 6)

				builder := endpoint.HostInfoBuilder{
					ConnectAddress:   connectAddr,
					BroadcastAddress: broadcastAddr,
					PreferredIP:      preferredIP,
					RpcAddress:       rpcAddr,
					Peer:             peer,
					ListenAddress:    listenAddr,
					Port:             9042,
				}

				host := builder.Build()

				if !host.UntranslatedConnectAddress().Equal(connectAddr) {
					t.Errorf("UntranslatedConnectAddress() = %v, expected %v", host.UntranslatedConnectAddress(), connectAddr)
				}
				if !host.BroadcastAddress().Equal(broadcastAddr) {
					t.Errorf("BroadcastAddress() = %v, expected %v", host.BroadcastAddress(), broadcastAddr)
				}
				if !host.PreferredIP().Equal(preferredIP) {
					t.Errorf("PreferredIP() = %v, expected %v", host.PreferredIP(), preferredIP)
				}
				if !host.RPCAddress().Equal(rpcAddr) {
					t.Errorf("RPCAddress() = %v, expected %v", host.RPCAddress(), rpcAddr)
				}
				if !host.Peer().Equal(peer) {
					t.Errorf("Peer() = %v, expected %v", host.Peer(), peer)
				}
				if !host.ListenAddress().Equal(listenAddr) {
					t.Errorf("ListenAddress() = %v, expected %v", host.ListenAddress(), listenAddr)
				}
			})

			t.Run("translated addresses", func(t *testing.T) {
				t.Parallel()

				translatedAddrs := &endpoint.InternalTranslatedAddresses{
					CQL: endpoint.AddressPort{
						Address: net.IPv4(10, 0, 0, 1),
						Port:    9042,
					},
					ShardAware: endpoint.AddressPort{
						Address: net.IPv4(10, 0, 0, 2),
						Port:    19042,
					},
					ShardAwareTLS: endpoint.AddressPort{
						Address: net.IPv4(10, 0, 0, 3),
						Port:    19043,
					},
				}

				builder := endpoint.HostInfoBuilder{
					TranslatedAddresses: translatedAddrs,
					Port:                9042,
				}

				host := builder.Build()

				// ConnectAddress should use translated CQL address
				expectedAddr := translatedAddrs.CQL.Address
				if !host.ConnectAddress().Equal(expectedAddr) {
					t.Errorf("ConnectAddress() = %v, expected %v", host.ConnectAddress(), expectedAddr)
				}

				// Verify translated addresses are set
				retrievedAddrs := implEndpoint.InternalHostInfoGetTranslatedConnectionInfo(&host)
				if retrievedAddrs == nil {
					t.Fatal("getTranslatedConnectionInfo() returned nil")
				}
				if !retrievedAddrs.Equal(translatedAddrs) {
					t.Errorf("translated addresses not equal: got %+v, expected %+v", retrievedAddrs, translatedAddrs)
				}
			})

			t.Run("version", func(t *testing.T) {
				t.Parallel()

				version := endpoint.InternalCassVersion{
					Major:     4,
					Minor:     0,
					Patch:     3,
					Qualifier: "rc1",
				}

				builder := endpoint.HostInfoBuilder{
					Version: version,
					Port:    9042,
				}

				host := builder.Build()

				if host.Version() != version {
					t.Errorf("Version() = %+v, expected %+v", host.Version(), version)
				}
			})

			t.Run("empty builder", func(t *testing.T) {
				t.Parallel()

				builder := endpoint.HostInfoBuilder{}
				host := builder.Build()

				if host.HostID() != "" {
					t.Errorf("HostID() = %q, expected empty string", host.HostID())
				}
				if host.DataCenter() != "" {
					t.Errorf("DataCenter() = %q, expected empty string", host.DataCenter())
				}
				if host.Port() != 0 {
					t.Errorf("Port() = %d, expected 0", host.Port())
				}
				if host.Tokens() != nil {
					t.Errorf("Tokens() = %v, expected nil", host.Tokens())
				}
			})

			t.Run("all fields populated", func(t *testing.T) {
				t.Parallel()

				translatedAddrs := &endpoint.InternalTranslatedAddresses{
					CQL: endpoint.AddressPort{
						Address: net.IPv4(10, 0, 0, 1),
						Port:    9042,
					},
				}

				version := endpoint.InternalCassVersion{
					Major: 3,
					Minor: 11,
					Patch: 4,
				}

				builder := endpoint.HostInfoBuilder{
					TranslatedAddresses: translatedAddrs,
					Workload:            "Cassandra",
					HostId:              "b0000000-0000-0000-0000-000000000456",
					SchemaVersion:       "schema-v2",
					Hostname:            "cassandra-node.local",
					ClusterName:         "production-cluster",
					Partitioner:         "Murmur3Partitioner",
					Rack:                "rack2",
					DseVersion:          "6.8.1",
					DataCenter:          "dc2",
					ConnectAddress:      net.IPv4(192, 168, 2, 1),
					BroadcastAddress:    net.IPv4(192, 168, 2, 2),
					PreferredIP:         net.IPv4(192, 168, 2, 3),
					RpcAddress:          net.IPv4(192, 168, 2, 4),
					Peer:                net.IPv4(192, 168, 2, 5),
					ListenAddress:       net.IPv4(192, 168, 2, 6),
					Tokens:              []string{"token-a", "token-b", "token-c"},
					Version:             version,
					Port:                9043,
				}

				host := builder.Build()

				// Verify all fields
				if host.WorkLoad() != builder.Workload {
					t.Errorf("WorkLoad() = %q, expected %q", host.WorkLoad(), builder.Workload)
				}
				if host.HostID() != builder.HostId {
					t.Errorf("HostID() = %q, expected %q", host.HostID(), builder.HostId)
				}
				if host.ClusterName() != builder.ClusterName {
					t.Errorf("ClusterName() = %q, expected %q", host.ClusterName(), builder.ClusterName)
				}
				if host.Partitioner() != builder.Partitioner {
					t.Errorf("Partitioner() = %q, expected %q", host.Partitioner(), builder.Partitioner)
				}
				if host.Rack() != builder.Rack {
					t.Errorf("Rack() = %q, expected %q", host.Rack(), builder.Rack)
				}
				if host.DSEVersion() != builder.DseVersion {
					t.Errorf("DSEVersion() = %q, expected %q", host.DSEVersion(), builder.DseVersion)
				}
				if host.DataCenter() != builder.DataCenter {
					t.Errorf("DataCenter() = %q, expected %q", host.DataCenter(), builder.DataCenter)
				}
				if !host.UntranslatedConnectAddress().Equal(builder.ConnectAddress) {
					t.Errorf("UntranslatedConnectAddress() = %v, expected %v", host.UntranslatedConnectAddress(), builder.ConnectAddress)
				}
				if !host.BroadcastAddress().Equal(builder.BroadcastAddress) {
					t.Errorf("BroadcastAddress() = %v, expected %v", host.BroadcastAddress(), builder.BroadcastAddress)
				}
				if !host.PreferredIP().Equal(builder.PreferredIP) {
					t.Errorf("PreferredIP() = %v, expected %v", host.PreferredIP(), builder.PreferredIP)
				}
				if !host.RPCAddress().Equal(builder.RpcAddress) {
					t.Errorf("RPCAddress() = %v, expected %v", host.RPCAddress(), builder.RpcAddress)
				}
				if !host.Peer().Equal(builder.Peer) {
					t.Errorf("Peer() = %v, expected %v", host.Peer(), builder.Peer)
				}
				if !host.ListenAddress().Equal(builder.ListenAddress) {
					t.Errorf("ListenAddress() = %v, expected %v", host.ListenAddress(), builder.ListenAddress)
				}
				if host.Version() != builder.Version {
					t.Errorf("Version() = %+v, expected %+v", host.Version(), builder.Version)
				}
				if host.Port() != builder.Port {
					t.Errorf("Port() = %d, expected %d", host.Port(), builder.Port)
				}
				if len(host.Tokens()) != len(builder.Tokens) {
					t.Errorf("len(Tokens()) = %d, expected %d", len(host.Tokens()), len(builder.Tokens))
				}

				// Verify ConnectAddress uses translated address
				if !host.ConnectAddress().Equal(translatedAddrs.CQL.Address) {
					t.Errorf("ConnectAddress() = %v, expected %v", host.ConnectAddress(), translatedAddrs.CQL.Address)
				}
			})

			t.Run("nil IP addresses", func(t *testing.T) {
				t.Parallel()

				builder := endpoint.HostInfoBuilder{
					ConnectAddress:   nil,
					BroadcastAddress: nil,
					PreferredIP:      nil,
					RpcAddress:       nil,
					Peer:             nil,
					ListenAddress:    nil,
					Port:             9042,
				}

				host := builder.Build()

				if host.UntranslatedConnectAddress() != nil {
					t.Errorf("UntranslatedConnectAddress() = %v, expected nil", host.UntranslatedConnectAddress())
				}
				if host.BroadcastAddress() != nil {
					t.Errorf("BroadcastAddress() = %v, expected nil", host.BroadcastAddress())
				}
				if host.PreferredIP() != nil {
					t.Errorf("PreferredIP() = %v, expected nil", host.PreferredIP())
				}
				if host.RPCAddress() != nil {
					t.Errorf("RPCAddress() = %v, expected nil", host.RPCAddress())
				}
				if host.Peer() != nil {
					t.Errorf("Peer() = %v, expected nil", host.Peer())
				}
				if host.ListenAddress() != nil {
					t.Errorf("ListenAddress() = %v, expected nil", host.ListenAddress())
				}
			})

			t.Run("connect address priority without translated addresses", func(t *testing.T) {
				t.Parallel()

				// Test that ConnectAddress follows the priority order when translated addresses are not set
				tests := []struct {
					name         string
					builder      endpoint.HostInfoBuilder
					expectedAddr net.IP
					shouldPanic  bool
				}{
					{
						name: "connectAddress takes priority",
						builder: endpoint.HostInfoBuilder{
							ConnectAddress:   net.IPv4(1, 1, 1, 1),
							RpcAddress:       net.IPv4(2, 2, 2, 2),
							PreferredIP:      net.IPv4(3, 3, 3, 3),
							BroadcastAddress: net.IPv4(4, 4, 4, 4),
							Peer:             net.IPv4(5, 5, 5, 5),
							Port:             9042,
						},
						expectedAddr: net.IPv4(1, 1, 1, 1),
					},
					{
						name: "rpcAddress when connectAddress is nil",
						builder: endpoint.HostInfoBuilder{
							RpcAddress:       net.IPv4(2, 2, 2, 2),
							PreferredIP:      net.IPv4(3, 3, 3, 3),
							BroadcastAddress: net.IPv4(4, 4, 4, 4),
							Peer:             net.IPv4(5, 5, 5, 5),
							Port:             9042,
						},
						expectedAddr: net.IPv4(2, 2, 2, 2),
					},
					{
						name: "preferredIP when connectAddress and rpcAddress are nil",
						builder: endpoint.HostInfoBuilder{
							PreferredIP:      net.IPv4(3, 3, 3, 3),
							BroadcastAddress: net.IPv4(4, 4, 4, 4),
							Peer:             net.IPv4(5, 5, 5, 5),
							Port:             9042,
						},
						expectedAddr: net.IPv4(3, 3, 3, 3),
					},
					{
						name: "broadcastAddress when others are nil",
						builder: endpoint.HostInfoBuilder{
							BroadcastAddress: net.IPv4(4, 4, 4, 4),
							Peer:             net.IPv4(5, 5, 5, 5),
							Port:             9042,
						},
						expectedAddr: net.IPv4(4, 4, 4, 4),
					},
					{
						name: "peer when all others are nil",
						builder: endpoint.HostInfoBuilder{
							Peer: net.IPv4(5, 5, 5, 5),
							Port: 9042,
						},
						expectedAddr: net.IPv4(5, 5, 5, 5),
					},
					{
						name: "no valid addresses panics",
						builder: endpoint.HostInfoBuilder{
							Port: 9042,
						},
						shouldPanic: true,
					},
				}

				for _, test := range tests {
					t.Run(test.name, func(t *testing.T) {
						host := test.builder.Build()

						if test.shouldPanic {
							defer func() {
								if r := recover(); r == nil {
									t.Error("ConnectAddress() should have panicked but did not")
								}
							}()
						}

						addr := host.ConnectAddress()
						if !test.shouldPanic && !addr.Equal(test.expectedAddr) {
							t.Errorf("ConnectAddress() = %v, expected %v", addr, test.expectedAddr)
						}
					})
				}
			})
		})
	})

	t.Run("CassVersion", func(t *testing.T) {
		t.Parallel()

		t.Run("Before", func(t *testing.T) {
			t.Parallel()

			tests := [...]struct {
				version             endpoint.InternalCassVersion
				major, minor, patch int
				Qualifier           string
			}{
				{endpoint.InternalCassVersion{Major: 1, Minor: 0, Patch: 0, Qualifier: ""}, 0, 0, 0, ""},
				{endpoint.InternalCassVersion{Major: 0, Minor: 1, Patch: 0, Qualifier: ""}, 0, 0, 0, ""},
				{endpoint.InternalCassVersion{Major: 0, Minor: 0, Patch: 1, Qualifier: ""}, 0, 0, 0, ""},

				{endpoint.InternalCassVersion{Major: 1, Minor: 0, Patch: 0, Qualifier: ""}, 0, 1, 0, ""},
				{endpoint.InternalCassVersion{Major: 0, Minor: 1, Patch: 0, Qualifier: ""}, 0, 0, 1, ""},
				{endpoint.InternalCassVersion{Major: 4, Minor: 1, Patch: 0, Qualifier: ""}, 3, 1, 2, ""},

				{endpoint.InternalCassVersion{Major: 4, Minor: 1, Patch: 0, Qualifier: ""}, 3, 1, 2, ""},
			}

			for i, test := range tests {
				if test.version.Before(test.major, test.minor, test.patch) {
					t.Errorf("%d: expected v%d.%d.%d to be before %v", i, test.major, test.minor, test.patch, test.version)
				}
			}

		})

		t.Run("Unmarshal", func(t *testing.T) {
			t.Parallel()

			tests := [...]struct {
				data    string
				version endpoint.InternalCassVersion
			}{
				{"3.2", endpoint.InternalCassVersion{Major: 3, Minor: 2, Patch: 0, Qualifier: ""}},
				{"2.10.1-SNAPSHOT", endpoint.InternalCassVersion{Major: 2, Minor: 10, Patch: 1, Qualifier: ""}},
				{"1.2.3", endpoint.InternalCassVersion{Major: 1, Minor: 2, Patch: 3, Qualifier: ""}},
				{"4.0-rc2", endpoint.InternalCassVersion{Major: 4, Minor: 0, Patch: 0, Qualifier: "rc2"}},
				{"4.3.2-rc1", endpoint.InternalCassVersion{Major: 4, Minor: 3, Patch: 2, Qualifier: "rc1"}},
				{"4.3.2-rc1-qualifier1", endpoint.InternalCassVersion{Major: 4, Minor: 3, Patch: 2, Qualifier: "rc1-qualifier1"}},
				{"4.3-rc1-qualifier1", endpoint.InternalCassVersion{Major: 4, Minor: 3, Patch: 0, Qualifier: "rc1-qualifier1"}},
			}

			for i, test := range tests {
				v := &endpoint.InternalCassVersion{}
				if err := v.Set(test.data); err != nil {
					t.Errorf("%d: %v", i, err)
				} else if *v != test.version {
					t.Errorf("%d: expected %#+v got %#+v", i, test.version, *v)
				}
			}
		})
	})

	t.Run("ConnectAddress", func(t *testing.T) {
		t.Parallel()

		var localhost = net.IPv4(127, 0, 0, 1)
		tests := []struct {
			name          string
			connectAddr   net.IP
			rpcAddr       net.IP
			broadcastAddr net.IP
			peer          net.IP
		}{
			{name: "rpc_address", rpcAddr: localhost},
			{name: "connect_address", connectAddr: localhost},
			{name: "broadcast_address", broadcastAddr: localhost},
			{name: "peer", peer: localhost},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				host := func() *endpoint.HostInfo {
					opaqueStorageValuexx := new(endpoint.HostInfo)
					implEndpoint.HostInfoData(opaqueStorageValuexx).InternalConnectAddress = test.connectAddr
					implEndpoint.HostInfoData(opaqueStorageValuexx).InternalRpcAddress = test.rpcAddr
					implEndpoint.HostInfoData(opaqueStorageValuexx).InternalBroadcastAddress = test.broadcastAddr
					implEndpoint.HostInfoData(opaqueStorageValuexx).InternalPeer = test.peer
					return opaqueStorageValuexx
				}()

				if addr := host.ConnectAddress(); !addr.Equal(localhost) {
					t.Fatalf("expected ConnectAddress to be %s got %s", localhost, addr)
				}
			})
		}
	})

	t.Run("FromIter", func(t *testing.T) {
		t.Parallel()

		// Column order matches systemLocalResultMetadata:
		// broadcast_address, cluster_name, data_center, host_id, listen_address, partitioner, rack, release_version, rpc_address, schema_version, tokens
		row := []any{
			net.IPv4(192, 168, 100, 12),
			"cluster",
			"datacenter1",
			cqltype.ParseUUIDMust("045859a7-6b9f-4efd-a5e7-acd64a295e13"),
			net.IPv4(192, 168, 100, 12),
			"org.apache.cassandra.dht.Murmur3Partitioner",
			"rack1",
			"3.0.8",
			net.IPv4(192, 168, 100, 12),
			cqltype.ParseUUIDMust("daf4df2c-b708-11ef-5c25-3004361afd71"),
			[]string{"1"},
		}
		framer := &trackingMockFramer{
			MockFramer: mock.MockFramer{Data: marshalMetadataMust(systemLocalResultMetadata, row)},
		}

		host, err := hostInfoFromFixture(&nativeResultFixture{
			meta:    systemLocalResultMetadata,
			framer:  framer,
			numRows: 1,
		}, 9042)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if host == nil {
			t.Fatal("expected host info")
		}
		if !framer.released {
			t.Fatal("expected iterator framer to be released")
		}
	})

	t.Run("FromMap", func(t *testing.T) {
		t.Parallel()

		t.Run("Columns", func(t *testing.T) {
			t.Parallel()

			hostID := testLocalHostID
			schema := cqltype.MustRandomUUID()
			row := map[string]any{
				"data_center":       "dc1",
				"rack":              "r1",
				"host_id":           hostID,
				"release_version":   "3.11.4",
				"peer":              "10.0.0.5",
				"cluster_name":      "test cluster",
				"partitioner":       "org.apache.cassandra.dht.Murmur3Partitioner",
				"broadcast_address": "10.0.0.6",
				"preferred_ip":      "10.0.0.7",
				"rpc_address":       "10.0.0.8",
				"listen_address":    "10.0.0.9",
				"tokens":            []string{"1", "2"},
				"schema_version":    schema,
				"unknown_column":    42,
			}

			h, err := InternalHostInfoFromMap(row, 9042)
			if err != nil {
				t.Fatal(err)
			}
			checks := []struct {
				name      string
				got, want any
			}{
				{"DataCenter", h.DataCenter(), "dc1"},
				{"Rack", h.Rack(), "r1"},
				{"HostID", h.HostID(), hostID.String()},
				{"Version", h.Version().String(), "v3.11.4"},
				{"Peer", h.Peer().String(), "10.0.0.5"},
				{"ClusterName", h.ClusterName(), "test cluster"},
				{"Partitioner", h.Partitioner(), "org.apache.cassandra.dht.Murmur3Partitioner"},
				{"BroadcastAddress", h.BroadcastAddress().String(), "10.0.0.6"},
				{"PreferredIP", h.PreferredIP().String(), "10.0.0.7"},
				{"RPCAddress", h.RPCAddress().String(), "10.0.0.8"},
				{"ListenAddress", h.ListenAddress().String(), "10.0.0.9"},
				{"Tokens", strings.Join(h.Tokens(), ","), "1,2"},
				{"SchemaVersion", implEndpoint.HostInfoData(h).InternalSchemaVersion, schema.String()},
				// no native_port: the caller's default applies
				{"Port", h.Port(), 9042},
				// rpc_address is the driver-facing address when it is set
				{"ConnectAddress", h.ConnectAddress().String(), "10.0.0.8"},
			}
			for _, c := range checks {
				if c.got != c.want {
					t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
				}
			}
		})

		// system.peers_v2 carries the address and port as native_address and
		// native_port.
		t.Run("PeersV2", func(t *testing.T) {
			t.Parallel()

			h, err := InternalHostInfoFromMap(map[string]any{
				"native_address": "10.1.0.1",
				"native_port":    19042,
			}, 9042)
			if err != nil {
				t.Fatal(err)
			}
			if h.RPCAddress().String() != "10.1.0.1" || h.Port() != 19042 || h.ConnectAddress().String() != "10.1.0.1" {
				t.Fatalf("host = %s:%d (rpc %s), want 10.1.0.1:19042", h.ConnectAddress(), h.Port(), h.RPCAddress())
			}
		})

		// A column of an unexpected Go type is an error naming the column, not a
		// panic: the type comes from the server's schema for a system table.
		t.Run("WrongType", func(t *testing.T) {
			t.Parallel()

			for _, col := range []string{
				"data_center", "rack", "host_id", "release_version", "peer", "cluster_name", "partitioner",
				"broadcast_address", "preferred_ip", "rpc_address", "native_address", "listen_address",
				"native_port", "tokens", "schema_version",
			} {
				_, err := InternalHostInfoFromMap(map[string]any{col: struct{}{}}, 9042)
				if err == nil || err.Error() != "Assertion failed for "+col {
					t.Errorf("%s: err = %v, want an assertion error for the column", col, err)
				}
			}
		})
	})

	// update only fills in what the receiver does not know yet; it never
	// overwrites a value it already has.
	t.Run("Update", func(t *testing.T) {
		t.Parallel()

		from := func() *endpoint.HostInfo {
			opaqueStorageValue := new(endpoint.HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValue).InternalDataCenter = "dc-new"
			implEndpoint.HostInfoData(opaqueStorageValue).InternalRack = "r-new"
			implEndpoint.HostInfoData(opaqueStorageValue).InternalHostId = cqltype.MustRandomUUID()
			implEndpoint.HostInfoData(opaqueStorageValue).InternalPort = 19042
			implEndpoint.HostInfoData(opaqueStorageValue).InternalRpcAddress = net.ParseIP("10.0.0.2")
			implEndpoint.HostInfoData(opaqueStorageValue).InternalTokens = []string{"9"}
			implEndpoint.HostInfoData(opaqueStorageValue).InternalPartitioner = "p-new"
			implEndpoint.HostInfoData(opaqueStorageValue).InternalClusterName = "c-new"
			implEndpoint.HostInfoData(opaqueStorageValue).InternalWorkload = "w-new"
			implEndpoint.HostInfoData(opaqueStorageValue).InternalDseVersion = "d-new"
			return opaqueStorageValue
		}()
		implEndpoint.HostInfoData(from).InternalVersion.Set("6.0.0")

		empty := func() *endpoint.HostInfo {
			opaqueStorageValuex := new(endpoint.HostInfo)
			return opaqueStorageValuex
		}()

		implEndpoint.InternalHostInfoUpdate(empty, from)
		if empty.DataCenter() != "dc-new" || empty.Rack() != "r-new" || empty.HostID() != from.HostID() ||
			empty.Port() != 19042 || !empty.RPCAddress().Equal(implEndpoint.HostInfoData(from).InternalRpcAddress) || empty.Version().String() != "v6.0.0" ||
			strings.Join(empty.Tokens(), ",") != "9" || empty.Partitioner() != "p-new" || empty.ClusterName() != "c-new" {
			t.Fatalf("update into an empty host = %s, want every field copied", empty)
		}

		known := func() *endpoint.HostInfo {
			opaqueStorageValuexx := new(endpoint.HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValuexx).InternalDataCenter = "dc-old"
			implEndpoint.HostInfoData(opaqueStorageValuexx).InternalRack = "r-old"
			implEndpoint.HostInfoData(opaqueStorageValuexx).InternalHostId = cqltype.MustRandomUUID()
			implEndpoint.HostInfoData(opaqueStorageValuexx).InternalPort = 9042
			implEndpoint.HostInfoData(opaqueStorageValuexx).InternalRpcAddress = net.ParseIP("10.0.0.1")
			implEndpoint.HostInfoData(opaqueStorageValuexx).InternalTokens = []string{"1"}
			implEndpoint.HostInfoData(opaqueStorageValuexx).InternalPartitioner = "p-old"
			return opaqueStorageValuexx
		}()
		implEndpoint.HostInfoData(known).InternalVersion.Set("5.0.0")
		knownID := known.HostID()
		implEndpoint.InternalHostInfoUpdate(known, from)
		if known.DataCenter() != "dc-old" || known.Rack() != "r-old" || known.HostID() != knownID ||
			known.Port() != 9042 || known.RPCAddress().String() != "10.0.0.1" || known.Version().String() != "v5.0.0" ||
			strings.Join(known.Tokens(), ",") != "1" || known.Partitioner() != "p-old" {
			t.Fatalf("update over known values = %s, want them kept", known)
		}
		implEndpoint.

			// updating from itself must not deadlock on its own lock
			InternalHostInfoUpdate(known, known)
	})
}

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
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/tests"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wirecompress"
)

func TestClusterSettings(t *testing.T) {
	t.Run("New", func(t *testing.T) {
		t.Parallel()

		t.Run("Defaults", func(t *testing.T) {
			t.Parallel()

			cfg := InternalNewClusterSettings()
			tests.AssertEqual(t, "cluster config cql version", "3.0.0", cfg.CQLVersion)
			tests.AssertEqual(t, "cluster config timeout", 11*time.Second, cfg.Timeout)
			tests.AssertEqual(t, "cluster config port", 0, cfg.Port)
			tests.AssertEqual(t, "cluster config num-conns", 2, cfg.NumConns)
			tests.AssertEqual(t, "cluster config consistency", cqltype.Quorum, cfg.Consistency)
			tests.AssertEqual(t, "cluster config max prepared statements", InternalDefaultMaxPreparedStmts, cfg.MaxPreparedStmts)
			tests.AssertEqual(t, "cluster config max routing key info", 1000, cfg.MaxRoutingKeyInfo)
			tests.AssertEqual(t, "cluster config page-size", 5000, cfg.PageSize)
			tests.AssertEqual(t, "cluster config default timestamp", true, cfg.DefaultTimestamp)
			tests.AssertEqual(t, "cluster config max wait schema agreement", 60*time.Second, cfg.MaxWaitSchemaAgreement)
			tests.AssertEqual(t, "cluster config reconnect interval", 60*time.Second, cfg.ReconnectInterval)
			tests.AssertTrue(t, "cluster config conviction policy",
				reflect.DeepEqual(&settings.SimpleConvictionPolicy{}, cfg.ConvictionPolicy))
			tests.AssertTrue(t, "cluster config reconnection policy",
				reflect.DeepEqual(&pool.ConstantReconnectionPolicy{MaxRetries: 3, Interval: 1 * time.Second}, cfg.ReconnectionPolicy))
		})

		t.Run("WithHosts", func(t *testing.T) {
			t.Parallel()

			cfg := InternalNewClusterSettings("addr1", "addr2")
			tests.AssertEqual(t, "cluster config hosts length", 2, len(cfg.Hosts))
			tests.AssertEqual(t, "cluster config host 0", "addr1", cfg.Hosts[0])
			tests.AssertEqual(t, "cluster config host 1", "addr2", cfg.Hosts[1])
		})
	})

	t.Run("TranslateAddressPort", func(t *testing.T) {
		t.Parallel()

		t.Run("EmptyAddr", func(t *testing.T) {
			t.Parallel()

			translator := staticAddressTranslator(net.ParseIP("10.10.10.10"), 5432)
			hh := endpoint.HostInfoBuilder{
				ConnectAddress: []byte{},
				Port:           0,
			}.Build()
			newAddr, err := endpoint.InternalTranslateAddressPort(translator, &hh, endpoint.AddressPort{
				Address: hh.UntranslatedConnectAddress(),
				Port:    uint16(hh.Port()),
			}, nil)
			tests.AssertNil(t, "should return no error", err)
			tests.AssertTrue(t, "translated address is still empty", len(newAddr.Address) == 0)
			tests.AssertEqual(t, "translated port", uint16(0), newAddr.Port)
		})

		t.Run("NilTranslator", func(t *testing.T) {
			t.Parallel()
			hh := endpoint.HostInfoBuilder{
				ConnectAddress: net.ParseIP("10.0.0.1"),
				Port:           1234,
			}.Build()
			newAddr, err := endpoint.InternalTranslateAddressPort(nil, &hh, endpoint.AddressPort{
				Address: hh.UntranslatedConnectAddress(),
				Port:    uint16(hh.Port()),
			}, nil)
			tests.AssertNil(t, "should return no error", err)
			tests.AssertTrue(t, "same address as provided", net.ParseIP("10.0.0.1").Equal(newAddr.Address))
			tests.AssertEqual(t, "translated host and port", uint16(1234), newAddr.Port)
		})

		t.Run("Success", func(t *testing.T) {
			t.Parallel()

			translator := staticAddressTranslator(net.ParseIP("10.10.10.10"), 5432)
			hh := endpoint.HostInfoBuilder{
				ConnectAddress: net.ParseIP("10.0.0.1"),
				Port:           2345,
			}.Build()
			newAddr, err := endpoint.InternalTranslateAddressPort(translator, &hh, endpoint.AddressPort{
				Address: hh.UntranslatedConnectAddress(),
				Port:    uint16(hh.Port()),
			}, nil)
			tests.AssertNil(t, "should return no error", err)
			tests.AssertTrue(t, "translated address", net.ParseIP("10.10.10.10").Equal(newAddr.Address))
			tests.AssertEqual(t, "translated port", uint16(5432), newAddr.Port)
		})
	})

	t.Run("Validate", func(t *testing.T) {
		t.Run("AcceptsBoundaryValues", func(t *testing.T) {
			t.Parallel()

			for _, serial := range []cqltype.Consistency{cqltype.Serial, cqltype.LocalSerial} {
				cfg := InternalNewClusterSettings("10.0.0.1")
				cfg.Logger = logging.InternalNopLogger{}
				cfg.SerialConsistency = serial
				cfg.Port = 65535
				if err := cfg.Validate(); err != nil {
					t.Fatalf("SerialConsistency=%v Port=65535: %v", serial, err)
				}
			}
		})

		// With no port anywhere, Validate fills in the CQL default; with client routes
		// it does so without looking at the hosts, whose ports are entry points only.
		t.Run("DefaultsPort", func(t *testing.T) {
			t.Parallel()

			cfg := InternalNewClusterSettings("10.0.0.1")
			cfg.Logger = logging.InternalNopLogger{}
			if err := cfg.Validate(); err != nil {
				t.Fatal(err)
			}
			if cfg.Port != InternalDefaultCQLPort {
				t.Fatalf("Port = %d, want %d", cfg.Port, InternalDefaultCQLPort)
			}

			cfg = InternalNewClusterSettings("10.0.0.1:19042", "10.0.0.2")
			cfg.Logger = logging.InternalNopLogger{}
			cfg.ClientRoutesConfig = &ClientRoutesConfig{}
			err := cfg.Validate()
			// The empty routes config is itself invalid; what matters is that mixed host
			// ports did not fail first and the port was defaulted.
			if err == nil || strings.Contains(err.Error(), "all include a port") {
				t.Fatalf("Validate() = %v, want the client routes error, not a host port error", err)
			}
			if cfg.Port != InternalDefaultCQLPort {
				t.Fatalf("Port = %d, want %d with client routes", cfg.Port, InternalDefaultCQLPort)
			}
		})

		// TestClusterSettings/Validate/HostPortNormalization covers the port-learning logic in Validate():
		// when cfg.Port is 0 (not explicitly set) the driver learns it from cfg.Hosts.
		t.Run("HostPortNormalization", func(t *testing.T) {
			t.Parallel()

			makeValid := func(hosts ...string) *InternalClusterSettings {
				cfg := InternalNewClusterSettings(hosts...)
				return cfg
			}

			t.Run("single host with explicit port sets cfg.Port", func(t *testing.T) {
				t.Parallel()
				cfg := makeValid("10.0.0.1:9142")
				if err := cfg.Validate(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				tests.AssertEqual(t, "cfg.Port after Validate", 9142, cfg.Port)
			})

			t.Run("multiple hosts with the same explicit port", func(t *testing.T) {
				t.Parallel()
				cfg := makeValid("10.0.0.1:9142", "10.0.0.2:9142")
				if err := cfg.Validate(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				tests.AssertEqual(t, "cfg.Port after Validate", 9142, cfg.Port)
			})

			t.Run("hosts without explicit port default to 9042", func(t *testing.T) {
				t.Parallel()
				cfg := makeValid("10.0.0.1", "10.0.0.2")
				if err := cfg.Validate(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				tests.AssertEqual(t, "cfg.Port after Validate", 9042, cfg.Port)
			})

			t.Run("mixed: some hosts with port, some without returns error", func(t *testing.T) {
				t.Parallel()
				cfg := makeValid("10.0.0.1:9142", "10.0.0.2")
				if err := cfg.Validate(); err == nil {
					t.Fatal("expected an error for mixed port/no-port hosts, got nil")
				}
			})

			t.Run("hosts with conflicting explicit ports returns error", func(t *testing.T) {
				t.Parallel()
				cfg := makeValid("10.0.0.1:9142", "10.0.0.2:9043")
				if err := cfg.Validate(); err == nil {
					t.Fatal("expected an error for conflicting host ports, got nil")
				}
			})

			t.Run("host with invalid port string returns error", func(t *testing.T) {
				t.Parallel()
				cfg := makeValid("10.0.0.1:notaport")
				if err := cfg.Validate(); err == nil {
					t.Fatal("expected an error for invalid port, got nil")
				}
			})

			t.Run("IPv6 bare address without port defaults to 9042", func(t *testing.T) {
				t.Parallel()
				// A bare IPv6 address (no brackets, no port) is not parseable by
				// net.SplitHostPort, so it is treated as a portless entry and the
				// driver falls back to the default port 9042.
				cfg := makeValid("::1")
				if err := cfg.Validate(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				tests.AssertEqual(t, "cfg.Port after Validate", 9042, cfg.Port)
			})

			t.Run("IPv6 bracketed address with port sets cfg.Port", func(t *testing.T) {
				t.Parallel()
				cfg := makeValid("[::1]:9142")
				if err := cfg.Validate(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				tests.AssertEqual(t, "cfg.Port after Validate", 9142, cfg.Port)
			})

			t.Run("multiple IPv6 hosts with the same explicit port", func(t *testing.T) {
				t.Parallel()
				cfg := makeValid("[::1]:9142", "[::2]:9142")
				if err := cfg.Validate(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				tests.AssertEqual(t, "cfg.Port after Validate", 9142, cfg.Port)
			})

			t.Run("mixed IPv4 and IPv6 hosts with the same port", func(t *testing.T) {
				t.Parallel()
				cfg := makeValid("10.0.0.1:9142", "[::1]:9142")
				if err := cfg.Validate(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				tests.AssertEqual(t, "cfg.Port after Validate", 9142, cfg.Port)
			})

			t.Run("mixed IPv4 and IPv6 hosts with conflicting ports returns error", func(t *testing.T) {
				t.Parallel()
				cfg := makeValid("10.0.0.1:9142", "[::1]:9043")
				if err := cfg.Validate(); err == nil {
					t.Fatal("expected an error for conflicting IPv4/IPv6 host ports, got nil")
				}
			})

			t.Run("user-set Port skips learning from hosts", func(t *testing.T) {
				t.Parallel()
				// User explicitly sets Port=9042 (same as default) while hosts say 9142.
				// The driver must NOT override the explicit Port.
				cfg := makeValid("10.0.0.1:9142")
				cfg.Port = 9042
				if err := cfg.Validate(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				tests.AssertEqual(t, "cfg.Port after Validate", 9042, cfg.Port)
			})

			t.Run("client routes enabled skips learning from hosts", func(t *testing.T) {
				t.Parallel()
				cfg := makeValid("10.0.0.1:9142")
				cfg.ClientRoutesConfig = &ClientRoutesConfig{
					TableName: "system.client_routes",
					Endpoints: []ClientRoutesEndpoint{{ConnectionID: "conn-1", ConnectionAddr: "10.0.0.1:9142"}},
				}
				if err := cfg.Validate(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				tests.AssertEqual(t, "cfg.Port after Validate", 9042, cfg.Port)
			})
		})

		// TestClusterSettings/Validate/ProtoV5CompressorCapability covers the capability gate in
		// Validate(): on ProtoVersion >= 5 a compressor must implement SegmentCompressor
		// (the real condition), not merely be a non-Snappy type. Below v5 the gate is
		// off.
		t.Run("ProtoV5CompressorCapability", func(t *testing.T) {
			t.Parallel()

			newCfg := func(proto int, comp protocol.Compressor) *InternalClusterSettings {
				cfg := InternalNewClusterSettings("10.0.0.1:9042")
				cfg.ProtoVersion = proto
				cfg.Compressor = comp
				return cfg
			}

			t.Run("v5 with Snappy is rejected", func(t *testing.T) {
				t.Parallel()
				err := newCfg(5, wirecompress.SnappyCompressor{}).Validate()
				if err == nil {
					t.Fatal("expected error for Snappy on protocol v5, got nil")
				}
				if !strings.Contains(err.Error(), "does not support protocol v5 segment framing") {
					t.Fatalf("unexpected error: %v", err)
				}
			})

			t.Run("v5 with a segment-capable compressor is allowed", func(t *testing.T) {
				t.Parallel()
				if err := newCfg(5, segmentCapableCompressor{}).Validate(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			})

			t.Run("v5 with no compressor is allowed", func(t *testing.T) {
				t.Parallel()
				if err := newCfg(5, nil).Validate(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			})

			t.Run("v4 with Snappy is allowed (gate off below v5)", func(t *testing.T) {
				t.Parallel()
				if err := newCfg(4, wirecompress.SnappyCompressor{}).Validate(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			})
		})

		t.Run("ProtoVersionRange", func(t *testing.T) {
			for _, version := range []int{-1, 128, 256} {
				t.Run(fmt.Sprint(version), func(t *testing.T) {
					cfg := InternalNewClusterSettings("127.0.0.1")
					cfg.ProtoVersion = version
					if err := cfg.Validate(); err == nil {
						t.Fatalf("expected protocol version %d to be rejected", version)
					}
				})
			}
		})

		// TestClusterSettings/Validate/ReadTimeout covers the ReadTimeout bound check in Validate():
		// a negative duration is rejected; zero (disabled) and positive are allowed.
		t.Run("ReadTimeout", func(t *testing.T) {
			t.Parallel()

			newCfg := func(d time.Duration) *InternalClusterSettings {
				cfg := InternalNewClusterSettings("10.0.0.1:9042")
				cfg.ReadTimeout = d
				return cfg
			}

			t.Run("negative is rejected", func(t *testing.T) {
				t.Parallel()
				err := newCfg(-time.Second).Validate()
				if err == nil || !strings.Contains(err.Error(), "invalid ReadTimeout") {
					t.Fatalf("expected ReadTimeout validation error, got: %v", err)
				}
			})

			t.Run("zero is allowed", func(t *testing.T) {
				t.Parallel()
				if err := newCfg(0).Validate(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			})

			t.Run("positive is allowed", func(t *testing.T) {
				t.Parallel()
				if err := newCfg(time.Second).Validate(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			})
		})

		// TestClusterSettings/Validate/Rejects walks every check in clusterSettings.Validate that turns a
		// misconfiguration into an error, so a user sees it at open rather than
		// as a confusing failure later.
		t.Run("Rejects", func(t *testing.T) {
			t.Parallel()

			tests := []struct {
				name    string
				mutate  func(cfg *InternalClusterSettings)
				wantErr string
			}{
				{"NoHosts", func(c *InternalClusterSettings) { c.Hosts = nil }, ErrNoHosts.Error()},
				{"AuthenticatorAndAuthProvider", func(c *InternalClusterSettings) {
					c.Authenticator = transport.PasswordAuthenticator{}
					c.AuthProvider = func(*endpoint.HostInfo) (transport.Authenticator, error) { return nil, nil }
				}, "invalid Authenticator: cannot be combined with AuthProvider"},
				{"NilInitialReconnectionPolicy", func(c *InternalClusterSettings) { c.InitialReconnectionPolicy = nil }, "invalid InitialReconnectionPolicy: is nil"},
				{"InitialReconnectionPolicyNoRetries", func(c *InternalClusterSettings) {
					c.InitialReconnectionPolicy = &pool.ConstantReconnectionPolicy{MaxRetries: 0}
				}, "invalid InitialReconnectionPolicy: GetMaxRetries"},
				{"NilReconnectionPolicy", func(c *InternalClusterSettings) { c.ReconnectionPolicy = nil }, "invalid ReconnectionPolicy: is nil"},
				// hostConnPool.InternalConnect makes GetMaxRetries attempts; with none every pool
				// fill fails.
				{"ReconnectionPolicyNoRetries", func(c *InternalClusterSettings) {
					c.ReconnectionPolicy = &pool.ConstantReconnectionPolicy{MaxRetries: 0}
				}, "invalid ReconnectionPolicy: GetMaxRetries"},
				{"NegativePageSize", func(c *InternalClusterSettings) { c.PageSize = -1 }, "PageSize"},
				{"NegativeMaxRoutingKeyInfo", func(c *InternalClusterSettings) { c.MaxRoutingKeyInfo = -1 }, "MaxRoutingKeyInfo"},
				{"NegativeMaxPreparedStmts", func(c *InternalClusterSettings) { c.MaxPreparedStmts = -1 }, "MaxPreparedStmts"},
				{"NegativeSocketKeepalive", func(c *InternalClusterSettings) { c.SocketKeepalive = -time.Second }, "SocketKeepalive"},
				{"NegativeMaxRequestsPerConn", func(c *InternalClusterSettings) { c.MaxRequestsPerConn = -1 }, "MaxRequestsPerConn"},
				{"NegativeNumConns", func(c *InternalClusterSettings) { c.NumConns = -1 }, "NumConns"},
				{"PortTooLarge", func(c *InternalClusterSettings) { c.Port = 65536 }, "invalid Port"},
				{"NegativePort", func(c *InternalClusterSettings) { c.Port = -1 }, "invalid Port"},
				{"MixedHostPorts", func(c *InternalClusterSettings) { c.Hosts = []string{"10.0.0.1:9042", "10.0.0.2"} }, "all include a port or none"},
				{"NegativeWriteTimeout", func(c *InternalClusterSettings) { c.WriteTimeout = -time.Second }, "WriteTimeout"},
				{"NegativeTimeout", func(c *InternalClusterSettings) { c.Timeout = -time.Second }, "invalid Timeout:"},
				{"NegativeConnectTimeout", func(c *InternalClusterSettings) { c.ConnectTimeout = -time.Second }, "ConnectTimeout"},
				{"NegativeMetadataSchemaRequestTimeout", func(c *InternalClusterSettings) { c.MetadataSchemaRequestTimeout = -time.Second }, "MetadataSchemaRequestTimeout"},
				{"NegativeWriteCoalesceWaitTime", func(c *InternalClusterSettings) { c.WriteCoalesceWaitTime = -time.Second }, "WriteCoalesceWaitTime"},
				{"NegativeReconnectInterval", func(c *InternalClusterSettings) { c.ReconnectInterval = -time.Second }, "ReconnectInterval"},
				{"NegativeMaxWaitSchemaAgreement", func(c *InternalClusterSettings) { c.MaxWaitSchemaAgreement = -time.Second }, "MaxWaitSchemaAgreement"},
				{"NonSerialSerialConsistency", func(c *InternalClusterSettings) { c.SerialConsistency = cqltype.Quorum }, "SERIAL or LOCAL_SERIAL"},
				{"NilDNSResolver", func(c *InternalClusterSettings) { c.DNSResolver = nil }, "invalid DNSResolver"},
				{"NegativeMaxExcessShardConnectionsRate", func(c *InternalClusterSettings) { c.MaxExcessShardConnectionsRate = -1 }, "MaxExcessShardConnectionsRate"},
				{"ClientRoutesWithAddressTranslator", func(c *InternalClusterSettings) {
					c.ClientRoutesConfig = &ClientRoutesConfig{}
					c.AddressTranslator = endpoint.IdentityTranslator()
				}, "invalid AddressTranslator: cannot be combined with ClientRoutesConfig"},
				{"InvalidClientRoutesConfig", func(c *InternalClusterSettings) { c.ClientRoutesConfig = &ClientRoutesConfig{} }, "invalid ClientRoutesConfig"},
			}

			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					cfg := InternalNewClusterSettings("10.0.0.1")
					cfg.Logger = logging.InternalNopLogger{}
					if err := cfg.Validate(); err != nil {
						t.Fatalf("baseline config invalid: %v", err)
					}

					cfg = InternalNewClusterSettings("10.0.0.1")
					cfg.Logger = logging.InternalNopLogger{}
					tc.mutate(cfg)
					err := cfg.Validate()
					if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
						t.Fatalf("Validate() = %v, want an error containing %q", err, tc.wantErr)
					}
					if _, ok := errors.AsType[*ValidationError](err); strings.HasPrefix(tc.wantErr, "invalid ") && !ok {
						t.Fatalf("Validate() = %T, want a *ValidationError", err)
					}
				})
			}
		})
	})

	t.Run("ValidateAndInitSSL", func(t *testing.T) {
		t.Parallel()

		cfg := InternalNewClusterSettings("127.0.0.1")
		cfg.SslOpts = &transport.SslOptions{
			Config: &tls.Config{
				InsecureSkipVerify: true,
			},
		}

		tlsCfg := *cfg
		if err := tlsCfg.ValidateAndInitSSL(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if InternalClusterSettingsGetActualTLSConfig(&tlsCfg) == nil {
			t.Fatal("expected copied TLS config to initialize TLS")
		}
		if InternalClusterSettingsGetActualTLSConfig(cfg) != nil {
			t.Fatal("expected original config not to retain copied TLS state")
		}

		noTLSCfg := *cfg
		noTLSCfg.SslOpts = nil
		if err := noTLSCfg.ValidateAndInitSSL(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if InternalClusterSettingsGetActualTLSConfig(&noTLSCfg) != nil {
			t.Fatal("expected copied config with nil SslOpts not to use TLS")
		}
	})
}

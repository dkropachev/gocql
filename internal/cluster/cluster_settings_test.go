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
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	endpoint2 "github.com/scylladb/gocql/v2/endpoint"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/protocol"
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
			tests.AssertEqual(t, "cluster config consistency", cql.Quorum, cfg.Consistency)
			tests.AssertEqual(t, "cluster config max prepared statements", defaultMaxPreparedStmts, cfg.MaxPreparedStmts)
			tests.AssertEqual(t, "cluster config max routing key info", 1000, cfg.MaxRoutingKeyInfo)
			tests.AssertEqual(t, "cluster config page-size", 5000, cfg.PageSize)
			tests.AssertEqual(t, "cluster config default timestamp", true, cfg.DefaultTimestamp)
			tests.AssertEqual(t, "cluster config max wait schema agreement", 60*time.Second, cfg.MaxWaitSchemaAgreement)
			tests.AssertTrue(t, "cluster config conviction policy",
				reflect.DeepEqual(&endpoint2.SimpleConvictionPolicy{}, cfg.ConvictionPolicy))
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

		ctx := context.Background()
		translated := netip.MustParseAddrPort("10.10.10.10:5432")

		t.Run("EmptyAddr", func(t *testing.T) {
			t.Parallel()

			newAddr, _, err := endpoint.InternalTranslateAddressPort(ctx, staticAddressTranslator(translated), nil, endpoint2.Peer{}, netip.AddrPort{}, netip.AddrPort{}, nil)
			tests.AssertNil(t, "should return no error", err)
			tests.AssertEqual(t, "translated address is still empty", netip.AddrPort{}, newAddr)
		})

		t.Run("NilTranslator", func(t *testing.T) {
			t.Parallel()
			addr := netip.MustParseAddrPort("10.0.0.1:1234")
			newAddr, _, err := endpoint.InternalTranslateAddressPort(ctx, nil, nil, endpoint2.Peer{}, addr, netip.AddrPort{}, nil)
			tests.AssertNil(t, "should return no error", err)
			tests.AssertEqual(t, "same address as provided", addr, newAddr)
		})

		t.Run("Success", func(t *testing.T) {
			t.Parallel()

			newAddr, _, err := endpoint.InternalTranslateAddressPort(ctx, staticAddressTranslator(translated), nil, endpoint2.Peer{}, netip.MustParseAddrPort("10.0.0.1:2345"), netip.AddrPort{}, nil)
			tests.AssertNil(t, "should return no error", err)
			tests.AssertEqual(t, "translated address", translated, newAddr)
		})
	})

	t.Run("Validate", func(t *testing.T) {
		t.Run("AcceptsBoundaryValues", func(t *testing.T) {
			t.Parallel()

			for _, serial := range []cql.Consistency{cql.Serial, cql.LocalSerial} {
				cfg := InternalNewClusterSettings("10.0.0.1")
				cfg.Logger = logging.Nop()
				cfg.SerialConsistency = serial
				cfg.Port = 65535
				if err := cfg.Validate(); err != nil {
					t.Fatalf("SerialConsistency=%v Port=65535: %v", serial, err)
				}
			}
		})

		// A zero Port defaults to the CQL port, whatever ports the hosts name:
		// config.Build checks contact point ports against WithPort.
		t.Run("DefaultsPort", func(t *testing.T) {
			t.Parallel()

			for _, hosts := range [][]string{{"10.0.0.1"}, {"10.0.0.1:9142"}, {"10.0.0.1:9142", "10.0.0.2"}, {"10.0.0.1:9142", "[::1]:9043"}} {
				cfg := InternalNewClusterSettings(hosts...)
				cfg.Logger = logging.Nop()
				if err := cfg.Validate(); err != nil {
					t.Fatalf("hosts %v: %v", hosts, err)
				}
				tests.AssertEqual(t, "cfg.Port after Validate", defaultCQLPort, cfg.Port)
			}
			cfg := InternalNewClusterSettings("10.0.0.1:9142")
			cfg.Port = 9043
			if err := cfg.Validate(); err != nil {
				t.Fatal(err)
			}
			tests.AssertEqual(t, "explicit cfg.Port after Validate", 9043, cfg.Port)
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
				{"NegativeWriteTimeout", func(c *InternalClusterSettings) { c.WriteTimeout = -time.Second }, "WriteTimeout"},
				{"NegativeTimeout", func(c *InternalClusterSettings) { c.Timeout = -time.Second }, "invalid Timeout:"},
				{"NegativeConnectTimeout", func(c *InternalClusterSettings) { c.ConnectTimeout = -time.Second }, "ConnectTimeout"},
				{"NegativeMetadataSchemaRequestTimeout", func(c *InternalClusterSettings) { c.MetadataSchemaRequestTimeout = -time.Second }, "MetadataSchemaRequestTimeout"},
				{"NegativeWriteCoalesceWaitTime", func(c *InternalClusterSettings) { c.WriteCoalesceWaitTime = -time.Second }, "WriteCoalesceWaitTime"},
				{"NegativeMaxWaitSchemaAgreement", func(c *InternalClusterSettings) { c.MaxWaitSchemaAgreement = -time.Second }, "MaxWaitSchemaAgreement"},
				{"NonSerialSerialConsistency", func(c *InternalClusterSettings) { c.SerialConsistency = cql.Quorum }, "SERIAL or LOCAL_SERIAL"},
				{"NilDNSResolver", func(c *InternalClusterSettings) { c.DNSResolver = nil }, "invalid DNSResolver"},
				{"NegativeMaxExcessShardConnectionsRate", func(c *InternalClusterSettings) { c.MaxExcessShardConnectionsRate = -1 }, "MaxExcessShardConnectionsRate"},
				{"ClientRoutesWithAddressTranslator", func(c *InternalClusterSettings) {
					c.ClientRoutesConfig = &ClientRoutesConfig{}
					c.AddressTranslator = endpoint2.TranslatorFunc(func(_ context.Context, _ endpoint2.Peer, addr netip.AddrPort) (endpoint2.Address, error) {
						return endpoint2.AddressOf(addr), nil
					})
				}, "invalid AddressTranslator: cannot be combined with ClientRoutesConfig"},
				{"InvalidClientRoutesConfig", func(c *InternalClusterSettings) { c.ClientRoutesConfig = &ClientRoutesConfig{} }, "invalid ClientRoutesConfig"},
			}

			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					cfg := InternalNewClusterSettings("10.0.0.1")
					cfg.Logger = logging.Nop()
					if err := cfg.Validate(); err != nil {
						t.Fatalf("baseline config invalid: %v", err)
					}

					cfg = InternalNewClusterSettings("10.0.0.1")
					cfg.Logger = logging.Nop()
					tc.mutate(cfg)
					err := cfg.Validate()
					if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
						t.Fatalf("Validate() = %v, want an error containing %q", err, tc.wantErr)
					}
					if _, ok := errors.AsType[*cql.ValidationError](err); strings.HasPrefix(tc.wantErr, "invalid ") && !ok {
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
		if clusterSettingsGetActualTLSConfig(&tlsCfg) == nil {
			t.Fatal("expected copied TLS config to initialize TLS")
		}
		if clusterSettingsGetActualTLSConfig(cfg) != nil {
			t.Fatal("expected original config not to retain copied TLS state")
		}

		noTLSCfg := *cfg
		noTLSCfg.SslOpts = nil
		if err := noTLSCfg.ValidateAndInitSSL(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if clusterSettingsGetActualTLSConfig(&noTLSCfg) != nil {
			t.Fatal("expected copied config with nil SslOpts not to use TLS")
		}
	})
}

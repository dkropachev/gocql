package cluster

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/transport"
)

// TestValidateRejects walks every check in clusterSettings.Validate that turns a
// misconfiguration into an error, so a user sees it at open rather than
// as a confusing failure later.
func TestValidateRejects(t *testing.T) {
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
		}, "both Authenticator and AuthProvider"},
		{"NilInitialReconnectionPolicy", func(c *InternalClusterSettings) { c.InitialReconnectionPolicy = nil }, "InitialReconnectionPolicy is nil"},
		{"InitialReconnectionPolicyNoRetries", func(c *InternalClusterSettings) {
			c.InitialReconnectionPolicy = &pool.ConstantReconnectionPolicy{MaxRetries: 0}
		}, "InitialReconnectionPolicy.GetMaxRetries"},
		{"NilReconnectionPolicy", func(c *InternalClusterSettings) { c.ReconnectionPolicy = nil }, "ReconnectionPolicy is nil"},
		// hostConnPool.InternalConnect makes GetMaxRetries attempts; with none every pool
		// fill fails.
		{"ReconnectionPolicyNoRetries", func(c *InternalClusterSettings) {
			c.ReconnectionPolicy = &pool.ConstantReconnectionPolicy{MaxRetries: 0}
		}, "ReconnectionPolicy.GetMaxRetries"},
		{"NegativePageSize", func(c *InternalClusterSettings) { c.PageSize = -1 }, "PageSize"},
		{"NegativeMaxRoutingKeyInfo", func(c *InternalClusterSettings) { c.MaxRoutingKeyInfo = -1 }, "MaxRoutingKeyInfo"},
		{"NegativeMaxPreparedStmts", func(c *InternalClusterSettings) { c.MaxPreparedStmts = -1 }, "MaxPreparedStmts"},
		{"NegativeSocketKeepalive", func(c *InternalClusterSettings) { c.SocketKeepalive = -time.Second }, "SocketKeepalive"},
		{"NegativeMaxRequestsPerConn", func(c *InternalClusterSettings) { c.MaxRequestsPerConn = -1 }, "MaxRequestsPerConn"},
		{"NegativeNumConns", func(c *InternalClusterSettings) { c.NumConns = -1 }, "NumConns"},
		{"PortTooLarge", func(c *InternalClusterSettings) { c.Port = 65536 }, "valid port number"},
		{"NegativePort", func(c *InternalClusterSettings) { c.Port = -1 }, "valid port number"},
		{"MixedHostPorts", func(c *InternalClusterSettings) { c.Hosts = []string{"10.0.0.1:9042", "10.0.0.2"} }, "all include a port or none"},
		{"NegativeWriteTimeout", func(c *InternalClusterSettings) { c.WriteTimeout = -time.Second }, "WriteTimeout"},
		{"NegativeTimeout", func(c *InternalClusterSettings) { c.Timeout = -time.Second }, "Timeout should be"},
		{"NegativeConnectTimeout", func(c *InternalClusterSettings) { c.ConnectTimeout = -time.Second }, "ConnectTimeout"},
		{"NegativeMetadataSchemaRequestTimeout", func(c *InternalClusterSettings) { c.MetadataSchemaRequestTimeout = -time.Second }, "MetadataSchemaRequestTimeout"},
		{"NegativeWriteCoalesceWaitTime", func(c *InternalClusterSettings) { c.WriteCoalesceWaitTime = -time.Second }, "WriteCoalesceWaitTime"},
		{"NegativeReconnectInterval", func(c *InternalClusterSettings) { c.ReconnectInterval = -time.Second }, "ReconnectInterval"},
		{"NegativeMaxWaitSchemaAgreement", func(c *InternalClusterSettings) { c.MaxWaitSchemaAgreement = -time.Second }, "MaxWaitSchemaAgreement"},
		{"NonSerialSerialConsistency", func(c *InternalClusterSettings) { c.SerialConsistency = cqltype.Quorum }, "SERIAL or LOCAL_SERIAL"},
		{"NilDNSResolver", func(c *InternalClusterSettings) { c.DNSResolver = nil }, "DNSResolver is empty"},
		{"NegativeMaxExcessShardConnectionsRate", func(c *InternalClusterSettings) { c.MaxExcessShardConnectionsRate = -1 }, "MaxExcessShardConnectionsRate"},
		{"ClientRoutesWithAddressTranslator", func(c *InternalClusterSettings) {
			c.ClientRoutesConfig = &ClientRoutesConfig{}
			c.AddressTranslator = endpoint.AddressTranslatorFunc(func(addr net.IP, port int) (net.IP, int) { return addr, port })
		}, "should not be set at the same time"},
		{"InvalidClientRoutesConfig", func(c *InternalClusterSettings) { c.ClientRoutesConfig = &ClientRoutesConfig{} }, "ClientRoutesConfig is invalid"},
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
		})
	}
}

func TestValidateAcceptsBoundaryValues(t *testing.T) {
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
}

// With no port anywhere, Validate fills in the CQL default; with client routes
// it does so without looking at the hosts, whose ports are entry points only.
func TestValidateDefaultsPort(t *testing.T) {
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
}

package cluster

import (
	"crypto/tls"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/retry"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

const driverConfigSchemaPath = "../../docs/driver-config-schema.json"

// loadDriverConfigSchema compiles the schema exactly as shipped. Nothing here
// may relax it: the point of validating is to prove a consumer that checks the
// payload accepts it, which a locally-adjusted schema would not establish.
func loadDriverConfigSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()

	f, err := os.Open(driverConfigSchemaPath)
	if err != nil {
		t.Fatalf("unable to open %s: %v", driverConfigSchemaPath, err)
	}
	defer f.Close()

	doc, err := jsonschema.UnmarshalJSON(f)
	if err != nil {
		t.Fatalf("unable to parse %s: %v", driverConfigSchemaPath, err)
	}

	c := jsonschema.NewCompiler()
	if err := c.AddResource(driverConfigSchemaPath, doc); err != nil {
		t.Fatalf("unable to add %s: %v", driverConfigSchemaPath, err)
	}
	sch, err := c.Compile(driverConfigSchemaPath)
	if err != nil {
		t.Fatalf("unable to compile %s: %v", driverConfigSchemaPath, err)
	}
	return sch
}

// driverConfigCases spans the report's decision points: every optional group
// present and absent, every policy discriminant, and the boundary values that
// the schema constrains but nothing in the driver rejects.
func driverConfigCases() []struct {
	name         string
	cfg          func(*InternalClusterSettings)
	policy       HostSelectionPolicy
	isScyllaConn bool
} {
	return []struct {
		name         string
		cfg          func(*InternalClusterSettings)
		policy       HostSelectionPolicy
		isScyllaConn bool
	}{
		{name: "defaults"},
		{name: "defaults against scylla", isScyllaConn: true},
		{
			name: "every optional duration disabled",
			cfg: func(c *InternalClusterSettings) {
				c.ConnectTimeout = 0
				c.ReadTimeout = 0
				c.WriteTimeout = 0
				c.Timeout = 0
				c.MetadataSchemaRequestTimeout = 0
				c.MaxWaitSchemaAgreement = 0
				c.PageSize = 0
			},
		},
		{
			name: "sub-millisecond durations",
			cfg: func(c *InternalClusterSettings) {
				c.ConnectTimeout = 500 * time.Microsecond
				c.ReadTimeout = 500 * time.Microsecond
				c.WriteTimeout = 500 * time.Microsecond
				c.Timeout = 500 * time.Microsecond
				c.MetadataSchemaRequestTimeout = 500 * time.Microsecond
			},
			isScyllaConn: true,
		},
		{
			name: "in-flight limit above the stream-id range",
			cfg:  func(c *InternalClusterSettings) { c.MaxRequestsPerConn = 40000 },
		},
		{
			name: "in-flight limit not a multiple of 64",
			cfg:  func(c *InternalClusterSettings) { c.MaxRequestsPerConn = 100 },
		},
		{
			name: "serial default consistency",
			cfg:  func(c *InternalClusterSettings) { c.Consistency = cqltype.LocalSerial },
		},
		{
			name: "serial consistency set",
			cfg:  func(c *InternalClusterSettings) { c.SerialConsistency = cqltype.LocalSerial },
		},
		{
			name: "no reconnection policy",
			cfg:  func(c *InternalClusterSettings) { c.ReconnectionPolicy = &pool.NoReconnectionPolicy{} },
		},
		{
			name: "constant reconnection with no attempts",
			cfg: func(c *InternalClusterSettings) {
				c.ReconnectionPolicy = &pool.ConstantReconnectionPolicy{MaxRetries: 0, Interval: time.Second}
			},
		},
		{
			name: "constant reconnection with a negative attempt limit",
			cfg: func(c *InternalClusterSettings) {
				c.ReconnectionPolicy = &pool.ConstantReconnectionPolicy{MaxRetries: -1, Interval: time.Second}
			},
		},
		{
			name: "nil reconnection policy",
			cfg:  func(c *InternalClusterSettings) { c.ReconnectionPolicy = nil },
		},
		{
			name: "typed-nil reconnection policy",
			cfg:  func(c *InternalClusterSettings) { c.ReconnectionPolicy = (*pool.ConstantReconnectionPolicy)(nil) },
		},
		{
			name: "constant reconnection with a negative interval",
			cfg: func(c *InternalClusterSettings) {
				c.ReconnectionPolicy = &pool.ConstantReconnectionPolicy{MaxRetries: 3, Interval: -5 * time.Second}
			},
		},
		{
			name: "typed-nil custom retry policy",
			cfg: func(c *InternalClusterSettings) {
				c.Data().defaultProfile.Retry = (*customRetry)(nil)
			},
		},
		{
			name:   "typed-nil load balancing fallback",
			policy: TokenAwareHostPolicy((*dcAwareRR)(nil)),
		},
		{
			name: "exponential reconnection with unset intervals",
			cfg: func(c *InternalClusterSettings) {
				c.ReconnectionPolicy = &pool.ExponentialReconnectionPolicy{MaxRetries: 3}
			},
		},
		{
			name: "exponential reconnection with max below base",
			cfg: func(c *InternalClusterSettings) {
				c.ReconnectionPolicy = &pool.ExponentialReconnectionPolicy{MaxRetries: 3, InitialInterval: 5 * time.Second, MaxInterval: time.Second}
			},
		},
		{
			name: "custom reconnection policy",
			cfg: func(c *InternalClusterSettings) {
				c.ReconnectionPolicy = &fakeReconnectionPolicy{ReconnectionPolicy: &pool.NoReconnectionPolicy{}}
			},
		},
		{
			name: "safe retry three attempts",
			cfg: func(c *InternalClusterSettings) {
				c.Data().defaultProfile.Retry = retry.Safe{MaxAttempts: 3}
			},
		},
		{
			name: "safe retry one attempt",
			cfg: func(c *InternalClusterSettings) {
				c.Data().defaultProfile.Retry = retry.Safe{MaxAttempts: 1}
			},
		},
		{
			name: "safe retry default limit",
			cfg: func(c *InternalClusterSettings) {
				c.Data().defaultProfile.Retry = retry.Safe{MaxAttempts: 0}
			},
		},
		{
			name: "custom retry policy",
			cfg: func(c *InternalClusterSettings) {
				c.Data().defaultProfile.Retry = &customRetry{}
			},
		},
		{
			name: "safe retry negative limit",
			cfg: func(c *InternalClusterSettings) {
				c.Data().defaultProfile.Retry = retry.Safe{MaxAttempts: -1}
			},
		},
		{
			name: "safe retry zero limit",
			cfg: func(c *InternalClusterSettings) {
				c.Data().defaultProfile.Retry = retry.Safe{MaxAttempts: 0}
			},
		},
		{
			name: "tls without hostname verification",
			cfg:  func(c *InternalClusterSettings) { c.SslOpts = &transport.SslOptions{} },
		},
		{
			name: "tls with a caller-supplied config",
			cfg: func(c *InternalClusterSettings) {
				c.SslOpts = &transport.SslOptions{Config: &tls.Config{InsecureSkipVerify: true}}
			},
		},
		{
			name: "datacenter host filter",
			cfg:  func(c *InternalClusterSettings) { c.HostFilter = DataCenterHostFilter("dc1") },
		},
		{
			name: "whitelist host filter",
			cfg: func(c *InternalClusterSettings) {
				f, err := WhiteListHostFilter("127.0.0.1")
				if err != nil {
					panic(err)
				}
				c.HostFilter = f
			},
		},
		{
			name: "host dialer that cannot target a shard",
			cfg:  func(c *InternalClusterSettings) { c.HostDialer = fakeHostDialer{} },
		},
		{
			name: "host dialer that can target a shard",
			cfg:  func(c *InternalClusterSettings) { c.HostDialer = fakeShardDialer{} },
		},
		{name: "dc-aware policy", policy: TokenAwareHostPolicy(DCAwareRoundRobinPolicy("dc1"))},
		{
			name:   "dc-aware policy without failover",
			policy: TokenAwareHostPolicy(DCAwareRoundRobinPolicy("dc1", HostPolicyOptionDisableDCFailover)),
		},
		{name: "rack-aware policy", policy: TokenAwareHostPolicy(RackAwareRoundRobinPolicy("dc1", "rack1"))},
		{
			name:   "rack-aware policy without dc failover",
			policy: TokenAwareHostPolicy(RackAwareRoundRobinPolicy("dc1", "rack1", HostPolicyOptionDisableDCFailover)),
		},
		{
			name:   "non-local replicas fallback over a confined fallback",
			policy: TokenAwareHostPolicy(DCAwareRoundRobinPolicy("dc1", HostPolicyOptionDisableDCFailover), NonLocalReplicasFallback()),
		},
		{name: "empty datacenter", policy: TokenAwareHostPolicy(DCAwareRoundRobinPolicy(""))},
		{name: "replica-set ordering", policy: TokenAwareHostPolicy(RoundRobinHostPolicy(), DontShuffleReplicas())},
		{
			name:   "adaptive ordering by in-flight requests",
			policy: TokenAwareHostPolicy(RoundRobinHostPolicy(), AvoidSlowReplicas(MAX_IN_FLIGHT_THRESHOLD)),
		},
		{name: "non-token-aware policy", policy: RoundRobinHostPolicy()},
		{name: "wrapped policy", policy: SingleHostReadyPolicy(TokenAwareHostPolicy(DCAwareRoundRobinPolicy("dc1")))},
		{name: "custom policy", policy: &fakeHostSelectionPolicy{HostSelectionPolicy: RoundRobinHostPolicy()}},
	}
}

// buildCaseReport produces the report a case describes, decoded for
// validation.
func buildCaseReport(t *testing.T, cfgFn func(*InternalClusterSettings), policy HostSelectionPolicy, isScyllaConn bool) any {
	t.Helper()

	cfg := *InternalNewClusterSettings("127.0.0.1")
	if cfgFn != nil {
		cfgFn(&cfg)
	}
	if policy == nil {
		policy = TokenAwareHostPolicy(RoundRobinHostPolicy())
	}
	// Resolve SslOpts the way session creation does, so the TLS group is built
	// from the config the driver would really dial with.
	if err := cfg.ValidateAndInitSSL(); err != nil {
		t.Fatalf("unable to initialize ssl config: %v", err)
	}

	raw, err := newDriverConfigReporter(newTestReportSession(cfg, policy)).buildReport(isScyllaConn)
	if err != nil {
		t.Fatalf("unable to build the report: %v", err)
	}
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatalf("report is not valid JSON: %v (%s)", err, raw)
	}
	return decoded
}

// leafCauses flattens a validation error to the individual constraint failures
// at its leaves; the intermediate nodes only describe which subschema the
// failures were found under.
func leafCauses(err *jsonschema.ValidationError) []*jsonschema.ValidationError {
	if len(err.Causes) == 0 {
		return []*jsonschema.ValidationError{err}
	}
	var out []*jsonschema.ValidationError
	for _, cause := range err.Causes {
		out = append(out, leafCauses(cause)...)
	}
	return out
}

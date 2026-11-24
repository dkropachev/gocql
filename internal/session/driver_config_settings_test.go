// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/scylladb/gocql/v2/auth"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/endpoint"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/logging"
	implProtocol "github.com/scylladb/gocql/v2/internal/protocol"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/retry"
	"github.com/scylladb/gocql/v2/routing"
)

const driverConfigSchemaPath = "../../docs/driver-config-schema.json"

// secretDialer opens plain connections and holds a value that must never
// reach the configuration report.
type secretDialer struct{ token string }

func (d secretDialer) DialContext(ctx context.Context, target endpoint.DialTarget) (net.Conn, error) {
	return endpoint.DefaultDialer{}.DialContext(ctx, target)
}

type reportSpeculation struct{}

func (reportSpeculation) Delay(int) (time.Duration, bool) { return time.Second, true }

type reportRouting struct{ routing.Policy }

// jsonNull asks for a key that is present with a JSON null value.
type jsonNull struct{}

type reportRetry struct{}

func (reportRetry) Decide(retry.Attempt) retry.Decision { return retry.Decision{} }

type reportReconnect struct{}

func (reportReconnect) Delay(int) (time.Duration, bool) { return time.Second, true }

// connectReport connects a session with the default settings, changed by
// edit, to a fake server and returns the DRIVER_CONFIG its control connection
// sent.
func connectReport(t *testing.T, edit func(*settings.Config)) string {
	t.Helper()
	var mu sync.Mutex
	var report string
	srv := newControlTestServer(t, func(srv *controlTestServer) {
		srv.onRecv = func(f *implProtocol.Framer) {
			if f.Header.Op != frm.OpStartup {
				return
			}
			opts := readStartupOptions(t, f)
			mu.Lock()
			defer mu.Unlock()
			if v, ok := opts["DRIVER_CONFIG"]; ok {
				report = v
			}
		}
	})
	resolved := settings.DefaultConfig(srv.Address)
	edit(&resolved)
	resolved.Metadata.Mode = metadata.ModeNone
	resolved.Metadata.InitialDiscovery = false
	resolved.Logger = logging.Nop()
	resolved.Protocol = 4
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := Connect(ctx, resolved)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	mu.Lock()
	defer mu.Unlock()
	if report == "" {
		t.Fatal("no DRIVER_CONFIG was sent")
	}
	return report
}

// TestDriverConfigReportFromSettings connects sessions with the settings
// config.ConfigBuilder produces (config's tests pin which setter sets which
// field; this package may not import config) and checks that the
// DRIVER_CONFIG they send conforms to docs/driver-config-schema.json, reports
// what was chosen and carries no secret. Every key the schema defines must be
// reported by some configuration here or listed as one the driver cannot
// report, and every listed key must exist and go unreported.
func TestDriverConfigReportFromSettings(t *testing.T) {
	schema := compileDriverConfigSchema(t)
	const password = "report-secret-password"
	const dialerToken = "report-secret-dialer-token"
	seen := map[string]bool{}
	cases, completed := 0, 0
	for _, tc := range []struct {
		name string
		edit func(*settings.Config)
		want map[string]any
	}{
		{
			name: "Defaults",
			edit: func(*settings.Config) {},
			want: map[string]any{
				"version":                                             float64(1),
				"connection.connect.timeout-ms":                       float64(5000),
				"connection.requests.in-flight.max":                   float64(32768),
				"connection.requests.orphaned.max":                    float64(1024),
				"connection.pool.shard-aware.enabled":                 true,
				"connection.socket.tcp-no-delay":                      true,
				"connection.socket.keep-alive":                        false,
				"connection.socket.reuse-address":                     false,
				"connection.socket.linger":                            nil,
				"connection.tls":                                      nil,
				"connection.reconnection.policy.type":                 "exponential",
				"connection.reconnection.policy.base-ms":              float64(1000),
				"connection.reconnection.policy.max-ms":               float64(32000),
				"connection.reconnection.policy.max-attempts":         nil,
				"control-plane.queries.system.timeout.client-side-ms": float64(31000),
				"control-plane.schema.agreement.timeout-ms":           float64(60000),
				"query.defaults.consistency":                          "LOCAL_QUORUM",
				"query.defaults.serial-consistency":                   "LOCAL_SERIAL",
				"query.defaults.page.size":                            float64(5000),
				"query.defaults.request.timeout-ms":                   float64(30000),
				"query.defaults.client-timestamps":                    true,
				"query.retry.policy.type":                             "simple",
				"query.retry.policy.max-retries":                      float64(2),
				"query.load-balancing.policy.type":                    "token-aware",
				"query.load-balancing.policy.load-distribution":       "shuffle",
				"query.speculative-execution":                         nil,
			},
		},
		{
			name: "Tuned",
			// The fields the ConfigBuilder setters WithAuth, WithDialer,
			// WithConnectTimeout, WithReadTimeout, WithWriteTimeout,
			// WithNagle(true), WithTCPKeepalive, WithSocketBuffers, WithLinger,
			// WithConnectionsPerHost, WithMaxRequestsPerConnection,
			// WithMaxOrphanedStreams, WithReconnect(nil), WithMetadataTimeout,
			// WithSchemaAgreement and WithTimestampGenerator(nil) set, and a default
			// profile with consistency ONE, no serial consistency, 10-row pages,
			// no timeout, no retry policy and a speculation policy.
			edit: func(c *settings.Config) {
				c.Auth = auth.Password("report-user", password)
				c.Dialer = secretDialer{token: dialerToken}
				c.ConnectTimeout, c.ReadTimeout, c.WriteTimeout = 2*time.Second, 3*time.Second, 4*time.Second
				c.NoDelay, c.SocketKeepalive = false, time.Minute
				c.ReceiveBuffer, c.SendBuffer, c.Linger = cql.Some(65536), cql.Some(32768), cql.Some(2*time.Second)
				c.Pool.ConnectionsPerHost, c.Pool.ConnectionsPerShard, c.Pool.AdvancedShardAwareness.Enabled = 2, 0, false
				c.Pool.MaxRequestsPerConnection, c.Pool.MaxOrphanedStreams = 100, 5
				c.Reconnect = nil
				c.Metadata.RequestTimeout = 7 * time.Second
				c.Metadata.AgreementTimeout, c.Metadata.AgreementPoll = 9*time.Second, time.Second
				c.Timestamps = settings.ClientTimestamps{}
				p := *c.DefaultProfile
				p.Consistency, p.SerialConsistency, p.PageSize, p.Timeout = cql.One, cql.Option[cql.Consistency]{}, 10, 0
				p.Retry, p.Speculation = nil, reportSpeculation{}
				c.DefaultProfile = &p
			},
			want: map[string]any{
				"connection.connect.timeout-ms":                       float64(2000),
				"connection.read.timeout-ms":                          float64(3000),
				"connection.write.timeout-ms":                         float64(4000),
				"connection.requests.in-flight.max":                   float64(100),
				"connection.requests.orphaned.max":                    float64(5),
				"connection.pool.shard-aware.enabled":                 false,
				"connection.socket.tcp-no-delay":                      false,
				"connection.socket.keep-alive":                        true,
				"connection.socket.linger.interval-s":                 float64(2),
				"connection.socket.receive-buffer.size-bytes":         float64(65536),
				"connection.socket.send-buffer.size-bytes":            float64(32768),
				"connection.reconnection.policy":                      jsonNull{},
				"control-plane.queries.system.timeout.client-side-ms": float64(7000),
				"control-plane.schema.agreement.timeout-ms":           float64(9000),
				"query.defaults.consistency":                          "ONE",
				"query.defaults.serial-consistency":                   nil,
				"query.defaults.page.size":                            float64(10),
				"query.defaults.request":                              nil,
				"query.defaults.client-timestamps":                    false,
				"query.retry.policy.type":                             "fallthrough",
				"query.speculative-execution.policy.type":             "custom",
				"query.speculative-execution.policy.name":             "reportSpeculation",
			},
		},
		{
			name: "Locality",
			edit: func(c *settings.Config) {
				p := *c.DefaultProfile
				p.Routing = implRouting.EngineOf(newReportRoutingWith(t, routing.Options{Datacenter: "dc1", Rack: "rack1", AllowRemote: true, Latency: routing.LatencyOptions{Enabled: true}, FixedReplicaOrder: true, NoLeaderAware: true}))
				c.DefaultProfile = &p
			},
			want: map[string]any{
				"query.load-balancing.node-preference.type":                   "rack",
				"query.load-balancing.node-preference.local-dc":               "dc1",
				"query.load-balancing.node-preference.local-rack":             "rack1",
				"query.load-balancing.policy.fallback-to-non-preferred-nodes": true,
			},
		},
		{
			name: "CustomRetry",
			edit: func(c *settings.Config) {
				p := *c.DefaultProfile
				p.Retry = reportRetry{}
				c.DefaultProfile = &p
			},
			want: map[string]any{
				"query.retry.policy.type": "custom",
				"query.retry.policy.name": "reportRetry",
			},
		},
		{
			name: "CustomPolicies",
			edit: func(c *settings.Config) {
				c.Reconnect = reportReconnect{}
				p := *c.DefaultProfile
				p.Retry, p.Routing = retry.Never{}, implRouting.EngineOf(reportRouting{newReportRouting(t)})
				c.DefaultProfile = &p
			},
			want: map[string]any{
				"query.load-balancing.policy.type":    "custom",
				"query.load-balancing.policy.name":    "reportRouting",
				"connection.reconnection.policy.type": "custom",
				"connection.reconnection.policy.name": "reportReconnect",
				"query.retry.policy.type":             "fallthrough",
			},
		},
	} {
		cases++
		t.Run(tc.name, func(t *testing.T) {
			raw := connectReport(t, tc.edit)
			var doc any
			if err := json.Unmarshal([]byte(raw), &doc); err != nil {
				t.Fatalf("DRIVER_CONFIG is not JSON: %v", err)
			}
			if err := schema.Validate(doc); err != nil {
				t.Fatalf("DRIVER_CONFIG does not conform to the schema: %v\n%s", err, raw)
			}
			for path, want := range tc.want {
				got, ok := lookupJSON(doc, path)
				if _, null := want.(jsonNull); null {
					if !ok || got != nil {
						t.Errorf("%s = %v (present %t), want null", path, got, ok)
					}
					continue
				}
				if want == nil {
					if ok {
						t.Errorf("%s = %v, want it absent", path, got)
					}
					continue
				}
				if !ok || got != want {
					t.Errorf("%s = %v (present %t), want %v", path, got, ok, want)
				}
			}
			for _, secret := range []string{password, "report-user", dialerToken} {
				if strings.Contains(raw, secret) {
					t.Errorf("DRIVER_CONFIG leaks %q: %s", secret, raw)
				}
			}
			collectJSONPaths(doc, "", seen)
			if !t.Failed() {
				completed++
			}
		})
	}
	// The key coverage below needs the reports of every case; with a case
	// filtered out by -run or failed, it would only add noise.
	if completed != cases {
		return
	}

	// Keys the driver never reports, and why. A key the schema adds fails
	// here until it is reported or listed.
	unreported := map[string]string{
		"connection.heartbeat":                                     "a placeholder with no properties in schema version 1",
		"connection.write.coalescing":                              "a placeholder with no properties in schema version 1",
		"connection.node-preference":                               "config.WithHostFilter is a function the report cannot see into",
		"connection.tls":                                           "the fake server speaks no TLS; TestDriverConfigTypedReport covers it",
		"connection.tls.hostname-verification":                     "as connection.tls",
		"control-plane.queries.system.timeout.server-side-ms":      "the fake server is not ScyllaDB; TestDriverConfigTypedReport covers it",
		"connection.reconnection.policy.delay-ms":                  "the builder offers no constant reconnection policy",
		"connection.reconnection.policy.max-attempts":              "the builder offers no attempt-limited reconnection policy",
		"query.load-balancing.policy.description":                  "custom policies are reported by name only",
		"query.retry.policy.description":                           "custom policies are reported by name only",
		"query.speculative-execution.policy.description":           "custom policies are reported by name only",
		"query.speculative-execution.policy.delay-ms":              "retry has no built-in speculation policy, so every one is custom",
		"query.speculative-execution.policy.max-executions":        "as query.speculative-execution.policy.delay-ms",
		"query.speculative-execution.policy.percentile":            "as query.speculative-execution.policy.delay-ms",
		"connection.node-preference.type":                          "as connection.node-preference",
		"connection.node-preference.local-dc":                      "as connection.node-preference",
		"connection.node-preference.local-rack":                    "as connection.node-preference",
		"connection.node-preference.inferred-local-dc":             "as connection.node-preference",
		"connection.node-preference.inferred-local-rack":           "as connection.node-preference",
		"query.load-balancing.node-preference.inferred-local-dc":   "the driver infers no datacenter or rack",
		"query.load-balancing.node-preference.inferred-local-rack": "the driver infers no datacenter or rack",
		"query.retry.backoff.type":                                 "as query.retry.backoff",
		"query.retry.backoff.base-ms":                              "as query.retry.backoff",
		"query.retry.backoff.max-ms":                               "as query.retry.backoff",
		"query.retry.backoff.delay-ms":                             "as query.retry.backoff",
		"query.retry.backoff":                                      "no built-in retry policy waits between attempts",
	}
	for _, path := range schemaPaths(t) {
		if seen[path] {
			continue
		}
		if _, ok := unreported[path]; ok {
			continue
		}
		t.Errorf("schema key %s is neither reported for any configuration here nor listed as unreported", path)
	}
	known := map[string]bool{}
	for _, path := range schemaPaths(t) {
		known[path] = true
	}
	for _, path := range sortedKeys(unreported) {
		if !known[path] {
			t.Errorf("unreported key %s is not in the schema", path)
		} else if seen[path] {
			t.Errorf("unreported key %s is reported; drop it from the list", path)
		}
	}
}

func newReportRouting(t *testing.T) routing.Policy {
	t.Helper()
	return newReportRoutingWith(t, routing.Options{FixedReplicaOrder: true, NoLeaderAware: true})
}

func newReportRoutingWith(t *testing.T, options routing.Options) routing.Policy {
	t.Helper()
	policy, err := routing.NewPolicy(options)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func compileDriverConfigSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	f, err := os.Open(driverConfigSchemaPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	doc, err := jsonschema.UnmarshalJSON(f)
	if err != nil {
		t.Fatal(err)
	}
	c := jsonschema.NewCompiler()
	if err := c.AddResource(driverConfigSchemaPath, doc); err != nil {
		t.Fatal(err)
	}
	schema, err := c.Compile(driverConfigSchemaPath)
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

// lookupJSON returns the value at the dotted path of doc.
func lookupJSON(doc any, path string) (any, bool) {
	for _, key := range strings.Split(path, ".") {
		object, ok := doc.(map[string]any)
		if !ok {
			return nil, false
		}
		if doc, ok = object[key]; !ok {
			return nil, false
		}
	}
	return doc, true
}

// collectJSONPaths adds the dotted path of every key in doc to seen.
func collectJSONPaths(doc any, prefix string, seen map[string]bool) {
	object, ok := doc.(map[string]any)
	if !ok {
		return
	}
	for key, value := range object {
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		seen[path] = true
		collectJSONPaths(value, path, seen)
	}
}

// schemaPaths lists the dotted path of every property the schema defines,
// following $ref and the branches of oneOf.
func schemaPaths(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile(driverConfigSchemaPath)
	if err != nil {
		t.Fatal(err)
	}
	var root map[string]any
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatal(err)
	}
	defs, _ := root["$defs"].(map[string]any)
	paths := map[string]bool{}
	var walk func(node map[string]any, prefix string, depth int)
	walk = func(node map[string]any, prefix string, depth int) {
		if depth > 32 {
			t.Fatalf("schema nesting too deep at %s", prefix)
		}
		if ref, ok := node["$ref"].(string); ok {
			if def, ok := defs[strings.TrimPrefix(ref, "#/$defs/")].(map[string]any); ok {
				walk(def, prefix, depth+1)
			}
		}
		for _, key := range []string{"oneOf", "anyOf", "allOf"} {
			branches, _ := node[key].([]any)
			for _, branch := range branches {
				if b, ok := branch.(map[string]any); ok {
					walk(b, prefix, depth+1)
				}
			}
		}
		properties, _ := node["properties"].(map[string]any)
		for key, value := range properties {
			path := key
			if prefix != "" {
				path = prefix + "." + key
			}
			paths[path] = true
			if v, ok := value.(map[string]any); ok {
				walk(v, path, depth+1)
			}
		}
	}
	walk(root, "", 0)
	return sortedKeys(paths)
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

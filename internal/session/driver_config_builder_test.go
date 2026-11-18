// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/scylladb/gocql/v2/auth"
	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/endpoint"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/logging"
	implProtocol "github.com/scylladb/gocql/v2/internal/protocol"
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

// connectReport connects a session built from build to a fake server and
// returns the DRIVER_CONFIG its control connection sent.
func connectReport(t *testing.T, build func(address string) *config.ConfigBuilder) string {
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
	cfg, err := build(srv.Address).WithMetadataMode(metadata.SchemaNone).WithDiscovery(false).WithLogger(logging.Nop()).Build()
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := settings.InternalResolvePublic(cfg)
	if err != nil {
		t.Fatal(err)
	}
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

// TestDriverConfigReportFromBuilder connects sessions built with
// config.ConfigBuilder and checks that the DRIVER_CONFIG they send conforms to
// docs/driver-config-schema.json, reports what the builder chose, carries no
// secret, and that every key the schema defines is either reported by some
// configuration here or listed as one the driver cannot report.
func TestDriverConfigReportFromBuilder(t *testing.T) {
	schema := compileDriverConfigSchema(t)
	const password = "report-secret-password"
	const dialerToken = "report-secret-dialer-token"
	seen := map[string]bool{}
	for _, tc := range []struct {
		name  string
		build func(*config.ConfigBuilder) *config.ConfigBuilder
		want  map[string]any
	}{
		{
			name:  "Defaults",
			build: func(b *config.ConfigBuilder) *config.ConfigBuilder { return b },
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
			build: func(b *config.ConfigBuilder) *config.ConfigBuilder {
				return b.WithAuth(auth.Password("report-user", password)).
					WithDialer(secretDialer{token: dialerToken}).
					WithConnectTimeout(2*time.Second).WithReadTimeout(3*time.Second).WithWriteTimeout(4*time.Second).
					WithNagle(true).WithTCPKeepalive(time.Minute).WithSocketBuffers(65536, 32768).WithLinger(2*time.Second).
					WithConnectionsPerHost(2).WithMaxRequestsPerConnection(100).WithMaxOrphanedStreams(5).
					WithReconnect(nil).WithMetadataTimeout(7*time.Second).WithSchemaAgreement(9*time.Second, time.Second).
					WithServerTimestamps().
					ConfigureDefaultProfile(func(p *config.ProfileBuilder) {
						p.WithConsistency(1).NoSerialConsistency().WithPageSize(10).NoTimeout().
							WithRetry(nil).WithSpeculation(reportSpeculation{})
					})
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
			name: "CustomRetry",
			build: func(b *config.ConfigBuilder) *config.ConfigBuilder {
				return b.ConfigureDefaultProfile(func(p *config.ProfileBuilder) { p.WithRetry(reportRetry{}) })
			},
			want: map[string]any{
				"query.retry.policy.type": "custom",
				"query.retry.policy.name": "reportRetry",
			},
		},
		{
			name: "CustomPolicies",
			build: func(b *config.ConfigBuilder) *config.ConfigBuilder {
				return b.WithReconnect(reportReconnect{}).ConfigureDefaultProfile(func(p *config.ProfileBuilder) {
					p.WithRetry(retry.Never{}).WithRouting(reportRouting{newReportRouting(t)})
				})
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
		t.Run(tc.name, func(t *testing.T) {
			raw := connectReport(t, func(address string) *config.ConfigBuilder { return tc.build(config.New(address)) })
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
		})
	}

	// Keys the driver never reports, and why. A key the schema adds fails
	// here until it is reported or listed.
	unreported := map[string]string{
		"connection.heartbeat":                                        "a placeholder with no properties in schema version 1",
		"connection.write.coalescing":                                 "a placeholder with no properties in schema version 1",
		"connection.node-preference":                                  "config.WithHostFilter is a function the report cannot see into",
		"connection.tls":                                              "the fake server speaks no TLS; TestDriverConfigReport/Typed covers it",
		"connection.tls.hostname-verification":                        "as connection.tls",
		"control-plane.queries.system.timeout.server-side-ms":         "the fake server is not ScyllaDB; TestDriverConfigReport/Typed covers it",
		"query.defaults.idempotence":                                  "always false, so not asserted; reported on every connection",
		"connection.reconnection.policy.delay-ms":                     "the builder offers no constant reconnection policy",
		"connection.reconnection.policy.max-attempts":                 "the builder offers no attempt-limited reconnection policy",
		"query.load-balancing.policy.description":                     "custom policies are reported by name only",
		"query.retry.policy.description":                              "custom policies are reported by name only",
		"query.speculative-execution.policy.description":              "custom policies are reported by name only",
		"query.speculative-execution.policy.delay-ms":                 "retry has no built-in speculation policy, so every one is custom",
		"query.speculative-execution.policy.max-executions":           "as query.speculative-execution.policy.delay-ms",
		"query.speculative-execution.policy.percentile":               "as query.speculative-execution.policy.delay-ms",
		"query.retry.backoff":                                         "no built-in retry policy waits between attempts",
		"query.load-balancing.node-preference":                        "covered with routing options by TestConfig/Report",
		"query.load-balancing.policy.adaptive-ordering":               "covered with routing options by TestConfig/Report",
		"query.load-balancing.policy.fallback-to-non-preferred-nodes": "reported on every connection",
	}
	for _, path := range schemaPaths(t) {
		if seen[path] {
			continue
		}
		if _, ok := unreported[path]; ok {
			continue
		}
		if slices.ContainsFunc(sortedKeys(unreported), func(p string) bool { return strings.HasPrefix(path, p+".") }) {
			continue
		}
		t.Errorf("schema key %s is neither reported for any builder configuration here nor listed as unreported", path)
	}
}

func newReportRouting(t *testing.T) routing.Policy {
	t.Helper()
	policy, err := routing.NewPolicy(routing.Options{TokenAware: true})
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

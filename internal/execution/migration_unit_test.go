/*
 * Copyright (C) 2026 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package execution

import (
	"reflect"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/settings"

	"github.com/scylladb/gocql/v2/retry"
)

type customRetry struct{}

func (*customRetry) Decide(retry.Attempt) retry.Decision {
	return retry.Decision{Action: retry.Stop}
}

func TestRetryConfigurationReport(t *testing.T) {
	for _, tc := range []struct{ attempts, want int }{{-1, 2}, {0, 2}, {1, 0}, {3, 2}, {7, 6}} {
		got := cluster.InternalBuildRetryPolicyReport(retry.Safe{MaxAttempts: tc.attempts})
		want := cluster.InternalRetryPolicySimpleReport{Type: "simple", MaxRetries: tc.want}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("attempts %d: got %#v want %#v", tc.attempts, got, want)
		}
	}
	for _, policy := range []retry.Policy{(*customRetry)(nil), &customRetry{}} {
		got := cluster.InternalBuildRetryPolicyReport(policy).(cluster.InternalRetryPolicyCustomReport)
		if got.Name != "customRetry" || got.Type != "custom" {
			t.Fatalf("custom report: %+v", got)
		}
	}
}

// scalarParity checks canonical validation against incremental conversion
// across every short read and every truncated scalar body.
func scalarParity[T any](t *testing.T, kind cqltype.Type, buffered codec.Decoder[T], incremental codec.StreamDecoder[T], bodies ...[]byte) {
	t.Helper()
	typ := foundationScalar(t, kind)
	for _, body := range bodies {
		for length := 0; length <= len(body); length++ {
			prefix := body[:length]
			encoded, err := codec.NewEncodedValue(typ, cqltype.Present, cqltype.BorrowBytes(prefix))
			if err != nil {
				t.Fatal(err)
			}
			want, wantErr := codec.DecodeValue(buffered, encoded)
			for step := 1; step <= len(prefix)+4; step++ {
				var got T
				err := streamDecodeFixture(t, typ, incremental, streamCell(prefix), &got, step)
				if (err == nil) != (wantErr == nil) {
					t.Fatalf("%s bytes %x split %d: stream %v buffered %v", kind, prefix, step, err, wantErr)
				}
				if err == nil && !reflect.DeepEqual(got, want) {
					t.Fatalf("%s bytes %x split %d: got %#v want %#v", kind, prefix, step, got, want)
				}
			}
		}
	}
}
func TestIncrementalScalarValidationParity(t *testing.T) {
	scalarParity(t, cqltype.TypeBoolean, codec.Bool, codec.StreamBool, []byte{0}, []byte{1}, []byte{2})
	scalarParity(t, cqltype.TypeDate, codec.DateCodec, codec.StreamDate, []byte{0, 0, 0, 0}, []byte{255, 255, 255, 255})
	scalarParity(t, cqltype.TypeTime, codec.TimeCodec, codec.StreamTime, []byte{255, 255, 255, 255, 255, 255, 255, 255}, []byte{0, 0, 78, 148, 145, 79, 0, 0})
	scalarParity(t, cqltype.TypeTimeUUID, codec.TimeUUIDCodec, codec.StreamTimeUUID, make([]byte, 16), []byte{0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	scalarParity(t, cqltype.TypeInet, codec.InetCodec, codec.StreamInet, []byte{127, 0, 0, 1}, make([]byte, 16))
	scalarParity(t, cqltype.TypeDuration, codec.DurationCodec, codec.StreamDuration,
		[]byte{0, 0, 0}, []byte{1, 2, 0}, []byte{128, 0, 0, 0}, // accepted nonminimal zero prefix
		[]byte{240, 255, 255, 255, 255, 240, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
		[]byte{248, 1, 0, 0, 0, 0, 0, 0}, // months overflow
		[]byte{0, 0, 0, 0},               // trailing byte
	)
}

func TestReportUsesCurrentTypedProfile(t *testing.T) {
	config := settings.DefaultConfig("127.0.0.1")
	profile := *config.DefaultProfile
	routing, err := implRouting.NewRoutingPolicy(implRouting.RoutingOptions{TokenAware: true, Datacenter: "dc", Rack: "rack", AllowRemote: true, Latency: implRouting.LatencyOptions{Enabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	profile.Routing = routing
	profile.Retry = retry.Safe{MaxAttempts: 7}
	profile.PageSize = 23
	config.DefaultProfile = &profile
	s := func() *cluster.Session {
		opaqueStorageValue := new(cluster.Session)
		implCluster.SessionData(opaqueStorageValue).InternalCfg = *cluster.InternalNewClusterSettings()
		implCluster.SessionData(opaqueStorageValue).InternalTypedState = &InternalSessionRuntime{InternalProfile: config.DefaultProfile, InternalConfig: config}
		return opaqueStorageValue
	}()
	report := cluster.InternalBuildQueryReport(s)
	if report.Defaults.Page.Size != 23 || report.Retry.Policy.(cluster.InternalRetryPolicySimpleReport).MaxRetries != 6 {
		t.Fatalf("typed defaults: %+v", report)
	}
	location := report.LoadBalancing.NodePreference.(cluster.InternalNodeLocationRackReport)
	if location.LocalDC != "dc" || location.LocalRack != "rack" {
		t.Fatalf("typed locality: %+v", location)
	}
	policy := report.LoadBalancing.Policy.(cluster.InternalLoadBalancingTokenAwareReport)
	if !policy.FallbackToNonPreferredNodes || policy.AdaptiveOrdering.Signals[0] != "latency" {
		t.Fatalf("typed routing: %+v", policy)
	}
	profile.PageSize = 37
	profile.Retry = retry.Safe{MaxAttempts: 1}
	config.DefaultProfile = &profile
	next := cluster.InternalBuildQueryReport(s)
	if next.Defaults.Page.Size != 37 || next.Retry.Policy.(cluster.InternalRetryPolicySimpleReport).MaxRetries != 0 {
		t.Fatalf("replaced profile: %+v", next)
	}
}

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

package hostpolicy

import (
	"fmt"
	"net/netip"
	"testing"

	"github.com/hailocab/go-hostpool"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	routing "github.com/scylladb/gocql/v2/routing"
)

func TestHostPool(t *testing.T) {
	t.Run("Plan", func(t *testing.T) {
		p := HostPool(hostpool.New(nil)).(*hostPoolHostPolicy)
		ids := []cql.UUID{cql.MustParseUUID("a0000000-0000-0000-0000-000000000000"), cql.MustParseUUID("a0000000-0000-0000-0000-000000000001")}
		hosts := []routing.HostSnapshot{{ID: ids[0], Address: netip.MustParseAddrPort("10.0.0.1:9042"), Enabled: true, Up: true}, {ID: ids[1], Address: netip.MustParseAddrPort("10.0.0.2:9042"), Enabled: true, Up: true}}
		p.setHosts(hosts, []routing.Route{implRouting.InternalRouteOf(routing.Target{HostID: ids[0]}), implRouting.InternalRouteOf(routing.Target{HostID: ids[1]})})
		plan := routing.NewPlan(p, routing.Info{}, nil)
		a, ok := plan.Next()
		if !ok || a.HostID() != ids[0] {
			t.Fatalf("first host: %+v %t", a, ok)
		}
		p.Observe(gocql.AttemptEvent{Target: a.Target()})
		b, ok := plan.Next()
		if !ok || b.HostID() != ids[1] {
			t.Fatalf("second host: %+v %t", b, ok)
		}
		p.Observe(gocql.AttemptEvent{Target: b.Target(), Err: fmt.Errorf("error")})
		if _, ok := plan.Next(); ok {
			t.Fatal("routing plan repeated a host")
		}
		next := routing.NewPlan(p, routing.Info{}, nil)
		a, ok = next.Next()
		if !ok || a.HostID() != ids[0] {
			t.Fatalf("healthy host: %+v %t", a, ok)
		}
		pinned := routing.NewPlan(p, routing.Info{HostID: cql.Option[cql.UUID]{Set: true, Value: ids[1]}, ShardID: cql.Option[uint32]{Set: true, Value: 2}}, nil)
		target, ok := pinned.Next()
		if !ok || target.HostID() != ids[1] || target.Shard != 2 {
			t.Fatalf("strict target: %+v %t", target, ok)
		}
		if _, ok := pinned.Next(); ok {
			t.Fatal("pinned plan repeated host")
		}
	})
}

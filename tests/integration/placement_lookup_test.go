//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package integration

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/exec"
	implSession "github.com/scylladb/gocql/v2/internal/session"
	"github.com/scylladb/gocql/v2/internal/tablets"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/routing"
)

// TestPlacementLookups checks the public placement lookups of a tablet
// table on a live cluster: TokenRanges' completeness, Replicas against
// TokenRanges, and a held snapshot's answers after the session forgets its
// tablets because more than 1024 tablet hints are pending.
func TestPlacementLookups(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	session := createV2Session(t)
	defer session.Close()
	supported, err := v2TabletsSupported(ctx, session)
	if err != nil {
		t.Fatal(err)
	}
	if !supported {
		t.Skip("Tablets are not supported by this server")
	}
	table := v2SchemaRoutingName(t)
	if err := v2CreateTable(ctx, session, fmt.Sprintf(`CREATE TABLE %s (pk int PRIMARY KEY, v int)`, table)); err != nil {
		t.Fatal(err)
	}
	prepared, err := session.Prepare(ctx, cql.Statement[[]cql.Value, cql.EmptyRow]{CQL: fmt.Sprintf(`INSERT INTO %s (pk, v) VALUES (?, 1)`, table), Params: cql.DynamicParams}, exec.PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	keyspace := prepared.Info().Keyspace
	learnTablets(ctx, t, session, prepared)

	t.Run("TabletTokenRanges", func(t *testing.T) {
		snapshot := session.Metadata()
		ranges, complete, err := snapshot.TokenRanges(keyspace, table)
		if err != nil {
			t.Fatal(err)
		}
		next, contiguous := int64(math.MinInt64), true
		for _, r := range ranges {
			contiguous = contiguous && r.Start == next
			next = r.End
		}
		contiguous = contiguous && len(ranges) > 0 && next == math.MaxInt64
		if complete != contiguous {
			t.Fatalf("TokenRanges reports complete %v for %d ranges that cover the ring: %v", complete, len(ranges), contiguous)
		}
		t.Logf("%d tablets known, complete %v", len(ranges), complete)
		for i, r := range ranges {
			got, err := snapshot.Replicas(keyspace, table, r.End)
			if err != nil || !slices.Equal(got, r.Replicas) {
				t.Fatalf("Replicas at the end of (%d, %d] = %v, %v; want %v", r.Start, r.End, got, err, r.Replicas)
			}
			// The token after the range, when no known tablet holds it,
			// has no known placement.
			if r.End < math.MaxInt64 && (i+1 == len(ranges) || ranges[i+1].Start != r.End) {
				if _, err := snapshot.Replicas(keyspace, table, r.End+1); !errors.Is(err, metadata.ErrPlacementUnknown) {
					t.Fatalf("Replicas of a token no known tablet holds: %v, want ErrPlacementUnknown", err)
				}
			}
		}
	})

	t.Run("ReplicasAfterFeedbackOverflow", func(t *testing.T) {
		held := session.Metadata()
		ranges, _, err := held.TokenRanges(keyspace, table)
		if err != nil {
			t.Fatal(err)
		}
		if len(ranges) == 0 {
			t.Skip("the session learned no tablet")
		}
		want, err := held.Replicas(keyspace, table, ranges[0].End)
		if err != nil || len(want) == 0 {
			t.Fatalf("Replicas before the overflow = %v, %v", want, err)
		}
		// Queue more than 1024 hints for tablets the cluster does not have,
		// faster than the session publishes them, which makes it forget
		// every tablet it knows.
		core := (*implSession.Session)(session).Core()
		host := tablets.HostUUID(want[0].HostID)
		info := routing.Info{Keyspace: keyspace, Table: table}
		before := core.Peripheral().InternalPlacementEpoch.Load()
		for i := range int64(1100) {
			hint, err := tablets.NewTabletInfo(keyspace, table, math.MaxInt64-2*(i+1), math.MaxInt64-2*i-1, []tablets.ReplicaInfo{tablets.NewReplicaInfo(host, 0)})
			if err != nil {
				t.Fatal(err)
			}
			core.QueueTabletFeedback(hint, core.CaptureTabletFeedback(info, core.Metadata()))
		}
		if core.Peripheral().InternalPlacementEpoch.Load() == before {
			// The worker kept up with the hints; move the epoch on as an
			// overflow does, so the held snapshot is stale either way.
			t.Log("the hints did not overflow the queue; forcing the placement epoch on")
			core.Peripheral().InternalPlacementEpoch.Add(1)
		}
		got, err := held.Replicas(keyspace, table, ranges[0].End)
		if err != nil || !slices.Equal(got, want) {
			t.Fatalf("held snapshot's Replicas after the overflow = %v, %v; want %v", got, err, want)
		}
		routes := routing.ReplicaRoutes(held, routing.Info{Keyspace: keyspace, Table: table, Token: cql.Some(ranges[0].End), Kind: cql.OrdinaryStatement})
		if len(routes) != len(want) {
			t.Fatalf("held snapshot's ReplicaRoutes after the overflow = %v, want %d routes", routes, len(want))
		}
		if again, _, err := held.TokenRanges(keyspace, table); err != nil || len(again) != len(ranges) {
			t.Fatalf("held snapshot's TokenRanges after the overflow: %d ranges, %v; want %d", len(again), err, len(ranges))
		}
	})
}

// learnTablets writes keys through prepared, pinned to every host and shard
// in turn so that requests reach replicas that do not own the key and the
// responses carry tablet hints, until the session knows a tablet or a
// minute passes.
func learnTablets(ctx context.Context, t *testing.T, session *gocql.Session, prepared *gocql.Prepared[[]cql.Value, cql.EmptyRow]) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for i := int32(0); time.Now().Before(deadline); i++ {
		options := exec.Options{Consistency: cql.Some(cql.One)}
		if hosts := session.Metadata().Hosts(); len(hosts) > 0 {
			h := hosts[int(i)%len(hosts)]
			options.HostID = cql.Some(h.HostID)
			if h.Shards > 0 {
				options.ShardID = cql.Some(uint32(i) % h.Shards)
				options.AnyShardFallback = true
			}
		}
		if _, err := prepared.Exec(ctx, []cql.Value{cql.IntValue(i)}, bindOptions(session, options)); err != nil {
			t.Fatal(err)
		}
		if i >= 256 && i%64 == 0 {
			if ranges, _, err := session.Metadata().TokenRanges(prepared.Info().Keyspace, prepared.Info().Table); err == nil && len(ranges) > 0 {
				return
			}
		}
	}
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"fmt"
	"testing"

	"github.com/scylladb/gocql/v2/internal/transport"
)

func TestShardAwarePort(t *testing.T) {
	t.Parallel()

	t.Run("Dial", func(t *testing.T) {
		t.Parallel()

		t.Run("MaliciousNAT", func(t *testing.T) {
			t.Parallel()

			testWithAndWithoutTLS(t, testShardAwarePortMaliciousNAT)
		})

		t.Run("NoReconnections", func(t *testing.T) {
			t.Parallel()

			testWithAndWithoutTLS(t, testShardAwarePortNoReconnections)
		})

		t.Run("Unreachable", func(t *testing.T) {
			t.Parallel()

			testWithAndWithoutTLS(t, testShardAwarePortUnreachable)
		})

		t.Run("UnusedIfNotEnabled", func(t *testing.T) {
			t.Parallel()

			testWithAndWithoutTLS(t, testShardAwarePortUnusedIfNotEnabled)
		})
	})

	t.Run("PortIterator", func(t *testing.T) {
		t.Parallel()

		for _shardCount := 1; _shardCount <= 64; _shardCount++ {
			shardCount := _shardCount
			t.Run(fmt.Sprintf("shard count %d", shardCount), func(t *testing.T) {
				for shardID := 0; shardID < shardCount; shardID++ {
					// Count by brute force ports that can be used to connect to requested shard
					expectedPortCount := 0
					for i := transport.InternalScyllaPortBasedBalancingMin; i <= transport.InternalScyllaPortBasedBalancingMax; i++ {
						if i%shardCount == shardID {
							expectedPortCount++
						}
					}

					// Enumerate all ports using the port iterator and assert various things
					iterator := transport.InternalNewScyllaPortIterator(shardID, shardCount)
					actualPortCount := 0
					previousPort := 0

					for {
						portU16, ok := iterator.Next()
						if !ok {
							break
						}

						port := int(portU16)

						if port < transport.InternalScyllaPortBasedBalancingMin || port > transport.InternalScyllaPortBasedBalancingMax {
							t.Errorf("expected port %d generated from iterator to be in range [%d..%d]",
								port, transport.InternalScyllaPortBasedBalancingMin, transport.InternalScyllaPortBasedBalancingMax)
						}

						if port <= previousPort {
							t.Errorf("expected port %d generated from iterator to be larger than the previous generated port %d",
								port, previousPort)
						}

						actualShardOfPort := transport.InternalScyllaShardForSourcePort(portU16, shardCount)
						if actualShardOfPort != shardID {
							t.Errorf("expected port %d returned from iterator to belong to shard %d, but belongs to %d",
								port, shardID, actualShardOfPort)
						}

						previousPort = port
						actualPortCount++
					}

					if expectedPortCount != actualPortCount {
						t.Errorf("expected port iterator to generate %d ports, but got %d",
							expectedPortCount, actualPortCount)
					}
				}
			})
		}
	})
}

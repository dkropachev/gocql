// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"context"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	implPool "github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func TestScyllaConnPicker(t *testing.T) {
	t.Run("HandleShardCountChange", func(t *testing.T) {
		tests := []struct {
			name             string
			initialShards    int
			newShards        int
			initialConns     []int // shard IDs of initial connections
			expectedMigrated int
			expectedClosed   int
		}{
			{
				name:             "shard increase from 4 to 8",
				initialShards:    4,
				newShards:        8,
				initialConns:     []int{0, 2, 3},
				expectedMigrated: 3, // All initial connections survive
				expectedClosed:   0,
			},
			{
				name:             "shard decrease from 8 to 4",
				initialShards:    8,
				newShards:        4,
				initialConns:     []int{0, 2, 5, 7},
				expectedMigrated: 2, // Only shards 0, 2 survive
				expectedClosed:   2, // Shards 5, 7 get closed
			},
			{
				name:             "no change same count",
				initialShards:    8,
				newShards:        8,
				initialConns:     []int{1, 3, 5},
				expectedMigrated: 4, // All initial connections survive + new one
				expectedClosed:   0,
			},
			{
				name:             "massive decrease from 16 to 2",
				initialShards:    16,
				newShards:        2,
				initialConns:     []int{0, 1, 5, 8, 12, 15},
				expectedMigrated: 2, // Only shards 0, 1 survive
				expectedClosed:   4, // Shards 5, 8, 12, 15 get closed
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				logger := &logging.InternalTestLogger{}
				picker := &implPool.InternalScyllaConnPicker{
					InternalLogger:                     logger,
					InternalDisableShardAwarePortUntil: new(atomic.Pointer[time.Time]),
					InternalHostId:                     tUUID(99),
					InternalAddress:                    "192.168.1.1:9042", // Regular port
					InternalConns:                      make([]*transport.Conn, tt.initialShards),
					InternalExcessConns:                make([]*transport.Conn, 0),
					InternalNrShards:                   tt.initialShards,
					InternalMsbIgnore:                  12,
					InternalNrConns:                    0,
					InternalPos:                        0,
					InternalLastAttemptedShard:         0,
					InternalShardAwarePortDisabled:     false,
					InternalExcessConnsLimitRate:       0.1,
				}

				var connectionsToCheck []*transport.Conn

				// Add initial connections
				for _, shardID := range tt.initialConns {
					conn := mockConnForPicker(shardID, tt.initialShards)
					err := picker.Put(conn)
					require.NoError(t, err)

					if shardID >= tt.newShards {
						connectionsToCheck = append(connectionsToCheck, conn)
					}
				}

				// Verify initial state
				assert.Equal(t, tt.initialShards, picker.InternalNrShards)
				assert.Equal(t, len(tt.initialConns), picker.InternalNrConns)

				// Execute topology change
				newConn := mockConnForPicker(0, tt.newShards)
				err := picker.Put(newConn)
				require.NoError(t, err)

				// Allow background goroutine to complete
				time.Sleep(50 * time.Millisecond)

				// Verify new topology
				assert.Equal(t, tt.newShards, picker.InternalNrShards)
				assert.Equal(t, len(picker.InternalConns), tt.newShards)

				// Count migrated connections
				migratedCount := 0
				for _, conn := range picker.InternalConns {
					if conn != nil {
						migratedCount++
					}
				}

				assert.Equal(t, tt.expectedMigrated, migratedCount)

				// Verify connections that should be closed are actually closed
				closedCount := 0
				for _, conn := range connectionsToCheck {
					if conn.Closed() {
						closedCount++
					}
				}

				assert.Equal(t, tt.expectedClosed, closedCount,
					"Expected %d connections to be closed, but %d were closed",
					tt.expectedClosed, closedCount)
			})
		}
	})

	t.Run("Pick", func(t *testing.T) {
		t.Parallel()

		t.Run("HammerNilToken", func(t *testing.T) {
			t.Parallel()

			s := implPool.InternalScyllaConnPicker{
				InternalNrShards:  4,
				InternalMsbIgnore: 12,
			}
			s.InternalConns = make([]*transport.Conn, 100)
			for i := range s.InternalConns {
				if i%7 == 0 {
					continue
				}
				s.InternalConns[i] = &transport.Conn{
					InternalStreams: streams.New(),
				}
			}

			n := runtime.GOMAXPROCS(0)
			loops := 10000 / n

			var wg sync.WaitGroup
			wg.Add(n)
			for i := 0; i < n; i++ {
				go hammerConnPicker(t, &wg, &s, loops)
			}
			wg.Wait()
		})

		t.Run("NilToken", func(t *testing.T) {
			t.Parallel()

			s := implPool.InternalScyllaConnPicker{
				InternalNrShards:  4,
				InternalMsbIgnore: 12,
			}

			t.Run("no conns", func(t *testing.T) {
				s.InternalConns = []*transport.Conn{{
					InternalStreams: streams.New(),
				}}
				if s.Pick(endpoint.Token(nil)) != s.InternalConns[0] {
					t.Fatal("expected connection")
				}
			})

			t.Run("one shard", func(t *testing.T) {
				s.InternalConns = []*transport.Conn{{
					InternalStreams: streams.New(),
				}}
				if s.Pick(endpoint.Token(nil)) != s.InternalConns[0] {
					t.Fatal("expected connection")
				}
			})

			t.Run("multiple shards", func(t *testing.T) {
				s.InternalConns = []*transport.Conn{nil, {
					InternalStreams: streams.New(),
				}}
				if s.Pick(endpoint.Token(nil)) != s.InternalConns[1] {
					t.Fatal("expected connection")
				}
				if s.Pick(endpoint.Token(nil)) != s.InternalConns[1] {
					t.Fatal("expected connection")
				}
			})

			t.Run("multiple shards no conns", func(t *testing.T) {
				s.InternalConns = []*transport.Conn{nil, nil}
				if s.Pick(endpoint.Token(nil)) != nil {
					t.Fatal("expected nil")
				}
				if s.Pick(endpoint.Token(nil)) != nil {
					t.Fatal("expected nil")
				}
			})
		})

		t.Run("RandomConn", func(t *testing.T) {
			t.Parallel()

			t.Run("max iterations", func(t *testing.T) {
				s := &implPool.InternalScyllaConnPicker{
					InternalNrShards:  4,
					InternalMsbIgnore: 12,
					InternalPos:       math.MaxUint64,
					InternalConns:     []*transport.Conn{nil, mockConn(1)},
				}

				if s.Pick(endpoint.Token(nil)) == nil {
					t.Fatal("expected connection")
				}
			})

			t.Run("async access of max iterations", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				s := &implPool.InternalScyllaConnPicker{
					InternalNrShards:  4,
					InternalMsbIgnore: 12,
					InternalPos:       math.MaxUint64,
					InternalConns:     []*transport.Conn{nil, mockConn(1)},
				}

				var wg sync.WaitGroup
				connCh := make(chan *transport.Conn, 9)
				for i := 0; i < 3; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for i := 0; i < 3; i++ {
							select {
							case connCh <- s.Pick(endpoint.Token(nil)):
							case <-ctx.Done():
							}
						}
					}()
				}
				wg.Wait()
				close(connCh)

				if s.InternalPos != 8 {
					t.Fatalf("expected position to be 8 | actual %d", s.InternalPos)
				}
				if len(connCh) != 9 {
					t.Fatalf("expected 9 connection picks, got %d", len(connCh))
				}
				for conn := range connCh {
					if conn == nil {
						t.Fatal("expected connection, got nil")
					}
				}
			})
		})
	})

	// TestScyllaConnPicker/PickInt64 verifies the raw-int64 fast path
	// PickInt64 selects exactly the same shard-aware connection as the boxed Pick.
	t.Run("PickInt64", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			name      string
			nrShards  int
			msbIgnore uint64
			tokens    []int64
		}{
			{"four-shards", 4, 12, []int64{math.MinInt64, -123456789, 0, 123456789, math.MaxInt64}},
			{"eight-shards", 8, 0, []int64{-100000, 42, 999999999999}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				s := implPool.InternalScyllaConnPicker{InternalNrShards: tc.nrShards, InternalMsbIgnore: tc.msbIgnore}
				s.InternalConns = make([]*transport.Conn, tc.nrShards)
				for i := range s.InternalConns {
					s.InternalConns[i] = &transport.Conn{InternalStreams: streams.New()}
				}

				for _, tok := range tc.tokens {
					tt := endpoint.InternalInt64Token(tok)
					want := s.Pick(tt)
					got := s.PickInt64(tok)
					if got != want {
						t.Fatalf("token %d: PickInt64=%p, want %p (Pick)", tok, got, want)
					}
					if shard := s.InternalShardOf(tt); got != s.InternalConns[shard] {
						t.Fatalf("token %d: shard %d conn %p, got %p", tok, shard, s.InternalConns[shard], got)
					}
				}
			})
		}
	})

	t.Run("Remove", func(t *testing.T) {
		t.Parallel()

		s := implPool.InternalScyllaConnPicker{
			InternalNrShards:  4,
			InternalMsbIgnore: 12,
		}

		conn := mockConn(0)
		s.Put(conn)
		s.Put(mockConn(1))

		if s.InternalNrConns != 2 {
			t.Error("added 2 connections, expected connection count to be 2")
		}

		s.Remove(conn)
		if s.InternalNrConns != 1 {
			t.Errorf("removed 1 connection, expected connection count to be 1 but was %d", s.InternalNrConns)
		}

		if s.InternalConns[0] != nil {
			t.Errorf("Expected %v to be removed from it's position", conn)
		}
	})

	t.Run("ShardOf", func(t *testing.T) {
		t.Parallel()

		s := implPool.InternalScyllaConnPicker{
			InternalNrShards:  4,
			InternalMsbIgnore: 12,
		}
		for _, test := range scyllaShardOfTests {
			if shard := s.InternalShardOf(endpoint.InternalInt64Token(test.token)); shard != test.shard {
				t.Errorf("wrong scylla shard calculated for token %d, expected %d, got %d", test.token, test.shard, shard)
			}
		}
	})
}

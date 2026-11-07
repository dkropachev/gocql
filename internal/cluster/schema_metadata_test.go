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

package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/metadata"
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

func TestSchemaMetadata(t *testing.T) {
	t.Run("ConcurrentRefresh", func(t *testing.T) {
		t.Parallel()

		// TestSchemaMetadata/ConcurrentRefresh/ErrorHandling verifies that concurrent
		// GetKeyspace and GetTable calls behave correctly when the underlying
		// schema queries succeed or fail, including mixed scenarios where
		// errors are injected mid-flight.
		t.Run("ErrorHandling", func(t *testing.T) {
			t.Parallel()

			const concurrency = 10

			defaultTables := map[string][]tableInfo{
				"test_ks": {
					{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
					{name: "tbl_b", columns: []columnInfo{{name: "pk", kind: "partition_key", position: 0}}},
				},
			}

			t.Run("GetKeyspace/all_succeed", func(t *testing.T) {
				t.Parallel()
				ctrl := &schemaDataMock{
					knownKeyspaces: defaultTables,
					queryDelay:     10 * time.Millisecond,
				}
				s := newSchemaEventTestSessionWithMock(ctrl)
				defer s.Close()

				var wg sync.WaitGroup
				results := make([]*metadata.KeyspaceMetadata, concurrency)
				errs := make([]error, concurrency)
				for i := range concurrency {
					wg.Add(1)
					go func(idx int) {
						defer wg.Done()
						results[idx], errs[idx] = SessionData(s).InternalMetadataDescriber.GetKeyspace("test_ks")
					}(i)
				}
				wg.Wait()

				for i := range concurrency {
					if errs[i] != nil {
						t.Errorf("goroutine %d: unexpected error: %v", i, errs[i])
					}
					if results[i] == nil {
						t.Errorf("goroutine %d: got nil metadata", i)
					} else if results[i].Name != "test_ks" {
						t.Errorf("goroutine %d: expected keyspace test_ks, got %s", i, results[i].Name)
					}
				}
			})

			t.Run("GetKeyspace/all_fail", func(t *testing.T) {
				t.Parallel()
				injectedErr := fmt.Errorf("injected query failure")
				ctrl := &schemaDataMock{
					knownKeyspaces: defaultTables,
					queryDelay:     10 * time.Millisecond,
					queryError:     injectedErr,
				}
				s := newSchemaEventTestSessionWithMock(ctrl)
				defer s.Close()

				var wg sync.WaitGroup
				errs := make([]error, concurrency)
				for i := range concurrency {
					wg.Add(1)
					go func(idx int) {
						defer wg.Done()
						_, errs[idx] = SessionData(s).InternalMetadataDescriber.GetKeyspace("test_ks")
					}(i)
				}
				wg.Wait()

				for i := range concurrency {
					if errs[i] == nil {
						t.Errorf("goroutine %d: expected error, got nil", i)
					}
				}
			})

			t.Run("GetKeyspace/fail_then_succeed", func(t *testing.T) {
				t.Parallel()
				// First wave fails, second wave succeeds — verifies singleflight
				// does not cache the error permanently.
				injectedErr := fmt.Errorf("transient failure")
				ctrl := &schemaDataMock{
					knownKeyspaces: defaultTables,
					queryDelay:     10 * time.Millisecond,
					queryError:     injectedErr,
				}
				s := newSchemaEventTestSessionWithMock(ctrl)
				defer s.Close()

				// Wave 1: all fail.
				var wg sync.WaitGroup
				for range concurrency {
					wg.Add(1)
					go func() {
						defer wg.Done()
						_, _ = SessionData(s).InternalMetadataDescriber.GetKeyspace("test_ks")
					}()
				}
				wg.Wait()

				// Clear error and retry — should succeed.
				ctrl.setQueryError(nil)
				ctrl.resetQueries()

				ks, err := SessionData(s).InternalMetadataDescriber.GetKeyspace("test_ks")
				if err != nil {
					t.Fatalf("second attempt should succeed, got: %v", err)
				}
				if ks.Name != "test_ks" {
					t.Fatalf("expected keyspace test_ks, got %s", ks.Name)
				}
			})

			t.Run("GetKeyspace/nonexistent_keyspace", func(t *testing.T) {
				t.Parallel()
				ctrl := &schemaDataMock{
					knownKeyspaces: defaultTables,
					queryDelay:     10 * time.Millisecond,
				}
				s := newSchemaEventTestSessionWithMock(ctrl)
				defer s.Close()

				var wg sync.WaitGroup
				errs := make([]error, concurrency)
				for i := range concurrency {
					wg.Add(1)
					go func(idx int) {
						defer wg.Done()
						_, errs[idx] = SessionData(s).InternalMetadataDescriber.GetKeyspace("no_such_ks")
					}(i)
				}
				wg.Wait()

				for i := range concurrency {
					if errs[i] == nil {
						t.Errorf("goroutine %d: expected ErrKeyspaceDoesNotExist, got nil", i)
					}
				}
			})

			t.Run("GetTable/all_succeed", func(t *testing.T) {
				t.Parallel()
				ctrl := &schemaDataMock{
					knownKeyspaces: defaultTables,
					queryDelay:     10 * time.Millisecond,
				}
				s := newSchemaEventTestSessionWithMock(ctrl)
				defer s.Close()
				populateKeyspace(s, "test_ks", "tbl_a")
				SessionData(s).InternalMetadataDescriber.InternalInvalidateTableSchema("test_ks", "tbl_a")

				var wg sync.WaitGroup
				results := make([]*metadata.TableMetadata, concurrency)
				errs := make([]error, concurrency)
				for i := range concurrency {
					wg.Add(1)
					go func(idx int) {
						defer wg.Done()
						results[idx], errs[idx] = SessionData(s).InternalMetadataDescriber.GetTable("test_ks", "tbl_a")
					}(i)
				}
				wg.Wait()

				for i := range concurrency {
					if errs[i] != nil {
						t.Errorf("goroutine %d: unexpected error: %v", i, errs[i])
					}
					if results[i] == nil {
						t.Errorf("goroutine %d: got nil table metadata", i)
					} else if results[i].Name != "tbl_a" {
						t.Errorf("goroutine %d: expected tbl_a, got %s", i, results[i].Name)
					}
				}
			})

			t.Run("GetTable/all_fail", func(t *testing.T) {
				t.Parallel()
				injectedErr := fmt.Errorf("injected table query failure")
				ctrl := &schemaDataMock{
					knownKeyspaces: defaultTables,
					queryError:     injectedErr,
					queryDelay:     10 * time.Millisecond,
				}
				s := newSchemaEventTestSessionWithMock(ctrl)
				defer s.Close()
				populateKeyspace(s, "test_ks", "tbl_a")
				SessionData(s).InternalMetadataDescriber.InternalInvalidateTableSchema("test_ks", "tbl_a")

				var wg sync.WaitGroup
				errs := make([]error, concurrency)
				for i := range concurrency {
					wg.Add(1)
					go func(idx int) {
						defer wg.Done()
						_, errs[idx] = SessionData(s).InternalMetadataDescriber.GetTable("test_ks", "tbl_a")
					}(i)
				}
				wg.Wait()

				for i := range concurrency {
					if errs[i] == nil {
						t.Errorf("goroutine %d: expected error, got nil", i)
					}
				}
			})

			t.Run("GetTable/fail_then_succeed", func(t *testing.T) {
				t.Parallel()
				injectedErr := fmt.Errorf("transient table failure")
				ctrl := &schemaDataMock{
					knownKeyspaces: defaultTables,
					queryDelay:     10 * time.Millisecond,
					queryError:     injectedErr,
				}
				s := newSchemaEventTestSessionWithMock(ctrl)
				defer s.Close()
				populateKeyspace(s, "test_ks", "tbl_a")
				SessionData(s).InternalMetadataDescriber.InternalInvalidateTableSchema("test_ks", "tbl_a")

				// Wave 1: all fail.
				var wg sync.WaitGroup
				for range concurrency {
					wg.Add(1)
					go func() {
						defer wg.Done()
						_, _ = SessionData(s).InternalMetadataDescriber.GetTable("test_ks", "tbl_a")
					}()
				}
				wg.Wait()

				// Clear error, re-invalidate (the failed refresh may have left
				// tablesInvalidated in an inconsistent state), and retry.
				ctrl.setQueryError(nil)
				ctrl.resetQueries()
				SessionData(s).InternalMetadataDescriber.InternalInvalidateTableSchema("test_ks", "tbl_a")

				tbl, err := SessionData(s).InternalMetadataDescriber.GetTable("test_ks", "tbl_a")
				if err != nil {
					t.Fatalf("second attempt should succeed, got: %v", err)
				}
				if tbl.Name != "tbl_a" {
					t.Fatalf("expected tbl_a, got %s", tbl.Name)
				}
			})

			t.Run("GetTable/nonexistent_table", func(t *testing.T) {
				t.Parallel()
				ctrl := &schemaDataMock{
					knownKeyspaces: defaultTables,
					queryDelay:     10 * time.Millisecond,
				}
				s := newSchemaEventTestSessionWithMock(ctrl)
				defer s.Close()
				populateKeyspace(s, "test_ks", "tbl_a")

				var wg sync.WaitGroup
				errs := make([]error, concurrency)
				for i := range concurrency {
					wg.Add(1)
					go func(idx int) {
						defer wg.Done()
						_, errs[idx] = SessionData(s).InternalMetadataDescriber.GetTable("test_ks", "no_such_table")
					}(i)
				}
				wg.Wait()

				for i := range concurrency {
					if errs[i] == nil {
						t.Errorf("goroutine %d: expected ErrNotFound, got nil", i)
					}
				}
			})

			t.Run("GetKeyspace_and_GetTable/concurrent_mixed", func(t *testing.T) {
				t.Parallel()
				// Exercises the interplay between concurrent keyspace and table
				// refreshes hitting the singleflight groups simultaneously.
				ctrl := &schemaDataMock{
					knownKeyspaces: defaultTables,
					queryDelay:     5 * time.Millisecond,
				}
				s := newSchemaEventTestSessionWithMock(ctrl)
				defer s.Close()

				var wg sync.WaitGroup
				ksErrs := make([]error, concurrency)
				tblErrs := make([]error, concurrency)
				for i := range concurrency {
					wg.Add(2)
					go func(idx int) {
						defer wg.Done()
						_, ksErrs[idx] = SessionData(s).InternalMetadataDescriber.GetKeyspace("test_ks")
					}(i)
					go func(idx int) {
						defer wg.Done()
						_, tblErrs[idx] = SessionData(s).InternalMetadataDescriber.GetTable("test_ks", "tbl_a")
					}(i)
				}
				wg.Wait()

				for i := range concurrency {
					if ksErrs[i] != nil {
						t.Errorf("GetKeyspace goroutine %d: unexpected error: %v", i, ksErrs[i])
					}
					if tblErrs[i] != nil {
						t.Errorf("GetTable goroutine %d: unexpected error: %v", i, tblErrs[i])
					}
				}
			})

			t.Run("GetTable/different_tables_concurrent", func(t *testing.T) {
				t.Parallel()
				// Two different tables invalidated concurrently: each gets its own
				// singleflight key, so both refresh independently.
				ctrl := &schemaDataMock{
					knownKeyspaces: defaultTables,
					queryDelay:     5 * time.Millisecond,
				}
				s := newSchemaEventTestSessionWithMock(ctrl)
				defer s.Close()
				populateKeyspace(s, "test_ks", "tbl_a", "tbl_b")
				SessionData(s).InternalMetadataDescriber.InternalInvalidateTableSchema("test_ks", "tbl_a")
				SessionData(s).InternalMetadataDescriber.InternalInvalidateTableSchema("test_ks", "tbl_b")

				var wg sync.WaitGroup
				aErrs := make([]error, concurrency)
				bErrs := make([]error, concurrency)
				for i := range concurrency {
					wg.Add(2)
					go func(idx int) {
						defer wg.Done()
						_, aErrs[idx] = SessionData(s).InternalMetadataDescriber.GetTable("test_ks", "tbl_a")
					}(i)
					go func(idx int) {
						defer wg.Done()
						_, bErrs[idx] = SessionData(s).InternalMetadataDescriber.GetTable("test_ks", "tbl_b")
					}(i)
				}
				wg.Wait()

				for i := range concurrency {
					if aErrs[i] != nil {
						t.Errorf("tbl_a goroutine %d: unexpected error: %v", i, aErrs[i])
					}
					if bErrs[i] != nil {
						t.Errorf("tbl_b goroutine %d: unexpected error: %v", i, bErrs[i])
					}
				}

				// Verify both tables are now cached.
				for _, name := range []string{"tbl_a", "tbl_b"} {
					tbl, err := SessionData(s).InternalMetadataDescriber.GetTable("test_ks", name)
					if err != nil {
						t.Errorf("GetTable(%s) after refresh: %v", name, err)
					} else if tbl.Name != name {
						t.Errorf("expected %s, got %s", name, tbl.Name)
					}
				}
			})

			t.Run("GetTable/different_tables_one_fails", func(t *testing.T) {
				t.Parallel()
				// tbl_a exists in the mock, tbl_x does not — concurrent refreshes
				// for both: one succeeds, one gets ErrNotFound.
				ctrl := &schemaDataMock{
					knownKeyspaces: defaultTables,
					queryDelay:     5 * time.Millisecond,
				}
				s := newSchemaEventTestSessionWithMock(ctrl)
				defer s.Close()
				populateKeyspace(s, "test_ks", "tbl_a", "tbl_x")
				SessionData(s).InternalMetadataDescriber.InternalInvalidateTableSchema("test_ks", "tbl_a")
				SessionData(s).InternalMetadataDescriber.InternalInvalidateTableSchema("test_ks", "tbl_x")

				var wg sync.WaitGroup
				aErrs := make([]error, concurrency)
				xErrs := make([]error, concurrency)
				for i := range concurrency {
					wg.Add(2)
					go func(idx int) {
						defer wg.Done()
						_, aErrs[idx] = SessionData(s).InternalMetadataDescriber.GetTable("test_ks", "tbl_a")
					}(i)
					go func(idx int) {
						defer wg.Done()
						_, xErrs[idx] = SessionData(s).InternalMetadataDescriber.GetTable("test_ks", "tbl_x")
					}(i)
				}
				wg.Wait()

				for i := range concurrency {
					if aErrs[i] != nil {
						t.Errorf("tbl_a goroutine %d: unexpected error: %v", i, aErrs[i])
					}
					if xErrs[i] == nil {
						t.Errorf("tbl_x goroutine %d: expected ErrNotFound, got nil", i)
					}
				}
			})
		})

		// TestSchemaMetadata/ConcurrentRefresh/SingleFetch validates that concurrent GetKeyspace/GetTable
		// calls for an uncached or invalidated keyspace result in only a single set
		// of schema queries, not one per caller.
		t.Run("SingleFetch", func(t *testing.T) {
			t.Parallel()

			const concurrency = 10

			knownKeyspaces := map[string][]tableInfo{
				"test_ks": {
					{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
				},
			}

			fullRefreshCount := 7  // keyspace + tables + columns + types + indexes + views + DESCRIBE
			tableRefreshCount := 4 // tables + columns + indexes + views (filtered by table_name)

			t.Run("GetKeyspace/uncached", func(t *testing.T) {
				t.Parallel()
				ctrl := &schemaDataMock{
					knownKeyspaces: knownKeyspaces,
					queryDelay:     10 * time.Millisecond,
				}
				s := newSchemaEventTestSessionWithMock(ctrl)
				defer s.Close()

				var wg sync.WaitGroup
				for range concurrency {
					wg.Add(1)
					go func() {
						defer wg.Done()
						_, _ = SessionData(s).InternalMetadataDescriber.GetKeyspace("test_ks")
					}()
				}
				wg.Wait()

				if got := ctrl.getQueryCount(); got != fullRefreshCount {
					t.Errorf("expected %d queries (single full refresh), got %d", fullRefreshCount, got)
				}
			})

			t.Run("GetKeyspace/after_invalidation", func(t *testing.T) {
				t.Parallel()
				ctrl := &schemaDataMock{
					knownKeyspaces: knownKeyspaces,
					queryDelay:     10 * time.Millisecond,
				}
				s := newSchemaEventTestSessionWithMock(ctrl)
				defer s.Close()
				populateKeyspace(s, "test_ks", "tbl_a")
				InternalSessionHandleSchemaEvent(s, []protocol.Frame{
					&frm.SchemaChangeKeyspace{Change: "UPDATED", Keyspace: "test_ks"},
				})

				ctrl.resetQueries()

				var wg sync.WaitGroup
				for range concurrency {
					wg.Add(1)
					go func() {
						defer wg.Done()
						_, _ = SessionData(s).InternalMetadataDescriber.GetKeyspace("test_ks")
					}()
				}
				wg.Wait()

				if got := ctrl.getQueryCount(); got != fullRefreshCount {
					t.Errorf("expected %d queries (single full refresh), got %d", fullRefreshCount, got)
				}
			})

			t.Run("GetTable/after_table_invalidation", func(t *testing.T) {
				t.Parallel()
				ctrl := &schemaDataMock{
					knownKeyspaces: knownKeyspaces,
					queryDelay:     10 * time.Millisecond,
				}
				s := newSchemaEventTestSessionWithMock(ctrl)
				defer s.Close()
				populateKeyspace(s, "test_ks", "tbl_a")
				InternalSessionHandleSchemaEvent(s, []protocol.Frame{
					&frm.SchemaChangeTable{Change: "UPDATED", Keyspace: "test_ks", Object: "tbl_a"},
				})

				ctrl.resetQueries()

				var wg sync.WaitGroup
				for range concurrency {
					wg.Add(1)
					go func() {
						defer wg.Done()
						_, _ = SessionData(s).InternalMetadataDescriber.GetTable("test_ks", "tbl_a")
					}()
				}
				wg.Wait()

				if got := ctrl.getQueryCount(); got != tableRefreshCount {
					t.Errorf("expected %d queries (single table refresh), got %d", tableRefreshCount, got)
				}
			})

			t.Run("GetTable/stale_snapshot_after_refresh_does_not_refresh_twice", func(t *testing.T) {
				t.Parallel()
				ctrl := &schemaDataMock{
					knownKeyspaces: knownKeyspaces,
				}
				s := newSchemaEventTestSessionWithMock(ctrl)
				defer s.Close()
				populateKeyspace(s, "test_ks", "tbl_a")
				InternalSessionHandleSchemaEvent(s, []protocol.Frame{
					&frm.SchemaChangeTable{Change: "UPDATED", Keyspace: "test_ks", Object: "tbl_a"},
				})

				staleKeyspace, wasReloaded, err := SessionData(s).InternalMetadataDescriber.InternalGetKeyspaceInternal("test_ks")
				if err != nil {
					t.Fatalf("getKeyspaceInternal returned unexpected error: %v", err)
				}
				if _, found := staleKeyspace.Tables["tbl_a"]; found {
					t.Fatal("expected stale keyspace snapshot to have invalidated table removed")
				}

				if err := SessionData(s).InternalMetadataDescriber.InternalDeduplicatedRefreshTable("test_ks", "tbl_a"); err != nil {
					t.Fatalf("deduplicatedRefreshTable returned unexpected error: %v", err)
				}

				ctrl.resetQueries()

				tbl, refreshNeeded, err := SessionData(s).InternalMetadataDescriber.InternalGetTableFromSnapshot("test_ks", "tbl_a", staleKeyspace, wasReloaded)
				if err != nil {
					t.Fatalf("getTableFromSnapshot returned unexpected error: %v", err)
				}
				if refreshNeeded {
					t.Fatal("expected latest published keyspace metadata to suppress an extra refresh")
				}
				if tbl == nil || tbl.Name != "tbl_a" {
					t.Fatalf("unexpected table metadata: %#v", tbl)
				}
				if got := ctrl.getQueryCount(); got != 0 {
					t.Fatalf("expected stale snapshot lookup to avoid extra queries, got %d", got)
				}
			})

			// A caller that found the keyspace missing but reaches the flight only
			// after another load published and completed must not refresh again.
			t.Run("GetKeyspace/late_caller_after_load_does_not_refresh_twice", func(t *testing.T) {
				t.Parallel()
				ctrl := &schemaDataMock{
					knownKeyspaces: knownKeyspaces,
				}
				s := newSchemaEventTestSessionWithMock(ctrl)
				defer s.Close()
				describer := SessionData(s).InternalMetadataDescriber
				if _, err := describer.GetKeyspace("test_ks"); err != nil {
					t.Fatalf("GetKeyspace returned unexpected error: %v", err)
				}

				ctrl.resetQueries()

				if err := describer.deduplicatedRefreshKeyspace("test_ks"); err != nil {
					t.Fatalf("deduplicatedRefreshKeyspace returned unexpected error: %v", err)
				}
				if got := ctrl.getQueryCount(); got != 0 {
					t.Fatalf("expected a late load to reuse the published keyspace, got %d queries", got)
				}
			})
		})
	})

	t.Run("HandleSchemaEvent", func(t *testing.T) {
		t.Parallel()

		t.Run("cache_state", func(t *testing.T) {
			t.Parallel()
			tests := []struct {
				name           string
				keyspaces      map[string][]string // ks → tables to pre-populate
				tablets        [][2]string         // (ks, table) pairs to add tablets for
				event          protocol.Frame
				wantKsGone     []string    // keyspaces removed from cache
				wantKsPresent  []string    // keyspaces still in cache
				wantTblGone    [][2]string // (ks, table) removed from Tables map
				wantTblPresent [][2]string // (ks, table) still in Tables map
				wantTblInvalid [][2]string // (ks, table) in tablesInvalidated
				wantTablets    int         // expected tablet count; -1 to skip check
			}{
				{
					name:        "keyspace/CREATED clears cache",
					keyspaces:   map[string][]string{"test_ks": {"tbl_a", "tbl_b"}},
					event:       &frm.SchemaChangeKeyspace{Change: "CREATED", Keyspace: "test_ks"},
					wantKsGone:  []string{"test_ks"},
					wantTablets: -1,
				},
				{
					name:        "keyspace/UPDATED clears cache and removes tablets",
					keyspaces:   map[string][]string{"test_ks": {"tbl_a"}},
					tablets:     [][2]string{{"test_ks", "tbl_a"}},
					event:       &frm.SchemaChangeKeyspace{Change: "UPDATED", Keyspace: "test_ks"},
					wantKsGone:  []string{"test_ks"},
					wantTablets: 0,
				},
				{
					name:        "keyspace/DROPPED clears cache and removes tablets",
					keyspaces:   map[string][]string{"test_ks": {"tbl_a"}},
					tablets:     [][2]string{{"test_ks", "tbl_a"}},
					event:       &frm.SchemaChangeKeyspace{Change: "DROPPED", Keyspace: "test_ks"},
					wantKsGone:  []string{"test_ks"},
					wantTablets: 0,
				},
				{
					name:        "keyspace/CREATED does not remove tablets",
					keyspaces:   map[string][]string{"test_ks": {"tbl_a"}},
					tablets:     [][2]string{{"test_ks", "tbl_a"}},
					event:       &frm.SchemaChangeKeyspace{Change: "CREATED", Keyspace: "test_ks"},
					wantKsGone:  []string{"test_ks"},
					wantTablets: 2,
				},
				{
					name:           "table/CREATED invalidates table only",
					keyspaces:      map[string][]string{"test_ks": {"tbl_a", "tbl_b"}},
					event:          &frm.SchemaChangeTable{Change: "CREATED", Keyspace: "test_ks", Object: "tbl_a"},
					wantKsPresent:  []string{"test_ks"},
					wantTblGone:    [][2]string{{"test_ks", "tbl_a"}},
					wantTblPresent: [][2]string{{"test_ks", "tbl_b"}},
					wantTblInvalid: [][2]string{{"test_ks", "tbl_a"}},
					wantTablets:    -1,
				},
				{
					name:           "table/UPDATED invalidates table and removes tablets",
					keyspaces:      map[string][]string{"test_ks": {"tbl_a", "tbl_b"}},
					tablets:        [][2]string{{"test_ks", "tbl_a"}},
					event:          &frm.SchemaChangeTable{Change: "UPDATED", Keyspace: "test_ks", Object: "tbl_a"},
					wantKsPresent:  []string{"test_ks"},
					wantTblGone:    [][2]string{{"test_ks", "tbl_a"}},
					wantTblPresent: [][2]string{{"test_ks", "tbl_b"}},
					wantTablets:    0,
				},
				{
					name:        "table/DROPPED removes tablets",
					keyspaces:   map[string][]string{"test_ks": {"tbl_a"}},
					tablets:     [][2]string{{"test_ks", "tbl_a"}},
					event:       &frm.SchemaChangeTable{Change: "DROPPED", Keyspace: "test_ks", Object: "tbl_a"},
					wantTablets: 0,
				},
				{
					name:        "table/CREATED does not remove tablets",
					keyspaces:   map[string][]string{"test_ks": {"tbl_a"}},
					tablets:     [][2]string{{"test_ks", "tbl_a"}},
					event:       &frm.SchemaChangeTable{Change: "CREATED", Keyspace: "test_ks", Object: "tbl_a"},
					wantTablets: 2,
				},
				{
					name:        "type/CREATED clears entire keyspace",
					keyspaces:   map[string][]string{"test_ks": {"tbl_a", "tbl_b"}},
					event:       &frm.SchemaChangeType{Change: "CREATED", Keyspace: "test_ks", Object: "my_type"},
					wantKsGone:  []string{"test_ks"},
					wantTablets: -1,
				},
				{
					name:        "function/CREATED clears entire keyspace",
					keyspaces:   map[string][]string{"test_ks": {"tbl_a"}},
					event:       &frm.SchemaChangeFunction{Change: "CREATED", Keyspace: "test_ks", Name: "fn", Args: []string{"int"}},
					wantKsGone:  []string{"test_ks"},
					wantTablets: -1,
				},
				{
					name:        "aggregate/CREATED clears entire keyspace",
					keyspaces:   map[string][]string{"test_ks": {"tbl_a"}},
					event:       &frm.SchemaChangeAggregate{Change: "CREATED", Keyspace: "test_ks", Name: "agg", Args: []string{"int"}},
					wantKsGone:  []string{"test_ks"},
					wantTablets: -1,
				},
				// Cross-isolation
				{
					name:           "keyspace/DROPPED does not affect other keyspace",
					keyspaces:      map[string][]string{"ks_a": {"tbl_a"}, "ks_b": {"tbl_b"}},
					event:          &frm.SchemaChangeKeyspace{Change: "DROPPED", Keyspace: "ks_a"},
					wantKsGone:     []string{"ks_a"},
					wantKsPresent:  []string{"ks_b"},
					wantTblPresent: [][2]string{{"ks_b", "tbl_b"}},
					wantTablets:    -1,
				},
				{
					name:           "table/UPDATED does not affect other tables",
					keyspaces:      map[string][]string{"test_ks": {"tbl_a", "tbl_b", "tbl_c"}},
					event:          &frm.SchemaChangeTable{Change: "UPDATED", Keyspace: "test_ks", Object: "tbl_a"},
					wantKsPresent:  []string{"test_ks"},
					wantTblGone:    [][2]string{{"test_ks", "tbl_a"}},
					wantTblPresent: [][2]string{{"test_ks", "tbl_b"}, {"test_ks", "tbl_c"}},
					wantTablets:    -1,
				},
				// Tablet cross-isolation
				{
					name:        "table/DROPPED for different table keeps tablets",
					keyspaces:   map[string][]string{"test_ks": {"tbl_a", "tbl_b"}},
					tablets:     [][2]string{{"test_ks", "tbl_a"}},
					event:       &frm.SchemaChangeTable{Change: "DROPPED", Keyspace: "test_ks", Object: "tbl_b"},
					wantTablets: 2,
				},
				{
					name:        "keyspace/DROPPED for different keyspace keeps tablets",
					keyspaces:   map[string][]string{"ks_a": {"tbl_a"}, "ks_b": {"tbl_b"}},
					tablets:     [][2]string{{"ks_a", "tbl_a"}},
					event:       &frm.SchemaChangeKeyspace{Change: "DROPPED", Keyspace: "ks_b"},
					wantTablets: 2,
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
					s := newSchemaEventTestSessionWithMock(ctrl)
					defer s.Close()
					for ks, tables := range tt.keyspaces {
						populateKeyspace(s, ks, tables...)
					}
					for _, tb := range tt.tablets {
						addTestTablets(t, s, tb[0], tb[1])
					}
					InternalSessionHandleSchemaEvent(s, []protocol.Frame{tt.event})
					MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalTabletsMetadata.Flush()

					for _, ks := range tt.wantKsGone {
						if _, found := MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalGetKeyspace(ks); found {
							t.Errorf("keyspace %q should have been removed from cache", ks)
						}
					}
					for _, ks := range tt.wantKsPresent {
						if _, found := MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalGetKeyspace(ks); !found {
							t.Errorf("keyspace %q should still be in cache", ks)
						}
					}
					for _, pair := range tt.wantTblGone {
						ks, _ := MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalGetKeyspace(pair[0])
						if ks != nil {
							if _, ok := ks.Tables[pair[1]]; ok {
								t.Errorf("table %s.%s should have been removed from Tables map", pair[0], pair[1])
							}
						}
					}
					for _, pair := range tt.wantTblPresent {
						ks, _ := MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalGetKeyspace(pair[0])
						if ks == nil {
							t.Errorf("keyspace %q not found when checking table %s", pair[0], pair[1])
						} else if _, ok := ks.Tables[pair[1]]; !ok {
							t.Errorf("table %s.%s should still be in Tables map", pair[0], pair[1])
						}
					}
					for _, pair := range tt.wantTblInvalid {
						ks, _ := MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalGetKeyspace(pair[0])
						if ks == nil {
							t.Errorf("keyspace %q not found when checking tablesInvalidated %s", pair[0], pair[1])
						} else if _, ok := implMetadata.KeyspaceData(ks).InternalTablesInvalidated[pair[1]]; !ok {
							t.Errorf("table %s.%s should be in tablesInvalidated", pair[0], pair[1])
						}
					}
					if tt.wantTablets >= 0 {
						if n := len(SessionData(s).InternalMetadataDescriber.InternalGetTablets()); n != tt.wantTablets {
							t.Errorf("expected %d tablets, got %d", tt.wantTablets, n)
						}
					}
				})
			}
		})

		t.Run("callbacks", func(t *testing.T) {
			t.Parallel()
			tests := []struct {
				name                string
				populateTables      []string
				event               protocol.Frame
				wantSchemaAgreement int
				wantKsChanged       []KeyspaceUpdateEvent
			}{
				{
					name:                "keyspace event calls policy without blocking on agreement",
					event:               &frm.SchemaChangeKeyspace{Change: "UPDATED", Keyspace: "test_ks"},
					wantSchemaAgreement: 0,
					wantKsChanged:       []KeyspaceUpdateEvent{{Keyspace: "test_ks", Change: "UPDATED"}},
				},
				{
					name:                "table event: no schema agreement, no policy callback",
					populateTables:      []string{"tbl_a"},
					event:               &frm.SchemaChangeTable{Change: "CREATED", Keyspace: "test_ks", Object: "tbl_a"},
					wantSchemaAgreement: 0,
				},
				{
					name:                "type event: no schema agreement, no policy callback",
					event:               &frm.SchemaChangeType{Change: "CREATED", Keyspace: "test_ks", Object: "my_type"},
					wantSchemaAgreement: 0,
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
					policy := &trackingPolicy{}
					s := newSchemaEventTestSession(ctrl, policy, "")
					defer s.Close()
					SessionData(s).InternalUseSystemSchema = true
					populateKeyspace(s, "test_ks", tt.populateTables...)
					InternalSessionHandleSchemaEvent(s, []protocol.Frame{tt.event})

					if got := ctrl.getAwaitSchemaAgreementCalls(); got != tt.wantSchemaAgreement {
						t.Fatalf("awaitSchemaAgreement: got %d, want %d", got, tt.wantSchemaAgreement)
					}
					kc := policy.getKeyspaceChangedCalls()
					if len(tt.wantKsChanged) == 0 {
						if len(kc) != 0 {
							t.Fatalf("KeyspaceChanged should not be called, got %+v", kc)
						}
					} else {
						if len(kc) != len(tt.wantKsChanged) {
							t.Fatalf("KeyspaceChanged: got %d calls, want %d", len(kc), len(tt.wantKsChanged))
						}
						for i, want := range tt.wantKsChanged {
							if kc[i] != want {
								t.Errorf("KeyspaceChanged[%d]: got %+v, want %+v", i, kc[i], want)
							}
						}
					}
				})
			}
		})

		fullRefresh := func(ksName string) map[string]int {
			return map[string]int{
				"SELECT durable_writes, replication FROM system_schema.keyspaces WHERE keyspace_name = ?":                  1,
				"SELECT * FROM system_schema.tables WHERE keyspace_name = ?":                                               1,
				"SELECT " + metadata.InternalColumnMetadataColumns + " FROM system_schema.columns WHERE keyspace_name = ?": 1,
				"SELECT * FROM system_schema.types WHERE keyspace_name = ?":                                                1,
				"SELECT * FROM system_schema.indexes WHERE keyspace_name = ?":                                              1,
				"SELECT * FROM system_schema.views WHERE keyspace_name = ?":                                                1,
				fmt.Sprintf("DESCRIBE KEYSPACE %q WITH INTERNALS", ksName):                                                 1,
			}
		}
		tableRefresh := map[string]int{
			"SELECT * FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?":                                               1,
			"SELECT " + metadata.InternalColumnMetadataColumns + " FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?": 1,
			"SELECT * FROM system_schema.indexes WHERE keyspace_name = ? AND table_name = ?":                                              1,
			"SELECT * FROM system_schema.views WHERE keyspace_name = ? AND base_table_name = ? ALLOW FILTERING":                           1,
		}
		noQueries := map[string]int{}

		assertExpectedQueries := func(t *testing.T, ctrl *schemaDataMock, expected map[string]int) {
			t.Helper()
			stmts := ctrl.getStatements()
			if len(expected) == 0 {
				if len(stmts) != 0 {
					t.Errorf("expected 0 queries, got %d:\n%s", len(stmts), strings.Join(stmts, "\n"))
				}
				return
			}
			wantTotal := 0
			for stmt, wantCount := range expected {
				wantTotal += wantCount
				gotCount := 0
				for _, s := range stmts {
					if s == stmt {
						gotCount++
					}
				}
				if gotCount != wantCount {
					t.Errorf("query %q: got %d, want %d", stmt, gotCount, wantCount)
				}
			}
			if len(stmts) != wantTotal {
				t.Errorf("total queries: got %d, want %d\nqueries:\n%s",
					len(stmts), wantTotal, strings.Join(stmts, "\n"))
			}
		}

		t.Run("GetKeyspace", func(t *testing.T) {
			t.Parallel()
			tests := []struct {
				name                string
				knownKeyspaces      map[string][]tableInfo
				populateKs          map[string][]string
				disableSystemSchema bool
				event               protocol.Frame // nil = no event
				getKeyspace         string
				wantError           bool
				expectedQueries     map[string]int // nil = skip check; empty = expect 0 queries
				wantNoRequery       bool           // second identical call fires 0 queries
			}{
				{
					name: "after keyspace event: refreshes and caches",
					knownKeyspaces: map[string][]tableInfo{
						"test_ks": {{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}}},
					},
					populateKs:      map[string][]string{"test_ks": {"tbl_a"}},
					event:           &frm.SchemaChangeKeyspace{Change: "UPDATED", Keyspace: "test_ks"},
					getKeyspace:     "test_ks",
					expectedQueries: fullRefresh("test_ks"),
					wantNoRequery:   true,
				},
				{
					name:            "after table event: returns cached, no queries",
					populateKs:      map[string][]string{"test_ks": {"tbl_a"}},
					event:           &frm.SchemaChangeTable{Change: "UPDATED", Keyspace: "test_ks", Object: "tbl_a"},
					getKeyspace:     "test_ks",
					expectedQueries: noQueries,
				},
				{
					name: "after type event: refreshes and caches",
					knownKeyspaces: map[string][]tableInfo{
						"test_ks": {{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}}},
					},
					populateKs:      map[string][]string{"test_ks": {"tbl_a"}},
					event:           &frm.SchemaChangeType{Change: "CREATED", Keyspace: "test_ks", Object: "my_type"},
					getKeyspace:     "test_ks",
					expectedQueries: fullRefresh("test_ks"),
					wantNoRequery:   true,
				},
				{
					name:            "uncached keyspace: refreshes and caches",
					knownKeyspaces:  map[string][]tableInfo{"new_ks": {}},
					getKeyspace:     "new_ks",
					expectedQueries: fullRefresh("new_ks"),
					wantNoRequery:   true,
				},
				{
					name:        "unknown keyspace: returns error",
					getKeyspace: "nonexistent",
					wantError:   true,
				},
				{
					name:                "useSystemSchema=false: returns error",
					disableSystemSchema: true,
					getKeyspace:         "test_ks",
					wantError:           true,
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					knownKs := tt.knownKeyspaces
					if knownKs == nil {
						knownKs = map[string][]tableInfo{}
					}
					ctrl := &schemaDataMock{knownKeyspaces: knownKs}
					s := newSchemaEventTestSessionWithMock(ctrl)
					defer s.Close()
					if tt.disableSystemSchema {
						SessionData(s).InternalUseSystemSchema = false
					}
					for ks, tables := range tt.populateKs {
						populateKeyspace(s, ks, tables...)
					}
					if tt.event != nil {
						InternalSessionHandleSchemaEvent(s, []protocol.Frame{tt.event})
					}

					ctrl.resetQueries()

					ks, err := SessionData(s).InternalMetadataDescriber.GetKeyspace(tt.getKeyspace)
					if tt.wantError {
						if err == nil {
							t.Fatal("expected error")
						}
						return
					}
					if err != nil {
						t.Fatalf("GetKeyspace failed: %v", err)
					}
					if ks.Name != tt.getKeyspace {
						t.Fatalf("expected keyspace %s, got %s", tt.getKeyspace, ks.Name)
					}
					if tt.expectedQueries != nil {
						assertExpectedQueries(t, ctrl, tt.expectedQueries)
					}
					if tt.wantNoRequery {
						ctrl.resetQueries()
						_, err = SessionData(s).InternalMetadataDescriber.GetKeyspace(tt.getKeyspace)
						if err != nil {
							t.Fatalf("second GetKeyspace failed: %v", err)
						}
						assertExpectedQueries(t, ctrl, noQueries)
					}
				})
			}
		})

		t.Run("GetTable", func(t *testing.T) {
			t.Parallel()
			tests := []struct {
				name            string
				knownKeyspaces  map[string][]tableInfo
				populateKs      map[string][]string
				event           protocol.Frame
				getTable        [2]string // [ks, table]
				wantError       bool
				expectedQueries map[string]int // nil = skip check; empty = expect 0 queries
				expectNoRequery bool
			}{
				{
					name: "after table event: refreshes only that table",
					knownKeyspaces: map[string][]tableInfo{
						"test_ks": {
							{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
							{name: "tbl_b", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
						},
					},
					populateKs:      map[string][]string{"test_ks": {"tbl_a", "tbl_b"}},
					event:           &frm.SchemaChangeTable{Change: "UPDATED", Keyspace: "test_ks", Object: "tbl_a"},
					getTable:        [2]string{"test_ks", "tbl_a"},
					expectedQueries: tableRefresh,
					expectNoRequery: true,
				},
				{
					name:            "after table event: other table returns cached directly",
					populateKs:      map[string][]string{"test_ks": {"tbl_a", "tbl_b"}},
					event:           &frm.SchemaChangeTable{Change: "UPDATED", Keyspace: "test_ks", Object: "tbl_a"},
					getTable:        [2]string{"test_ks", "tbl_b"},
					expectedQueries: noQueries,
				},
				{
					name: "after keyspace event: refreshes full keyspace",
					knownKeyspaces: map[string][]tableInfo{
						"test_ks": {{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}}},
					},
					populateKs:      map[string][]string{"test_ks": {"tbl_a"}},
					event:           &frm.SchemaChangeKeyspace{Change: "UPDATED", Keyspace: "test_ks"},
					getTable:        [2]string{"test_ks", "tbl_a"},
					expectedQueries: fullRefresh("test_ks"),
					expectNoRequery: true,
				},
				{
					name: "after type event: refreshes full keyspace",
					knownKeyspaces: map[string][]tableInfo{
						"test_ks": {{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}}},
					},
					populateKs:      map[string][]string{"test_ks": {"tbl_a"}},
					event:           &frm.SchemaChangeType{Change: "CREATED", Keyspace: "test_ks", Object: "my_type"},
					getTable:        [2]string{"test_ks", "tbl_a"},
					expectedQueries: fullRefresh("test_ks"),
				},
				{
					name: "unknown table: returns error",
					knownKeyspaces: map[string][]tableInfo{
						"test_ks": {{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}}},
					},
					populateKs: map[string][]string{"test_ks": {"tbl_a"}},
					getTable:   [2]string{"test_ks", "nonexistent"},
					wantError:  true,
				},
				{
					name:      "unknown keyspace: returns error",
					getTable:  [2]string{"nonexistent", "tbl_a"},
					wantError: true,
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					knownKs := tt.knownKeyspaces
					if knownKs == nil {
						knownKs = map[string][]tableInfo{}
					}
					ctrl := &schemaDataMock{knownKeyspaces: knownKs}
					s := newSchemaEventTestSessionWithMock(ctrl)
					defer s.Close()
					for ks, tables := range tt.populateKs {
						populateKeyspace(s, ks, tables...)
					}
					if tt.event != nil {
						InternalSessionHandleSchemaEvent(s, []protocol.Frame{tt.event})
					}

					ctrl.resetQueries()

					tbl, err := SessionData(s).InternalMetadataDescriber.GetTable(tt.getTable[0], tt.getTable[1])
					if tt.wantError {
						if err == nil {
							t.Fatal("expected error")
						}
						return
					}
					if err != nil {
						t.Fatalf("GetTable failed: %v", err)
					}
					if tbl.Name != tt.getTable[1] {
						t.Fatalf("expected table %s, got %s", tt.getTable[1], tbl.Name)
					}
					if tt.expectedQueries != nil {
						assertExpectedQueries(t, ctrl, tt.expectedQueries)
					}
					if tt.expectNoRequery {
						ctrl.resetQueries()
						_, err = SessionData(s).InternalMetadataDescriber.GetTable(tt.getTable[0], tt.getTable[1])
						if err != nil {
							t.Fatalf("second GetTable failed: %v", err)
						}
						assertExpectedQueries(t, ctrl, noQueries)
					}
				})
			}
		})

		t.Run("batch/multiple_table_events", func(t *testing.T) {
			t.Parallel()
			ctrl := &schemaDataMock{
				knownKeyspaces: map[string][]tableInfo{
					"test_ks": {
						{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
						{name: "tbl_b", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
						{name: "tbl_c", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
					},
				},
			}
			s := newSchemaEventTestSessionWithMock(ctrl)
			defer s.Close()
			populateKeyspace(s, "test_ks", "tbl_a", "tbl_b", "tbl_c")
			InternalSessionHandleSchemaEvent(s, []protocol.Frame{
				&frm.SchemaChangeTable{Change: "UPDATED", Keyspace: "test_ks", Object: "tbl_a"},
				&frm.SchemaChangeTable{Change: "UPDATED", Keyspace: "test_ks", Object: "tbl_b"},
			})

			ks, _ := MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalGetKeyspace("test_ks")
			if _, ok := ks.Tables["tbl_a"]; ok {
				t.Fatal("tbl_a should be invalidated")
			}
			if _, ok := ks.Tables["tbl_b"]; ok {
				t.Fatal("tbl_b should be invalidated")
			}
			if _, ok := ks.Tables["tbl_c"]; !ok {
				t.Fatal("tbl_c should still be cached")
			}

			ctrl.resetQueries()

			_, err := SessionData(s).InternalMetadataDescriber.GetTable("test_ks", "tbl_a")
			if err != nil {
				t.Fatalf("GetTable(tbl_a) failed: %v", err)
			}
			if ctrl.getQueryCount() == 0 {
				t.Fatal("expected queries for tbl_a")
			}

			ctrl.resetQueries()

			_, err = SessionData(s).InternalMetadataDescriber.GetTable("test_ks", "tbl_b")
			if err != nil {
				t.Fatalf("GetTable(tbl_b) failed: %v", err)
			}
			if ctrl.getQueryCount() == 0 {
				t.Fatal("expected queries for tbl_b")
			}

			ctrl.resetQueries()

			_, err = SessionData(s).InternalMetadataDescriber.GetTable("test_ks", "tbl_c")
			if err != nil {
				t.Fatalf("GetTable(tbl_c) failed: %v", err)
			}
			if got := ctrl.getQueryCount(); got != 0 {
				t.Fatalf("tbl_c not invalidated, expected 0 queries, got %d", got)
			}
		})

		t.Run("batch/mixed_keyspace_and_table_events", func(t *testing.T) {
			t.Parallel()
			ctrl := &schemaDataMock{
				knownKeyspaces: map[string][]tableInfo{
					"test_ks": {
						{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
					},
				},
			}
			policy := &trackingPolicy{}
			s := newSchemaEventTestSession(ctrl, policy, "")
			defer s.Close()
			SessionData(s).InternalUseSystemSchema = true
			SessionData(s).InternalHasAggregatesAndFunctions = false
			populateKeyspace(s, "test_ks", "tbl_a")
			InternalSessionHandleSchemaEvent(s, []protocol.Frame{
				&frm.SchemaChangeTable{Change: "CREATED", Keyspace: "test_ks", Object: "tbl_a"},
				&frm.SchemaChangeKeyspace{Change: "UPDATED", Keyspace: "test_ks"},
			})

			if got := ctrl.getAwaitSchemaAgreementCalls(); got != 0 {
				t.Fatalf("awaitSchemaAgreement: got %d, want 0 (background events do not impose DDL agreement)", got)
			}
			if _, found := MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalGetKeyspace("test_ks"); found {
				t.Fatal("keyspace should have been removed from cache by keyspace event")
			}

			ctrl.resetQueries()

			ks, err := SessionData(s).InternalMetadataDescriber.GetKeyspace("test_ks")
			if err != nil {
				t.Fatalf("GetKeyspace failed: %v", err)
			}
			if ks.Name != "test_ks" {
				t.Fatalf("expected test_ks, got %s", ks.Name)
			}
			if got := ctrl.getQueryCount(); got == 0 {
				t.Fatal("expected queries after keyspace was cleared")
			}
		})
	})

	t.Run("InvalidateTableSchema", func(t *testing.T) {
		ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{"test_ks": {{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}}}}}
		s := newSchemaEventTestSessionWithMock(ctrl)
		defer s.Close()
		populateKeyspace(s, "test_ks", "tbl_a")
		SessionData(s).InternalMetadataDescriber.InternalInvalidateTableSchema("test_ks", "tbl_a")
		snapshot, reloaded, err := SessionData(s).InternalMetadataDescriber.InternalGetKeyspaceInternal("test_ks")
		if err != nil {
			t.Fatal(err)
		}
		_, needed, err := SessionData(s).InternalMetadataDescriber.InternalGetTableFromSnapshot("test_ks", "tbl_a", snapshot, reloaded)
		if err != nil || !needed {
			t.Fatal(needed, err)
		}
		// Caller A is now paused after deciding refreshNeeded. Caller B completes
		// its entire flight before A can enter the singleflight group.
		if err := SessionData(s).InternalMetadataDescriber.InternalDeduplicatedRefreshTable("test_ks", "tbl_a"); err != nil {
			t.Fatal(err)
		}
		ctrl.resetQueries()
		if err := SessionData(s).InternalMetadataDescriber.InternalDeduplicatedRefreshTable("test_ks", "tbl_a"); err != nil {
			t.Fatal(err)
		}
		if got := ctrl.getQueryCount(); got != 0 {
			t.Fatalf("stale entrant after completed flight reissued %d schema queries", got)
		}
		SessionData(s).InternalMetadataDescriber.InternalInvalidateTableSchema("test_ks", "tbl_a")
		if err := SessionData(s).InternalMetadataDescriber.InternalDeduplicatedRefreshTable("test_ks", "tbl_a"); err != nil {
			t.Fatal(err)
		}
		if got := ctrl.getQueryCount(); got != 4 {
			t.Fatalf("newer invalidation skipped required refresh: %d", got)
		}
	})

	t.Run("KeyspaceMetadata", func(t *testing.T) {
		t.Parallel()

		ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{"ks1": nil}}
		s := newSchemaEventTestSessionWithMock(ctrl)
		defer s.Close()
		ctrl.setQueryError(errors.New("schema unavailable"))

		_, err := InternalGetKeyspaceMetadata(s, "ks1")
		if err == nil || errors.Is(err, runtimeerrors.ErrKeyspaceDoesNotExist) || !strings.Contains(err.Error(), "schema unavailable") {
			t.Fatalf("getKeyspaceMetadata error = %v, want the query error", err)
		}
	})

	t.Run("RefreshAllSchema", func(t *testing.T) {
		t.Parallel()

		t.Run("Error", func(t *testing.T) {
			t.Parallel()

			s, ctrl := refreshAllSchemaSession(t)
			ctrl.setQueryError(errors.New("schema unavailable"))

			err := SessionData(s).InternalMetadataDescriber.InternalRefreshAllSchema()
			if err == nil || !strings.Contains(err.Error(), "schema unavailable") {
				t.Fatalf("refreshAllSchema error = %v, want the query error", err)
			}
			MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalTabletsMetadata.Flush()
			for _, ks := range []string{"ks1", "ks2"} {
				if _, found := MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalGetKeyspace(ks); !found {
					t.Errorf("keyspace %s was dropped after a failed refresh", ks)
				}
			}
			requireTablets(t, s, map[string]bool{"ks1.t1": true, "ks1.t2": true, "ks2.t3": true})
		})

		t.Run("ErrorUnwraps", func(t *testing.T) {
			t.Parallel()

			for i := 0; i < 20; i++ {
				s, ctrl := refreshAllSchemaSession(t)
				errSchema := errors.New("schema unavailable")
				ctrl.setQueryError(errSchema)

				if err := SessionData(s).InternalMetadataDescriber.InternalRefreshAllSchema(); !errors.Is(err, errSchema) {
					t.Fatalf("run %d: refreshAllSchema error = %v, which does not wrap the query error", i, err)
				}
			}
		})

		t.Run("KeyspaceDropped", func(t *testing.T) {
			t.Parallel()

			s, ctrl := refreshAllSchemaSession(t)
			delete(ctrl.knownKeyspaces, "ks2")

			refreshAllSchemaNow(t, s)

			if _, found := MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalGetKeyspace("ks2"); found {
				t.Error("dropped keyspace ks2 is still cached")
			}
			if _, found := MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalGetKeyspace("ks1"); !found {
				t.Error("keyspace ks1 was dropped from the cache")
			}
			requireTablets(t, s, map[string]bool{"ks1.t1": true, "ks1.t2": true, "ks2.t3": false})
		})

		t.Run("ReplicationChanged", func(t *testing.T) {
			t.Parallel()

			s, _ := refreshAllSchemaSession(t)
			cached, _ := MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalGetKeyspace("ks1")
			stale := cached.Clone()
			stale.StrategyOptions = map[string]any{"replication_factor": "3"}
			MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalSet("ks1", stale)

			refreshAllSchemaNow(t, s)
			requireTablets(t, s, map[string]bool{"ks1.t1": false, "ks1.t2": false, "ks2.t3": true})
		})

		t.Run("TableChanged", func(t *testing.T) {
			t.Parallel()

			s, ctrl := refreshAllSchemaSession(t)
			ctrl.knownKeyspaces["ks1"][1].columns = append(ctrl.knownKeyspaces["ks1"][1].columns,
				columnInfo{name: "added", kind: "regular"})

			refreshAllSchemaNow(t, s)
			requireTablets(t, s, map[string]bool{"ks1.t1": true, "ks1.t2": false, "ks2.t3": true})
		})

		t.Run("TableDropped", func(t *testing.T) {
			t.Parallel()

			s, ctrl := refreshAllSchemaSession(t)
			ctrl.knownKeyspaces["ks1"] = ctrl.knownKeyspaces["ks1"][1:] // t1 is gone

			refreshAllSchemaNow(t, s)
			requireTablets(t, s, map[string]bool{"ks1.t1": false, "ks1.t2": true, "ks2.t3": true})
		})

		t.Run("Unchanged", func(t *testing.T) {
			t.Parallel()

			s, _ := refreshAllSchemaSession(t)
			refreshAllSchemaNow(t, s)
			requireTablets(t, s, map[string]bool{"ks1.t1": true, "ks1.t2": true, "ks2.t3": true})
		})
	})

	t.Run("RefreshV2AllSchema", func(t *testing.T) {
		t.Run("DropDuringFetchIsConfirmed", func(t *testing.T) {
			s, mock := refreshAllSchemaSession(t)
			var dropped atomic.Bool
			mock.iterFor = func(statement string) *nativeResultFixture {
				if strings.HasPrefix(statement, "SELECT * FROM system_schema.tables WHERE") && dropped.CompareAndSwap(false, true) {
					mock.mu.Lock()
					delete(mock.knownKeyspaces, "ks2")
					mock.mu.Unlock()
					return &nativeResultFixture{err: errors.New("keyspace disappeared between enumeration and query")}
				}
				return nil
			}
			// The injected failure may target either fetched keyspace. The keyspace that
			// still exists must retain and report its failure; the confirmed absent one
			// must never be restored from the older enumerated rows.
			err := InternalSessionRefreshV2AllSchema(s, context.Background())
			if err != nil {
				var partial *metadata.PartialRefreshError
				if !errors.As(err, &partial) {
					t.Fatal(err)
				}
				if _, wrong := partial.Keyspaces["ks2"]; wrong {
					t.Fatal("confirmed drop reported as stale", err)
				}
			}
			if _, exists := MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalGetKeyspace("ks2"); exists {
				t.Fatal("confirmed dropped keyspace was restored")
			}
		})

		t.Run("PartialRefreshRetainsPriorSnapshot", func(t *testing.T) {
			s, mock := refreshAllSchemaSession(t)
			before := s.Metadata()
			cause := errors.New("temporary schema read failure")
			// The actual column query selects an explicit column list.
			mock.iterFor = func(statement string) *nativeResultFixture {
				if strings.Contains(statement, "FROM system_schema.columns") {
					return &nativeResultFixture{err: cause}
				}
				return nil
			}
			err := InternalSessionRefreshV2AllSchema(s, context.Background())
			var partial *metadata.PartialRefreshError
			if !errors.As(err, &partial) || !errors.Is(err, cause) {
				t.Fatal("partial error lost cause", err)
			}
			for _, snapshot := range []*routing.MetadataSnapshot{before, s.Metadata()} {
				ks, ok := snapshot.Keyspace("ks1")
				if !ok || ks.Tables["t1"] == nil {
					t.Fatal("valid previous keyspace lost")
				}
			}
		})

		// A concurrent DDL can fail one schema read while the keyspace is still
		// listed, as DESCRIBE does for a keyspace being dropped or a table read does
		// for a table created without its columns yet. The refresh reads the keyspace
		// again instead of reporting stale data.
		t.Run("TransientReadFailureIsRetried", func(t *testing.T) {
			for _, full := range []bool{true, false} {
				s, mock := refreshAllSchemaSession(t)
				var failed atomic.Int32
				mock.iterFor = func(statement string) *nativeResultFixture {
					if strings.Contains(statement, "FROM system_schema.columns") && failed.Add(1) == 1 {
						return &nativeResultFixture{err: errors.New("'ks1' not found in keyspaces")}
					}
					return nil
				}
				var err error
				if full {
					err = InternalSessionRefreshV2AllSchema(s, context.Background())
				} else {
					err = InternalSessionRefreshV2Keyspace(s, context.Background(), "ks1")
				}
				if err != nil {
					t.Fatalf("full=%t: transient failure reported: %v", full, err)
				}
				if failed.Load() < 2 {
					t.Fatalf("full=%t: schema was not read again", full)
				}
				if ks, ok := s.Metadata().Keyspace("ks1"); !ok || ks.Tables["t1"] == nil {
					t.Fatalf("full=%t: keyspace not refreshed", full)
				}
			}
		})
	})

	t.Run("ScyllaIsCdcTable", func(t *testing.T) {
		t.Parallel()

		t.Run("AfterInvalidation", func(t *testing.T) {
			t.Parallel()

			ctrl := &schemaDataMock{
				knownKeyspaces: map[string][]tableInfo{
					"test_ks": {
						{name: "tbl_scylla_cdc_log", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
					},
				},
			}
			s := newTestSessionForTableMetadata(ctrl)
			defer s.Close()
			populateKeyspace(s, "test_ks", "tbl_scylla_cdc_log")

			_, err := InternalScyllaIsCdcTable(s, "test_ks", "tbl_scylla_cdc_log")
			if err != nil {
				t.Fatalf("initial scyllaIsCdcTable failed: %v", err)
			}
			SessionData(s).InternalMetadataDescriber.InternalInvalidateTableSchema("test_ks", "tbl_scylla_cdc_log")
			ctrl.resetQueries()

			_, err = InternalScyllaIsCdcTable(s, "test_ks", "tbl_scylla_cdc_log")
			if err != nil {
				t.Fatalf("scyllaIsCdcTable after invalidation failed: %v", err)
			}
			if ctrl.getQueryCount() == 0 {
				t.Fatal("expected queries to refresh tbl_scylla_cdc_log after invalidation")
			}
		})

		t.Run("NotCdcSuffix", func(t *testing.T) {
			t.Parallel()

			ctrl := &schemaDataMock{
				knownKeyspaces: map[string][]tableInfo{
					"test_ks": {
						{name: "regular_table", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
					},
				},
			}
			s := newTestSessionForTableMetadata(ctrl)
			defer s.Close()
			populateKeyspace(s, "test_ks", "regular_table")

			isCdc, err := InternalScyllaIsCdcTable(s, "test_ks", "regular_table")
			if err != nil {
				t.Fatalf("scyllaIsCdcTable failed: %v", err)
			}
			if isCdc {
				t.Fatal("expected regular_table to not be a CDC table")
			}
		})
	})

	t.Run("Snapshot", func(t *testing.T) {
		column := &metadata.ColumnMetadata{Name: "id", Type: "int", Index: metadata.ColumnIndexMetadata{Options: map[string]any{"nested": map[string]any{"bytes": []byte{1}}}}}
		ks := &metadata.KeyspaceMetadata{Name: "ks", StrategyOptions: map[string]any{"replication_factor": "1"}, Tables: map[string]*metadata.TableMetadata{"t": {Columns: map[string]*metadata.ColumnMetadata{"id": column}, PartitionKey: []*metadata.ColumnMetadata{column}, Extensions: map[string]any{"bytes": []byte{2}}, Options: metadata.TableMetadataOptions{CDC: map[string]string{"enabled": "true"}}}}, Types: map[string]*metadata.TypeMetadata{"u": {FieldNames: []string{"a"}, FieldTypes: []string{"int"}}}, Functions: map[string]*metadata.FunctionMetadata{"f": {ArgumentNames: []string{"a"}, ArgumentTypes: []string{"int"}}}}
		m := routing.NewMetadataSnapshot(routing.SnapshotState{InternalGeneration: 8, InternalKeyspaces: map[string]*metadata.KeyspaceMetadata{"ks": metadata.InternalCloneV2Keyspace(ks)}, InternalHosts: []model.HostSnapshot{{Tokens: []int64{1}}}})
		ks.Tables["t"].Columns["id"].Name = "mutated"
		ks.Tables["t"].Extensions["bytes"].([]byte)[0] = 9
		first, _ := m.Keyspace("ks")
		if first.Tables["t"].Columns["id"].Name != "id" || first.Tables["t"].Extensions["bytes"].([]byte)[0] != 2 {
			t.Fatal("snapshot retained mutable schema")
		}
		first.Types["u"].FieldNames[0] = "changed"
		first.Functions["f"].ArgumentNames[0] = "changed"
		first.Tables["t"].PartitionKey[0].Index.Options["nested"].(map[string]any)["bytes"].([]byte)[0] = 9
		second, _ := m.Keyspace("ks")
		if second.Types["u"].FieldNames[0] != "a" || second.Functions["f"].ArgumentNames[0] != "a" || second.Tables["t"].Columns["id"].Index.Options["nested"].(map[string]any)["bytes"].([]byte)[0] != 1 {
			t.Fatal("getter leaked mutable snapshot")
		}
		hosts := m.Hosts()
		hosts[0].Tokens[0] = 9
		if m.Hosts()[0].Tokens[0] != 1 {
			t.Fatal("host tokens alias")
		}
	})

	t.Run("TableMetadata", func(t *testing.T) {
		t.Parallel()

		t.Run("AfterKeyspaceInvalidation", func(t *testing.T) {
			t.Parallel()

			ctrl := &schemaDataMock{
				knownKeyspaces: map[string][]tableInfo{
					"test_ks": {
						{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
					},
				},
			}
			s := newSchemaEventTestSessionWithMock(ctrl)
			defer s.Close()
			SessionData(s).InternalIsInitialized = true
			populateKeyspace(s, "test_ks", "tbl_a")

			_, err := InternalSessionTableMetadata(s, "test_ks", "tbl_a")
			if err != nil {
				t.Fatalf("initial TableMetadata failed: %v", err)
			}
			SessionData(s).InternalMetadataDescriber.InternalInvalidateKeyspaceSchema("test_ks")

			ctrl.resetQueries()

			tbl, err := InternalSessionTableMetadata(s, "test_ks", "tbl_a")
			if err != nil {
				t.Fatalf("TableMetadata after keyspace invalidation failed: %v", err)
			}
			if tbl.Name != "tbl_a" {
				t.Fatalf("expected table name tbl_a, got %s", tbl.Name)
			}
			if ctrl.getQueryCount() == 0 {
				t.Fatal("expected queries to reload keyspace after invalidation")
			}
		})

		t.Run("AfterTableInvalidation", func(t *testing.T) {
			t.Parallel()

			ctrl := &schemaDataMock{
				knownKeyspaces: map[string][]tableInfo{
					"test_ks": {
						{name: "tbl_a", columns: []columnInfo{{name: "id", kind: "partition_key", position: 0}}},
					},
				},
			}
			s := newSchemaEventTestSessionWithMock(ctrl)
			defer s.Close()
			SessionData(s).InternalIsInitialized = true
			populateKeyspace(s, "test_ks", "tbl_a")

			tbl, err := InternalSessionTableMetadata(s, "test_ks", "tbl_a")
			if err != nil {
				t.Fatalf("initial TableMetadata failed: %v", err)
			}
			if tbl.Name != "tbl_a" {
				t.Fatalf("expected table name tbl_a, got %s", tbl.Name)
			}
			SessionData(s).InternalMetadataDescriber.InternalInvalidateTableSchema("test_ks", "tbl_a")

			ctrl.resetQueries()

			tbl, err = InternalSessionTableMetadata(s, "test_ks", "tbl_a")
			if err != nil {
				t.Fatalf("TableMetadata after invalidation failed: %v", err)
			}
			if tbl.Name != "tbl_a" {
				t.Fatalf("expected table name tbl_a, got %s", tbl.Name)
			}
			if ctrl.getQueryCount() == 0 {
				t.Fatal("expected queries to refresh tbl_a after invalidation")
			}
		})

		t.Run("Validation", func(t *testing.T) {
			t.Parallel()

			t.Run("EmptyTableReturnsErrNoTable", func(t *testing.T) {
				t.Parallel()

				ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
				s := newSchemaEventTestSessionWithMock(ctrl)
				defer s.Close()
				SessionData(s).InternalIsInitialized = true

				_, err := InternalSessionTableMetadata(s, "ks", "")
				if !errors.Is(err, runtimeerrors.ErrNoTable) {
					t.Fatalf("TableMetadata: expected ErrNoTable, got %v", err)
				}
			})
		})
	})

	t.Run("ValidateCQLType", func(t *testing.T) {
		for _, typ := range []string{"int", "empty", "map<text, frozen<tuple<int, text>>>", "vector<float, 3>", `frozen<"quoted-type">`} {
			if _, err := metadata.InternalValidateV2CQLType("ks", typ, map[string]bool{"quoted-type": true}); err != nil {
				t.Fatal(typ, err)
			}
		}
		for _, typ := range []string{"map<int>", "list<>", "tuple<int", "vector<float, 0>", "vector<float, 65536>", "int garbage", "frozen<missing>", "map<int,text>>"} {
			if _, err := metadata.InternalValidateV2CQLType("ks", typ, nil); err == nil {
				t.Fatal("invalid type accepted", typ)
			}
		}
	})

	t.Run("ValidateSchemaParts", func(t *testing.T) {
		ks := &metadata.KeyspaceMetadata{Name: "ks"}
		tables := []metadata.TableMetadata{{Name: "t"}}
		columns := []metadata.ColumnMetadata{{Table: "t", Name: "p", Type: "int", Kind: metadata.ColumnPartitionKey, ComponentIndex: 0}}
		check := func(cols []metadata.ColumnMetadata, types []metadata.TypeMetadata) error {
			return metadata.InternalValidateV2SchemaParts(ks, tables, cols, types, nil, nil, nil, nil)
		}
		if err := check(columns, nil); err != nil {
			t.Fatal(err)
		}
		malformed := append([]metadata.ColumnMetadata(nil), columns...)
		malformed[0].ComponentIndex = 2
		if err := check(malformed, nil); err == nil {
			t.Fatal("key gap accepted")
		}
		malformed = append(columns, columns[0])
		if err := check(malformed, nil); err == nil {
			t.Fatal("duplicate identity accepted")
		}
		types := []metadata.TypeMetadata{{Name: "a", FieldNames: []string{"b"}, FieldTypes: []string{"frozen<b>"}}, {Name: "b", FieldNames: []string{"a"}, FieldTypes: []string{"frozen<a>"}}}
		if err := check(columns, types); err == nil {
			t.Fatal("UDT cycle accepted")
		}
		types = []metadata.TypeMetadata{{Name: "a", FieldNames: []string{"x", "x"}, FieldTypes: []string{"int", "int"}}}
		if err := check(columns, types); err == nil {
			t.Fatal("duplicate UDT field accepted")
		}
	})
}

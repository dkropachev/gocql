/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package session

import (
	"github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/transport"

	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type scopedSchemaBackend struct {
	*schemaDataMock
	keyspace string
	fail     atomic.Bool
	queried  chan struct{}
	once     sync.Once
	calls    atomic.Int32
}

func (b *scopedSchemaBackend) InternalQueryV2System(ctx context.Context, r *model.AttemptRequest) (*transport.InternalResponsePage, error) {
	args, err := v2SchemaMockArguments(r)
	if err != nil {
		return nil, err
	}
	scopedDescribe := len(args) == 0 && r.Statement == `DESCRIBE KEYSPACE "`+b.keyspace+`" WITH INTERNALS`
	if !scopedDescribe && (len(args) != 1 || args[0] != b.keyspace) {
		return nil, fmt.Errorf("unrelated or unscoped required schema query: %s %v", r.Statement, args)
	}
	if strings.Contains(r.Statement, "FROM system_schema.keyspaces") {
		b.calls.Add(1)
		b.once.Do(func() { close(b.queried) })
		if b.fail.CompareAndSwap(true, false) {
			return nil, errors.New("injected temporary required refresh failure")
		}
	}
	return b.schemaDataMock.InternalQueryV2System(ctx, r)
}
func waitReconciliation(t *testing.T, p *cluster.InternalV2PeripheralState) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for {
		p.InternalMu.Lock()
		pending := len(p.InternalPreparedRefreshScopes)
		p.InternalMu.Unlock()
		if pending == 0 && !p.InternalRetryScheduled.Load() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("required reconciliation did not finish")
		}
		time.Sleep(time.Millisecond)
	}
}

func pendingConflictSession(t *testing.T) *cluster.Core {
	t.Helper()
	s := new(cluster.Core)
	cluster.SessionData(s).InternalLogger = logging.Nop()
	cluster.SessionData(s).InternalCtx, cluster.SessionData(s).InternalCancel = context.WithCancel(context.Background())
	t.Cleanup(func() { s.Close() })
	return s
}

// While a scope is pending only the schema retry worker can clear it, so a
// dirty statement's attempts must neither take the session-wide peripheral
// lock nor its own conflictMu.
func TestPendingConflictAttemptDoesNotBlockOnPeripheralLock(t *testing.T) {
	s := pendingConflictSession(t)
	state := &InternalPreparedState{InternalSession: s, InternalInfo: PreparedInfo{Keyspace: "ks", Table: "tbl"}}
	if err := state.noteMetadataConflict(); err != nil {
		t.Fatal(err)
	}
	p := s.Peripheral()
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()
	done := make(chan error, 1)
	go func() { done <- state.reconcilePendingConflict() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("attempt blocked on the peripheral lock while its scope is pending")
	}
}

func TestPendingConflictConcurrentAttemptsDoNotQueue(t *testing.T) {
	s := pendingConflictSession(t)
	state := &InternalPreparedState{InternalSession: s, InternalInfo: PreparedInfo{Keyspace: "ks", Table: "tbl"}}
	if err := state.noteMetadataConflict(); err != nil {
		t.Fatal(err)
	}
	state.conflictMu.Lock()
	defer state.conflictMu.Unlock()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := state.reconcilePendingConflict(); err != nil {
				t.Error(err)
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("attempts queued on conflictMu while the scope is pending")
	}
}

// The flag must stay set until the scope is reconciled, so that the first
// attempt after reconciliation clears it.
func TestPendingConflictKeepsDirty(t *testing.T) {
	s := pendingConflictSession(t)
	state := &InternalPreparedState{InternalSession: s, InternalInfo: PreparedInfo{Keyspace: "ks", Table: "tbl"}}
	if err := state.noteMetadataConflict(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		if err := state.reconcilePendingConflict(); err != nil {
			t.Fatal(err)
		}
	}
	if !state.conflictDirty.Load() {
		t.Fatal("dirty cleared while pending")
	}
}

// Once the pending scope is cleared by the retry worker (delete, then
// InternalSchemaReconciled.Add), the next attempt takes the locked path and
// clears the dirty flag.
func TestPendingConflictClearsDirtyAfterReconcile(t *testing.T) {
	s := pendingConflictSession(t)
	state := &InternalPreparedState{InternalSession: s, InternalInfo: PreparedInfo{Keyspace: "ks", Table: "tbl"}}
	if err := state.noteMetadataConflict(); err != nil {
		t.Fatal(err)
	}
	p := s.Peripheral()
	p.InternalMu.Lock()
	delete(p.InternalPreparedRefreshScopes, "ks")
	p.InternalMu.Unlock()
	p.InternalSchemaReconciled.Add(1)
	if err := state.reconcilePendingConflict(); err != nil {
		t.Fatal(err)
	}
	if state.conflictDirty.Load() {
		t.Fatal("dirty not cleared after the scope was reconciled")
	}
}

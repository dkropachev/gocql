//go:build integration

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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package integration

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"

	gocql "github.com/scylladb/gocql/v2"

	"github.com/scylladb/gocql/v2/cql"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/retry"

	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clustertestkit "github.com/scylladb/gocql/v2/internal/testkit/cluster"
)

// Typed request helpers keep parameter construction explicit and bind inputs once.
func sessionTestBind(t testing.TB, session *gocql.Session, stmt string, values ...cql.Value) *Bound[cql.EmptyRow] {
	t.Helper()
	prepared, err := gocql.Prepare(context.Background(), session, cql.Statement[[]cql.Value, cql.EmptyRow]{CQL: stmt, Params: cql.DynamicParams, Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bound, err := BindOwned(prepared, values)
	if err != nil {
		t.Fatal(err)
	}
	return bound
}
func sessionTestItem[R any](t testing.TB, bound *Bound[R]) batchItem {
	t.Helper()
	item, err := BatchEntry(bound)
	if err != nil {
		t.Fatal(err)
	}
	return item
}
func sessionTestInt(t testing.TB, row cql.DynamicRow, name string) int64 {
	t.Helper()
	value, err := row.Get(name)
	if err != nil {
		t.Fatal(err)
	}
	integer, err := value.Signed()
	if err != nil {
		t.Fatal(err)
	}
	return integer
}

func TestSessionAPI(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	config := gocql.DefaultConfig("127.0.0.1")
	profile := *config.DefaultProfile
	profile.Consistency, profile.PageSize, profile.Prefetch = cql.All, 100, 1
	config.DefaultProfile = &profile
	// Session setters are replaced by atomic profile snapshots. Fractional
	// prefetch became a bounded count of zero or one physical page.
	session := func() *gocql.Session {
		opaqueStorageValue := new(gocql.Session)
		implCluster.SessionData(opaqueStorageValue).InternalLogger = nopLogger{}
		return opaqueStorageValue
	}()
	runtimeFor(session).InternalSetProfile(config.DefaultProfile)
	defer session.Close()
	resolved, err := resolveOptions(session, gocql.Options{Trace: true})
	require.NoError(t, err)
	require.Equal(t, cql.All, resolved.InternalProfile.Consistency)
	require.Equal(t, int32(100), resolved.InternalProfile.PageSize)
	require.Equal(t, 1, resolved.InternalProfile.Prefetch)
	require.True(t, resolved.InternalOptions.Trace)

	intType := cql.IntType
	var encoded int32
	statement := cql.Statement[int32, cql.EmptyRow]{CQL: "INSERT INTO test(id) VALUES (?)", Params: cql.Params(cql.ParamAt(0, cql.Int32, func(n int32) int32 { encoded = n; return n })), Rows: cql.EmptyRowDecoder}
	bound, err := BindUnpreparedOwned(statement, cql.NewColumns(cql.ColumnSpec{Name: "id", Type: intType}), int32(1))
	require.NoError(t, err)
	defer bound.Close()
	require.Equal(t, int32(1), encoded, "typed parameter accessor replaces callback binding")
	// An incomplete session now reports ErrNoConnections from the typed path.
	// It does not construct the old mutable Query/Iter error holder.
	_, err = Run(ctx, session, bound, gocql.SessionOptions{})
	require.ErrorIs(t, err, gocql.ErrNoConnections)
	batch := []batchItem{sessionTestItem(t, bound)}
	_, err = execBatch(ctx, session, cql.LoggedBatch, batch, gocql.SessionOptions{})
	require.ErrorIs(t, err, gocql.ErrNoConnections)
	require.NoError(t, session.Close())
	require.True(t, session.Closed())
	require.NoError(t, session.Close())
	_, err = execBatch(ctx, session, cql.LoggedBatch, batch, gocql.SessionOptions{})
	require.ErrorIs(t, err, gocql.ErrClosed)
	_, err = Exec(ctx, session, bound, gocql.SessionOptions{})
	require.ErrorIs(t, err, gocql.ErrClosed)
}

type sessionTestObserver struct {
	mu       sync.Mutex
	requests []gocql.RequestEvent
	attempts []gocql.AttemptEvent
}

func (o *sessionTestObserver) ObserveRequest(e gocql.RequestEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.requests = append(o.requests, e)
}
func (o *sessionTestObserver) ObserveAttempt(e gocql.AttemptEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.attempts = append(o.attempts, e)
}
func (o *sessionTestObserver) snapshot() ([]gocql.RequestEvent, []gocql.AttemptEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]gocql.RequestEvent(nil), o.requests...), append([]gocql.AttemptEvent(nil), o.attempts...)
}

type sessionTestRetry struct {
	action      retry.Action
	maxAttempts int
}

func (p sessionTestRetry) Decide(e retry.Attempt) retry.Decision {
	if e.Number >= p.maxAttempts {
		return retry.Decision{Action: retry.Stop}
	}
	return retry.Decision{Action: p.action}
}

func TestQueryBasicAPI(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	require.NoError(t, v2CreateTable(ctx, session, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", table)))
	prepared, err := gocql.Prepare(ctx, session, cql.Statement[int32, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s(id) VALUES (?)", table), Params: cql.OneParam(cql.Int32), Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, prepared.Parameters().Len())
	require.Equal(t, table, prepared.Info().Table)
	original, err := BindOwned(prepared, int32(1))
	require.NoError(t, err)
	defer original.Close()
	rebound, err := BindOwned(prepared, int32(2))
	require.NoError(t, err)
	defer rebound.Close()

	// Query setters become per-admission Options. Trace IDs and timing belong
	// to each execution result; a Bound never accumulates mutable metrics.
	zero := gocql.Result{}
	require.Zero(t, zero.Attempts)
	require.Zero(t, zero.Latency)
	retryPolicy := sessionTestRetry{action: retry.Next, maxAttempts: 3}
	observer := new(sessionTestObserver)
	options := gocql.Options{Consistency: cql.Option[cql.Consistency]{Value: cql.All, Set: true}, SerialConsistency: cql.Some(cql.LocalSerial), PageSize: cql.Option[int32]{Value: 10, Set: true}, Prefetch: cql.Option[int]{Value: 1, Set: true}, Retry: retryPolicy, Trace: true, Observer: observer}
	resolved, err := resolveOptions(session, options)
	require.NoError(t, err)
	require.Equal(t, cql.All, resolved.InternalProfile.Consistency)
	require.Equal(t, cql.LocalSerial, resolved.InternalProfile.SerialConsistency.Value)
	require.Equal(t, int32(10), resolved.InternalProfile.PageSize)
	require.Equal(t, 1, resolved.InternalProfile.Prefetch)
	require.Equal(t, retryPolicy, resolved.InternalProfile.Retry)
	serialValues := options
	serialValues.Consistency.Value = cql.LocalSerial
	serialRead := serialValues
	resolvedSerial, err := resolveOptions(session, serialRead)
	require.NoError(t, err)
	require.Equal(t, cql.LocalSerial, resolvedSerial.InternalProfile.Consistency)

	for _, bound := range []*Bound[cql.EmptyRow]{original, rebound, original} {
		result, err := Exec(ctx, session, bound, bindOptions(session, options))
		require.NoError(t, err)
		require.Equal(t, 1, result.Attempts)
		require.Greater(t, result.Latency, time.Duration(0))
		require.Equal(t, cql.All, result.AchievedConsistency)
		require.NotEmpty(t, result.TraceIDs)
	}
	requests, attempts := observer.snapshot()
	require.Len(t, requests, 3)
	require.Len(t, attempts, 3)
	for i := range attempts {
		require.Equal(t, 1, attempts[i].Attempt)
		require.NoError(t, attempts[i].Err)
	}
	for _, id := range []int32{1, 2} {
		row, err := v2ReadOne(ctx, session, fmt.Sprintf("SELECT id FROM %s WHERE id=?", table), cql.IntValue(id))
		require.NoError(t, err)
		require.Equal(t, int64(id), sessionTestInt(t, row, "id"))
	}
	// Admission copies the profile, so a request keeps the values it was
	// admitted with.
	profile := *runtimeFor(session).InternalProfile
	profile.PageSize = 10
	_, cancel, admitted, err := admitExecution(ctx, session, original, bindOptions(session, gocql.Options{Profile: &profile}))
	require.NoError(t, err)
	profile.PageSize = 100
	require.Equal(t, int32(10), admitted.InternalOptions.InternalProfile.PageSize)
	admitted.InternalLogical.InternalFinish(gocql.Result{}, nil)
	admitted.InternalOwner.InternalRelease()
	cancel()
	original.Close()
	require.NoError(t, original.Wait(ctx))
	_, err = Exec(ctx, session, original, gocql.SessionOptions{})
	require.ErrorIs(t, err, gocql.ErrClosed)
}

func TestQueryShouldPrepare(t *testing.T) {
	t.Parallel()
	toPrepare := []string{"select * ", "INSERT INTO", "update table", "delete from", "begin batch"}
	cantPrepare := []string{"create table", "USE table", "LIST keyspaces", "alter table", "drop table", "grant user", "revoke user"}
	// Preparation is explicit in v2. Preserve every old classification input:
	// non-USE statements bind without keyword guessing, and explicit Prepare
	// reaches connection selection even for formerly unprepared DDL.
	session := func() *gocql.Session {
		opaqueStorageValuex := new(gocql.Session)
		implCluster.SessionData(opaqueStorageValuex).InternalLogger = nopLogger{}
		return opaqueStorageValuex
	}()
	defer session.Close()
	for _, stmt := range append(toPrepare, cantPrepare...) {
		t.Run(stmt, func(t *testing.T) {
			statement := cql.Command(stmt)
			bound, err := BindUnpreparedOwned(statement, cql.NewColumns(), cql.EmptyParams{})
			if firstCQLWord(stmt) == "USE" {
				require.ErrorIs(t, err, gocql.ErrUseStmt)
				_, err = gocql.Prepare(context.Background(), session, statement, gocql.PrepareOptions{})
				require.ErrorIs(t, err, gocql.ErrUseStmt)
				return
			}
			require.NoError(t, err)
			bound.Close()
			_, err = gocql.Prepare(context.Background(), session, statement, gocql.PrepareOptions{})
			require.ErrorIs(t, err, gocql.ErrNoConnections)
		})
	}
}

func TestBatchBasicAPI(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	retryPolicy := sessionTestRetry{action: retry.Next, maxAttempts: 2}
	session := createV2Session(t, func(config *gocql.Config) {
		profile := *config.DefaultProfile
		profile.Retry = retryPolicy
		config.DefaultProfile = &profile
	})
	defer session.Close()
	table := testTableName(t)
	require.NoError(t, v2CreateTable(ctx, session, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", table)))
	first := sessionTestBind(t, session, fmt.Sprintf("INSERT INTO %s(id) VALUES (?)", table), cql.IntValue(1))
	defer first.Close()
	second := sessionTestBind(t, session, fmt.Sprintf("INSERT INTO %s(id) VALUES (?)", table), cql.IntValue(2))
	defer second.Close()
	items := []batchItem{sessionTestItem(t, first), sessionTestItem(t, second)}
	require.Len(t, items, 2)
	inherited, err := resolveOptions(session, gocql.Options{})
	require.NoError(t, err)
	require.Equal(t, retryPolicy, inherited.InternalProfile.Retry)
	override := sessionTestRetry{action: retry.Next, maxAttempts: 4}
	observer := new(sessionTestObserver)
	options := gocql.Options{Consistency: cql.Option[cql.Consistency]{Value: cql.One, Set: true}, Retry: override, Trace: true, Observer: observer}
	resolved, err := resolveOptions(session, options)
	require.NoError(t, err)
	require.Equal(t, override, resolved.InternalProfile.Retry)
	require.Equal(t, cql.One, resolved.InternalProfile.Consistency)
	require.True(t, resolved.InternalOptions.Trace)
	// Native batch kind remains explicit; entries are immutable bound plans.
	// Per-request Result/Observer replace attempts/latency on a mutable batch.
	require.Zero(t, (gocql.Result{}).Attempts)
	require.Zero(t, (gocql.Result{}).Latency)
	for _, kind := range []cql.BatchKind{cql.UnloggedBatch, cql.LoggedBatch} {
		result, err := execBatch(ctx, session, kind, items, bindOptions(session, options))
		require.NoError(t, err)
		require.Equal(t, 1, result.Attempts)
		require.Greater(t, result.Latency, time.Duration(0))
		require.Equal(t, cql.One, result.AchievedConsistency)
		require.NotEmpty(t, result.TraceIDs)
	}
	requests, attempts := observer.snapshot()
	require.Len(t, requests, 2)
	require.Len(t, attempts, 2)
	for _, request := range requests {
		require.Len(t, request.BatchStatements, 2)
	}
	for _, id := range []int32{1, 2} {
		row, err := v2ReadOne(ctx, session, fmt.Sprintf("SELECT id FROM %s WHERE id=?", table), cql.IntValue(id))
		require.NoError(t, err)
		require.Equal(t, int64(id), sessionTestInt(t, row, "id"))
	}
	// A closed binding cannot be added to a batch; the other members remain
	// available for separate work.
	first.Close()
	_, err = execBatch(ctx, session, cql.LoggedBatch, items, bindOptions(session, options))
	require.ErrorIs(t, err, gocql.ErrClosed)
	require.NoError(t, first.Wait(ctx))
	_, err = Exec(ctx, session, second, gocql.SessionOptions{})
	require.NoError(t, err)
}

func TestConsistencyNames(t *testing.T) {
	t.Parallel()

	names := map[fmt.Stringer]string{
		cql.Any:         "ANY",
		cql.One:         "ONE",
		cql.Two:         "TWO",
		cql.Three:       "THREE",
		cql.Quorum:      "QUORUM",
		cql.All:         "ALL",
		cql.LocalQuorum: "LOCAL_QUORUM",
		cql.EachQuorum:  "EACH_QUORUM",
		cql.Serial:      "SERIAL",
		cql.LocalSerial: "LOCAL_SERIAL",
		cql.LocalOne:    "LOCAL_ONE",
	}

	for k, v := range names {
		if k.String() != v {
			t.Fatalf("expected '%v', got '%v'", v, k.String())
		}
	}
}

func TestIsUseStatement(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		input string
		exp   bool
	}{
		{"USE foo", true},
		{"USe foo", true},
		{"UsE foo", true},
		{"Use foo", true},
		{"uSE foo", true},
		{"uSe foo", true},
		{"usE foo", true},
		{"use foo", true},
		{"SELECT ", false},
		{"UPDATE ", false},
		{"INSERT ", false},
		{"", false},
	}

	for _, tc := range testCases {
		bound, err := BindUnpreparedOwned(cql.Command(tc.input), cql.NewColumns(), cql.EmptyParams{})
		if bound != nil {
			bound.Close()
		}
		v := errors.Is(err, gocql.ErrUseStmt)
		if v != tc.exp {
			t.Fatalf("expected %v but got %v for statement %q", tc.exp, v, tc.input)
		}
	}
}

// The old Ignore mode stopped retries but preserved errors. Its v2 equivalent
// is retry.Stop; retry.IgnoreWrite is a separate explicit error-suppression policy.
func TestRetryType_IgnoreRethrow(t *testing.T) {
	t.Parallel()
	session := createV2Session(t)
	defer session.Close()
	ctx := context.Background()
	bound, err := BindUnpreparedOwned(cql.Command(("INSERT INTO " + fixtureKeyspace("") + ".invalid_table(value) VALUES(1)")), cql.NewColumns(), cql.EmptyParams{})
	require.NoError(t, err)
	defer bound.Close()
	for _, mode := range []string{"Ignore", "Rethrow"} {
		for _, retries := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/%d", mode, retries), func(t *testing.T) {
				observer := new(sessionTestObserver)
				retryPolicy := sessionTestRetry{action: retry.Stop, maxAttempts: retries + 1}
				_, err := Exec(ctx, session, bound, bindOptions(session, gocql.Options{Idempotent: true, Retry: retryPolicy, Observer: observer}))
				require.Error(t, err)
				var server cql.RequestError
				require.ErrorAs(t, err, &server)
				requests, attempts := observer.snapshot()
				require.Len(t, requests, 1)
				require.Len(t, attempts, 1)
				require.Error(t, requests[0].Err)
				require.Error(t, attempts[0].Err)
				require.Equal(t, 1, requests[0].Result.Attempts)
			})
		}
	}
}

type sessionCache struct {
	orig       tls.ClientSessionCache
	values     map[string][][]byte
	caches     map[string][]int64
	valuesLock sync.Mutex
}

func (c *sessionCache) Get(sessionKey string) (session *tls.ClientSessionState, ok bool) {
	return c.orig.Get(sessionKey)
}

func (c *sessionCache) Put(sessionKey string, cs *tls.ClientSessionState) {
	if cs == nil {
		c.orig.Put(sessionKey, nil)
		return
	}
	ticket, _, err := cs.ResumptionState()
	if err != nil {
		panic(err)
	}
	if len(ticket) == 0 {
		panic("ticket should not be empty")
	}
	c.valuesLock.Lock()
	c.values[sessionKey] = append(c.values[sessionKey], ticket)
	c.valuesLock.Unlock()
	c.orig.Put(sessionKey, cs)
}

func (c *sessionCache) NumberOfTickets() int {
	c.valuesLock.Lock()
	defer c.valuesLock.Unlock()
	total := 0
	for _, tickets := range c.values {
		total += len(tickets)
	}
	return total
}

func newSessionCache() *sessionCache {
	return &sessionCache{
		orig:       tls.NewLRUClientSessionCache(1024),
		values:     make(map[string][][]byte),
		caches:     make(map[string][]int64),
		valuesLock: sync.Mutex{},
	}
}

func withSessionCache(cache tls.ClientSessionCache) func(*gocql.Config) {
	return func(config *gocql.Config) {
		// Preserve fixture roots, mutual TLS identity, and verifier callbacks.
		config.TLS = config.TLS.Clone()
		config.TLS.ClientSessionCache = cache
	}
}

func TestTLSTicketResumption(t *testing.T) {
	t.Parallel()

	if !*flagRunSslTest || *flagDistribution != "scylla" {
		t.Skip("requires a ScyllaDB TLS fixture")
	}
	if flagCassVersion.Major < 2025 || (flagCassVersion.Major == 2025 && flagCassVersion.Minor < 2) {
		t.Skip("TLS ticket resumption requires ScyllaDB 2025.2 or later")
	}

	c := newSessionCache()
	var handshakes, resumed atomic.Int64
	session := createV2Session(t, withSessionCache(c), func(config *gocql.Config) {
		config.TLS.VerifyConnection = func(state tls.ConnectionState) error {
			handshakes.Add(1)
			if state.DidResume {
				resumed.Add(1)
			}
			return nil
		}
	})
	defer session.Close()

	waitAllConnectionsOpened := func() error {
		endtime := time.Now().UTC().Add(time.Second * 10)
		for {
			if time.Now().UTC().After(endtime) {
				return fmt.Errorf("timed out waiting for all connections opened")
			}
			missing, err := clustertestkit.MissingConnections(session)
			if err != nil {
				return fmt.Errorf("failed to get missing connections count: %w", err)
			}
			if missing == 0 {
				return nil
			}
			time.Sleep(time.Millisecond * 100)
		}
	}

	if err := waitAllConnectionsOpened(); err != nil {
		t.Fatal(err)
	}
	tickets := c.NumberOfTickets()
	if tickets == 0 {
		t.Fatal("no tickets learned, which means that server does not support TLS tickets")
	}

	for round := 0; round < 2; round++ {
		previousHandshakes, previousResumed := handshakes.Load(), resumed.Load()
		clustertestkit.CloseAllConnections(session)
		if err := waitAllConnectionsOpened(); err != nil {
			t.Fatal(err)
		}
		if handshakes.Load() <= previousHandshakes {
			t.Fatal("pool reconnection did not establish new TLS connections")
		}
		if resumed.Load() <= previousResumed {
			t.Fatal("none of the replacement TLS connections resumed a session")
		}
	}
}

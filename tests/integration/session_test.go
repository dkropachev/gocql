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
func sessionTestBind(t testing.TB, session *gocql.Session, cql string, values ...gocql.Value) *Bound[gocql.Unit] {
	t.Helper()
	prepared, err := gocql.Prepare(context.Background(), session, gocql.Statement[[]gocql.Value, gocql.Unit]{CQL: cql, Params: gocql.DynamicParams, Rows: gocql.NoRows}, gocql.PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bound, err := BindOwned(prepared, values)
	if err != nil {
		t.Fatal(err)
	}
	return bound
}
func sessionTestItem[R any](t testing.TB, bound *Bound[R]) gocql.BatchItem {
	t.Helper()
	item, err := BatchEntry(bound)
	if err != nil {
		t.Fatal(err)
	}
	return item
}
func sessionTestInt(t testing.TB, row gocql.DynamicRow, name string) int64 {
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
	profile.Consistency, profile.PageSize, profile.Prefetch = gocql.All, 100, 1
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
	resolved, err := resolveOptions(session, testOptions(gocql.OptionValues{Trace: true}))
	require.NoError(t, err)
	require.Equal(t, gocql.All, resolved.InternalProfile.Consistency)
	require.Equal(t, int32(100), resolved.InternalProfile.PageSize)
	require.Equal(t, 1, resolved.InternalProfile.Prefetch)
	require.True(t, resolved.InternalOptions.Trace)

	intType, err := gocql.ScalarType(gocql.TypeInt)
	require.NoError(t, err)
	var encoded int32
	statement := gocql.Statement[int32, gocql.Unit]{CQL: "INSERT INTO test(id) VALUES (?)", Params: gocql.Params(gocql.ParamAt(0, gocql.Int32, func(n int32) int32 { encoded = n; return n })), Rows: gocql.NoRows}
	bound, err := BindUnpreparedOwned(statement, gocql.NewColumns(gocql.ColumnSpec{Name: "id", Type: intType}), int32(1))
	require.NoError(t, err)
	defer bound.Close()
	require.Equal(t, int32(1), encoded, "typed parameter accessor replaces callback binding")
	// An incomplete session now reports ErrNoConnections from the typed path.
	// It does not construct the old mutable Query/Iter error holder.
	_, err = Run(ctx, session, bound, gocql.Options{})
	require.ErrorIs(t, err, gocql.ErrNoConnections)
	batch := []gocql.BatchItem{sessionTestItem(t, bound)}
	_, err = gocql.ExecBatch(ctx, session, gocql.LoggedBatch, batch, gocql.Options{})
	require.ErrorIs(t, err, gocql.ErrNoConnections)
	require.NoError(t, session.Close())
	require.True(t, session.Closed())
	require.NoError(t, session.Close())
	_, err = gocql.ExecBatch(ctx, session, gocql.LoggedBatch, batch, gocql.Options{})
	require.ErrorIs(t, err, gocql.ErrClosed)
	_, err = Exec(ctx, session, bound, gocql.Options{})
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
	prepared, err := gocql.Prepare(ctx, session, gocql.Statement[int32, gocql.Unit]{CQL: fmt.Sprintf("INSERT INTO %s(id) VALUES (?)", table), Params: gocql.OneParam(gocql.Int32), Rows: gocql.NoRows}, gocql.PrepareOptions{})
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
	options := testOptions(gocql.OptionValues{Consistency: gocql.Option[gocql.Consistency]{Value: gocql.All, Set: true}, SerialConsistency: gocql.Option[gocql.Option[gocql.Consistency]]{Value: gocql.Option[gocql.Consistency]{Value: gocql.LocalSerial, Set: true}, Set: true}, PageSize: gocql.Option[int32]{Value: 10, Set: true}, Prefetch: gocql.Option[int]{Value: 1, Set: true}, Retry: retryPolicy, Trace: true, Observer: observer})
	resolved, err := resolveOptions(session, options)
	require.NoError(t, err)
	require.Equal(t, gocql.All, resolved.InternalProfile.Consistency)
	require.Equal(t, gocql.LocalSerial, resolved.InternalProfile.SerialConsistency.Value)
	require.Equal(t, int32(10), resolved.InternalProfile.PageSize)
	require.Equal(t, 1, resolved.InternalProfile.Prefetch)
	require.Equal(t, retryPolicy, resolved.InternalProfile.Retry)
	serialValues := options.Values()
	serialValues.Consistency.Value = gocql.LocalSerial
	serialRead := testOptions(serialValues)
	resolvedSerial, err := resolveOptions(session, serialRead)
	require.NoError(t, err)
	require.Equal(t, gocql.LocalSerial, resolvedSerial.InternalProfile.Consistency)

	for _, bound := range []*Bound[gocql.Unit]{original, rebound, original} {
		result, err := Exec(ctx, session, bound, options)
		require.NoError(t, err)
		require.Equal(t, 1, result.Attempts)
		require.Greater(t, result.Latency, time.Duration(0))
		require.Equal(t, gocql.All, result.AchievedConsistency)
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
		row, err := v2ReadOne(ctx, session, fmt.Sprintf("SELECT id FROM %s WHERE id=?", table), gocql.IntValue(id))
		require.NoError(t, err)
		require.Equal(t, int64(id), sessionTestInt(t, row, "id"))
	}
	// Admission copies the profile, so a request keeps the values it was
	// admitted with.
	profile := *runtimeFor(session).InternalProfile
	profile.PageSize = 10
	_, cancel, admitted, err := admitExecution(ctx, session, original, testOptions(gocql.OptionValues{Profile: &profile}))
	require.NoError(t, err)
	profile.PageSize = 100
	require.Equal(t, int32(10), admitted.InternalOptions.InternalProfile.PageSize)
	admitted.InternalLogical.InternalFinish(gocql.Result{}, nil)
	admitted.InternalOwner.InternalRelease()
	cancel()
	original.Close()
	require.NoError(t, original.Wait(ctx))
	_, err = Exec(ctx, session, original, gocql.Options{})
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
	for _, cql := range append(toPrepare, cantPrepare...) {
		t.Run(cql, func(t *testing.T) {
			statement := gocql.Statement[gocql.Unit, gocql.Unit]{CQL: cql, Params: gocql.NoParams, Rows: gocql.NoRows}
			bound, err := BindUnpreparedOwned(statement, gocql.NewColumns(), gocql.Unit{})
			if firstCQLWord(cql) == "USE" {
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
	first := sessionTestBind(t, session, fmt.Sprintf("INSERT INTO %s(id) VALUES (?)", table), gocql.IntValue(1))
	defer first.Close()
	second := sessionTestBind(t, session, fmt.Sprintf("INSERT INTO %s(id) VALUES (?)", table), gocql.IntValue(2))
	defer second.Close()
	items := []gocql.BatchItem{sessionTestItem(t, first), sessionTestItem(t, second)}
	require.Len(t, items, 2)
	inherited, err := resolveOptions(session, gocql.Options{})
	require.NoError(t, err)
	require.Equal(t, retryPolicy, inherited.InternalProfile.Retry)
	override := sessionTestRetry{action: retry.Next, maxAttempts: 4}
	observer := new(sessionTestObserver)
	options := testOptions(gocql.OptionValues{Consistency: gocql.Option[gocql.Consistency]{Value: gocql.One, Set: true}, Retry: override, Trace: true, Observer: observer})
	resolved, err := resolveOptions(session, options)
	require.NoError(t, err)
	require.Equal(t, override, resolved.InternalProfile.Retry)
	require.Equal(t, gocql.One, resolved.InternalProfile.Consistency)
	require.True(t, resolved.InternalOptions.Trace)
	// Native batch kind remains explicit; entries are immutable bound plans.
	// Per-request Result/Observer replace attempts/latency on a mutable batch.
	require.Zero(t, (gocql.Result{}).Attempts)
	require.Zero(t, (gocql.Result{}).Latency)
	for _, kind := range []gocql.BatchKind{gocql.UnloggedBatch, gocql.LoggedBatch} {
		result, err := gocql.ExecBatch(ctx, session, kind, items, options)
		require.NoError(t, err)
		require.Equal(t, 1, result.Attempts)
		require.Greater(t, result.Latency, time.Duration(0))
		require.Equal(t, gocql.One, result.AchievedConsistency)
		require.NotEmpty(t, result.TraceIDs)
	}
	requests, attempts := observer.snapshot()
	require.Len(t, requests, 2)
	require.Len(t, attempts, 2)
	for _, request := range requests {
		require.Len(t, request.BatchStatements, 2)
	}
	for _, id := range []int32{1, 2} {
		row, err := v2ReadOne(ctx, session, fmt.Sprintf("SELECT id FROM %s WHERE id=?", table), gocql.IntValue(id))
		require.NoError(t, err)
		require.Equal(t, int64(id), sessionTestInt(t, row, "id"))
	}
	// BatchEntry is only a descriptor. Closing any member prevents the entire
	// batch admission, and other members remain available for separate work.
	first.Close()
	_, err = gocql.ExecBatch(ctx, session, gocql.LoggedBatch, items, options)
	require.ErrorIs(t, err, gocql.ErrClosed)
	require.NoError(t, first.Wait(ctx))
	_, err = Exec(ctx, session, second, gocql.Options{})
	require.NoError(t, err)
}

func TestConsistencyNames(t *testing.T) {
	t.Parallel()

	names := map[fmt.Stringer]string{
		gocql.Any:         "ANY",
		gocql.One:         "ONE",
		gocql.Two:         "TWO",
		gocql.Three:       "THREE",
		gocql.Quorum:      "QUORUM",
		gocql.All:         "ALL",
		gocql.LocalQuorum: "LOCAL_QUORUM",
		gocql.EachQuorum:  "EACH_QUORUM",
		gocql.Serial:      "SERIAL",
		gocql.LocalSerial: "LOCAL_SERIAL",
		gocql.LocalOne:    "LOCAL_ONE",
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
		bound, err := BindUnpreparedOwned(gocql.Statement[gocql.Unit, gocql.Unit]{CQL: tc.input, Params: gocql.NoParams, Rows: gocql.NoRows}, gocql.NewColumns(), gocql.Unit{})
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
	bound, err := BindUnpreparedOwned(gocql.Statement[gocql.Unit, gocql.Unit]{CQL: ("INSERT INTO " + fixtureKeyspace("") + ".invalid_table(value) VALUES(1)"), Params: gocql.NoParams, Rows: gocql.NoRows}, gocql.NewColumns(), gocql.Unit{})
	require.NoError(t, err)
	defer bound.Close()
	for _, mode := range []string{"Ignore", "Rethrow"} {
		for _, retries := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/%d", mode, retries), func(t *testing.T) {
				observer := new(sessionTestObserver)
				retryPolicy := sessionTestRetry{action: retry.Stop, maxAttempts: retries + 1}
				_, err := Exec(ctx, session, bound, testOptions(gocql.OptionValues{Idempotent: true, Retry: retryPolicy, Observer: observer}))
				require.Error(t, err)
				var server gocql.RequestError
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

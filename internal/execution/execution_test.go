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

package execution

import (
	implCql "github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/retry"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/routing"
	implRuntimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

func TestKindDeclarationAndServerRefinement(t *testing.T) {
	for _, kind := range []cqltype.StatementKind{cqltype.UnknownStatement, cqltype.OrdinaryStatement, cqltype.ConditionalStatement} {
		for _, cql := range []string{"SELECT value FROM ks.tbl", "INSERT INTO t (x) VALUES (1) IF NOT EXISTS", "SELECT 'IF' FROM t"} {
			b, err := BindUnpreparedOwned(binding.Statement[codec.Unit, codec.Unit]{CQL: cql, Kind: kind, Params: codec.NoParams, Rows: codec.NoRows}, cqltype.NewColumns(), codec.Unit{})
			if err != nil {
				t.Fatal(err)
			}
			if b.Routing().Kind != kind || b.Routing().Conditional != (kind != cqltype.OrdinaryStatement) {
				t.Fatal(cql, b.Routing())
			}
			b.Close()
		}
	}
	s, cs, _ := runtimeTestSession(t, 1)
	for _, test := range []struct {
		negotiated, lwt bool
		want            cqltype.StatementKind
	}{{false, false, cqltype.UnknownStatement}, {true, false, cqltype.OrdinaryStatement}, {true, true, cqltype.ConditionalStatement}, {false, true, cqltype.ConditionalStatement}} {
		cs[0].InternalFramers.InternalDefaults.InternalFlagLWT = 0
		if test.negotiated {
			cs[0].InternalFramers.InternalDefaults.InternalFlagLWT = 1 << 30
		}
		cs[0].InternalRefreshNativeProtocol()
		info, err := InternalRuntimeFor(s).InternalHostPreparedInfo(cs[0], &transport.InternalResponsePage{InternalResponseData: model.ResponseData{Lwt: test.lwt}}, &InternalPreparedState{InternalStatement: "INSERT x IF y"})
		if err != nil || info.Kind != test.want {
			t.Fatal(info, err)
		}
	}
}

type countSpeculation struct{ calls atomic.Int32 }

func (p *countSpeculation) Delay(int, routing.MetricsSnapshot) (time.Duration, bool) {
	p.calls.Add(1)
	return 0, false
}
func TestUnknownConditionalNeverSpeculateAndProfileStable(t *testing.T) {
	for _, kind := range []cqltype.StatementKind{cqltype.UnknownStatement, cqltype.OrdinaryStatement, cqltype.ConditionalStatement} {
		t.Run(map[cqltype.StatementKind]string{cqltype.UnknownStatement: "unknown", cqltype.OrdinaryStatement: "ordinary", cqltype.ConditionalStatement: "conditional"}[kind], func(t *testing.T) {
			s, cs, ps := runtimeTestSession(t, 1)
			spec := &countSpeculation{}
			profile := settings.InternalDefaultProfile()
			profile.Speculation = spec
			profile.Prefetch = 0
			handle := profileOf(profile)
			seen := make(chan struct{})
			resume := make(chan struct{})
			done := make(chan struct{})
			go func() {
				defer close(done)
				stream := typedTestReadRequest(t, cs[0], ps[0])
				close(seen)
				<-resume
				_, _ = ps[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "result")))
			}()
			b := runtimeQuery(t)
			BoundData(b).InternalRequest.InternalRouting.Kind = kind
			BoundData(b).InternalRequest.InternalRouting.Conditional = kind != cqltype.OrdinaryStatement
			result := make(chan error, 1)
			go func() {
				page, err := RunPage(context.Background(), s, b, model.StartPage(), testOptions(OptionValues{Profile: handle, Idempotent: true}))
				if page != nil {
					page.Close()
				}
				result <- err
			}()
			<-seen
			close(resume)
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			want := int32(0)
			if kind == cqltype.OrdinaryStatement {
				want = 1
			}
			if spec.calls.Load() != want {
				t.Fatal("speculation classification", kind, spec.calls.Load())
			}
			b.Close()
			<-done
		})
	}
}
func TestSessionClosePreservesDeliveredPageAndDrains(t *testing.T) {
	for _, automatic := range []bool{false, true} {
		t.Run(map[bool]string{false: "page", true: "rows"}[automatic], func(t *testing.T) {
			s, cs, ps := runtimeTestSession(t, 1)
			implCluster.SessionData(s).InternalCtx, implCluster.SessionData(s).InternalCancel = context.WithCancel(context.Background())
			go func() {
				stream := typedTestReadRequest(t, cs[0], ps[0])
				_, _ = ps[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "retained")))
			}()
			b := runtimeQuery(t)
			var view cqltype.Bytes
			var advance func() bool
			var closeRows func() error
			var retain func() (*PageLease, error)
			if automatic {
				r, err := Run(context.Background(), s, b, Options{})
				if err != nil {
					t.Fatal(err)
				}
				if !r.Next() {
					t.Fatal(r.Err())
				}
				view = r.Row()
				advance = r.Next
				closeRows = r.Close
				retain = r.Retain
			} else {
				p, err := RunPage(context.Background(), s, b, model.StartPage(), Options{})
				if err != nil {
					t.Fatal(err)
				}
				if !p.Next() {
					t.Fatal(p.Err())
				}
				view = p.Row()
				advance = p.Next
				closeRows = p.Close
				retain = p.Retain
			}
			lease, err := retain()
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Close()
			b.Close()
			var wg sync.WaitGroup
			for i := 0; i < 4; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); s.Close() }()
			}
			wg.Wait()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := b.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			if string(view.OwnedBytes()) != "retained" {
				t.Fatal("shutdown invalidated current row")
			}
			if advance() {
				t.Fatal("advance after shutdown")
			}
			closeRows()
			if string(view.OwnedBytes()) != "retained" {
				t.Fatal("retained lease lost")
			}
			if _, err := Run(context.Background(), s, b, Options{}); !errors.Is(err, implRuntimeerrors.ErrClosed) {
				t.Fatal(err)
			}
		})
	}
}
func TestBatchAdmissionRollbackAndClassification(t *testing.T) {
	s, _, _ := runtimeTestSession(t, 0)
	a := runtimeQuery(t)
	b := runtimeQuery(t)
	b.Close()
	ea, _ := BatchEntry(a)
	eb, _ := BatchEntry(b)
	_, err := RunBatch(context.Background(), s, cqltype.LoggedBatch, []BatchItem{ea, eb}, codec.NoRows, Options{})
	if !errors.Is(err, implRuntimeerrors.ErrClosed) {
		t.Fatal(err)
	}
	a.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := a.Wait(ctx); err != nil {
		t.Fatal("partial batch admission retained", err)
	}
}

type panicObserver struct{}

func (panicObserver) ObserveRequest(settings.RequestEvent) { panic("request") }
func (panicObserver) ObserveAttempt(routing.AttemptEvent)  { panic("attempt") }

type panicLogger struct{}

func (panicLogger) Print(...any)          { panic("logger") }
func (panicLogger) Printf(string, ...any) { panic("logger") }
func (panicLogger) Println(...any)        { panic("logger") }
func TestObserverAndLoggerPanicsContained(t *testing.T) {
	logger := logging.InternalV2SafeLogger{InternalLogger: panicLogger{}}
	logger.Print("x")
	logger.Printf("%s", "x")
	logger.Println("x")
	s, cs, ps := runtimeTestSession(t, 1)
	implCluster.SessionData(s).InternalLogger = panicLogger{}
	go func() {
		stream := typedTestReadRequest(t, cs[0], ps[0])
		_, _ = ps[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "row")))
	}()
	b := runtimeQuery(t)
	defer b.Close()
	rows, err := Run(context.Background(), s, b, testOptions(OptionValues{Observer: panicObserver{}}))
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal(rows.Err())
	}
	rows.Close()
	if m := s.Metrics(); m.Requests != 1 || m.Attempts != 1 || m.InFlight != 0 {
		t.Fatal(m)
	}
}
func TestConfigurationDefaultsAndZeroSemantics(t *testing.T) {
	c := settings.DefaultConfig("localhost")
	p := *c.DefaultProfile
	if c.Metadata.PeriodicRefresh != time.Minute || c.Metadata.RequestTimeout != 31*time.Second || p.Timeout != 30*time.Second || p.PageTimeout != 0 || p.Prefetch != 1 || p.PageSize != 5000 || p.Consistency != cqltype.LocalQuorum {
		t.Fatal(c.Metadata, p)
	}
	for _, edit := range []func(*settings.Config){func(c *settings.Config) { c.Metadata.AgreementTimeout = 0 }, func(c *settings.Config) { c.Metadata.AgreementPoll = 0 }, func(c *settings.Config) {
		c.Metadata.ServerTimeout = implCql.Option[time.Duration]{Set: true, Value: time.Nanosecond}
	}} {
		invalid := settings.DefaultConfig("localhost")
		edit(&invalid)
		if settings.InternalValidateConfig(invalid) == nil {
			t.Fatal("invalid explicit setting accepted")
		}
	}
	c.ConnectTimeout = 0
	c.DNSTimeout = 0
	c.Metadata.PeriodicRefresh = 0
	c.Metadata.RequestTimeout = 0
	c.Metadata.ServerTimeout = implCql.Option[time.Duration]{Set: true, Value: 0}
	if err := settings.InternalValidateConfig(c); err != nil {
		t.Fatal(err)
	}
	s := func() *cluster.Session {
		opaqueStorageValue := new(cluster.Session)
		return opaqueStorageValue
	}()

	InternalRuntimeFor(s).InternalSetProfile(c.DefaultProfile)
	options := testOptions(OptionValues{Timeout: implCql.Option[time.Duration]{Set: true}, PageTimeout: implCql.Option[time.Duration]{Set: true}, SerialConsistency: implCql.Option[implCql.Option[implCql.Consistency]]{Set: true}, Prefetch: implCql.Option[int]{Set: true}, Timestamp: implCql.Option[int64]{Set: true}, NowInSeconds: implCql.Option[int32]{Set: true}})
	resolved, err := resolveOptions(s, options)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.InternalProfile.Timeout != 0 || resolved.InternalProfile.PageTimeout != 0 || resolved.InternalProfile.Prefetch != 0 || resolved.InternalProfile.SerialConsistency.Set || !resolved.InternalOptions.Timestamp.Set || !resolved.InternalOptions.NowInSeconds.Set {
		t.Fatal(resolved)
	}
}

func TestQualifiedLegacyProtocolKeyspace(t *testing.T) {
	for _, test := range []struct {
		cql, keyspace string
		want          bool
	}{
		{"SELECT value FROM ks.tbl", "ks", true}, {"INSERT INTO ks.tbl(k)VALUES('ks.tbl')", "ks", true}, {"UPDATE ks.tbl SET value=1 WHERE k=2", "ks", true}, {"DELETE FROM ks.tbl WHERE k=2", "ks", true}, {"TRUNCATE TABLE ks.tbl", "ks", true}, {"CREATE TABLE IF NOT EXISTS ks.tbl(k int PRIMARY KEY)", "ks", true}, {"SELECT * FROM \"Ks\".\"Tbl\"", "Ks", true}, {"SELECT * FROM tbl WHERE value='ks.tbl'", "ks", false}, {"SELECT * FROM other.tbl -- ks.tbl", "ks", false}, {"SELECT * FROM ks.tbl /* unfinished", "ks", false},
	} {
		if got := InternalQualifiedForKeyspace(test.cql, test.keyspace); got != test.want {
			t.Fatal(test.cql, got, test.want)
		}
	}
}

func TestHostPinSurvivesCustomPolicy(t *testing.T) {
	s, cs, _ := runtimeTestSession(t, 2)
	pinned := implEndpoint.InternalHostInfoHostUUID(cs[0].InternalHost)
	other := implEndpoint.InternalHostInfoHostUUID(cs[1].InternalHost)
	plan := &routing.InternalTargetPlan{InternalTargets: []model.Target{{HostID: other}}}
	if conn, err := implCluster.InternalSessionSelectTypedConn(s, context.Background(), model.RoutingInfo{HostID: implCql.Option[cqltype.UUID]{Set: true, Value: pinned}}, plan); conn != nil || !errors.Is(err, implRuntimeerrors.ErrNoConnections) {
		t.Fatal("host pin escaped", conn, err)
	}
}
func TestSafeRetryBytesWritten(t *testing.T) {
	for _, written := range []bool{false, true} {
		err := runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: errors.New("write"), InternalPotentiallyExecuted: written})
		decision := (retry.Safe{}).Decide(retry.Attempt{Err: err, Number: 1, BytesWritten: InternalAttemptBytesWritten(err)})
		if (decision.Action == retry.Next) == written {
			t.Fatal(written, decision)
		}
	}
	if !InternalAttemptBytesWritten(errors.New("unknown completion")) {
		t.Fatal("unproven write classified safe")
	}
}

func TestManualAndBatchPagingSettingsAreExplicit(t *testing.T) {
	s, _, _ := runtimeTestSession(t, 0)
	b := runtimeQuery(t)
	defer b.Close()
	_, err := RunUnpaged(context.Background(), s, b, testOptions(OptionValues{PageSize: implCql.Option[int32]{Set: true, Value: 1}}))
	var unsupported *settings.UnsupportedSettingError
	if !errors.As(err, &unsupported) {
		t.Fatal(err)
	}
	_, err = RunPage(context.Background(), s, b, model.StartPage(), testOptions(OptionValues{Prefetch: implCql.Option[int]{Set: true, Value: 1}}))
	if !errors.As(err, &unsupported) {
		t.Fatal(err)
	}
	item, _ := BatchEntry(b)
	_, err = RunBatch(context.Background(), s, cqltype.LoggedBatch, []BatchItem{item}, codec.NoRows, testOptions(OptionValues{PageSize: implCql.Option[int32]{Set: true, Value: 1}}))
	if !errors.As(err, &unsupported) {
		t.Fatal(err)
	}
}

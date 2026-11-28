// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/retry"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

// canExecuteDirect reports whether request's pages run on the calling
// goroutine. Executions of a reusable query and one-shot executions both
// qualify; a request pinned to a connection with a stream observer does not,
// since executePageDirect hands such a connection to the general path.
func canExecuteDirect(request *InternalExecutionRequest) bool {
	if request.InternalLogical == nil || request.options.InternalProfile.Speculation != nil {
		return false
	}
	if request.connection != nil && request.connection.InternalStreamObserver != nil {
		return false
	}
	return canUseDirectProfile(request.options.InternalProfile)
}
func canUseDirectProfile(profile settings.Profile) bool {
	if profile.Speculation != nil {
		return false
	}
	switch profile.Routing.(type) {
	case nil:
		return true
	case *routing.InternalRoutingPolicy:
		return true
	default:
		return false // custom routing callbacks retain cancellation isolation
	}
}

// Submission transfers buffer ownership to the persistent connection writer.
// The calling goroutine coordinates ordinary attempts directly, without an
// outcome channel or per-attempt goroutine. Observers and latency-aware
// routing are told about each attempt on this goroutine; speculation and
// custom routing policies keep the general execution path.
func (s *InternalSessionRuntime) executePageDirect(ctx context.Context, request *InternalExecutionRequest, state cql.PageState, plan routing.QueryPlan) (*transport.InternalResponsePage, error) {
	consistency := request.options.InternalProfile.Consistency
	aggregate := cql.Result{AchievedConsistency: consistency}
	var log *attemptLog // allocated on the first failed attempt
	failure := func(err error) (*transport.InternalResponsePage, error) { return nil, log.error(err, aggregate) }
	var conn *transport.Conn
	pinned := false // the first attempt went to request.pinned
	observe := request.InternalLogical.observesAttempts() || routingObservesLatency(request.options.InternalProfile.Routing)
	var first time.Time // when the page's first attempt started
	for attempt := 1; ; attempt++ {
		if err := transport.ContextError(ctx); err != nil {
			return failure(err)
		}
		if conn == nil {
			conn = request.connection
		}
		if conn == nil && attempt == 1 && pinnedUsable(request.pinned) {
			conn, pinned = request.pinned, true
		}
		if conn == nil {
			if plan == nil && request.pinned != nil {
				// A continuation page came without a plan. When the pinned
				// connection took its first attempt, as a plan's first target
				// would have, the retry plan skips that host.
				var err error
				if plan, err = s.pagePlan(request, s.Core.Metadata(), nil, true); err != nil {
					return failure(err)
				}
				if pinned && plan != nil {
					plan = &skipHostPlan{plan: plan, host: request.pinned.InternalHostID()}
				}
			}
			var err error
			conn, err = s.Core.SelectTypedConn(ctx, request.routing, plan)
			if err != nil {
				return failure(err)
			}
		}
		if conn.InternalStreamObserver != nil {
			fallback := *request
			fallback.query, fallback.connection, fallback.responseDest = nil, conn, nil
			// The same page continues on the general path, under the
			// number executePage gave it.
			page, err := s.executePageAttempts(ctx, &fallback, state)
			if err != nil {
				// Keep the attempts that failed before the hand-off.
				err = log.merge(err, aggregate)
			}
			return page, err
		}
		s.countAttempt(request, conn)
		var started time.Time
		if l := request.InternalLogical; l != nil && !l.attemptTimed && !l.started.IsZero() {
			// The first attempt starts with the execution: routing it took
			// no time worth another clock reading.
			l.attemptTimed = true
			started = l.started
		} else {
			started = time.Now()
		}
		if attempt == 1 {
			first = started
		}
		var end func(routing2.AttemptEvent)
		var observed time.Time
		if observe {
			end, observed = startDirectAttempt(request, conn, attempt, consistency)
		}
		response, err := s.executeTypedAttempt(ctx, conn, request, state, consistency)
		latency := time.Since(started)
		if observe {
			s.endDirectAttempt(request, conn, attempt, consistency, observed, response, err, end)
		}
		if err != nil {
			s.countAttemptEnd(request, conn, err)
		}
		aggregate.Attempts = attempt
		aggregate.AttemptLatency += latency
		aggregate.HostID = conn.InternalHostID()
		aggregate.Address = conn.Address()
		aggregate.AchievedConsistency = consistency
		mergeResponseResult(&aggregate, response, cql.CQLTraceRef{HostID: aggregate.HostID, Page: int(request.page), Attempt: attempt, Failed: err != nil})
		trace := responseTraceID(response)
		if err == nil {
			s.recordPage(request, time.Since(first))
			// Attempts run one at a time here, and a failed attempt's response
			// is released before the next; later pages get their own storage.
			request.responseDest = nil
			request.served = conn
			response.Result = aggregate
			response.Result.FetchedRows = int64(response.RowCount)
			if response.PagingState != nil {
				response.State = cqltype.PageStateOwned(response.PagingState)
			} else {
				response.State = cqltype.CompletePageState()
			}
			return response, nil
		}
		if response != nil {
			ddl := response.SchemaChange()
			response.Owner.Release()
			if ddl {
				if log == nil {
					log = new(attemptLog)
				}
				log.fail(err, conn, attempt, consistency, trace, request.partialBatch())
				log.raise(cqlerr.PartiallyExecuted)
				return failure(schemaChangeFailure(err))
			}
		}
		if log == nil {
			log = new(attemptLog)
		}
		attemptStatus := log.fail(err, conn, attempt, consistency, trace, request.partialBatch())
		if ctxErr := transport.ContextError(ctx); ctxErr != nil {
			// The context ended before the attempt failed. A cancellation, such
			// as Session.Close's, and the failure it causes (the closed
			// connection) can reach the attempt in either order; the
			// cancellation is the cause.
			return failure(ctxErr)
		}
		decision := decideRetry(ctx, request, conn, err, attempt, consistency, attemptStatus, first)
		if decision.Action == retry.Stop {
			return failure(err)
		}
		if invalid := checkDecision(decision, err); invalid != nil {
			return failure(invalid)
		}
		if decision.Consistency.Set {
			consistency = decision.Consistency.Value
		}
		if decision.Delay > 0 {
			timer := time.NewTimer(decision.Delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return failure(transport.ContextError(ctx))
			case <-s.sessionDone():
				// No attempt is in flight during the delay, so a closing
				// session ends the execution here.
				timer.Stop()
				return failure(cqlerr.ErrClosed)
			case <-timer.C:
			}
		}
		if decision.Action == retry.Next {
			conn = nil
		}
		s.countRetry(request)
	}
}

// skipHostPlan is a retry plan for a page whose first attempt went to a
// pinned connection: it omits that connection's host.
type skipHostPlan struct {
	plan routing.QueryPlan
	host cql.UUID
}

func (p *skipHostPlan) Next() (routing.Route, bool) {
	for {
		r, ok := p.plan.Next()
		if !ok || r.HostID() != p.host {
			return r, ok
		}
	}
}

// mergeResponseResult adds a response's diagnostics to result. A trace
// session the response names is recorded as attempt, with its ID filled in.
func mergeResponseResult(result *cql.Result, response *transport.InternalResponsePage, attempt cql.CQLTraceRef) {
	// Warnings, trace IDs and custom payload all live in the rare extras.
	if response == nil || response.Extras == nil {
		return
	}
	codec.AddWarnings(result, response.Warnings())
	if id := response.CQLTraceID(); id.Set {
		attempt.ID = id.Value
		codec.AddCQLTrace(result, attempt)
	}
	for _, entry := range response.Payload() {
		if entry.Value.Valid {
			entry.Value.Value = entry.Value.Value.Clone()
		}
		codec.AddCustomPayload(result, entry)
	}
}

// routingObservesLatency reports whether policy learns from attempt latencies.
func routingObservesLatency(policy routing.RoutingPolicy) bool {
	p, ok := policy.(*routing.InternalRoutingPolicy)
	return ok && p.InternalOptions.Latency.Enabled
}

// attemptTarget is the host and shard an attempt on conn goes to.
func attemptTarget(request *InternalExecutionRequest, conn *transport.Conn) metadata.Target {
	target := metadata.Target{HostID: conn.InternalHostID(), ShardID: request.routing.ShardID}
	if implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0 {
		target.ShardID = cql.Option[uint32]{Value: uint32(implEndpoint.ScyllaConnectionFeaturesData(&conn.InternalScyllaSupported).InternalShard), Set: true}
	}
	return target
}

// startDirectAttempt tells the request's observer, on the calling
// goroutine, that a direct attempt starts, and returns its end callback with
// the attempt's start. That start excludes routing, as on the general path,
// although the execution's own latency may include it.
//
//go:noinline
func startDirectAttempt(request *InternalExecutionRequest, conn *transport.Conn, attempt int, consistency cql.Consistency) (func(routing2.AttemptEvent), time.Time) {
	l := request.InternalLogical
	started := time.Now()
	return l.startAttempt(routing2.AttemptStart{RequestID: l.id, Page: int(request.page), Attempt: attempt, Target: attemptTarget(request, conn), Address: conn.Address(), AddrPort: conn.AddrPort(), Consistency: consistency, Started: started}), started
}

// endDirectAttempt reports one finished direct attempt to the observer's end
// callback and to a latency-aware routing policy, on the calling goroutine.
//
//go:noinline
func (s *InternalSessionRuntime) endDirectAttempt(request *InternalExecutionRequest, conn *transport.Conn, attempt int, consistency cql.Consistency, started time.Time, response *transport.InternalResponsePage, err error, end func(routing2.AttemptEvent)) {
	event := routing2.AttemptEvent{RequestID: request.InternalLogical.id, Page: int(request.page), Attempt: attempt, Target: attemptTarget(request, conn), Address: conn.Address(), AddrPort: conn.AddrPort(), Consistency: consistency, Started: started, Latency: time.Since(started), Err: err, CQLTraceID: responseTraceID(response)}
	attemptEnded(&event, request.partialBatch())
	if end != nil {
		perfAdd(1, 1)
		s.Core.ObserveSafely(request.InternalLogical.logContext(), func() { end(event) })
	}
	if policy := request.options.InternalProfile.Routing; routingObservesLatency(policy) {
		s.Core.ObserveSafely(request.InternalLogical.logContext(), func() { policy.Learn(event) })
	}
}

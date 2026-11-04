// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"fmt"
	"time"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/routing"
)

func canExecuteDirect(request *InternalExecutionRequest) bool {
	if request.query == nil || request.InternalLogical == nil || request.InternalLogical.observer != nil || request.InternalOptions.InternalProfile.Speculation != nil {
		return false
	}
	return canUseDirectProfile(request.InternalOptions.InternalProfile) && request.query.runtime.InternalConfig.CoalesceWait == 0
}
func canUseDirectProfile(profile Profile) bool {
	if profile.Speculation != nil {
		return false
	}
	switch policy := profile.Routing.(type) {
	case nil:
		return true
	case *routing.InternalRoutingPolicy:
		return !policy.InternalOptions.Latency.Enabled
	default:
		return false // custom routing callbacks retain cancellation isolation
	}
}

// Submission transfers buffer ownership to the persistent connection writer.
// The calling goroutine coordinates ordinary attempts directly, without an
// outcome channel or per-attempt goroutine. Optional callbacks/speculation keep
// their general execution path.
func (s *InternalSessionRuntime) executePageDirect(ctx context.Context, request *InternalExecutionRequest, state PageState, plan QueryPlan) (*responsePage, error) {
	consistency := request.InternalOptions.InternalProfile.Consistency
	aggregate := Result{AchievedConsistency: consistency}
	failure := func(err error) (*responsePage, error) { return nil, &ExecutionError{Err: err, Result: aggregate} }
	var conn *Conn
	pinned := false // the first attempt went to request.pinned
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
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
				if plan, err = s.pagePlan(request, s.Session.Metadata(), true); err != nil {
					return failure(err)
				}
				if pinned && plan != nil {
					plan = &skipHostPlan{plan: plan, host: request.pinned.InternalHostID()}
				}
			}
			var err error
			conn, err = implCluster.InternalSessionSelectTypedConn(s.Session, ctx, request.InternalRouting, plan)
			if err != nil {
				return failure(err)
			}
		}
		if conn.InternalStreamObserver != nil {
			fallback := *request
			fallback.query, fallback.connection, fallback.responseDest = nil, conn, nil
			return s.executePage(ctx, &fallback, state)
		}
		s.attempts.Add(1)
		started := time.Now()
		response, err := s.executeTypedAttempt(ctx, conn, request, state, consistency)
		aggregate.Attempts = attempt
		aggregate.Latency += time.Since(started)
		aggregate.HostID = conn.InternalHostID()
		aggregate.AchievedConsistency = consistency
		mergeResponseResult(&aggregate, response)
		if err == nil {
			// Attempts run one at a time here, and a failed attempt's response
			// is released before the next; later pages get their own storage.
			request.responseDest = nil
			request.served = conn
			response.Result = aggregate
			response.Result.Rows = int64(response.RowCount)
			if response.PagingState != nil {
				response.State = model.PageStateOwned(response.PagingState, 1)
			} else {
				response.State = completePageState()
			}
			return response, nil
		}
		if response != nil {
			ddl := response.SchemaChange()
			response.Owner.Release()
			if ddl {
				return failure(fmt.Errorf("gocql: DDL succeeded; automatic agreement/refresh failed: %w", err))
			}
		}
		decision := RetryDecision{}
		if retry := request.InternalOptions.InternalProfile.Retry; retry != nil {
			decision = retry.Decide(AttemptError{Err: err, Attempt: attempt, Idempotent: request.InternalOptions.InternalOptions.Idempotent, Consistency: consistency, Conditional: request.InternalRouting.Conditional, BytesWritten: InternalAttemptBytesWritten(err)})
		}
		switch decision.Action {
		case IgnoreWriteError:
			return &responsePage{InternalResponseData: responseData{State: completePageState(), Result: aggregate}}, nil
		case StopRetry:
			return failure(err)
		case RetrySame, RetryNext:
		default:
			return failure(typeValidation("retry", "unknown retry action"))
		}
		if decision.Consistency.Set {
			if !validConsistency(decision.Consistency.Value) {
				return failure(typeValidation("retry consistency", "invalid consistency"))
			}
			consistency = decision.Consistency.Value
		}
		if decision.Delay < 0 {
			return failure(typeValidation("retry delay", "negative delay"))
		}
		if decision.Delay > 0 {
			timer := time.NewTimer(decision.Delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return failure(ctx.Err())
			case <-timer.C:
			}
		}
		if decision.Action == RetryNext {
			conn = nil
		}
		s.retries.Add(1)
	}
}

// skipHostPlan is a retry plan for a page whose first attempt went to a
// pinned connection: it omits that connection's host.
type skipHostPlan struct {
	plan QueryPlan
	host UUID
}

func (p *skipHostPlan) Next() (Target, bool) {
	for {
		target, ok := p.plan.Next()
		if !ok || target.HostID != p.host {
			return target, ok
		}
	}
}

func mergeResponseResult(result *Result, response *responsePage) {
	// Warnings, trace IDs and custom payload all live in the rare extras.
	if response == nil || response.Extras == nil {
		return
	}
	result.AddWarnings(response.Warnings()...)
	if len(response.TraceID()) == 16 {
		var id UUID
		copy(id[:], response.TraceID())
		result.AddTraceIDs(id)
	}
	for _, entry := range response.Payload() {
		if entry.Value.Valid {
			entry.Value.Value = entry.Value.Value.Clone()
		}
		result.AddCustomPayload(entry)
	}
}

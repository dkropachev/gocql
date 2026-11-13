// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

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
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/paging"
	"github.com/scylladb/gocql/v2/internal/protocol"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/retry"

	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/scylladb/gocql/v2/internal/runtimeerrors"

	"time"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implPool "github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// StreamOptions configures a stream query; streaming is pull-driven. The
// query validates and resolves CompiledOptions for its session once, when it is
// created.
type StreamOptions struct {
	Options Options
	Limits  transport.StreamLimits
	// Incremental reserves an exclusive connection and bounds receive scratch.
	// The default buffers a complete page on normal multiplexed connections.
	Incremental bool
}
type StreamInfo struct {
	Result               model.Result
	DeliveredRows, Pages int64
	OutputStarted        bool
}

// RowStream writes only during NextInto. Incremental mode owns a pool slot.
// Output belongs to the caller and remains valid after Close.
type RowStream[R any] struct {
	state          model.PageState
	privateStorage rowStreamStorage[R]
}

func (rt *InternalSessionRuntime) runStream[B, R any](ctx context.Context, bound *Bound[B], decoder codec.StreamRowDecoder[R], options CompiledOptions, streamLimits transport.StreamLimits, incremental bool, plans *streamPlanCache[R]) (*RowStream[R], error) {
	if decoder == nil {
		return nil, cqltype.TypeValidation("stream decoder", "nil decoder")
	}
	limits, err := transport.InternalCheckedStreamLimits(streamLimits)
	if err != nil {
		return nil, err
	}
	if v := options.values(); v.Prefetch.Set && v.Prefetch.Value != 0 {
		return nil, cqltype.TypeValidation("stream prefetch", "must be zero")
	}
	runctx, cancel, request, err := rt.admit(ctx, bound, options)
	if err != nil {
		return nil, err
	}
	session := rt.Core
	// A stream never prefetches or speculates, whatever its profile says.
	if o := request.options; o.InternalProfile.Prefetch != 0 || o.InternalProfile.Speculation != nil {
		o = request.ownOptions()
		o.InternalProfile.Prefetch = 0
		o.InternalOptions.Prefetch = cqltype.Option[int]{Set: true, Value: 0}
		o.InternalProfile.Speculation = nil
	}
	request.responseDest = nil // a stream handle keeps its pages beyond the query's generation
	cursor := func() *RowStream[R] {
		opaqueStorageValue := new(RowStream[R])
		opaqueStorageValue.privateStorage.cursor = executionCursor(request)
		opaqueStorageValue.privateStorage.ctx = runctx
		opaqueStorageValue.privateStorage.cancel = cancel
		opaqueStorageValue.privateStorage.session = session
		opaqueStorageValue.privateStorage.request = request
		opaqueStorageValue.privateStorage.limits = limits
		opaqueStorageValue.privateStorage.decoder = decoder
		opaqueStorageValue.state = model.StartPage()
		opaqueStorageValue.privateStorage.history = paging.InternalPagingHistory{InternalMaxCount: rt.config.Limits.MaxPagingStates, InternalMaxBytes: rt.config.Limits.MaxPagingStateBytes}
		opaqueStorageValue.privateStorage.pageBuffered = !incremental
		opaqueStorageValue.privateStorage.bufferedPlans = plans
		if plans != nil {
			plans.mu.Lock()
			opaqueStorageValue.privateStorage.plan = plans.plan
			opaqueStorageValue.privateStorage.columns = plans.columns
			plans.mu.Unlock()
		}
		return opaqueStorageValue
		// Construction owns active until headers/metadata have selected a checked plan.
	}()
	cursor.privateStorage.active.Lock()
	err = cursor.loadPage()
	if err == nil {
		cursor.privateStorage.stop = context.AfterFunc(runctx, func() {
			cause := runctx.Err()
			if session.Closed() {
				cause = runtimeerrors.ErrClosed
			}
			rowStreamCloseWithError(cursor, cause)
		})
	}
	cursor.privateStorage.active.Unlock()
	if err != nil {
		rowStreamFail(cursor, err)
		cursor.privateStorage.active.Lock()
		rowStreamCleanup(cursor, false)
		cursor.privateStorage.active.Unlock()
		if failure, ok := errors.AsType[*ExecutionError](err); ok {
			failure.Result = cursor.Info().Result
			return nil, failure
		}
		return nil, newExecutionError(err, cursor.Info().Result, runtimeerrors.ExecutionStatusOf(err), nil)
	}
	request.InternalLogical.installStop(stopFunc(func() { rowStreamCloseWithError(cursor, runtimeerrors.ErrClosed) }))
	return cursor, nil
}

func (s *RowStream[R]) setError(err error) {
	if err == nil {
		return
	}
	if errors.Is(err, context.Canceled) && s.privateStorage.session.Closed() {
		err = runtimeerrors.ErrClosed
	}
	s.privateStorage.mu.Lock()
	if s.privateStorage.err == nil {
		s.privateStorage.err = err
	}
	s.privateStorage.mu.Unlock()
}
func rowStreamFail[R any](s *RowStream[R], err error) {
	if errors.Is(err, context.Canceled) && s.privateStorage.session.Closed() {
		err = runtimeerrors.ErrClosed
	}
	s.privateStorage.mu.Lock()
	if !s.privateStorage.terminal && s.privateStorage.err == nil {
		s.privateStorage.err = err
	}
	s.privateStorage.terminal = true
	s.privateStorage.mu.Unlock()
}

func (s *RowStream[R]) Err() error {
	if s == nil {
		return nil
	}
	s.privateStorage.mu.Lock()
	defer s.privateStorage.mu.Unlock()
	return s.privateStorage.err
}
func (s *RowStream[R]) Info() StreamInfo {
	if s == nil {
		return StreamInfo{}
	}
	s.privateStorage.mu.Lock()
	defer s.privateStorage.mu.Unlock()
	info := s.privateStorage.info
	info.Result = model.CloneResult(info.Result)
	return info
}
func (s *RowStream[R]) Columns() cqltype.Columns {
	if s == nil {
		return cqltype.Columns{}
	}
	s.privateStorage.mu.Lock()
	defer s.privateStorage.mu.Unlock()
	return s.privateStorage.columns
}
func (s *RowStream[R]) Close() error {
	if s == nil {
		return nil
	}
	if !s.privateStorage.cursor.current() {
		return s.Err()
	}
	return rowStreamCloseWithError(s, nil)
}
func rowStreamCloseWithError[R any](s *RowStream[R], err error) error {
	s.privateStorage.mu.Lock()
	if s.privateStorage.closeDone == nil {
		s.privateStorage.closeDone = make(chan struct{})
	}
	done := s.privateStorage.closeDone
	if s.privateStorage.closing.Load() {
		s.privateStorage.mu.Unlock()
		<-done
		return s.Err()
	}
	s.privateStorage.closing.Store(true)
	if !s.privateStorage.terminal && s.privateStorage.err == nil && err != nil {
		s.privateStorage.err = err
	}
	s.privateStorage.terminal = true
	s.privateStorage.mu.Unlock()
	defer close(done)

	// An explicit Close of an incremental stream that no NextInto is running
	// keeps its connection when the rest of the current response is small:
	// discarding those bytes is far cheaper than closing the connection and
	// dialing a replacement. The reservation is returned before cancellation,
	// whose callback would otherwise close the connection.
	if err == nil && s.privateStorage.active.TryLock() {
		if finishAbandonedResponse(s) {
			if err := s.privateStorage.reservation.InternalClose(true); err != nil {
				s.setError(err)
			}
			s.privateStorage.reservation = nil
		}
		s.privateStorage.active.Unlock()
	}

	// Publish termination before cancellation so our own cancellation callback
	// cannot turn successful explicit Close into a context error. Every caller
	// joins the same completion, including callback and session shutdown paths.
	s.privateStorage.cancel()
	s.privateStorage.active.Lock()
	rowStreamCleanup(s, false)
	s.privateStorage.active.Unlock()
	return s.Err()
}

// abandonedResponseDrainLimit bounds the unread response bytes an explicit
// Close discards to keep an incremental stream's connection.
const abandonedResponseDrainLimit = 1 << 20

// finishAbandonedResponse reads and discards the unread rest of an
// incremental stream's current response, reporting whether the reservation
// is back at a response boundary and its connection can be reused. Active is
// held.
func finishAbandonedResponse[R any](s *RowStream[R]) bool {
	p := &s.privateStorage
	if p.pageBuffered || p.cleaned || p.reservation == nil || p.response == nil || p.err != nil {
		return false
	}
	response := p.response
	if response.InternalTerminal {
		return p.reservation.InternalComplete
	}
	body := response.InternalBody
	if body == nil || body.Remaining() > abandonedResponseDrainLimit {
		return false
	}
	err := p.reservation.InternalWithIO(response.InternalCtx, func() error {
		if err := body.Skip(); err != nil {
			return err
		}
		if err := body.RequireEnd(); err != nil {
			return err
		}
		response.InternalTerminal = true
		return p.reservation.InternalFinishResponse()
	})
	return err == nil
}

// active is held; no driver mutex is held while calling application code.
func rowStreamCleanup[R any](s *RowStream[R], reusable bool) {
	if s.privateStorage.cleaned {
		return
	}
	s.privateStorage.cleaned = true
	if s.privateStorage.bufferedPage != nil {
		s.privateStorage.bufferedPage.Owner.Release()
		s.privateStorage.bufferedPage = nil
		s.privateStorage.bufferedBody = nil
		s.privateStorage.bufferedReader = nil
	}
	if s.privateStorage.stop != nil {
		s.privateStorage.stop()
	}
	if s.privateStorage.reservation != nil {
		if err := s.privateStorage.reservation.InternalClose(reusable); err != nil {
			s.setError(err)
		}
		s.privateStorage.reservation = nil
	}
	if s.privateStorage.pageCancel != nil {
		s.privateStorage.pageCancel()
		s.privateStorage.pageCancel = nil
	}
	if s.privateStorage.response != nil {
		s.privateStorage.response.InternalReleaseMetadata()
		s.privateStorage.response = nil
	}
	if s.privateStorage.attemptMetadata != nil {
		s.privateStorage.attemptMetadata.InternalReleaseMetadata()
		s.privateStorage.attemptMetadata = nil
	}
	s.privateStorage.history.InternalClear()
	s.privateStorage.cancel()
	s.privateStorage.request.InternalLogical.InternalFinish(s.Info().Result, s.Err())
	s.privateStorage.request.InternalLogical.releaseInput()
}
func (s *RowStream[R]) NextInto(dst *R) bool {
	if s == nil {
		return false
	}
	if !s.privateStorage.cursor.current() {
		return false
	}
	s.privateStorage.mu.Lock()
	terminalBefore := s.privateStorage.terminal
	s.privateStorage.mu.Unlock()
	if terminalBefore {
		return false
	}
	if !s.privateStorage.advancing.CompareAndSwap(false, true) {
		s.privateStorage.mu.Lock()
		active := !s.privateStorage.terminal
		if active && s.privateStorage.err == nil {
			s.privateStorage.err = cqltype.TypeValidation("stream advance", "concurrent NextInto")
		}
		s.privateStorage.mu.Unlock()
		if active {
			s.privateStorage.cancel()
		}
		return false
	}
	defer s.privateStorage.advancing.Store(false)
	s.privateStorage.active.Lock()
	defer s.privateStorage.active.Unlock()
	s.privateStorage.mu.Lock()
	terminal := s.privateStorage.terminal
	s.privateStorage.mu.Unlock()
	if terminal || s.privateStorage.closing.Load() {
		return false
	}
	if err := s.privateStorage.ctx.Err(); err != nil {
		rowStreamFail(s, err)
		rowStreamCleanup(s, false)
		return false
	}
	if dst == nil {
		rowStreamFail(s, cqltype.TypeValidation("stream destination", "nil destination"))
		rowStreamCleanup(s, false)
		return false
	}
	if s.privateStorage.pageBuffered {
		return s.nextBuffered(dst)
	}
	for {
		if s.privateStorage.response.InternalIndex < s.privateStorage.response.InternalRowCount {
			s.privateStorage.mu.Lock()
			s.privateStorage.info.OutputStarted = true
			s.privateStorage.mu.Unlock()
			ok, err := transport.InternalDecodeStreamRow(s.privateStorage.response, s.privateStorage.plan, dst)
			if err != nil {
				rowStreamFail(s, err)
				rowStreamCleanup(s, false)
				return false
			}
			if ok {
				s.privateStorage.mu.Lock()
				s.privateStorage.info.DeliveredRows++
				s.privateStorage.info.Result.Rows = s.privateStorage.info.DeliveredRows
				s.privateStorage.mu.Unlock()
				return true
			}
		} else if !s.privateStorage.response.InternalTerminal {
			// Finish an empty response without touching a destination or Reset.
			err := s.privateStorage.reservation.InternalWithIO(s.privateStorage.response.InternalCtx, func() error {
				if err := s.privateStorage.response.InternalBody.RequireEnd(); err != nil {
					return err
				}
				s.privateStorage.response.InternalTerminal = true
				return s.privateStorage.reservation.InternalFinishResponse()
			})
			if err != nil {
				rowStreamFail(s, err)
				rowStreamCleanup(s, false)
				return false
			}
		}
		next := model.CompletePageState()
		if s.privateStorage.response.InternalMetadata.PagingState != nil {
			next = model.PageStateFromToken(s.privateStorage.response.InternalMetadata.PagingState)
		}
		if !next.More() {
			s.privateStorage.mu.Lock()
			s.privateStorage.terminal = true
			s.privateStorage.mu.Unlock()
			rowStreamCleanup(s, true)
			return false
		}
		if err := s.privateStorage.history.InternalAdd(next); err != nil {
			rowStreamFail(s, err)
			rowStreamCleanup(s, false)
			return false
		}
		s.state = next
		if s.privateStorage.pageCancel != nil {
			s.privateStorage.pageCancel()
			s.privateStorage.pageCancel = nil
		}
		if err := s.loadPage(); err != nil {
			rowStreamFail(s, err)
			rowStreamCleanup(s, false)
			return false
		}
	}
}

func (s *RowStream[R]) acquire(plan implRouting.QueryPlan) error {
	request := s.privateStorage.request
	var target model.Target
	var routed *implPool.InternalHostConnPool // the pool of the plan's route
	if s.privateStorage.retryTarget != nil {
		target = *s.privateStorage.retryTarget
		s.privateStorage.retryTarget = nil
	} else if request.connection != nil {
		target = model.Target{HostID: implEndpoint.InternalHostInfoHostUUID(request.connection.InternalHost)}
	} else if plan != nil {
		r, ok := plan.Next()
		if !ok {
			return runtimeerrors.ErrNoConnections
		}
		target = r.Target()
		routed = r.InternalHost().Pool()
	} else if request.routing.HostID.Set {
		target = model.Target{HostID: request.routing.HostID.Value, ShardID: request.routing.ShardID, StrictShard: request.routing.ShardID.Set}
	} else {
		hosts := implCluster.SessionData(s.privateStorage.session).InternalHostSource.InternalGetHostsList()
		for _, host := range hosts {
			if host != nil && host.IsUp() && !(&implCluster.SessionData(s.privateStorage.session).InternalCfg).FilterHost(host) {
				target.HostID = implEndpoint.InternalHostInfoHostUUID(host)
				break
			}
		}
		if target.HostID.IsEmpty() {
			return runtimeerrors.ErrNoConnections
		}
	}
	pool := routed
	if pool == nil {
		implCluster.SessionData(s.privateStorage.session).InternalPool.InternalMu.RLock()
		pool = implCluster.SessionData(s.privateStorage.session).InternalPool.InternalHostConnPools[target.HostID]
		implCluster.SessionData(s.privateStorage.session).InternalPool.InternalMu.RUnlock()
	}
	if pool == nil || pool.InternalHost == nil || !pool.InternalHost.IsUp() || (&implCluster.SessionData(s.privateStorage.session).InternalCfg).FilterHost(pool.InternalHost) {
		return runtimeerrors.ErrNoConnections
	}
	for {
		var reservation *transport.InternalExclusiveConn
		var err error
		if request.connection != nil {
			reservation, err = request.connection.InternalReserveExclusiveWithLimits(s.privateStorage.ctx, s.privateStorage.limits)
		} else {
			reservation, err = pool.InternalReserveStreamWithLimits(s.privateStorage.ctx, target, request.routing, s.privateStorage.limits)
		}
		if err != nil {
			return err
		}
		s.privateStorage.reservation = reservation
		state := request.InternalPrepared()
		if state == nil {
			return nil
		}
		prepared := state.host(implEndpoint.InternalHostInfoHostUUID(reservation.InternalConn.InternalHost))
		if prepared != nil && prepared.InternalEpoch.InternalCurrent() {
			s.privateStorage.prepared = prepared
			// Conditional PREPARE responses may omit result columns. The actual
			// response supplies and validates their shape before decoding output.
			knownColumns := prepared.InternalColumns.Len() != 0 || request.routing.Kind == cqltype.OrdinaryStatement
			if knownColumns && (s.privateStorage.plan == nil || !s.privateStorage.columns.Equal(prepared.InternalColumns)) {
				plan, err := s.privateStorage.decoder.Compile(prepared.InternalColumns)
				if err != nil {
					return err
				}
				if plan == nil {
					return cqltype.TypeValidation("stream plan", "decoder returned nil plan")
				}
				s.privateStorage.plan = plan
				s.rememberPlan(prepared.InternalColumns, plan)
				s.privateStorage.mu.Lock()
				s.privateStorage.columns = prepared.InternalColumns
				s.privateStorage.mu.Unlock()
			}
			return nil
		}
		// Preparation uses shared I/O before any execution submission. Return the
		// idle reservation before entering that path; no decoder/output has run.
		conn := reservation.InternalConn
		if err = reservation.InternalClose(true); err != nil {
			return err
		}
		s.privateStorage.reservation = nil
		s.privateStorage.prepared, err = request.source().onConn(s.privateStorage.ctx, InternalRuntimeFor(s.privateStorage.session), conn, request.InternalValues, false, nil)
		if err != nil {
			return err
		}
	}
}
func (s *RowStream[R]) wireRequest(consistency cqltype.Consistency) (*model.AttemptRequest, error) {
	r := s.privateStorage.request
	conn := s.privateStorage.reservation.InternalConn
	rt := InternalRuntimeFor(s.privateStorage.session)
	w := new(model.AttemptRequest)
	if err := rt.newAttempt(w, conn, r, s.state, consistency); err != nil {
		return nil, err
	}
	setPrepared(w, s.privateStorage.prepared, conn, r.options.InternalOptions.IncludeMetadata)
	w.Params = paramsTemplate(r.options, w, conn.InternalVersion)
	return w, nil
}

// loadPage loads s's next page.
func (s *RowStream[R]) loadPage() error {
	if s.privateStorage.pageBuffered {
		return s.loadBufferedPage()
	}
	var log attemptLog
	err := rowStreamLoadPage(s, &log)
	if err == nil || log.empty() {
		return err
	}
	return log.error(err, model.Result{})
}

// rowStreamLoadPage loads the next page, recording its failed attempts in log.
func rowStreamLoadPage[R any](s *RowStream[R], log *attemptLog) error {
	request := s.privateStorage.request
	metadata := s.privateStorage.session.Metadata()
	request.tabletFeedback = s.privateStorage.session.CaptureTabletFeedback(request.routing, metadata)
	tablet := lookupRequestTablet(metadata, request)
	request.tabletVersionBlock = tablet.VersionBlock(request.routing)
	var plan implRouting.QueryPlan
	if policy := request.options.InternalProfile.Routing; policy != nil {
		if err := policy.Validate(metadata); err != nil {
			return err
		}
		if builtin, ok := policy.(*implRouting.InternalRoutingPolicy); ok {
			plan = builtin.InternalPlanWith(request.routing, metadata, new(implRouting.InternalLazyRoutingPlan), &tablet)
		} else {
			plan = implRouting.NewPlan(policy, request.routing, metadata)
		}
	}
	pagectx := s.privateStorage.ctx
	if timeout := request.options.InternalProfile.PageTimeout; timeout > 0 {
		pagectx, s.privateStorage.pageCancel = context.WithTimeout(s.privateStorage.ctx, timeout)
	}
	consistency := request.options.InternalProfile.Consistency
	for attempt := 1; ; attempt++ {
		if err := pagectx.Err(); err != nil {
			return err
		}
		if s.privateStorage.reservation == nil {
			if err := s.acquire(plan); err != nil {
				return err
			}
		}
		if s.privateStorage.prepared != nil && !s.privateStorage.prepared.InternalEpoch.InternalCurrent() {
			// Reconcile before submitting a continuation page. This prepares
			// and validates the same canonical input, then sends the next page
			// state; it never replays a page that has exposed caller output.
			if s.Info().OutputStarted && !s.state.More() {
				return &runtimeerrors.SchemaChangedError{Statement: request.statement, Previous: request.InternalValues.Columns, Current: request.InternalPrepared().InternalParameters}
			}
			conn := s.privateStorage.reservation.InternalConn
			if err := s.privateStorage.reservation.InternalClose(true); err != nil {
				return err
			}
			s.privateStorage.reservation = nil
			prepared, err := request.source().onConn(pagectx, InternalRuntimeFor(s.privateStorage.session), conn, request.InternalValues, false, nil)
			if err != nil {
				return err
			}
			s.privateStorage.prepared = prepared
			s.privateStorage.retryTarget = &model.Target{HostID: conn.InternalHostID(), ShardID: cqltype.Option[uint32]{Set: implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0, Value: uint32(implEndpoint.ScyllaConnectionFeaturesData(&conn.InternalScyllaSupported).InternalShard)}, StrictShard: implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0}
			if err := s.acquire(plan); err != nil {
				return err
			}
		}
		wireRequest, err := s.wireRequest(consistency)
		if err != nil {
			return err
		}
		wireRequest.Lease = request.InternalOwner.InternalWriterLease(request.InternalValues.Framed)
		observed := request.source().observe()
		started := time.Now()
		response, err := s.privateStorage.reservation.InternalExchange(pagectx, wireRequest)
		wireRequest.Lease.Release()
		conn := s.privateStorage.reservation.InternalConn
		var traces []cqltype.UUID
		if response != nil {
			traces = response.InternalInfo.CQLTraceIDs()
		}
		event := implRouting.AttemptEvent{RequestID: request.InternalLogical.id, Attempt: attempt, Target: model.Target{HostID: conn.InternalHostID()}, Started: started, Latency: time.Since(started), Err: err, Cancelled: errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)}
		if len(traces) > 0 {
			event.CQLTraceID = cqltype.Option[cqltype.UUID]{Value: traces[len(traces)-1], Set: true}
		}
		if implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0 {
			event.Target.ShardID = cqltype.Option[uint32]{Set: true, Value: uint32(implEndpoint.ScyllaConnectionFeaturesData(&conn.InternalScyllaSupported).InternalShard)}
		}
		rt := InternalRuntimeFor(s.privateStorage.session)
		rt.attempts.Add(1)
		if observer := request.InternalLogical.observer; observer != nil {
			s.privateStorage.session.ObserveSafely(func() { observer.ObserveAttempt(event) })
		}
		if policy := request.options.InternalProfile.Routing; policy != nil {
			s.privateStorage.session.ObserveSafely(func() { policy.Observe(event) })
		}
		s.privateStorage.mu.Lock()
		s.privateStorage.info.Result.Attempts++
		s.privateStorage.info.Result.Latency += event.Latency
		s.privateStorage.info.Result.HostID = event.Target.HostID
		s.privateStorage.info.Result.AchievedConsistency = consistency

		// Warnings and custom payload describe the latest attempt, including
		// terminal server errors after output; the metadata parser already owns
		// their bytes. CQL trace IDs accumulate across attempts and pages, since
		// each names a trace session the caller may want to read.
		var warnings []string
		var payload []model.PayloadEntry
		if response != nil {
			warnings, payload = response.InternalInfo.Warnings(), response.InternalInfo.CustomPayload()
		}
		if prior := s.privateStorage.info.Result.CQLTraceIDs(); len(prior) > 0 {
			traces = slices.Concat(prior, traces)
		}
		s.privateStorage.info.Result.SetDiagnostics(warnings, traces, payload)
		s.privateStorage.mu.Unlock()
		// Latest failed-attempt extensions remain budgeted until replacement or
		// terminal cleanup, including while another attempt allocates metadata.
		if s.privateStorage.attemptMetadata != nil {
			s.privateStorage.attemptMetadata.InternalReleaseMetadata()
			s.privateStorage.attemptMetadata = nil
		}
		if err == nil {
			page := &resultMetadataObservation{kind: response.InternalKind, columns: response.InternalColumns, columnCount: response.InternalColumnCount, noMetadata: response.InternalMetadata.NoMetadata, metadataChanged: response.InternalMetadata.MetadataChanged, metadataID: response.InternalMetadata.MetadataID}
			err = selectPreparedMetadata(request.InternalPrepared(), conn.InternalHostID(), s.privateStorage.prepared, wireRequest.ResultMetadataID, conn.InternalTracksResultMetadataID(), page, observed)
			if err == nil {
				if s.privateStorage.plan == nil || !s.privateStorage.columns.Equal(page.columns) {
					s.privateStorage.plan, err = s.privateStorage.decoder.Compile(page.columns)
					if err == nil && s.privateStorage.plan == nil {
						err = cqltype.TypeValidation("stream plan", "decoder returned nil plan")
					}
					if err == nil {
						s.rememberPlan(page.columns, s.privateStorage.plan)
					}
				}
				if err == nil {
					if s.privateStorage.response == nil {
						err = response.InternalInstallColumns(page.columns)
					} else {
						err = response.InternalInstallColumnsReusing(page.columns, s.privateStorage.response)
					}
				}
			}
			if err == nil {
				prior := s.privateStorage.response
				s.privateStorage.response = response
				s.privateStorage.mu.Lock()
				s.privateStorage.columns = page.columns
				s.privateStorage.info.Pages++
				s.privateStorage.mu.Unlock()
				if prior != nil {
					prior.InternalReleaseMetadata()
				}
				return nil
			}
		}
		if response != nil {
			s.privateStorage.attemptMetadata = response
		}
		attemptStatus := log.fail(err, conn, attempt, consistency, event.CQLTraceID)
		// OutputStarted is set before Reset, so no decoder/partial row ever replays.
		if s.Info().OutputStarted {
			return err
		}
		if unprepared, ok := errors.AsType[*protocol.UnpreparedError](err); attempt == 1 && ok && request.source().rejected(unprepared.StatementID, wireRequest.PreparedID) {
			if closeErr := s.privateStorage.reservation.InternalClose(true); closeErr != nil {
				return closeErr
			}
			s.privateStorage.reservation = nil
			s.privateStorage.retryTarget = &model.Target{HostID: conn.InternalHostID(), ShardID: cqltype.Option[uint32]{Set: implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0, Value: uint32(implEndpoint.ScyllaConnectionFeaturesData(&conn.InternalScyllaSupported).InternalShard)}, StrictShard: implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0}
			s.privateStorage.prepared, err = request.source().onConn(pagectx, InternalRuntimeFor(s.privateStorage.session), conn, request.InternalValues, true, s.privateStorage.prepared)
			if err != nil {
				return err
			}
			rt.retries.Add(1)
			continue
		}
		decision := retry.Decision{}
		if policy := request.options.InternalProfile.Retry; policy != nil {
			decision = policy.Decide(retry.Attempt{Err: err, Number: attempt, Idempotent: request.options.InternalOptions.Idempotent, Consistency: consistency, Conditional: request.routing.Conditional, BytesWritten: attemptStatus != cqltype.NotExecuted, ExecutionStatus: attemptStatus})
		}
		if decision.Action == retry.Stop {
			return err
		}
		if decision.Action != retry.Same && decision.Action != retry.Next {
			return cqltype.TypeValidation("stream retry", "requires retry same, next or stop")
		}
		if decision.Delay < 0 {
			return cqltype.TypeValidation("retry delay", "negative delay")
		}
		if decision.Consistency.Set {
			if !settings.InternalValidConsistency(decision.Consistency.Value) {
				return cqltype.TypeValidation("retry consistency", "invalid consistency")
			}
			consistency = decision.Consistency.Value
		}
		if decision.Action == retry.Same && s.privateStorage.reservation.InternalClosing.Load() {
			s.privateStorage.retryTarget = &model.Target{HostID: conn.InternalHostID(), ShardID: cqltype.Option[uint32]{Set: implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0, Value: uint32(implEndpoint.ScyllaConnectionFeaturesData(&conn.InternalScyllaSupported).InternalShard)}, StrictShard: implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0}
		}
		if decision.Action == retry.Next || s.privateStorage.reservation.InternalClosing.Load() {
			s.privateStorage.reservation.InternalClose(false)
			s.privateStorage.reservation = nil
			s.privateStorage.prepared = nil
		}
		if decision.Delay > 0 {
			timer := time.NewTimer(decision.Delay)
			select {
			case <-pagectx.Done():
				timer.Stop()
				return pagectx.Err()
			case <-timer.C:
			}
		}
		rt.retries.Add(1)
	}
}

type rowStreamStorage[R any] struct {
	ctx             context.Context
	decoder         codec.StreamRowDecoder[R]
	plan            codec.StreamRowPlan[R]
	err             error
	bufferedPage    *transport.InternalResponsePage
	bufferedBody    *wire.Stream
	bufferedReader  *codec.StreamRowReader
	bufferedPlans   *streamPlanCache[R]
	attemptMetadata *transport.InternalStreamResponse
	closeDone       chan struct{}
	retryTarget     *model.Target
	cancel          context.CancelFunc
	stop            func() bool
	session         *implCluster.Core
	request         *InternalExecutionRequest
	response        *transport.InternalStreamResponse
	reservation     *transport.InternalExclusiveConn
	prepared        *InternalHostPrepared
	pageCancel      context.CancelFunc
	cursor          queryCursor
	columns         cqltype.Columns
	history         paging.InternalPagingHistory
	info            StreamInfo
	limits          transport.StreamLimits
	active          sync.Mutex
	mu              sync.Mutex
	bufferedIndex   int32
	advancing       atomic.Bool
	closing         atomic.Bool
	pageBuffered    bool
	terminal        bool
	cleaned         bool
}

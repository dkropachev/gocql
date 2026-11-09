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

package execution

import (
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/retry"

	"context"
	"errors"

	"github.com/scylladb/gocql/v2/internal/runtimeerrors"

	"time"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implPool "github.com/scylladb/gocql/v2/internal/pool"
)

// StreamOptions configures a stream query; streaming is pull-driven. The
// query validates and resolves Options for its session once, when it is
// created.
type StreamOptions struct {
	Options OptionValues
	Limits  StreamLimits
	// Incremental reserves an exclusive connection and bounds receive scratch.
	// The default buffers a complete page on normal multiplexed connections.
	Incremental bool
}
type StreamInfo struct {
	Result               Result
	DeliveredRows, Pages int64
	OutputStarted        bool
}

// RowStream writes only during NextInto. Incremental mode owns a pool slot.
// Output belongs to the caller and remains valid after Close.
type RowStream[R any] struct {
	state          PageState
	privateStorage RowStreamStorage[R]
}

func RunStream[B, R any](ctx context.Context, session *implCluster.Session, bound *Bound[B], decoder StreamRowDecoder[R], options StreamOptions) (*RowStream[R], error) {
	frozen, err := queryOptions(session, options.Options)
	if err != nil {
		return nil, err
	}
	return runStreamWithCache(ctx, session, bound, decoder, frozen, options.Limits, options.Incremental, nil)
}
func runStreamWithCache[B, R any](ctx context.Context, session *implCluster.Session, bound *Bound[B], decoder StreamRowDecoder[R], options Options, streamLimits StreamLimits, incremental bool, plans *streamPlanCache[R]) (*RowStream[R], error) {
	if decoder == nil {
		return nil, typeValidation("stream decoder", "nil decoder")
	}
	limits, err := checkedStreamLimits(streamLimits)
	if err != nil {
		return nil, err
	}
	if v := options.values(); v.Prefetch.Set && v.Prefetch.Value != 0 {
		return nil, typeValidation("stream prefetch", "must be zero")
	}
	runctx, cancel, request, err := InternalAdmitExecution(ctx, session, bound, options)
	if err != nil {
		return nil, err
	}
	// A stream never prefetches or speculates, whatever its profile says.
	request.InternalOptions.InternalProfile.Prefetch = 0
	request.InternalOptions.InternalOptions.Prefetch = Option[int]{Set: true, Value: 0}
	request.InternalOptions.InternalProfile.Speculation = nil
	request.responseDest = nil // a stream handle keeps its pages beyond the query's generation
	cursor := func() *RowStream[R] {
		opaqueStorageValue := new(RowStream[R])
		opaqueStorageValue.privateStorage.cursor = executionCursor(request)
		opaqueStorageValue.privateStorage.InternalCtx = runctx
		opaqueStorageValue.privateStorage.InternalCancel = cancel
		opaqueStorageValue.privateStorage.InternalSession = session
		opaqueStorageValue.privateStorage.InternalRequest = request
		opaqueStorageValue.privateStorage.limits = limits
		opaqueStorageValue.privateStorage.InternalDecoder = decoder
		opaqueStorageValue.state = StartPage()
		opaqueStorageValue.privateStorage.InternalHistory = pagingHistory{InternalMaxCount: InternalRuntimeFor(session).InternalConfig.Limits.MaxPagingStates, InternalMaxBytes: InternalRuntimeFor(session).InternalConfig.Limits.MaxPagingStateBytes}
		opaqueStorageValue.privateStorage.pageBuffered = !incremental
		opaqueStorageValue.privateStorage.bufferedPlans = plans
		return opaqueStorageValue
		// Construction owns active until headers/metadata have selected a checked plan.
	}()
	cursor.privateStorage.InternalActive.Lock()
	err = InternalRowStreamLoadPage(cursor)
	if err == nil {
		cursor.privateStorage.stop = context.AfterFunc(runctx, func() {
			cause := runctx.Err()
			if session.Closed() {
				cause = ErrClosed
			}
			InternalRowStreamCloseWithError(cursor, cause)
		})
	}
	cursor.privateStorage.InternalActive.Unlock()
	if err != nil {
		InternalRowStreamFail(cursor, err)
		cursor.privateStorage.InternalActive.Lock()
		InternalRowStreamCleanup(cursor, false)
		cursor.privateStorage.InternalActive.Unlock()
		if failure, ok := errors.AsType[*ExecutionError](err); ok {
			failure.Result = cursor.Info().Result
			return nil, failure
		}
		return nil, newExecutionError(err, cursor.Info().Result, runtimeerrors.ExecutionStatusOf(err), nil)
	}
	request.InternalLogical.installStop(stopFunc(func() { InternalRowStreamCloseWithError(cursor, ErrClosed) }))
	return cursor, nil
}

func (s *RowStream[R]) setError(err error) {
	if err == nil {
		return
	}
	if errors.Is(err, context.Canceled) && s.privateStorage.InternalSession.Closed() {
		err = ErrClosed
	}
	s.privateStorage.mu.Lock()
	if s.privateStorage.err == nil {
		s.privateStorage.err = err
	}
	s.privateStorage.mu.Unlock()
}
func InternalRowStreamFail[R any](s *RowStream[R], err error) {
	if errors.Is(err, context.Canceled) && s.privateStorage.InternalSession.Closed() {
		err = ErrClosed
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
	info.Result = cloneResult(info.Result)
	return info
}
func (s *RowStream[R]) Columns() Columns {
	if s == nil {
		return Columns{}
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
	return InternalRowStreamCloseWithError(s, nil)
}
func InternalRowStreamCloseWithError[R any](s *RowStream[R], err error) error {
	s.privateStorage.mu.Lock()
	if s.privateStorage.closeDone == nil {
		s.privateStorage.closeDone = make(chan struct{})
	}
	done := s.privateStorage.closeDone
	if s.privateStorage.InternalClosing.Load() {
		s.privateStorage.mu.Unlock()
		<-done
		return s.Err()
	}
	s.privateStorage.InternalClosing.Store(true)
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
	if err == nil && s.privateStorage.InternalActive.TryLock() {
		if finishAbandonedResponse(s) {
			if err := s.privateStorage.InternalReservation.InternalClose(true); err != nil {
				s.setError(err)
			}
			s.privateStorage.InternalReservation = nil
		}
		s.privateStorage.InternalActive.Unlock()
	}

	// Publish termination before cancellation so our own cancellation callback
	// cannot turn successful explicit Close into a context error. Every caller
	// joins the same completion, including callback and session shutdown paths.
	s.privateStorage.InternalCancel()
	s.privateStorage.InternalActive.Lock()
	InternalRowStreamCleanup(s, false)
	s.privateStorage.InternalActive.Unlock()
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
	if p.pageBuffered || p.cleaned || p.InternalReservation == nil || p.response == nil || p.err != nil {
		return false
	}
	response := p.response
	if response.InternalTerminal {
		return p.InternalReservation.InternalComplete
	}
	body := response.InternalBody
	if body == nil || body.Remaining() > abandonedResponseDrainLimit {
		return false
	}
	err := p.InternalReservation.InternalWithIO(response.InternalCtx, func() error {
		if err := body.Skip(); err != nil {
			return err
		}
		if err := body.RequireEnd(); err != nil {
			return err
		}
		response.InternalTerminal = true
		return p.InternalReservation.InternalFinishResponse()
	})
	return err == nil
}

// active is held; no driver mutex is held while calling application code.
func InternalRowStreamCleanup[R any](s *RowStream[R], reusable bool) {
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
	if s.privateStorage.InternalReservation != nil {
		if err := s.privateStorage.InternalReservation.InternalClose(reusable); err != nil {
			s.setError(err)
		}
		s.privateStorage.InternalReservation = nil
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
	s.privateStorage.InternalHistory.InternalClear()
	s.privateStorage.InternalCancel()
	s.privateStorage.InternalRequest.InternalLogical.InternalFinish(s.Info().Result, s.Err())
	s.privateStorage.InternalRequest.InternalLogical.InternalReleaseInput()
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
			s.privateStorage.err = typeValidation("stream advance", "concurrent NextInto")
		}
		s.privateStorage.mu.Unlock()
		if active {
			s.privateStorage.InternalCancel()
		}
		return false
	}
	defer s.privateStorage.advancing.Store(false)
	s.privateStorage.InternalActive.Lock()
	defer s.privateStorage.InternalActive.Unlock()
	s.privateStorage.mu.Lock()
	terminal := s.privateStorage.terminal
	s.privateStorage.mu.Unlock()
	if terminal || s.privateStorage.InternalClosing.Load() {
		return false
	}
	if err := s.privateStorage.InternalCtx.Err(); err != nil {
		InternalRowStreamFail(s, err)
		InternalRowStreamCleanup(s, false)
		return false
	}
	if dst == nil {
		InternalRowStreamFail(s, typeValidation("stream destination", "nil destination"))
		InternalRowStreamCleanup(s, false)
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
			ok, err := decodeStreamRow(s.privateStorage.response, s.privateStorage.plan, dst)
			if err != nil {
				InternalRowStreamFail(s, err)
				InternalRowStreamCleanup(s, false)
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
			err := s.privateStorage.InternalReservation.InternalWithIO(s.privateStorage.response.InternalCtx, func() error {
				if err := s.privateStorage.response.InternalBody.RequireEnd(); err != nil {
					return err
				}
				s.privateStorage.response.InternalTerminal = true
				return s.privateStorage.InternalReservation.InternalFinishResponse()
			})
			if err != nil {
				InternalRowStreamFail(s, err)
				InternalRowStreamCleanup(s, false)
				return false
			}
		}
		next := completePageState()
		if s.privateStorage.response.InternalMetadata.PagingState != nil {
			next = RestorePageState(s.privateStorage.response.InternalMetadata.PagingState)
		}
		if !next.More() {
			s.privateStorage.mu.Lock()
			s.privateStorage.terminal = true
			s.privateStorage.mu.Unlock()
			InternalRowStreamCleanup(s, true)
			return false
		}
		if err := s.privateStorage.InternalHistory.InternalAdd(next); err != nil {
			InternalRowStreamFail(s, err)
			InternalRowStreamCleanup(s, false)
			return false
		}
		s.state = next
		if s.privateStorage.pageCancel != nil {
			s.privateStorage.pageCancel()
			s.privateStorage.pageCancel = nil
		}
		if err := InternalRowStreamLoadPage(s); err != nil {
			InternalRowStreamFail(s, err)
			InternalRowStreamCleanup(s, false)
			return false
		}
	}
}

func (s *RowStream[R]) acquire(plan QueryPlan) error {
	request := s.privateStorage.InternalRequest
	var target Target
	var routed *implPool.InternalHostConnPool // the pool of the plan's route
	if s.privateStorage.retryTarget != nil {
		target = *s.privateStorage.retryTarget
		s.privateStorage.retryTarget = nil
	} else if request.connection != nil {
		target = Target{HostID: implEndpoint.InternalHostInfoHostUUID(request.connection.InternalHost)}
	} else if plan != nil {
		r, ok := plan.Next()
		if !ok {
			return ErrNoConnections
		}
		target = r.Target()
		routed = r.InternalHost().Pool()
	} else if request.InternalRouting.HostID.Set {
		target = Target{HostID: request.InternalRouting.HostID.Value, ShardID: request.InternalRouting.ShardID, StrictShard: request.InternalRouting.ShardID.Set}
	} else {
		hosts := implCluster.SessionData(s.privateStorage.InternalSession).InternalHostSource.InternalGetHostsList()
		for _, host := range hosts {
			if host != nil && host.IsUp() && !implCluster.InternalClusterSettingsFilterHost(&implCluster.SessionData(s.privateStorage.InternalSession).InternalCfg, host) {
				target.HostID = implEndpoint.InternalHostInfoHostUUID(host)
				break
			}
		}
		if target.HostID.IsEmpty() {
			return ErrNoConnections
		}
	}
	pool := routed
	if pool == nil {
		implCluster.SessionData(s.privateStorage.InternalSession).InternalPool.InternalMu.RLock()
		pool = implCluster.SessionData(s.privateStorage.InternalSession).InternalPool.InternalHostConnPools[target.HostID]
		implCluster.SessionData(s.privateStorage.InternalSession).InternalPool.InternalMu.RUnlock()
	}
	if pool == nil || pool.InternalHost == nil || !pool.InternalHost.IsUp() || implCluster.InternalClusterSettingsFilterHost(&implCluster.SessionData(s.privateStorage.InternalSession).InternalCfg, pool.InternalHost) {
		return ErrNoConnections
	}
	for {
		var reservation *exclusiveConn
		var err error
		if request.connection != nil {
			reservation, err = request.connection.InternalReserveExclusiveWithLimits(s.privateStorage.InternalCtx, s.privateStorage.limits)
		} else {
			reservation, err = pool.InternalReserveStreamWithLimits(s.privateStorage.InternalCtx, target, request.InternalRouting, s.privateStorage.limits)
		}
		if err != nil {
			return err
		}
		s.privateStorage.InternalReservation = reservation
		state := request.InternalPrepared()
		if state == nil {
			return nil
		}
		state.InternalMu.RLock()
		prepared := state.InternalHosts[implEndpoint.InternalHostInfoHostUUID(reservation.InternalConn.InternalHost)]
		state.InternalMu.RUnlock()
		if prepared != nil && prepared.InternalEpoch.InternalCurrent() {
			s.privateStorage.prepared = prepared
			if s.privateStorage.plan == nil || !s.privateStorage.columns.Equal(prepared.InternalColumns) {
				plan, err := s.privateStorage.InternalDecoder.Compile(prepared.InternalColumns)
				if err != nil {
					return err
				}
				if plan == nil {
					return typeValidation("stream plan", "decoder returned nil plan")
				}
				s.privateStorage.plan = plan
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
		s.privateStorage.InternalReservation = nil
		s.privateStorage.prepared, err = request.source().onConn(s.privateStorage.InternalCtx, InternalRuntimeFor(s.privateStorage.InternalSession), conn, request.InternalValues, false, nil)
		if err != nil {
			return err
		}
	}
}
func (s *RowStream[R]) wireRequest(consistency Consistency) (*attemptRequest, error) {
	r := s.privateStorage.InternalRequest
	conn := s.privateStorage.InternalReservation.InternalConn
	rt := InternalRuntimeFor(s.privateStorage.InternalSession)
	w := new(attemptRequest)
	if err := rt.newAttempt(w, conn, r, s.state, consistency); err != nil {
		return nil, err
	}
	setPrepared(w, s.privateStorage.prepared, conn, r.InternalOptions.InternalOptions.IncludeMetadata)
	w.Params = paramsTemplate(&r.InternalOptions, w, conn.InternalVersion)
	return w, nil
}
func InternalRowStreamLoadPage[R any](s *RowStream[R]) error {
	if s.privateStorage.pageBuffered {
		return s.loadBufferedPage()
	}
	var log attemptLog
	err := rowStreamLoadPage(s, &log)
	if err == nil || log.empty() {
		return err
	}
	return log.error(err, Result{})
}

// rowStreamLoadPage loads the next page, recording its failed attempts in log.
func rowStreamLoadPage[R any](s *RowStream[R], log *attemptLog) error {
	request := s.privateStorage.InternalRequest
	metadata := s.privateStorage.InternalSession.Metadata()
	request.tabletFeedback = implCluster.InternalSessionCaptureTabletFeedback(s.privateStorage.InternalSession, request.InternalRouting, metadata)
	tablet := lookupRequestTablet(metadata, request)
	request.tabletVersionBlock = tablet.VersionBlock(request.InternalRouting)
	var plan QueryPlan
	if policy := request.InternalOptions.InternalProfile.Routing; policy != nil {
		if err := policy.Validate(metadata); err != nil {
			return err
		}
		if builtin, ok := policy.(*implRouting.InternalRoutingPolicy); ok {
			plan = builtin.InternalPlanWith(request.InternalRouting, metadata, new(implRouting.InternalLazyRoutingPlan), &tablet)
		} else {
			plan = implRouting.NewPlan(policy, request.InternalRouting, metadata)
		}
	}
	pagectx := s.privateStorage.InternalCtx
	if timeout := request.InternalOptions.InternalProfile.PageTimeout; timeout > 0 {
		pagectx, s.privateStorage.pageCancel = context.WithTimeout(s.privateStorage.InternalCtx, timeout)
	}
	consistency := request.InternalOptions.InternalProfile.Consistency
	for attempt := 1; ; attempt++ {
		if err := pagectx.Err(); err != nil {
			return err
		}
		if s.privateStorage.InternalReservation == nil {
			if err := s.acquire(plan); err != nil {
				return err
			}
		}
		if s.privateStorage.prepared != nil && !s.privateStorage.prepared.InternalEpoch.InternalCurrent() {
			// Reconcile before submitting a continuation page. This prepares
			// and validates the same canonical input, then sends the next page
			// state; it never replays a page that has exposed caller output.
			if s.Info().OutputStarted && !s.state.More() {
				return &SchemaChangedError{Statement: request.InternalStatement, Previous: request.InternalValues.Columns, Current: request.InternalPrepared().InternalParameters}
			}
			conn := s.privateStorage.InternalReservation.InternalConn
			if err := s.privateStorage.InternalReservation.InternalClose(true); err != nil {
				return err
			}
			s.privateStorage.InternalReservation = nil
			prepared, err := request.source().onConn(pagectx, InternalRuntimeFor(s.privateStorage.InternalSession), conn, request.InternalValues, false, nil)
			if err != nil {
				return err
			}
			s.privateStorage.prepared = prepared
			s.privateStorage.retryTarget = &Target{HostID: conn.InternalHostID(), ShardID: Option[uint32]{Set: implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0, Value: uint32(implEndpoint.ScyllaConnectionFeaturesData(&conn.InternalScyllaSupported).InternalShard)}, StrictShard: implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0}
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
		response, err := s.privateStorage.InternalReservation.InternalExchange(pagectx, wireRequest)
		wireRequest.Lease.Release()
		conn := s.privateStorage.InternalReservation.InternalConn
		event := AttemptEvent{RequestID: request.InternalLogical.id, Attempt: attempt, Target: Target{HostID: conn.InternalHostID()}, Started: started, Latency: time.Since(started), Err: err, Cancelled: errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)}
		if implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0 {
			event.Target.ShardID = Option[uint32]{Set: true, Value: uint32(implEndpoint.ScyllaConnectionFeaturesData(&conn.InternalScyllaSupported).InternalShard)}
		}
		rt := InternalRuntimeFor(s.privateStorage.InternalSession)
		rt.attempts.Add(1)
		if observer := request.InternalLogical.observer; observer != nil {
			implCluster.InternalSessionObserveSafely(s.privateStorage.InternalSession, func() { observer.ObserveAttempt(event) })
		}
		if policy := request.InternalOptions.InternalProfile.Routing; policy != nil {
			implCluster.InternalSessionObserveSafely(s.privateStorage.InternalSession, func() { policy.Observe(event) })
		}
		s.privateStorage.mu.Lock()
		s.privateStorage.info.Result.Attempts++
		s.privateStorage.info.Result.Latency += event.Latency
		s.privateStorage.info.Result.HostID = event.Target.HostID
		s.privateStorage.info.Result.AchievedConsistency = consistency

		// Result extensions describe the latest attempt, including terminal server
		// errors after output. The metadata parser already owns their bytes.
		s.privateStorage.info.Result.SetDiagnostics(nil, nil, nil)
		if response != nil {
			info := response.InternalInfo
			s.privateStorage.info.Result.SetDiagnostics(info.Warnings(), info.TraceIDs(), info.CustomPayload())
		}
		s.privateStorage.mu.Unlock()
		// Latest failed-attempt extensions remain budgeted until replacement or
		// terminal cleanup, including while another attempt allocates metadata.
		if s.privateStorage.attemptMetadata != nil {
			s.privateStorage.attemptMetadata.InternalReleaseMetadata()
			s.privateStorage.attemptMetadata = nil
		}
		if err == nil {
			page := &InternalResultMetadataObservation{kind: response.InternalKind, columns: response.InternalColumns, columnCount: response.InternalColumnCount, noMetadata: response.InternalMetadata.NoMetadata, metadataChanged: response.InternalMetadata.MetadataChanged, metadataID: response.InternalMetadata.MetadataID}
			err = InternalSelectPreparedMetadata(request.InternalPrepared(), conn.InternalHostID(), s.privateStorage.prepared, wireRequest.ResultMetadataID, conn.InternalTracksResultMetadataID(), page, observed)
			if err == nil {
				if s.privateStorage.plan == nil || !s.privateStorage.columns.Equal(page.columns) {
					s.privateStorage.plan, err = s.privateStorage.InternalDecoder.Compile(page.columns)
					if err == nil && s.privateStorage.plan == nil {
						err = typeValidation("stream plan", "decoder returned nil plan")
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
		attemptStatus := log.fail(err, conn, attempt, consistency)
		// OutputStarted is set before Reset, so no decoder/partial row ever replays.
		if s.Info().OutputStarted {
			return err
		}
		if unprepared, ok := errors.AsType[*RequestErrUnprepared](err); attempt == 1 && ok && request.source().rejected(unprepared.StatementId, wireRequest.PreparedID) {
			if closeErr := s.privateStorage.InternalReservation.InternalClose(true); closeErr != nil {
				return closeErr
			}
			s.privateStorage.InternalReservation = nil
			s.privateStorage.retryTarget = &Target{HostID: conn.InternalHostID(), ShardID: Option[uint32]{Set: implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0, Value: uint32(implEndpoint.ScyllaConnectionFeaturesData(&conn.InternalScyllaSupported).InternalShard)}, StrictShard: implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0}
			s.privateStorage.prepared, err = request.source().onConn(pagectx, InternalRuntimeFor(s.privateStorage.InternalSession), conn, request.InternalValues, true, s.privateStorage.prepared)
			if err != nil {
				return err
			}
			rt.retries.Add(1)
			continue
		}
		decision := retry.Decision{}
		if policy := request.InternalOptions.InternalProfile.Retry; policy != nil {
			decision = policy.Decide(retry.Attempt{Err: err, Number: attempt, Idempotent: request.InternalOptions.InternalOptions.Idempotent, Consistency: consistency, Conditional: request.InternalRouting.Conditional, BytesWritten: attemptStatus != NotExecuted, ExecutionStatus: attemptStatus})
		}
		if decision.Action == retry.Stop {
			return err
		}
		if decision.Action != retry.Same && decision.Action != retry.Next {
			return typeValidation("stream retry", "requires retry same, next or stop")
		}
		if decision.Delay < 0 {
			return typeValidation("retry delay", "negative delay")
		}
		if decision.Consistency.Set {
			if !validConsistency(decision.Consistency.Value) {
				return typeValidation("retry consistency", "invalid consistency")
			}
			consistency = decision.Consistency.Value
		}
		if decision.Action == retry.Same && s.privateStorage.InternalReservation.InternalClosing.Load() {
			s.privateStorage.retryTarget = &Target{HostID: conn.InternalHostID(), ShardID: Option[uint32]{Set: implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0, Value: uint32(implEndpoint.ScyllaConnectionFeaturesData(&conn.InternalScyllaSupported).InternalShard)}, StrictShard: implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0}
		}
		if decision.Action == retry.Next || s.privateStorage.InternalReservation.InternalClosing.Load() {
			s.privateStorage.InternalReservation.InternalClose(false)
			s.privateStorage.InternalReservation = nil
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

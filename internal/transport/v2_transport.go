// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"sync"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"

	"context"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/protocol"

	"errors"
	"fmt"

	"time"

	frm "github.com/scylladb/gocql/v2/internal/frame"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// executeTyped issues one attempt with the connection's negotiated protocol.
// Canonical values are borrowed unchanged; retries and metadata repair belong to
// the execution layer. A received page owns its storage independently of Conn.
func (c *Conn) InternalExecuteTyped(ctx context.Context, req *attemptRequest) (*InternalResponsePage, error) {
	return c.InternalExecuteTypedInto(ctx, req, nil)
}

// InternalExecuteTypedInto is InternalExecuteTyped building a successful
// response in dest when dest is not nil. dest must stay unused by anyone else
// until the response is released; a response that arrives after the caller
// stopped waiting is built elsewhere.
func (c *Conn) InternalExecuteTypedInto(ctx context.Context, req *attemptRequest, dest *InternalResponsePage) (*InternalResponsePage, error) {
	if err := c.beginShared(ctx); err != nil {
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err})
	}
	// The shared admission and the input reference move to the writer's
	// request lease once the request is submitted; until then they are ours.
	sharedHeld, inputHeld := true, false
	defer func() {
		if inputHeld {
			req.Lease.Release()
		}
		if sharedHeld {
			c.endShared()
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err})
	}
	if req == nil {
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: fmt.Errorf("gocql: nil typed attempt")})
	}
	if req.Lease != nil {
		if !req.Lease.Retain() {
			return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: ErrReleased})
		}
		// The writer's completion is an explicit input lifetime boundary.
		inputHeld = true
	}
	// Custom payload views can belong to a previously received page. Each
	// accepted attempt holds those leases just like canonical bound values.
	var payloadOwners []*wire.Lease
	defer func() {
		for _, owner := range payloadOwners {
			owner.Release()
		}
	}()
	for _, entry := range req.Payload {
		if owner := cqltype.BytesLease(entry.Value.Value); entry.Value.Valid && !owner.IsZero() {
			if !owner.Retain() {
				return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: ErrReleased})
			}
			payloadOwners = append(payloadOwners, owner.Lease())
		}
	}
	stream, ok := c.InternalStreams.GetStream()
	if !ok {
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: ErrNoStreams})
	}
	call := InternalGetCallReq(stream)
	call.InternalTyped = true
	call.dest = dest
	if c.InternalStreamObserver != nil {
		call.streamObserverContext = c.InternalStreamObserver.StreamContext(ctx)
	}
	if err := c.addCall(call); err != nil {
		c.InternalStreams.Clear(stream)
		call.finishExec()
		InternalPutCallReq(call)
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err})
	}
	var stopWaiting, releaseStream, recycleCall bool
	var closeErr error
	defer func() {
		if closeErr != nil {
			c.InternalCloseWithError(closeErr)
		}
	}()
	defer func() {
		if stopWaiting {
			if resp, ok := call.InternalStopWaiting(); ok {
				// The response arrived as we stopped waiting; it is ours to drop.
				if resp.InternalPage != nil {
					resp.InternalPage.Owner.Release()
				}
				if resp.removedFromCalls {
					releaseStream = true
					recycleCall = true
				}
			}
		}
		call.finishExec()
		if releaseStream {
			c.releaseStream(call)
		}
		if recycleCall {
			InternalPutCallReq(call)
		}
	}()
	failBeforeWrite := func(err error) (*InternalResponsePage, error) {
		stopWaiting = true
		if c.InternalRemoveCallIfOpen(stream) {
			releaseStream = true
			recycleCall = true
		}
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err})
	}
	scratch := getFrameScratch()
	packet, used, err := c.nativeProtocol().BuildTypedRequestInto(req, stream, scratch)
	if !used || err != nil {
		putFrameScratch(scratch)
		scratch = nil
	}
	if err != nil {
		return failBeforeWrite(err)
	}
	writer, ok := c.InternalW.(spanContextWriter)
	if !ok {
		putFrameScratch(scratch)
		return failBeforeWrite(fmt.Errorf("gocql: connection writer does not support immutable spans"))
	}
	if call.streamObserverContext != nil {
		call.streamObserverContext.StreamStarted(ObservedStream{Host: c.InternalHost})
	}
	var ticket *writeTicket
	if c.InternalCoalesceWait == 0 {
		// Hold both input and shared-connection admission until the persistent
		// writer finishes, even if cancellation returns to the calling goroutine.
		t := getTicket()
		owner := &t.request
		owner.conn, owner.payloads, owner.scratch = c, payloadOwners, scratch
		if inputHeld {
			owner.input = req.Lease
		}
		wire.InitLease(&owner.Lease, packet, owner)
		lease := &owner.Lease
		sharedHeld, inputHeld, payloadOwners = false, false, nil
		// The caller does not wait for the write: the response wait below
		// covers it, and write failures close the connection.
		ticket, err = c.enqueueTyped(ctx, t, packet, lease)
		if err != nil {
			return failBeforeWrite(err)
		}
		defer ticket.release()
	} else {
		n, err := writer.InternalWriteSpans(ctx, packet)
		putFrameScratch(scratch) // the writer returned: no span references remain
		if err != nil {
			stopWaiting = true
			if n == 0 && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				if c.InternalRemoveCallIfOpen(stream) {
					releaseStream = true
					recycleCall = true
				}
			} else {
				closeErr = err
			}
			return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err, InternalPotentiallyExecuted: n > 0})
		}
	}
	// potentiallyExecuted reports whether any byte of this request may have
	// reached the server, after the wait ended without a response.
	potentiallyExecuted := func() bool {
		if ticket == nil {
			return true // written synchronously above
		}
		if ticket.unsent() {
			return false
		}
		n, known := ticket.written()
		return !known || n > 0
	}
	var timeoutCh <-chan time.Time
	if req.Timeout > 0 {
		if call.timer == nil {
			call.timer = time.NewTimer(req.Timeout)
		} else {
			if !call.timer.Stop() {
				select {
				case <-call.timer.C:
				default:
				}
			}
			call.timer.Reset(req.Timeout)
		}
		timeoutCh = call.timer.C
	}
	select {
	case resp := <-call.InternalResp:
		if ticket != nil {
			ticket.awaitWritten()
		}
		if resp.removedFromCalls {
			releaseStream = true
			recycleCall = true
		}
		if resp.InternalErr != nil {
			// A frame from the server proves execution; a connection failure
			// delivered by close proves only what the writer transmitted.
			executed := resp.removedFromCalls || potentiallyExecuted()
			return resp.InternalPage, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: resp.InternalErr, InternalPotentiallyExecuted: executed})
		}
		return resp.InternalPage, nil
	case <-timeoutCh:
		stopWaiting = true
		if ticket != nil && ticket.unsent() {
			if c.InternalRemoveCallIfOpen(stream) {
				releaseStream = true
				recycleCall = true
			}
			return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: ErrTimeoutNoResponse, InternalTimeout: req.Timeout})
		}
		if c.markOrphan(call) {
			closeErr = InternalErrOrphanBudget
		}
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: ErrTimeoutNoResponse, InternalPotentiallyExecuted: true, InternalTimeout: req.Timeout, InternalInFlight: c.InternalStreams.InUse()})
	case <-attemptDone(ctx):
		stopWaiting = true
		if ticket != nil && ticket.unsent() {
			// Withdrawn before transmission: the stream is free again.
			if c.InternalRemoveCallIfOpen(stream) {
				releaseStream = true
				recycleCall = true
			}
			return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: ctx.Err()})
		}
		if c.markOrphan(call) {
			closeErr = InternalErrOrphanBudget
		}
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: ctx.Err(), InternalPotentiallyExecuted: true, InternalTimeout: req.Timeout, InternalInFlight: c.InternalStreams.InUse()})
	case <-c.InternalCtx.Done():
		stopWaiting = true
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: ErrConnectionClosed, InternalPotentiallyExecuted: potentiallyExecuted()})
	}
}

func (c *Conn) typedProtocolLimits() ProtocolLimits {
	limits := ProtocolLimits{MaxFrameBytes: frm.MaxFrameSize, MaxDecompressedBytes: frm.MaxFrameSize, MaxMetadataDepth: 64}
	if c.InternalCfg != nil {
		configured := c.InternalCfg.TypedLimits
		if configured.MaxFrameBytes > 0 {
			limits.MaxFrameBytes = configured.MaxFrameBytes
		}
		if configured.MaxDecompressedBytes > 0 {
			limits.MaxDecompressedBytes = configured.MaxDecompressedBytes
		}
		if configured.MaxMetadataDepth > 0 {
			limits.MaxMetadataDepth = min(configured.MaxMetadataDepth, 128)
		}
	}
	return limits
}

// typedRequestLease owns a submitted request until its writer finishes: the
// shared-connection admission, the input and payload references, and the
// frame buffer. It is embedded in the request's write ticket.
type typedRequestLease struct {
	conn     *Conn
	input    *wire.Lease
	scratch  *wire.BuilderScratch
	payloads []*wire.Lease
	wire.Lease
}

func (r *typedRequestLease) ReleaseLease() {
	if r.input != nil {
		r.input.Release()
	}
	for _, owner := range r.payloads {
		owner.Release()
	}
	// The writer is done with the frame: its envelope storage can be reused.
	putFrameScratch(r.scratch)
	r.conn.endShared()
}

// Frame scratch holds a request envelope's scalar bytes and span descriptors.
// It returns to the pool only once the writer finished with the frame.
var frameScratchPool = sync.Pool{New: func() any { return wire.NewBuilderScratch(256, 16) }}

func getFrameScratch() *wire.BuilderScratch { return frameScratchPool.Get().(*wire.BuilderScratch) }

func putFrameScratch(s *wire.BuilderScratch) {
	if s == nil {
		return
	}
	s.Reset()
	frameScratchPool.Put(s)
}

func (c *Conn) InternalBuildTypedRequest(req *attemptRequest, stream int) (wire.Chain, error) {
	return c.nativeProtocol().BuildTypedRequest(req, stream)
}
func (c *Conn) writeTypedParams(e *typedEnvelope, req *attemptRequest, batch bool) {
	c.nativeProtocol().WriteTypedParams(e, req, batch)
}

// InternalHostID returns the ID of the connection's host without taking the
// host lock once it is known. A host ID is filled in at most once and never
// changes afterwards, so the first non-empty value is cached.
func (c *Conn) InternalHostID() UUID {
	if id := c.hostID.Load(); id != nil {
		return *id
	}
	id := implEndpoint.InternalHostInfoHostUUID(c.InternalHost)
	if !id.IsEmpty() {
		c.hostID.Store(&id)
	}
	return id
}

// nativeProtocol returns the connection's protocol settings. They are built
// once, when the framer cache is initialised; a connection that skipped that
// step builds them on every call.
func (c *Conn) nativeProtocol() *protocol.Settings {
	if p := c.typedSettings.Load(); p != nil {
		return p
	}
	return c.buildNativeProtocol()
}

// InternalRefreshNativeProtocol rebuilds the cached protocol settings, for
// tests that change framer defaults after initialisation.
func (c *Conn) InternalRefreshNativeProtocol() {
	c.typedSettings.Store(c.buildNativeProtocol())
}

func (c *Conn) buildNativeProtocol() *protocol.Settings {
	limits := c.typedProtocolLimits()
	p := protocol.Settings{MetadataReuse: &c.typedMetadataReuse, Protocol: c.InternalVersion, Flags: c.InternalFramers.InternalDefaults.InternalFlags, MetadataID: c.InternalFramers.InternalDefaults.InternalScyllaUseMetadataID, TabletsRoutingV2: c.InternalFramers.InternalDefaults.InternalTabletsRoutingV2, MaxFrameBytes: limits.MaxFrameBytes, MaxDecompressedBytes: limits.MaxDecompressedBytes, MaxMetadataDepth: limits.MaxMetadataDepth, FlagLWT: c.InternalFramers.InternalDefaults.InternalFlagLWT, RateLimitingErrorCode: c.InternalFramers.InternalDefaults.rateLimitingErrorCode, SegmentCompression: c.InternalSegCompressor != nil}
	if c.InternalCompressor != nil {
		p.Compression = c.InternalCompressor.Name()
	}
	return &p
}

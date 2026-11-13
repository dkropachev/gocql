// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"github.com/scylladb/gocql/v2/internal/connectivity"
	"github.com/scylladb/gocql/v2/internal/model"
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/syncx"

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
func (c *Conn) InternalExecuteTyped(ctx context.Context, req *model.AttemptRequest) (*InternalResponsePage, error) {
	return c.InternalExecuteTypedInto(ctx, req, nil)
}

// InternalExecuteTypedInto is InternalExecuteTyped building a successful
// response in dest when dest is not nil. dest must stay unused by anyone else
// until the response is released; a response that arrives after the caller
// stopped waiting is built elsewhere.
func (c *Conn) InternalExecuteTypedInto(ctx context.Context, req *model.AttemptRequest, dest *InternalResponsePage) (*InternalResponsePage, error) {
	// One deferred cleanup with one return keeps the defer open-coded.
	// req stays out of x: x's payload owners move to the heap with the
	// writer's lease, and req would move with them.
	x := typedExec{conn: c}
	defer x.cleanup(req)
	return x.run(ctx, req, dest)
}

// typedExec is the state of one InternalExecuteTypedInto that its cleanup
// reads: what the attempt still holds when it returns.
type typedExec struct {
	conn *Conn
	// The shared admission and the input reference move to the writer's
	// request lease once the request is submitted; until then they are ours.
	// The admission is taken together with the stream.
	sharedHeld, inputHeld bool
	// Custom payload views can belong to a previously received page. Each
	// accepted attempt holds those leases just like canonical bound values.
	payloadOwners []*wire.Lease
	// call is set once the call holds a stream.
	call                                    *InternalCallReq
	stopWaiting, releaseStream, recycleCall bool
	closeErr                                error
	ticket                                  *writeTicket
}

// cleanup releases what the attempt holds, in the reverse order of taking it.
func (x *typedExec) cleanup(req *model.AttemptRequest) {
	c := x.conn
	if x.ticket != nil {
		x.ticket.release()
	}
	if call := x.call; call != nil {
		if x.stopWaiting {
			if resp, ok := call.stopWaiting(); ok {
				// The response arrived as we stopped waiting; it is ours to drop.
				if resp.page != nil {
					resp.page.Owner.Release()
				}
				if resp.removedFromCalls {
					x.releaseStream = true
					x.recycleCall = true
				}
			}
		}
		call.finishExec()
		if x.releaseStream {
			c.releaseStream(call)
		}
		if x.recycleCall {
			putCallReq(call)
		}
		if x.closeErr != nil {
			c.InternalCloseWithError(x.closeErr)
		}
	}
	for _, owner := range x.payloadOwners {
		owner.Release()
	}
	if x.inputHeld {
		req.Lease.Release()
	}
	if x.sharedHeld {
		c.endShared()
	}
}

func (x *typedExec) run(ctx context.Context, req *model.AttemptRequest, dest *InternalResponsePage) (*InternalResponsePage, error) {
	c := x.conn
	if err := ctx.Err(); err != nil {
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err})
	}
	if req == nil {
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: fmt.Errorf("gocql: nil typed attempt")})
	}
	if req.Lease != nil {
		if !req.Lease.Retain() {
			return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: cqltype.ErrReleased})
		}
		// The writer's completion is an explicit input lifetime boundary.
		x.inputHeld = true
	}
	for _, entry := range req.Payload {
		if owner := cqltype.BytesLease(entry.Value.Value); entry.Value.Valid && !owner.IsZero() {
			if !owner.Retain() {
				return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: cqltype.ErrReleased})
			}
			x.payloadOwners = append(x.payloadOwners, owner.Lease())
		}
	}
	call := getCallReq(0)
	call.typed = true
	call.dest = dest
	// A context that wakes a wait through a channel of its own lets the wait
	// receive the response and the cancellation from that one channel, unless
	// something else can end the wait.
	wake := attemptWake(ctx)
	if wake != nil && req.Timeout <= 0 && c.closesOnCancel && attemptParentDone(ctx) == nil {
		call.InternalResp = wake
	} else {
		wake = nil
	}
	if err := c.allocSharedCall(ctx, call); err != nil {
		call.finishExec()
		putCallReq(call)
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err})
	}
	x.sharedHeld = true
	x.call = call
	stream := call.InternalStreamID
	if c.InternalStreamObserver != nil {
		call.streamObserverContext = c.InternalStreamObserver.StreamContext(ctx)
	}
	failBeforeWrite := func(err error) (*InternalResponsePage, error) {
		x.stopWaiting = true
		if c.InternalRemoveCallIfOpen(call) {
			x.releaseStream = true
			x.recycleCall = true
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
	if c.coalesceWait == 0 {
		// Hold both input and shared-connection admission until the persistent
		// writer finishes, even if cancellation returns to the calling goroutine.
		t := getTicket()
		owner := &t.request
		owner.conn, owner.payloads, owner.scratch = c, x.payloadOwners, scratch
		if x.inputHeld {
			owner.input = req.Lease
		}
		wire.InitLease(&owner.Lease, packet, owner)
		lease := &owner.Lease
		x.sharedHeld, x.inputHeld, x.payloadOwners = false, false, nil
		// The caller does not wait for the write: the response wait below
		// covers it, and write failures close the connection.
		ticket, err = c.enqueueTyped(ctx, t, packet, lease)
		if err != nil {
			return failBeforeWrite(err)
		}
		x.ticket = ticket
	} else {
		n, err := writer.InternalWriteSpans(ctx, packet)
		putFrameScratch(scratch) // the writer returned: no span references remain
		if err != nil {
			x.stopWaiting = true
			if n == 0 && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				if c.InternalRemoveCallIfOpen(call) {
					x.releaseStream = true
					x.recycleCall = true
				}
			} else {
				x.closeErr = err
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
	// A connection that closes when its context is cancelled delivers the
	// closure as a response, so the wait need not watch the context; a nil
	// channel takes no part in the select.
	connDone := c.InternalCtx.Done()
	if c.closesOnCancel {
		connDone = nil
	}
	// abandon ends a wait whose attempt context, or the parent it watches, was
	// cancelled.
	abandon := func() (*InternalResponsePage, error) {
		x.stopWaiting = true
		if ticket != nil && ticket.unsent() {
			// Withdrawn before transmission: the stream is free again.
			if c.InternalRemoveCallIfOpen(call) {
				x.releaseStream = true
				x.recycleCall = true
			}
			return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: ctx.Err()})
		}
		if c.markOrphan(call) {
			x.closeErr = errOrphanBudget
		}
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: ctx.Err(), InternalPotentiallyExecuted: true, InternalTimeout: req.Timeout, InternalInFlight: c.InternalStreams.InUse()})
	}
	received := func(resp InternalCallResp) (*InternalResponsePage, error) {
		if ticket != nil {
			ticket.awaitWritten()
		}
		if resp.removedFromCalls {
			x.releaseStream = true
			x.recycleCall = true
		}
		if resp.InternalErr != nil {
			// A frame from the server proves execution; a connection failure
			// delivered by close proves only what the writer transmitted.
			executed := resp.removedFromCalls || potentiallyExecuted()
			return resp.page, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: resp.InternalErr, InternalPotentiallyExecuted: executed})
		}
		return resp.page, nil
	}
	if wake != nil {
		// The context sends a wake into the call's channel when cancelled.
		if resp := <-wake; !resp.wake {
			return received(resp)
		}
		return abandon()
	}
	parentDone := attemptParentDone(ctx)
	select {
	case resp := <-call.InternalResp:
		return received(resp)
	case <-timeoutCh:
		x.stopWaiting = true
		if ticket != nil && ticket.unsent() {
			if c.InternalRemoveCallIfOpen(call) {
				x.releaseStream = true
				x.recycleCall = true
			}
			return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: runtimeerrors.ErrTimeoutNoResponse, InternalTimeout: req.Timeout})
		}
		if c.markOrphan(call) {
			x.closeErr = errOrphanBudget
		}
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: runtimeerrors.ErrTimeoutNoResponse, InternalPotentiallyExecuted: true, InternalTimeout: req.Timeout, InternalInFlight: c.InternalStreams.InUse()})
	case <-attemptDone(ctx):
		return abandon()
	case <-parentDone:
		return abandon()
	case <-connDone:
		x.stopWaiting = true
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: runtimeerrors.ErrConnectionClosed, InternalPotentiallyExecuted: potentiallyExecuted()})
	}
}

func (c *Conn) typedProtocolLimits() connectivity.ProtocolLimits {
	limits := connectivity.ProtocolLimits{MaxFrameBytes: frm.MaxFrameSize, MaxDecompressedBytes: frm.MaxFrameSize, MaxMetadataDepth: 64}
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
var frameScratchPool = syncx.NewPool(func() *wire.BuilderScratch { return wire.NewBuilderScratch(256, 16) })

func getFrameScratch() *wire.BuilderScratch { return frameScratchPool.Get() }

func putFrameScratch(s *wire.BuilderScratch) {
	if s == nil {
		return
	}
	s.Reset()
	frameScratchPool.Put(s)
}

func (c *Conn) InternalBuildTypedRequest(req *model.AttemptRequest, stream int) (wire.Chain, error) {
	return c.nativeProtocol().BuildTypedRequest(req, stream)
}
func (c *Conn) writeTypedParams(e *protocol.TypedEnvelope, req *model.AttemptRequest, batch bool) {
	c.nativeProtocol().WriteTypedParams(e, req, batch)
}

// InternalCloseOnCancel makes cancelling InternalCtx close the connection,
// which delivers the closure to every waiting typed call, so their waits
// need not watch InternalCtx. Call it once, before the connection is used.
func (c *Conn) InternalCloseOnCancel() {
	c.stopCloseOnCancel = context.AfterFunc(c.InternalCtx, func() { c.InternalCloseWithError(nil) })
	c.closesOnCancel = true
}

// InternalKeepOnCancel undoes InternalCloseOnCancel, for tests that cancel a
// connection's context and then reuse the connection.
func (c *Conn) InternalKeepOnCancel() {
	if c.stopCloseOnCancel != nil {
		c.stopCloseOnCancel()
	}
	c.closesOnCancel, c.stopCloseOnCancel = false, nil
}

// InternalHostID returns the ID of the connection's host without taking the
// host lock once it is known. A host ID is filled in at most once and never
// changes afterwards, so the first non-empty value is cached.
func (c *Conn) InternalHostID() cqltype.UUID {
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
	p := protocol.Settings{MetadataReuse: &c.typedMetadataReuse, Version: *c.InternalWire(), Flags: c.InternalFramers.InternalDefaults.InternalFlags, MetadataID: c.InternalFramers.InternalDefaults.InternalScyllaUseMetadataID, TabletsRoutingV2: c.InternalFramers.InternalDefaults.InternalTabletsRoutingV2, MaxFrameBytes: limits.MaxFrameBytes, MaxDecompressedBytes: limits.MaxDecompressedBytes, MaxMetadataDepth: limits.MaxMetadataDepth, FlagLWT: c.InternalFramers.InternalDefaults.InternalFlagLWT, RateLimitingErrorCode: c.InternalFramers.InternalDefaults.rateLimitingErrorCode, SegmentCompression: c.InternalSegCompressor != nil}
	if c.InternalCompressor != nil {
		p.Compression = c.InternalCompressor.Name()
	}
	return &p
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package runtimeerrors

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/protocol"
)

// referenceKindOf is KindOf as a sequence of errors.Is and errors.As checks,
// each searching the whole tree, in the order of precedence KindOf
// documents. KindOf must agree with it on every tree.
func referenceKindOf(err error) Kind {
	if err == nil {
		return KindUnknown
	}
	if request, ok := errors.AsType[*ExecutionError](err); ok {
		err = request.Err
	}
	has := func(targets ...error) bool {
		for _, target := range targets {
			if errors.Is(err, target) {
				return true
			}
		}
		return false
	}
	switch {
	case has(context.Canceled):
		return KindCanceled
	case has(context.DeadlineExceeded):
		return KindTimeout
	}
	if server, ok := errors.AsType[protocol.ServerError](err); ok {
		if _, ok := server.(*protocol.RateLimitReachedError); ok {
			return KindRateLimited
		}
		return protocol.CodeKind(server.Code())
	}
	switch {
	case has(ErrClosed, ErrSessionClosed, ErrStaleRows, cqltype.ErrReleased):
		return KindClosed
	case hasType[*cqltype.ValidationError](err), hasType[*cqltype.CodecError](err), hasType[*SchemaChangedError](err),
		hasType[*UnsupportedSettingError](err), hasType[*ServerMismatchError](err),
		has(ErrUseStmt, ErrNoKeyspace, ErrNoTable, ErrKeyspaceDoesNotExist, ErrBusy, ErrConditionalExec, ErrPagingComplete):
		return KindInvalid
	case hasType[*cqltype.ProtocolError](err), has(protocol.ErrFrameTooBig):
		return KindProtocol
	case has(ErrTimeoutNoResponse, protocol.ErrReadHeaderTimeout):
		return KindTimeout
	}
	if network, ok := errors.AsType[net.Error](err); ok && network.Timeout() {
		return KindTimeout
	}
	switch {
	case hasType[*ConnectError](err), hasType[net.Error](err),
		has(ErrNoConnections, ErrNoConnectionsStarted, ErrConnectionClosed, ErrNoStreams, ErrNoHosts, ErrSessionNotReady,
			io.EOF, io.ErrUnexpectedEOF, net.ErrClosed):
		return KindConnection
	}
	return KindUnknown
}

// referenceExecutionStatusOf is ExecutionStatusOf as separate errors.Is and
// errors.As checks.
func referenceExecutionStatusOf(err error) cqltype.ExecutionStatus {
	if err == nil {
		return cqltype.NotExecuted
	}
	if reporter, ok := errors.AsType[executionStatusReporter](err); ok {
		return reporter.ExecutionStatus()
	}
	if server, ok := errors.AsType[protocol.ServerError](err); ok {
		return protocol.ServerErrorExecutionStatus(server)
	}
	if hasType[*SchemaChangedError](err) {
		return cqltype.NotExecuted
	}
	for _, target := range []error{ErrNoConnections, ErrSessionClosed, ErrSessionNotReady, ErrBusy, ErrConditionalExec, context.Canceled, context.DeadlineExceeded} {
		if errors.Is(err, target) {
			return cqltype.NotExecuted
		}
	}
	return cqltype.MaybeExecuted
}

func hasType[E error](err error) bool {
	_, ok := errors.AsType[E](err)
	return ok
}

type netTimeout struct{ timeout bool }

func (e netTimeout) Error() string   { return "i/o" }
func (e netTimeout) Timeout() bool   { return e.timeout }
func (e netTimeout) Temporary() bool { return false }

// classifiedLeaves returns one error of every kind KindOf and
// ExecutionStatusOf tell apart.
func classifiedLeaves() []error {
	leaves := []error{
		errors.New("unclassified"),
		context.Canceled, context.DeadlineExceeded,
		ErrClosed, ErrSessionClosed, ErrStaleRows, cqltype.ErrReleased,
		ErrUseStmt, ErrNoKeyspace, ErrNoTable, ErrKeyspaceDoesNotExist, ErrBusy, ErrConditionalExec, ErrPagingComplete,
		protocol.ErrFrameTooBig, ErrTimeoutNoResponse, protocol.ErrReadHeaderTimeout,
		ErrNoConnections, ErrNoConnectionsStarted, ErrConnectionClosed, ErrNoStreams, ErrNoHosts, ErrSessionNotReady,
		io.EOF, io.ErrUnexpectedEOF, net.ErrClosed, ErrNotFound, ErrNoRows, ErrPagingStateLimit,
		&cqltype.ValidationError{Field: "f", Err: errors.New("bad")},
		&cqltype.CodecError{Stage: "decode", Err: errors.New("bad")},
		&cqltype.ProtocolError{Stage: "rows", Err: errors.New("bad")},
		cqltype.NewFrameError("unknown opcode"),
		&SchemaChangedError{Statement: "s"},
		&UnsupportedSettingError{Setting: "s", Reason: "r"},
		&ServerMismatchError{Want: "ScyllaDB", Got: "Cassandra"},
		&ConnectError{Stage: "s"},
		&net.OpError{Op: "dial", Err: errors.New("refused")},
		&net.OpError{Op: "read", Err: netTimeout{timeout: true}},
		netTimeout{timeout: true}, netTimeout{},
		&protocol.UnavailableError{},
		&protocol.WriteTimeoutError{WriteType: "SIMPLE"},
		&protocol.WriteTimeoutError{WriteType: "SIMPLE", Received: 1},
		&protocol.WriteTimeoutError{WriteType: "CAS", Received: 1},
		&protocol.WriteFailureError{WriteType: "BATCH", Received: 1},
		&protocol.ReadTimeoutError{}, &protocol.ReadFailureError{},
		&protocol.CDCWriteFailureError{}, &protocol.CASWriteUnknownError{},
		&protocol.AlreadyExistsError{}, &protocol.UnpreparedError{}, &protocol.FunctionFailureError{},
		&protocol.RateLimitReachedError{}, &protocol.RateLimitReachedError{RejectedByCoordinator: true},
		protocol.NewOtherServerError(0x7777, ""),
		protocol.NewOtherServerError(protocol.CodeRateLimitReached, ""),
	}
	for _, code := range []protocol.ErrorCode{
		protocol.CodeServer, protocol.CodeProtocol, protocol.CodeBadCredentials, protocol.CodeUnavailable,
		protocol.CodeOverloaded, protocol.CodeBootstrapping, protocol.CodeTruncate, protocol.CodeWriteTimeout,
		protocol.CodeReadTimeout, protocol.CodeReadFailure, protocol.CodeFunctionFailure, protocol.CodeWriteFailure,
		protocol.CodeCDCWriteFailure, protocol.CodeCASWriteUnknown, protocol.CodeSyntax, protocol.CodeUnauthorized,
		protocol.CodeInvalid, protocol.CodeConfig, protocol.CodeAlreadyExists, protocol.CodeUnprepared,
	} {
		leaves = append(leaves, protocol.NewOtherServerError(code, ""))
	}
	return leaves
}

// TestKindOfMatchesReference checks KindOf and ExecutionStatusOf against
// their reference on every classified error alone, wrapped, and paired with
// every other one in each kind of tree the driver builds.
func TestKindOfMatchesReference(t *testing.T) {
	t.Parallel()
	leaves := classifiedLeaves()
	var trees []error
	for _, a := range leaves {
		trees = append(trees, a,
			fmt.Errorf("wrapped: %w", a),
			NewQueryError(QueryErrorState{InternalErr: a}),
			NewQueryError(QueryErrorState{InternalErr: a, InternalPotentiallyExecuted: true}),
			NewExecutionError(a, model.Result{}, cqltype.PartiallyExecuted, nil),
			&ConnectError{Stage: "s", Hosts: []HostError{{Address: "h", Err: a}}},
		)
		for _, b := range leaves {
			trees = append(trees,
				errors.Join(a, b),
				fmt.Errorf("%w: %w", a, fmt.Errorf("inner: %w", b)),
				&ConnectError{Stage: "s", Hosts: []HostError{{Address: "h1", Err: a}, {Address: "h2", Err: b}}},
				NewExecutionError(a, model.Result{}, cqltype.MaybeExecuted, []AttemptFailure{{Err: b}}),
			)
		}
	}
	for _, tree := range trees {
		if got, want := KindOf(tree), referenceKindOf(tree); got != want {
			t.Errorf("KindOf(%v) = %v, want %v", tree, got, want)
		}
		if got, want := ExecutionStatusOf(tree), referenceExecutionStatusOf(tree); got != want {
			t.Errorf("ExecutionStatusOf(%v) = %v, want %v", tree, got, want)
		}
	}
}

// A user's own ServerError implementation, without the driver's Kind
// method, is classified by its code.
type customServerError struct{ code protocol.ErrorCode }

func (e customServerError) Error() string            { return "custom" }
func (e customServerError) Code() protocol.ErrorCode { return e.code }
func (e customServerError) Message() string          { return "" }
func (e customServerError) Warnings() []string       { return nil }

func TestKindOfCustomServerError(t *testing.T) {
	t.Parallel()
	if got := KindOf(fmt.Errorf("x: %w", customServerError{code: protocol.CodeOverloaded})); got != KindOverloaded {
		t.Fatalf("KindOf = %v, want overloaded", got)
	}
}

// KindOf and ExecutionStatusOf run on every failed attempt, so they must not
// allocate.
func TestKindOfAllocs(t *testing.T) {
	err := fmt.Errorf("attempt: %w", NewQueryError(QueryErrorState{InternalErr: errors.Join(&net.OpError{Op: "read", Err: netTimeout{timeout: true}}, ErrConnectionClosed), InternalPotentiallyExecuted: true}))
	if allocs := testing.AllocsPerRun(100, func() {
		_ = KindOf(err)
		_ = ExecutionStatusOf(err)
	}); allocs != 0 {
		t.Fatalf("%v allocations per run, want 0", allocs)
	}
}

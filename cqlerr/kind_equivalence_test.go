// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/cqltype"
)

// invalidCodecError reports whether the tree of err holds a cql.CodecError
// that does not wrap a cql.ProtocolError.
func invalidCodecError(err error) bool {
	if codec, ok := err.(*cql.CodecError); ok && !hasType[*cql.ProtocolError](codec.Err) {
		return true
	}
	switch e := err.(type) {
	case interface{ Unwrap() error }:
		return e.Unwrap() != nil && invalidCodecError(e.Unwrap())
	case interface{ Unwrap() []error }:
		for _, inner := range e.Unwrap() {
			if invalidCodecError(inner) {
				return true
			}
		}
	}
	return false
}

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
	if server, ok := errors.AsType[ServerError](err); ok {
		if _, ok := server.(*RateLimitReachedError); ok {
			return KindRateLimited
		}
		return codeKind(server.Code())
	}
	switch {
	case has(ErrClosed, ErrStaleRows, cql.ErrReleased):
		return KindClosed
	case hasType[*cql.ValidationError](err), invalidCodecError(err),
		hasType[*SchemaChangedError](err),
		hasType[*UnsupportedSettingError](err), hasType[*ServerMismatchError](err),
		has(ErrUseStmt, ErrNoKeyspace, ErrNoTable, ErrBusy, ErrConditionalExec, ErrPagingComplete,
			ErrPagingStateLimit, ErrNoCurrentRow):
		return KindInvalid
	case hasType[*cql.ProtocolError](err), has(ErrFrameTooBig, ErrPagingNoProgress):
		return KindProtocol
	case has(ErrTimeoutNoResponse, ErrReadHeaderTimeout):
		return KindTimeout
	}
	if network, ok := errors.AsType[net.Error](err); ok && network.Timeout() {
		return KindTimeout
	}
	switch {
	case hasType[*ConnectError](err), hasType[*NoHostsError](err), hasType[net.Error](err),
		has(ErrNoConnections, ErrNoConnectionsStarted, ErrConnectionClosed, ErrNoStreams,
			ErrNoControl, ErrScyllaSourcePortAlreadyInUse, io.EOF, io.ErrUnexpectedEOF, net.ErrClosed):
		return KindConnection
	}
	return KindUnknown
}

// referenceExecutionStatusOf is ExecutionStatusOf as separate errors.Is and
// errors.As checks.
func referenceExecutionStatusOf(err error) ExecutionStatus {
	if err == nil {
		return NotExecuted
	}
	if reporter, ok := errors.AsType[executionStatusReporter](err); ok {
		return reporter.ExecutionStatus()
	}
	if server, ok := errors.AsType[ServerError](err); ok {
		return serverErrorExecutionStatus(server)
	}
	if hasType[*SchemaChangedError](err) {
		return NotExecuted
	}
	for _, target := range []error{ErrNoConnections, ErrBusy, ErrConditionalExec,
		ErrNoStreams, ErrNoConnectionsStarted, ErrUseStmt, ErrPagingComplete, context.Canceled, context.DeadlineExceeded} {
		if errors.Is(err, target) {
			return NotExecuted
		}
	}
	return MaybeExecuted
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
		ErrClosed, ErrStaleRows, cql.ErrReleased,
		ErrUseStmt, ErrNoKeyspace, ErrNoTable, ErrBusy, ErrConditionalExec, ErrPagingComplete,
		ErrFrameTooBig, ErrTimeoutNoResponse, ErrReadHeaderTimeout,
		ErrNoConnections, ErrNoConnectionsStarted, ErrConnectionClosed, ErrNoStreams,
		io.EOF, io.ErrUnexpectedEOF, net.ErrClosed, ErrNoCurrentRow, ErrPagingStateLimit,
		ErrPagingNoProgress, ErrNoControl, ErrScyllaSourcePortAlreadyInUse,
		&NoHostsError{Operation: "o"}, &NoHostsError{Operation: "o", Causes: []error{ErrNoConnections}},
		&cql.ValidationError{Field: "f", Err: errors.New("bad")},
		&cql.CodecError{Stage: "decode", Err: errors.New("bad")},
		&cql.CodecError{Stage: "decode", Err: &cql.ProtocolError{Stage: "cell", Err: io.ErrUnexpectedEOF}},
		&cql.ProtocolError{Stage: "rows", Err: errors.New("bad")},
		cqltype.NewFrameError("unknown opcode"),
		&SchemaChangedError{Statement: "s"},
		&UnsupportedSettingError{Setting: "s", Reason: "r"},
		&ServerMismatchError{Want: "ScyllaDB", Got: "Cassandra"},
		&ConnectError{Stage: "s"},
		&net.OpError{Op: "dial", Err: errors.New("refused")},
		&net.OpError{Op: "read", Err: netTimeout{timeout: true}},
		netTimeout{timeout: true}, netTimeout{},
		&UnavailableError{},
		&WriteTimeoutError{WriteType: WriteTypeSimple},
		&WriteTimeoutError{WriteType: WriteTypeSimple, Received: 1},
		&WriteTimeoutError{WriteType: WriteTypeCAS, Received: 1},
		&WriteFailureError{WriteType: WriteTypeBatch, Received: 1},
		&ReadTimeoutError{}, &ReadFailureError{},
		NewOtherServerError(CodeCDCWriteFailure, ""), &CASWriteUnknownError{},
		&AlreadyExistsError{}, &UnpreparedError{}, &FunctionFailureError{},
		&RateLimitReachedError{}, &RateLimitReachedError{RejectedByCoordinator: true},
		NewOtherServerError(0x7777, ""),
		NewOtherServerError(CodeRateLimitReached, ""),
	}
	for _, code := range []ErrorCode{
		CodeServer, CodeProtocol, CodeBadCredentials, CodeUnavailable,
		CodeOverloaded, CodeBootstrapping, CodeTruncate, CodeWriteTimeout,
		CodeReadTimeout, CodeReadFailure, CodeFunctionFailure, CodeWriteFailure,
		CodeCDCWriteFailure, CodeCASWriteUnknown, CodeSyntax, CodeUnauthorized,
		CodeInvalid, CodeConfig, CodeAlreadyExists, CodeUnprepared,
	} {
		leaves = append(leaves, NewOtherServerError(code, ""))
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
			newQueryError(queryErrorState{InternalErr: a}),
			newQueryError(queryErrorState{InternalErr: a, InternalPotentiallyExecuted: true}),
			NewExecutionError(a, PartiallyExecuted),
			&ConnectError{Stage: "s", Hosts: []HostError{{Address: "h", Err: a}}},
		)
		for _, b := range leaves {
			trees = append(trees,
				errors.Join(a, b),
				fmt.Errorf("%w: %w", a, fmt.Errorf("inner: %w", b)),
				&ConnectError{Stage: "s", Hosts: []HostError{{Address: "h1", Err: a}, {Address: "h2", Err: b}}},
				NewExecutionError(a, MaybeExecuted, AttemptFailure{Err: b}),
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

// KindOf and ExecutionStatusOf run on every failed attempt, so they must not
// allocate.
func TestKindOfAllocs(t *testing.T) {
	err := fmt.Errorf("attempt: %w", newQueryError(queryErrorState{InternalErr: errors.Join(&net.OpError{Op: "read", Err: netTimeout{timeout: true}}, ErrConnectionClosed), InternalPotentiallyExecuted: true}))
	if allocs := testing.AllocsPerRun(100, func() {
		_ = KindOf(err)
		_ = ExecutionStatusOf(err)
	}); allocs != 0 {
		t.Fatalf("%v allocations per run, want 0", allocs)
	}
}

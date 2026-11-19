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

func TestKindOf(t *testing.T) {
	t.Parallel()
	server := func(code ErrorCode) error { return NewOtherServerError(code, "") }
	overloaded := newQueryError(queryErrorState{InternalErr: server(CodeOverloaded), InternalPotentiallyExecuted: true})
	dial := &net.OpError{Op: "dial", Err: errors.New("connection refused")}
	for _, test := range []struct {
		name string
		err  error
		want Kind
	}{
		{"nil", nil, KindUnknown},
		{"unknown", errors.New("x"), KindUnknown},
		{"canceled", fmt.Errorf("wrap: %w", context.Canceled), KindCanceled},
		{"deadline", context.DeadlineExceeded, KindTimeout},
		{"write timeout", &WriteTimeoutError{}, KindTimeout},
		{"read timeout", &ReadTimeoutError{}, KindTimeout},
		{"no response", ErrTimeoutNoResponse, KindTimeout},
		{"unavailable", &UnavailableError{}, KindUnavailable},
		{"overloaded", overloaded, KindOverloaded},
		{"bootstrapping", server(CodeBootstrapping), KindOverloaded},
		{"rate limited", &RateLimitReachedError{}, KindRateLimited},
		{"syntax", server(CodeSyntax), KindInvalid},
		{"validation", &cql.ValidationError{Field: "f", Err: errors.New("bad")}, KindInvalid},
		{"unauthorized", server(CodeUnauthorized), KindUnauthorized},
		{"credentials", server(CodeBadCredentials), KindUnauthorized},
		{"server protocol", server(CodeProtocol), KindProtocol},
		{"client protocol", &cql.ProtocolError{Stage: "s", Err: errors.New("bad")}, KindProtocol},
		{"frame violation", cqltype.NewFrameError("unknown opcode %d", 0x42), KindProtocol},
		{"server failure", server(CodeServer), KindServer},
		{"write failure", &WriteFailureError{}, KindServer},
		{"closed", ErrSessionClosed, KindClosed},
		{"no connections", ErrNoConnections, KindConnection},
		{"connect", &ConnectError{Stage: "s", Hosts: []HostError{{Address: "h", Err: dial}}}, KindConnection},
		{"connect rejected credentials", &ConnectError{Stage: "s", Hosts: []HostError{{Address: "h", Err: server(CodeBadCredentials)}}}, KindUnauthorized},
		{"canceled after server error", errors.Join(server(CodeOverloaded), context.Canceled), KindCanceled},
		{"closed over timeout", errors.Join(ErrTimeoutNoResponse, ErrClosed), KindClosed},
		{"eof", io.ErrUnexpectedEOF, KindConnection},
		{"dial", dial, KindConnection},
		{"request stops on cancel", newExecutionError(context.Canceled, cql.Result{}, MaybeExecuted, []AttemptFailure{{Err: overloaded}}), KindCanceled},
		{"request last attempt", newExecutionError(overloaded, cql.Result{}, NotExecuted, []AttemptFailure{{Err: overloaded}}), KindOverloaded},
		{"cas write unknown", &CASWriteUnknownError{}, KindTimeout},
		{"cas write unknown code", server(CodeCASWriteUnknown), KindTimeout},
		{"no control connection", ErrNoControl, KindConnection},
		{"source port in use", ErrScyllaSourcePortAlreadyInUse, KindConnection},
		{"no hosts", &NoHostsError{Operation: "o"}, KindConnection},
		{"no hosts, server cause", &NoHostsError{Operation: "o", Causes: []error{server(CodeUnauthorized)}}, KindUnauthorized},
		{"paging state limit", ErrPagingStateLimit, KindInvalid},
		{"not found", ErrNotFound, KindInvalid},
		{"no rows", ErrNoRows, KindInvalid},
		{"paging no progress", ErrPagingNoProgress, KindProtocol},
	} {
		if got := KindOf(test.err); got != test.want {
			t.Errorf("%s: KindOf = %v, want %v", test.name, got, test.want)
		}
	}
	for _, k := range []Kind{KindTimeout, KindUnavailable, KindOverloaded, KindRateLimited, KindConnection} {
		if !k.Transient() {
			t.Errorf("%v not transient", k)
		}
	}
	for _, k := range []Kind{KindInvalid, KindUnauthorized, KindCanceled, KindClosed, KindProtocol, KindServer, KindUnknown} {
		if k.Transient() {
			t.Errorf("%v transient", k)
		}
	}
}

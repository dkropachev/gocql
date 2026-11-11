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

func TestKindOf(t *testing.T) {
	t.Parallel()
	server := func(code protocol.ErrorCode) error { return protocol.NewOtherServerError(code, "") }
	overloaded := NewQueryError(QueryErrorState{InternalErr: server(protocol.CodeOverloaded), InternalPotentiallyExecuted: true})
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
		{"write timeout", &protocol.WriteTimeoutError{}, KindTimeout},
		{"read timeout", &protocol.ReadTimeoutError{}, KindTimeout},
		{"no response", ErrTimeoutNoResponse, KindTimeout},
		{"unavailable", &protocol.UnavailableError{}, KindUnavailable},
		{"overloaded", overloaded, KindOverloaded},
		{"bootstrapping", server(protocol.CodeBootstrapping), KindOverloaded},
		{"rate limited", &protocol.RateLimitReachedError{}, KindRateLimited},
		{"syntax", server(protocol.CodeSyntax), KindInvalid},
		{"validation", &cqltype.ValidationError{Field: "f", Err: errors.New("bad")}, KindInvalid},
		{"unauthorized", server(protocol.CodeUnauthorized), KindUnauthorized},
		{"credentials", server(protocol.CodeBadCredentials), KindUnauthorized},
		{"server protocol", server(protocol.CodeProtocol), KindProtocol},
		{"client protocol", &cqltype.ProtocolError{Stage: "s", Err: errors.New("bad")}, KindProtocol},
		{"frame violation", cqltype.NewFrameError("unknown opcode %d", 0x42), KindProtocol},
		{"server failure", server(protocol.CodeServer), KindServer},
		{"write failure", &protocol.WriteFailureError{}, KindServer},
		{"closed", ErrSessionClosed, KindClosed},
		{"no connections", ErrNoConnections, KindConnection},
		{"connect", &ConnectError{Stage: "s", Hosts: []HostError{{Address: "h", Err: dial}}}, KindConnection},
		{"connect rejected credentials", &ConnectError{Stage: "s", Hosts: []HostError{{Address: "h", Err: server(protocol.CodeBadCredentials)}}}, KindUnauthorized},
		{"canceled after server error", errors.Join(server(protocol.CodeOverloaded), context.Canceled), KindCanceled},
		{"closed over timeout", errors.Join(ErrTimeoutNoResponse, ErrClosed), KindClosed},
		{"eof", io.ErrUnexpectedEOF, KindConnection},
		{"dial", dial, KindConnection},
		{"request stops on cancel", NewExecutionError(context.Canceled, model.Result{}, cqltype.MaybeExecuted, []AttemptFailure{{Err: overloaded}}), KindCanceled},
		{"request last attempt", NewExecutionError(overloaded, model.Result{}, cqltype.NotExecuted, []AttemptFailure{{Err: overloaded}}), KindOverloaded},
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

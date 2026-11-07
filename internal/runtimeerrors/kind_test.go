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
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/protocol"
)

func TestKindOf(t *testing.T) {
	t.Parallel()
	server := func(code int) error { return &protocol.RequestErrServer{ErrorFrame: frm.ErrorFrame{Code: code}} }
	overloaded := NewQueryError(QueryErrorState{InternalErr: server(protocol.ErrCodeOverloaded), InternalPotentiallyExecuted: true})
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
		{"write timeout", &protocol.RequestErrWriteTimeout{ErrorFrame: frm.ErrorFrame{Code: protocol.ErrCodeWriteTimeout}}, KindTimeout},
		{"read timeout", &protocol.RequestErrReadTimeout{ErrorFrame: frm.ErrorFrame{Code: protocol.ErrCodeReadTimeout}}, KindTimeout},
		{"no response", ErrTimeoutNoResponse, KindTimeout},
		{"unavailable", &protocol.RequestErrUnavailable{ErrorFrame: frm.ErrorFrame{Code: protocol.ErrCodeUnavailable}}, KindUnavailable},
		{"overloaded", overloaded, KindOverloaded},
		{"bootstrapping", server(protocol.ErrCodeBootstrapping), KindOverloaded},
		{"rate limited", &protocol.RequestErrRateLimitReached{ErrorFrame: frm.ErrorFrame{Code: 0xF000}}, KindRateLimited},
		{"syntax", server(protocol.ErrCodeSyntax), KindInvalid},
		{"validation", &cqltype.ValidationError{Field: "f", Err: errors.New("bad")}, KindInvalid},
		{"unauthorized", server(protocol.ErrCodeUnauthorized), KindUnauthorized},
		{"credentials", server(protocol.ErrCodeCredentials), KindUnauthorized},
		{"server protocol", server(protocol.ErrCodeProtocol), KindProtocol},
		{"client protocol", &cqltype.ProtocolError{Stage: "s", Err: errors.New("bad")}, KindProtocol},
		{"server failure", server(protocol.ErrCodeServer), KindServer},
		{"write failure", &protocol.RequestErrWriteFailure{ErrorFrame: frm.ErrorFrame{Code: protocol.ErrCodeWriteFailure}}, KindServer},
		{"closed", ErrSessionClosed, KindClosed},
		{"no connections", ErrNoConnections, KindConnection},
		{"connect", &ConnectError{Stage: "s", Hosts: []HostError{{Address: "h", Err: dial}}}, KindConnection},
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

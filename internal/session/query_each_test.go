// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"net"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/retry"
)

func eachTestQuery(t *testing.T) (*Query[codec.EmptyRecord, []byte], func(bodies ...[]byte) chan struct{}) {
	t.Helper()
	s, conns, peers := runtimeTestSession(t, 1)
	statement := binding.Statement[codec.EmptyRecord, []byte]{Kind: cqltype.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: codec.EmptyParamsEncoder, Rows: codec.RowInto(func(*[]byte) {}, codec.Column("value", codec.OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
	q := codecTestMust(unpreparedQuery(s, statement, Options{}))
	t.Cleanup(q.Close)
	serve := func(bodies ...[]byte) chan struct{} {
		served := make(chan struct{})
		go func() {
			defer close(served)
			for _, body := range bodies {
				stream := typedTestReadRequest(t, conns[0], peers[0])
				if _, err := peers[0].Write(typedTestResponse(t, 4, stream, body)); err != nil {
					t.Error(err)
					return
				}
			}
		}()
		return served
	}
	return q, serve
}

// eachPageTimeoutQuery is eachTestQuery with a page timeout below the
// execution timeout, so pages run under the query's reused page context.
func eachPageTimeoutQuery(t *testing.T) (*Query[codec.EmptyRecord, []byte], net.Conn) {
	t.Helper()
	s, _, peers := runtimeTestSession(t, 1)
	statement := binding.Statement[codec.EmptyRecord, []byte]{Kind: cqltype.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: codec.EmptyParamsEncoder, Rows: codec.RowInto(func(*[]byte) {}, codec.Column("value", codec.OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
	options := testOptions(Options{Timeout: cqltype.Option[time.Duration]{Set: true, Value: 5 * time.Second}, PageTimeout: cqltype.Option[time.Duration]{Set: true, Value: 100 * time.Millisecond}})
	q := codecTestMust(unpreparedQuery(s, statement, options.Values()))
	t.Cleanup(q.Close)
	return q, peers[0]
}

type eachRetryAfterDelay struct{}

func (eachRetryAfterDelay) Decide(retry.Attempt) retry.Decision {
	return retry.Decision{Action: retry.Same, Delay: time.Minute}
}

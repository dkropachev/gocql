// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/binding"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/observe"
	"github.com/scylladb/gocql/v2/retry"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

// redactionSecret is a bound value that must never reach an error message
// or a log record.
const redactionSecret = "s3cr3t-P@ssw0rd-1188"

// panickingObserver panics with every event it gets, so that the session
// logs each one, the request's error included.
type panickingObserver struct{}

func (panickingObserver) StartRequest(ctx context.Context, _ observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
	return ctx, func(e observe.RequestEvent) { panic(e) }
}
func (panickingObserver) StartAttempt(context.Context, routing2.AttemptStart) func(routing2.AttemptEvent) {
	return func(e routing2.AttemptEvent) { panic(e) }
}

// errorText renders err every way a caller might print it.
func errorText(err error) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%v\n%+v\n%#v\n%s\n", err, err, err, err)
	if failure, ok := errors.AsType[*cqlerr.ExecutionError](err); ok {
		for _, a := range failure.Attempts {
			fmt.Fprintf(&b, "%v\n%+v\n", a, a.Err)
		}
	}
	return b.String()
}

// TestErrorsDoNotLeakBoundValues binds a secret-looking value and checks that
// it never appears in the error, in any attempt's error, or in a log record,
// whatever the request fails with. Each failure names the host and attempt.
func TestErrorsDoNotLeakBoundValues(t *testing.T) {
	type server func(t *testing.T, attempt int, stream int, peer net.Conn)
	answer := func(build func(t *testing.T, stream int) []byte) server {
		return func(t *testing.T, _ int, stream int, peer net.Conn) { _, _ = peer.Write(build(t, stream)) }
	}
	for _, tc := range []struct {
		name    string
		timeout time.Duration
		// attempts is how many attempts the error lists; zero for a
		// failure after the response arrived.
		attempts int
		serve    server
	}{
		{"Invalid", 0, 1, answer(func(t *testing.T, stream int) []byte {
			return serverError(t, stream, cqlerr.CodeInvalid, nil)
		})},
		{"UnavailableThenWriteTimeout", 0, 2, func(t *testing.T, attempt int, stream int, peer net.Conn) {
			if attempt == 0 {
				_, _ = peer.Write(serverError(t, stream, cqlerr.CodeUnavailable, unavailableBody()))
				return
			}
			_, _ = peer.Write(serverError(t, stream, cqlerr.CodeWriteTimeout, writeTimeoutBody(1, "SIMPLE")))
		}},
		{"ConnectionClosed", 0, 2, func(_ *testing.T, _ int, _ int, peer net.Conn) { peer.Close() }},
		{"NoResponse", 100 * time.Millisecond, 1, func(*testing.T, int, int, net.Conn) {}},
		{"UndecodableRows", 0, 0, answer(func(t *testing.T, stream int) []byte {
			// The rows say the blob column is an int, which the
			// statement's blob decoder rejects.
			body := runtimeRowsBody(nil, "abc")
			body[len(body)-12] = byte(cql.TypeInt) // low byte of the column type
			return typedTestResponse(t, 4, stream, body)
		})},
	} {
		for _, general := range []bool{false, true} {
			// statementBytes 0 leaves the statement out of errors; a
			// positive limit adds its CQL text, never the bound value.
			for _, statementBytes := range []int{0, 1000} {
				t.Run(fmt.Sprintf("%s/general=%t/statement=%d", tc.name, general, statementBytes), func(t *testing.T) {
					logger, logs := logging.NewCapture()
					s, conns, peers := runtimeTestSession(t, 2)
					InternalRuntimeFor(s).config.ErrorStatementBytes = statementBytes
					implCluster.SessionData(s).InternalLogger = logger
					routes := &runtimePolicy{}
					for _, c := range conns {
						c.InternalLogger = logger
						routes.targets = append(routes.targets, metadata.Target{HostID: implEndpoint.InternalHostInfoHostUUID(c.InternalHost)})
					}
					for i := range conns {
						go func() {
							_, stream, body, err := readRequest(peers[i])
							if err != nil {
								return
							}
							if !bytes.Contains(body, []byte(redactionSecret)) {
								t.Errorf("host %d: the request does not carry the bound value", i)
							}
							tc.serve(t, i, stream, peers[i])
						}()
					}
					profile := settings.InternalDefaultProfile()
					profile.Routing = routes
					profile.Retry = retry.Safe{}
					if general {
						profile.Speculation = neverSpeculate{}
					}
					bound, err := BindUnpreparedOwned(binding.Statement[string, cql.Bytes]{
						Kind:   cql.OrdinaryStatement,
						CQL:    "SELECT value FROM ks.tbl WHERE k = ?",
						Params: cql.OneParam(cql.StringCodec),
						Rows:   cql.OneColumn(cql.BlobCodec),
					}, cql.NewColumns(cql.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "k", Type: cql.TextType}), redactionSecret)
					if err != nil {
						t.Fatal(err)
					}
					defer bound.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					opts := Options{Profile: new(profile), Idempotent: true, Observer: panickingObserver{}}
					if tc.timeout > 0 {
						opts.Timeout = cql.Some(tc.timeout)
					}
					page, err := RunPage(ctx, s, bound, cql.StartPage(), 0, testOptions(opts))
					if err == nil {
						var row cql.Bytes
						for page.NextInto(&row) {
						}
						err = page.Err()
						page.Close()
					}
					if err == nil {
						t.Fatal("request succeeded")
					}
					text := errorText(err)
					if strings.Contains(text, redactionSecret) {
						t.Errorf("error leaks the bound value: %s", text)
					}
					if logged := logs.String(); !strings.Contains(logged, "observer panicked") {
						t.Errorf("nothing logged the failure: %q", logged)
					} else if strings.Contains(logged, redactionSecret) || logs.Contains(redactionSecret) {
						t.Errorf("a log record leaks the bound value: %s", logged)
					}
					if tc.attempts == 0 {
						return // a decoding failure, reported by the page
					}
					// The message names every attempt and where it ran.
					failure, ok := errors.AsType[*cqlerr.ExecutionError](err)
					if !ok || len(failure.Attempts) != tc.attempts {
						t.Fatalf("want an ExecutionError of %d attempts: %v", tc.attempts, err)
					}
					// The statement is named only when the session asks for it.
					const named = `statement "SELECT value FROM ks.tbl WHERE k = ?"`
					if statementBytes == 0 && (failure.Statement != "" || strings.Contains(err.Error(), "statement")) {
						t.Errorf("statement reported while off: %q in %v", failure.Statement, err)
					}
					if statementBytes > 0 && (failure.Statement != "SELECT value FROM ks.tbl WHERE k = ?" || failure.BatchSize != 0 || !strings.Contains(err.Error(), named)) {
						t.Errorf("message %q, Statement %q: want %s", err, failure.Statement, named)
					}
					for _, a := range failure.Attempts {
						target := a.Address
						if target == "" {
							target = a.HostID.String()
						}
						if !strings.Contains(err.Error(), fmt.Sprintf("#%d %s", a.Number, target)) {
							t.Errorf("message %q does not name attempt #%d on %s", err, a.Number, target)
						}
					}
				})
			}
		}
	}

	// A value the codec rejects is reported without the value.
	t.Run("BindRejected", func(t *testing.T) {
		_, err := BindUnpreparedOwned(binding.Statement[string, cql.Bytes]{
			Kind:   cql.OrdinaryStatement,
			CQL:    "SELECT value FROM ks.tbl WHERE k = ?",
			Params: cql.OneParam(cql.StringCodec),
			Rows:   cql.OneColumn(cql.BlobCodec),
		}, cql.NewColumns(cql.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "k", Type: cql.ASCIIType}), redactionSecret+"\xff")
		if err == nil {
			t.Fatal("invalid ASCII bound")
		}
		if text := errorText(err); strings.Contains(text, redactionSecret) {
			t.Errorf("error leaks the bound value: %s", text)
		}
	})
}

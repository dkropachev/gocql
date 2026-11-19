// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
)

// sentinels maps every exported sentinel to the message it has always had.
// A changed message breaks callers that match on text, so it is pinned.
var sentinels = map[string]struct {
	err  error
	text string
}{
	"ErrBusy":                         {ErrBusy, "gocql: query is busy"},
	"ErrClosed":                       {ErrClosed, "gocql: closed"},
	"ErrConditionalExec":              {ErrConditionalExec, "gocql: conditional statements return rows: use One, Each, All or Run, or RunBatch for a batch"},
	"ErrConnectionClosed":             {ErrConnectionClosed, "gocql: connection closed waiting for response"},
	"ErrFrameTooBig":                  {ErrFrameTooBig, "frame length is bigger than the maximum allowed"},
	"ErrKeyspaceDoesNotExist":         {ErrKeyspaceDoesNotExist, "gocql: keyspace does not exist"},
	"ErrNoConnections":                {ErrNoConnections, "gocql: no hosts available in the pool"},
	"ErrNoConnectionsStarted":         {ErrNoConnectionsStarted, "gocql: no connections were made when creating the session"},
	"ErrNoControl":                    {ErrNoControl, "gocql: no control connection available"},
	"ErrNoHosts":                      {ErrNoHosts, "gocql: no hosts provided"},
	"ErrNoKeyspace":                   {ErrNoKeyspace, "gocql: no keyspace provided"},
	"ErrNoRows":                       {ErrNoRows, "gocql: no rows"},
	"ErrNoStreams":                    {ErrNoStreams, "gocql: no streams available on connection"},
	"ErrNoTable":                      {ErrNoTable, "gocql: no table name provided"},
	"ErrNotFound":                     {ErrNotFound, "gocql: not found"},
	"ErrPagingComplete":               {ErrPagingComplete, "gocql: completed page cannot be resumed"},
	"ErrPagingNoProgress":             {ErrPagingNoProgress, "gocql: paging made no progress"},
	"ErrPagingStateLimit":             {ErrPagingStateLimit, "gocql: paging state history limit exceeded"},
	"ErrReadHeaderTimeout":            {ErrReadHeaderTimeout, "unable to read frame header"},
	"ErrScyllaSourcePortAlreadyInUse": {ErrScyllaSourcePortAlreadyInUse, "scylla: source port is already in use"},
	"ErrSessionClosed":                {ErrSessionClosed, "gocql: session has been closed"},
	"ErrSessionNotReady":              {ErrSessionNotReady, "gocql: session is not ready yet"},
	"ErrStaleRows":                    {ErrStaleRows, "gocql: rows belong to an earlier execution of the query"},
	"ErrTimeoutNoResponse":            {ErrTimeoutNoResponse, "gocql: no response received from cassandra within timeout period"},
	"ErrUseStmt":                      {ErrUseStmt, "gocql: USE statements are not supported: set the session keyspace in the cluster config, or qualify table names"},
}

// errorTypes holds one value of every exported error type.
func errorTypes() map[string]error {
	return map[string]error{
		"AlreadyExistsError":      &AlreadyExistsError{Keyspace: "ks", Table: "t"},
		"CASWriteUnknownError":    &CASWriteUnknownError{Received: 1, BlockFor: 2},
		"CDCWriteFailureError":    &CDCWriteFailureError{},
		"ConnectError":            &ConnectError{Stage: "dial"},
		"ExecutionError":          newExecutionError(ErrTimeoutNoResponse, cql.Result{}, MaybeExecuted, nil),
		"FunctionFailureError":    &FunctionFailureError{Keyspace: "ks", Function: "f"},
		"NoHostsError":            &NoHostsError{Operation: "schema agreement"},
		"OtherServerError":        NewOtherServerError(CodeOverloaded, "overloaded"),
		"PartialRefreshError":     &PartialRefreshError{Topology: ErrNoControl},
		"QueryError":              newQueryError(queryErrorState{InternalErr: ErrConnectionClosed, InternalPotentiallyExecuted: true}),
		"RateLimitReachedError":   &RateLimitReachedError{OpType: OpTypeWrite},
		"ReadFailureError":        &ReadFailureError{Received: 1, BlockFor: 2},
		"ReadTimeoutError":        &ReadTimeoutError{Received: 1, BlockFor: 2},
		"SchemaChangedError":      &SchemaChangedError{Statement: "SELECT"},
		"ServerMismatchError":     &ServerMismatchError{Want: "ScyllaDB", Got: "Cassandra"},
		"UnavailableError":        &UnavailableError{Required: 2, Alive: 1},
		"UnpreparedError":         &UnpreparedError{StatementID: []byte{1}},
		"UnsupportedSettingError": &UnsupportedSettingError{Setting: "s", Reason: "r"},
		"WriteFailureError":       &WriteFailureError{WriteType: "SIMPLE"},
		"WriteTimeoutError":       &WriteTimeoutError{WriteType: "SIMPLE"},
	}
}

// apiNames returns the names testdata/api.txt lists for re, so that a new
// sentinel or error type cannot skip these tests.
func apiNames(t *testing.T, re *regexp.Regexp) map[string]bool {
	t.Helper()
	api, err := os.ReadFile("../testdata/api.txt")
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, m := range re.FindAllSubmatch(api, -1) {
		names[string(m[1])] = true
	}
	return names
}

func TestSentinels(t *testing.T) {
	t.Parallel()
	listed := apiNames(t, regexp.MustCompile(`(?m)^\tvar cqlerr\.(Err\w+) error$`))
	for name := range listed {
		if _, ok := sentinels[name]; !ok {
			t.Errorf("sentinel %s is not covered", name)
		}
	}
	texts := map[string]string{}
	for name, s := range sentinels {
		if !listed[name] {
			t.Errorf("%s is not an exported sentinel", name)
		}
		if got := s.err.Error(); got != s.text {
			t.Errorf("%s = %q, want %q", name, got, s.text)
		}
		if other, ok := texts[s.text]; ok {
			t.Errorf("%s and %s share the message %q", name, other, s.text)
		}
		texts[s.text] = name
		// Every sentinel is classified, and is the same value however it is
		// reached.
		if KindOf(s.err) == KindUnknown {
			t.Errorf("KindOf(%s) is unknown", name)
		}
		if !errors.Is(fmt.Errorf("context: %w", s.err), s.err) {
			t.Errorf("%s lost through wrapping", name)
		}
	}
}

// TestErrorTypesClassified checks that every exported error type has a Kind,
// except the wrappers whose kind is their cause's.
func TestErrorTypesClassified(t *testing.T) {
	t.Parallel()
	listed := apiNames(t, regexp.MustCompile(`(?m)^\ttype cqlerr\.(\w+Error) struct$`))
	delete(listed, "HostError") // one host's failure inside a ConnectError, not an error
	types := errorTypes()
	for name := range listed {
		if _, ok := types[name]; !ok {
			t.Errorf("error type %s is not covered", name)
		}
	}
	// Wrappers: their Kind is that of the error they carry.
	wrappers := map[string]bool{"ExecutionError": true, "QueryError": true, "PartialRefreshError": true}
	for name, err := range types {
		if !listed[name] {
			t.Errorf("%s is not an exported error type", name)
		}
		got := KindOf(err)
		if got == KindUnknown {
			t.Errorf("KindOf(%s) is unknown", name)
		}
		if !wrappers[name] {
			if _, ok := err.(kinded); !ok {
				t.Errorf("%s has no Kind method", name)
			}
		}
	}
}

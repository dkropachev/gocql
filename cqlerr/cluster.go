// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import "errors"

// ErrNoConnectionsStarted reports that a session could not open a connection
// to any host while connecting. ConnectError matches it.
var ErrNoConnectionsStarted = errors.New("gocql: no connections were made when creating the session")

// ErrBusy indicates that a Worker or StreamWorker still owns an active
// execution, transport write, or callback. Wait joins that execution before
// the worker is reused.
var ErrBusy = errors.New("gocql: worker is busy")

// ServerMismatchError reports that Connect reached a database other than
// the one its dialect names.
//
// The driver returns it filled in; its zero value is an error without detail.
type ServerMismatchError struct {
	// Want names the database the dialect expects, and Got the one the
	// cluster runs.
	Want, Got string
}

// Error names the expected and the actual database.
func (e *ServerMismatchError) Error() string {
	return "gocql: the session's dialect expects " + e.Want + " but the cluster is " + e.Got
}

// Kind returns KindInvalid.
func (e *ServerMismatchError) Kind() Kind { return KindInvalid }

// ErrConditionalExec reports an Exec or ExecBatch call for a statement or
// batch known to be conditional. Nothing was sent.
var ErrConditionalExec = errors.New("gocql: conditional statements return rows: use Apply, or One, Each, All or Run to read them; for a batch, ApplyBatch or RunConditionalBatch")

// ErrStaleRows is reported by a Rows handle whose query has started a later
// execution. Its storage now belongs to that execution.
var ErrStaleRows = errors.New("gocql: rows belong to an earlier execution of the query")

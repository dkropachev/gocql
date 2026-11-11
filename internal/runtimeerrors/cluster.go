// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package runtimeerrors

import "errors"

var (
	ErrNoHosts              = errors.New("gocql: no hosts provided")
	ErrNoConnectionsStarted = errors.New("gocql: no connections were made when creating the session")
	ErrHostQueryFailed      = errors.New("gocql: unable to populate Hosts")
)

// ErrBusy indicates that a query still owns an active execution, transport
// write, or callback. Wait joins that execution before the query is reused.
var ErrBusy = errors.New("gocql: query is busy")

// ErrConditionalExec is returned, before anything is sent, by an Exec call
// or ExecBatch for a statement or batch the driver knows to be conditional: its
// result is a row reporting whether it applied, which Exec cannot return.
var ErrConditionalExec = errors.New("gocql: conditional statements return rows: use One, Each, All or Run, or RunBatch for a batch")

// ErrStaleRows is reported by a Rows handle whose query has started a later
// execution. Its storage now belongs to that execution.
var ErrStaleRows = errors.New("gocql: rows belong to an earlier execution of the query")

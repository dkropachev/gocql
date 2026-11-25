// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/exec"
	implSession "github.com/scylladb/gocql/v2/internal/session"
)

// PreparedStream is an immutable statement prepared for caller-storage decoding.
// Share it between goroutines and create one StreamWorker per goroutine. It needs no Close.
// PrepareStream validates the streaming decoder against known result metadata;
// execution revalidates it whenever the actual result metadata changes.
//
// Session.PrepareStream makes a PreparedStream. A zero PreparedStream has no
// columns, and its Worker returns a cql.ValidationError.
type PreparedStream[P, R any] implSession.PreparedStream[P, R]

// Worker creates a reusable stream execution with fixed options. Close the
// worker when its goroutine finishes, and close each execution cursor before
// reusing it. At
// most one exec.StreamOptions may be passed; omitting it buffers one page
// with the session defaults. Options with a Timestamp are rejected with a
// cql.ValidationError, since every write of the worker would carry it.
func (p *PreparedStream[P, R]) Worker(options ...exec.StreamOptions) (*StreamWorker[P, R], error) {
	var o exec.StreamOptions
	switch len(options) {
	case 0:
	case 1:
		o = options[0]
	default:
		return nil, tooManyWorkerOptions(true)
	}
	if o.Options.Timestamp.Set {
		return nil, fixedWorkerTimestamp()
	}
	q, err := (*implSession.PreparedStream[P, R])(p).Query(internalStreamOptions(o))
	return (*StreamWorker[P, R])(q), err
}

// BindMarkers returns the immutable bind-marker metadata, one column per
// marker, as the server reported it when preparing.
func (p *PreparedStream[P, R]) BindMarkers() cql.Columns {
	return (*implSession.PreparedStream[P, R])(p).BindMarkers()
}

// Columns returns the prepared statement's immutable result metadata.
func (p *PreparedStream[P, R]) Columns() cql.Columns {
	return (*implSession.PreparedStream[P, R])(p).Columns()
}

// Info returns preparation information.
func (p *PreparedStream[P, R]) Info() PreparedInfo {
	return PreparedInfo((*implSession.PreparedStream[P, R])(p).Info())
}

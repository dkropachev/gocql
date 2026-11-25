// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/exec"
	implSession "github.com/scylladb/gocql/v2/internal/session"
)

// PreparedStream is an immutable statement prepared for caller-storage decoding.
// Share it between goroutines and create one Query per worker. It needs no Close.
// PrepareStream validates the streaming decoder against known result metadata;
// execution revalidates it whenever the actual result metadata changes.
//
// Session.PrepareStream makes a PreparedStream. A zero PreparedStream has no
// columns, and its Query returns a cql.ValidationError.
type PreparedStream[P, R any] implSession.PreparedStream[P, R]

// Query creates a reusable stream query with fixed options. Close the query when
// its worker finishes, and close each execution cursor before reusing it. At
// most one exec.StreamOptions may be passed; omitting it buffers one page
// with the session defaults. Options with a Timestamp are rejected with a
// cql.ValidationError, since every write of the worker would carry it.
func (p *PreparedStream[P, R]) Query(options ...exec.StreamOptions) (*StreamQuery[P, R], error) {
	var o exec.StreamOptions
	switch len(options) {
	case 0:
	case 1:
		o = options[0]
	default:
		return nil, tooManyQueryOptions(true)
	}
	if o.Options.Timestamp.Set {
		return nil, fixedWorkerTimestamp()
	}
	q, err := (*implSession.PreparedStream[P, R])(p).Query(internalStreamOptions(o))
	return (*StreamQuery[P, R])(q), err
}

// Parameters returns the immutable bind-marker metadata.
func (p *PreparedStream[P, R]) Parameters() cql.Columns {
	return (*implSession.PreparedStream[P, R])(p).Parameters()
}

// Columns returns the prepared statement's immutable result metadata.
func (p *PreparedStream[P, R]) Columns() cql.Columns {
	return (*implSession.PreparedStream[P, R])(p).Columns()
}

// Info returns preparation information.
func (p *PreparedStream[P, R]) Info() PreparedInfo {
	return PreparedInfo((*implSession.PreparedStream[P, R])(p).Info())
}

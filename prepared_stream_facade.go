// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/execopt"
	implSession "github.com/scylladb/gocql/v2/internal/session"
	implTransport "github.com/scylladb/gocql/v2/internal/transport"
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
// reusing it. At most one execopt.StreamOptions may be passed; omitting it
// buffers one page. Execution overrides are the defaults of a view made by
// With, or else the session defaults. Overrides with a Timestamp are
// rejected with a cql.ValidationError, since every write of the worker would
// carry it.
func (p *PreparedStream[P, R]) Worker(options ...execopt.StreamOptions) (*StreamWorker[P, R], error) {
	var o execopt.StreamOptions
	switch len(options) {
	case 0:
	case 1:
		o = options[0]
	default:
		return nil, tooManyStreamWorkerOptions()
	}
	internal := (*implSession.PreparedStream[P, R])(p)
	values := internal.Defaults().Values()
	if values.Timestamp.Set {
		return nil, fixedWorkerTimestamp()
	}
	q, err := internal.Query(implSession.StreamOptions{Options: values, Limits: implTransport.StreamLimits(o.Limits), Incremental: o.Incremental})
	return (*StreamWorker[P, R])(q), err
}

// With returns a view of p whose workers use defaults instead of the session
// defaults, as Prepared.With does. The view shares p's preparation; making
// one costs one small allocation, and neither needs closing. With on a view
// replaces its defaults.
func (p *PreparedStream[P, R]) With(defaults execopt.CompiledOptions) *PreparedStream[P, R] {
	return (*PreparedStream[P, R])((*implSession.PreparedStream[P, R])(p).With(implSession.CompiledOptions(defaults)))
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

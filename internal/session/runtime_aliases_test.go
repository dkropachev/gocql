// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session_test

import (
	. "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/session"
	"github.com/scylladb/gocql/v2/internal/transport"
)

var newInputOwner = session.InternalNewInputOwner
var conditionalCQL = binding.InternalConditionalCQL
var newRowReader = codec.NewRowReader

type responsePage = transport.InternalResponsePage
type responseData = model.ResponseData
type pageFuture = session.InternalPageFuture

func newPage[R any](response *responsePage, decoder cql.RowDecoder[R], release func()) (*Page[R], error) {
	page, err := session.InternalNewPage(response, decoder, release)
	return (*Page[R])(page), err
}

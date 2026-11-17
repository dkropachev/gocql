// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"github.com/scylladb/gocql/v2/internal/wire"
)

// Runtime constructors reuse the caller's cursor and final row-reader storage.
func rowReaderFromCursor(columns Columns, cursor *wire.Cursor, lease *wire.Lease) RowReader {
	return RowReader{columns: columns, cursor: cursor, lease: wire.RefOf(lease)}
}
func setParamOwnership(w *ParamWriter, owned bool) { w.ownBorrowed = owned }
func beginStreamRow(r *StreamRowReader)            { r.index = 0 }

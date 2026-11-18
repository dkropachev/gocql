// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/link"
)

// columnsRep mirrors cql.Columns; TestColumnsMirror checks the layout.
type columnsRep struct{ columns []cql.ColumnSpec }

// sameColumns reports whether a and b are the same specifications, not only
// equal ones: the same backing, which cql.Columns never modifies.
func sameColumns(a, b cql.Columns) bool {
	x, y := link.Mirror[columnsRep](&a).columns, link.Mirror[columnsRep](&b).columns
	return len(x) == len(y) && (len(x) == 0 || &x[0] == &y[0])
}

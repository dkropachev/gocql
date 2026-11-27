// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import "strings"

// Name returns the internal name of the CQL identifier cqlIdent, the name
// system_schema stores and Snapshot lookups take: a quoted identifier
// without its quotes, with each doubled quote inside it made single, and an
// unquoted identifier folded to lower case, as the server folds it. So
// Name(`"MyTable"`) is "MyTable" and Name("MyTable") is "mytable". It does
// not check that cqlIdent is a valid identifier.
func Name(cqlIdent string) string {
	if len(cqlIdent) >= 2 && cqlIdent[0] == '"' && cqlIdent[len(cqlIdent)-1] == '"' {
		return strings.ReplaceAll(cqlIdent[1:len(cqlIdent)-1], `""`, `"`)
	}
	return strings.ToLower(cqlIdent)
}

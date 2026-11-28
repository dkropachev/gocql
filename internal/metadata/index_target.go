// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"encoding/json"
	"strings"

	"github.com/scylladb/gocql/v2/metadata"
)

// InternalParseIndexTarget parses the "target" option of a secondary index
// into the internal name of its column and what of the column it indexes. A
// plain column name is IndexTargetColumn, keys(c), values(c), entries(c) and
// full(c) the collection kinds, each with c unquoted. A ScyllaDB local
// index's JSON target, {"pk": [...], "ck": [...]}, targets its one "ck"
// entry, parsed as above. Any other target, a local index on several
// columns among them, is ("", IndexTargetUnknown).
func InternalParseIndexTarget(target string) (string, metadata.IndexTargetKind) {
	if strings.HasPrefix(target, "{") {
		var local struct {
			ClusteringKeys []string `json:"ck"`
		}
		if json.Unmarshal([]byte(target), &local) != nil || len(local.ClusteringKeys) != 1 {
			return "", metadata.IndexTargetUnknown
		}
		target = local.ClusteringKeys[0]
	}
	kind := metadata.IndexTargetColumn
	if open := strings.IndexByte(target, '('); open > 0 && !strings.HasPrefix(target, `"`) && strings.HasSuffix(target, ")") {
		switch strings.ToLower(target[:open]) {
		case "keys":
			kind = metadata.IndexTargetKeys
		case "values":
			kind = metadata.IndexTargetValues
		case "entries":
			kind = metadata.IndexTargetEntries
		case "full":
			kind = metadata.IndexTargetFull
		default:
			return "", metadata.IndexTargetUnknown
		}
		target = target[open+1 : len(target)-1]
	}
	column, ok := unquoteIndexColumn(target)
	if !ok {
		return "", metadata.IndexTargetUnknown
	}
	return column, kind
}

// unquoteIndexColumn returns the internal name of a column as an index target
// writes it: a quoted name with its doubled quotes undone, or an unquoted one
// as it is.
func unquoteIndexColumn(s string) (string, bool) {
	if s == "" {
		return "", false
	}
	if !strings.HasPrefix(s, `"`) {
		return s, !strings.ContainsAny(s, `"(), `)
	}
	if len(s) < 2 || !strings.HasSuffix(s, `"`) {
		return "", false
	}
	inner := s[1 : len(s)-1]
	if strings.Count(inner, `"`) != 2*strings.Count(inner, `""`) {
		return "", false
	}
	return strings.ReplaceAll(inner, `""`, `"`), inner != ""
}

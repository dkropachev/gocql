// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

import (
	cql "github.com/scylladb/gocql/v2/cql"
)

func NativeKind(n NativeType) cql.Type { return n.Type() }

func NativeCustom(n NativeType) string { return n.Custom() }

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"github.com/scylladb/gocql/v2/cql"
)

type Result = cql.Result

// CQLTraceRef names a server-side CQL trace session and the attempt it
// records; Result.CQLTraces lists them and package cqltrace reads them.
type CQLTraceRef = cql.CQLTraceRef

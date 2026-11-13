// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import model "github.com/scylladb/gocql/v2/internal/model"

type Result = model.Result

// CQLTraceRef names a server-side CQL trace session and the attempt it
// records; Result.CQLTraces lists them and package cqltrace reads them.
type CQLTraceRef = model.CQLTraceRef

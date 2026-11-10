// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import "github.com/scylladb/gocql/v2/internal/execution"

type Query[P, R any] = execution.Query[P, R]

type StreamQuery[P, R any] = execution.StreamQuery[P, R]

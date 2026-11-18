// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import "github.com/scylladb/gocql/v2/internal/cqlperf"

func perfCompile()               { cqlperf.Compile() }
func perfNameMap()               { cqlperf.NameMap() }
func perfCheckType()             { cqlperf.CheckType() }
func perfDynamicIndex(cells int) { cqlperf.DynamicIndex(cells) }

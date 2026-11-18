//go:build gocql_perf

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import "github.com/scylladb/gocql/v2/internal/cqlperf"

func ResetPerfCounters()                                  { cqlperf.Reset() }
func ReadPerfCounters() (compiles, nameMaps int64)        { return cqlperf.Counters() }
func ReadPerfTypeChecks() int64                           { return cqlperf.TypeChecks() }
func ReadPerfDynamicIndexCounters() (builds, cells int64) { return cqlperf.DynamicIndexCounters() }

//go:build gocql_perf

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlperf

import "sync/atomic"

// Enabled reports whether this build counts work.
const Enabled = true

var compiles, nameMaps, typeChecks, dynamicBuilds, dynamicCells atomic.Int64

// Compile counts one codec plan compilation.
func Compile() { compiles.Add(1) }

// NameMap counts one column name mapping.
func NameMap() { nameMaps.Add(1) }

// CheckType counts one real built-in CheckType call.
func CheckType() { typeChecks.Add(1) }

// DynamicIndex counts one dynamic row index build over cells columns.
func DynamicIndex(cells int) { dynamicBuilds.Add(1); dynamicCells.Add(int64(cells)) }

// Reset zeroes every counter. It requires a quiescent workload.
func Reset() {
	compiles.Store(0)
	nameMaps.Store(0)
	typeChecks.Store(0)
	dynamicBuilds.Store(0)
	dynamicCells.Store(0)
}

// Counters reports compiler and name mapping calls.
func Counters() (compileCalls, nameMapCalls int64) { return compiles.Load(), nameMaps.Load() }

// TypeChecks reports real built-in CheckType calls, separately from constant
// metadata identity/kind guards on already compiled immutable plans.
func TypeChecks() int64 { return typeChecks.Load() }

// DynamicIndexCounters reports dynamic row index builds and the cells they covered.
func DynamicIndexCounters() (builds, cells int64) { return dynamicBuilds.Load(), dynamicCells.Load() }

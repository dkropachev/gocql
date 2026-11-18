//go:build !gocql_perf

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlperf

// Enabled reports whether this build counts work.
const Enabled = false

func Compile()           {}
func NameMap()           {}
func CheckType()         {}
func DynamicIndex(_ int) {}

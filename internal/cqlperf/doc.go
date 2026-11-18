// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package cqlperf counts codec work in measurement builds (-tags gocql_perf).
// In other builds every counter is a no-op and the read functions do not
// exist, so tests that read them carry the same build tag.
package cqlperf

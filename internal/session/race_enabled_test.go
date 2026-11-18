//go:build race

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

// raceEnabled reports a -race build, whose instrumentation, together with
// coverage, changes what escapes to the heap; allocation counts are not
// compared there.
const raceEnabled = true

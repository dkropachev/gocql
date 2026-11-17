// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

type PayloadEntry struct {
	Key   string
	Value Nullable[Bytes]
}

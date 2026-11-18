// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

// PayloadEntry is one entry of a custom payload: a name and an optional value
// the server or an extension attaches to a request or a response.
//
// Its zero value has an empty name and no value.
type PayloadEntry struct {
	Key   string
	Value Nullable[Bytes]
}

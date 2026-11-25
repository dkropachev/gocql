// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import "github.com/scylladb/gocql/v2/cql"

// ownedBytesForTest copies b, panicking once its view has expired.
func ownedBytesForTest(b cql.Bytes) []byte {
	v, err := b.Copy()
	if err != nil {
		panic(err)
	}
	return v
}

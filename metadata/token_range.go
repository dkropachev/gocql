// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

// TokenRange is a range of the token ring, (Start, End], with its replicas
// in placement order.
//
// The driver fills it; its zero value is empty.
type TokenRange struct {
	// Replicas lists the range's replicas, in placement order.
	Replicas []Replica
	// Start and End bound the range: it holds the tokens greater than Start
	// and at most End.
	Start, End int64
}

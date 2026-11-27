// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package vetcopy copies the values that must not be copied, for
// TestNoCopyVet: go vet must report each copy.
package vetcopy

import gocql "github.com/scylladb/gocql/v2"

func copies(b *gocql.Batch, w *gocql.Worker[int, int], s *gocql.StreamWorker[int, int]) (gocql.Batch, gocql.Worker[int, int], gocql.StreamWorker[int, int]) {
	return *b, *w, *s
}

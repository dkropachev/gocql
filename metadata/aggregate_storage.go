// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

// aggregateStorage is what the schema loader keeps on an Aggregate until its
// functions are resolved. The loader reaches it through the mirror in
// internal/metadata.
type aggregateStorage struct {
	InternalStateFunc string
	InternalFinalFunc string
}

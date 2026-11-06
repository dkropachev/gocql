//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package integration

type concurrentMemory struct {
	IdleCacheBytes, IdleCacheObjects, ParkedCacheBytes, ParkedCacheObjects, PostCloseCacheBytes, PostCloseCacheObjects                                  int
	API                                                                                                                                                 string
	Workers, Rows, Cell, Page                                                                                                                           int
	RuntimeHeapBeforeConnect, FixedSessionHeap, IdleHeapWithApplication, AllParkedPostGCHeap, AllParkedStackBytes, PeakSampledHeap, PostClosePostGCHeap uint64
	ApplicationStorageBytes, InclusiveDriverAndHarnessBytes, IncrementalOverWarmIdleBytes                                                               int64
	WorkerAllocatedBytes, WorkerAllocations                                                                                                             uint64
}

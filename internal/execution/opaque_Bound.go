// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import "github.com/scylladb/gocql/v2/internal/codec"

type BoundStorage[R any] struct {
	InternalRows    codec.RowDecoder[R]
	plans           *InternalResultPlanCache[R]
	rows            *rowsHolder[R] // a reusable query's iterator storage
	InternalRequest InternalExecutionRequest
}

func BoundData[R any](v *Bound[R]) *BoundStorage[R] { return &v.privateStorage }

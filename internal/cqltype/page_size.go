// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

import "math"

// Unpaged is the page size that requests a result without paging: the request
// carries no page size, so the server returns the whole result in one
// response, within its own result size limits.
const Unpaged int32 = math.MaxInt32

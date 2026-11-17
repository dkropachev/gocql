// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import "math"

// Unpaged is the page size that requests a result without paging, as in
// Options{PageSize: cql.Some(cql.Unpaged)} or Profile.PageSize. The request
// carries no page size, so the server returns the whole result in one
// response, within its own result size limits. Use it for results known to be
// small; a large unpaged result costs server and client memory at once.
const Unpaged int32 = math.MaxInt32

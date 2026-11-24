// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import "errors"

var (
	// ErrReadHeaderTimeout reports that a frame header did not arrive in time.
	ErrReadHeaderTimeout = errors.New("gocql: unable to read frame header")
	// ErrFrameTooBig reports a frame longer than the configured maximum.
	ErrFrameTooBig = errors.New("gocql: frame length is bigger than the maximum allowed")
	// ErrScyllaSourcePortAlreadyInUse reports that the source port a
	// shard-aware connection needs is taken.
	ErrScyllaSourcePortAlreadyInUse = errors.New("gocql: shard-aware source port is already in use")
)

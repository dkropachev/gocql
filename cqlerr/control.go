// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import "errors"

// ErrNoControl reports that no control connection was available, for example
// for a metadata refresh.
var ErrNoControl = errors.New("gocql: no control connection available")

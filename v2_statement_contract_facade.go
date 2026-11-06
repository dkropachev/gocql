// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import binding "github.com/scylladb/gocql/v2/internal/binding"

type Statement[P any, R any] = binding.Statement[P, R]

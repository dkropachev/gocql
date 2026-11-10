// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import binding "github.com/scylladb/gocql/v2/internal/binding"

// validateStatement fills the statement's empty defaults and validates it.
func validateStatement[P any, R any](a0 *binding.Statement[P, R]) error {
	binding.InternalStatementDefaults(a0)
	return binding.InternalValidateStatement[P, R](*a0)
}

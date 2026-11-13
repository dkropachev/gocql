// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"errors"

	"github.com/scylladb/gocql/v2/internal/settings"
)

// testOptions freezes v, failing loudly on values a test meant to be valid.
func testOptions(v Options) CompiledOptions {
	o, err := NewCompiledOptions(nil, v)
	if err != nil {
		panic(err)
	}
	return o
}

// hasErrorType reports whether err's tree holds an error of type E.
func hasErrorType[E error](err error) bool {
	_, ok := errors.AsType[E](err)
	return ok
}

// generalPathProfile is the default profile with a speculation policy, which
// keeps executions on the general path: attempts run on goroutines of their
// own, so a blocked observer keeps an execution draining after its caller
// returned. On the direct path observers run on the caller.
func generalPathProfile() *settings.Profile {
	p := settings.InternalDefaultProfile()
	p.Speculation = neverSpeculate{}
	return &p
}

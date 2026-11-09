// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import "errors"

// testOptions freezes v, failing loudly on values a test meant to be valid.
func testOptions(v OptionValues) Options {
	o, err := NewOptions(nil, v)
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

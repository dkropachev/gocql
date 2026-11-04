// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

// testOptions freezes v, failing loudly on values a test meant to be valid.
func testOptions(v OptionValues) Options {
	o, err := NewOptions(nil, v)
	if err != nil {
		panic(err)
	}
	return o
}

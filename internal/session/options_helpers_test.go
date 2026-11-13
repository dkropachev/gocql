// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"errors"

	"github.com/scylladb/gocql/v2/internal/routing"
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

// goroutineRouting is the default routing policy under a type the direct
// path does not know, which keeps attempts on goroutines of their own.
type goroutineRouting struct{ routing.RoutingPolicy }

// generalPathProfile keeps executions on the general path: attempts run on
// goroutines of their own, so a blocked observer keeps an execution
// draining after its caller returned. On the direct path, and for the first
// attempt of a speculating execution, observers run on the caller.
func generalPathProfile() *settings.Profile {
	base := settings.DefaultConfig("localhost")
	p := *base.DefaultProfile
	p.Routing = goroutineRouting{base.DefaultProfile.Routing}
	return &p
}

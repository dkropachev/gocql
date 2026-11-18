// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import (
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/observe"
)

// ObserverHooks are the hooks an observe.Observer implements, found once
// when it is installed; a hook it does not implement is nil.
type ObserverHooks struct {
	Request observe.RequestObserver
	Attempt observe.AttemptObserver
}

// NewObserverHooks returns o's hooks, nil for a nil o, or an error when o
// implements no hook.
func NewObserverHooks(o observe.Observer) (*ObserverHooks, error) {
	if o == nil {
		return nil, nil
	}
	h := &ObserverHooks{}
	h.Request, _ = o.(observe.RequestObserver)
	h.Attempt, _ = o.(observe.AttemptObserver)
	if h.Request == nil && h.Attempt == nil {
		return nil, cqltype.TypeValidation("observer", "%T implements neither observe.RequestObserver nor observe.AttemptObserver", o)
	}
	return h, nil
}

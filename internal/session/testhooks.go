// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import "github.com/scylladb/gocql/v2/internal/cqltype"

// Test hooks: accessors for the storage of handles that tests/integration
// builds or inspects directly.

func BoundData[R any](v *Bound[R]) *BoundStorage[R] { return &v.privateStorage }

func OptionsData(v *Options) *OptionsStorage { return &v.privateStorage }

func PreparedData[P any, R any](v *Prepared[P, R]) *PreparedStorage[P, R] { return &v.privateStorage }

func PrepareOptionsData(v *PrepareOptions) *PrepareOptionsStorage { return &v.privateStorage }

// InternalHostPreparedEntry returns a copy of a host's prepared entry, for
// tests/integration to inspect.
func InternalHostPreparedEntry(state *InternalPreparedState, host cqltype.UUID) (InternalHostPrepared, bool) {
	entry := state.host(host)
	if entry == nil {
		return InternalHostPrepared{}, false
	}
	return *entry, true
}

// InternalSetHostPrepared replaces a host's prepared entry, so tests/integration
// can plant a stale or invalid one. The result metadata is left as it is.
func InternalSetHostPrepared(state *InternalPreparedState, host cqltype.UUID, entry InternalHostPrepared) {
	state.InternalMu.Lock()
	defer state.InternalMu.Unlock()
	state.publishHost(host, &entry, false, nil)
}

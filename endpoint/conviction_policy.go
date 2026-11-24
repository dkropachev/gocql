// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

// SimpleConvictionPolicy convicts a host whenever opening its connections
// leaves it with none, or the control connection fails to dial it, whatever
// the error. It ignores single-shard failures.
//
// The zero SimpleConvictionPolicy is ready to use.
type SimpleConvictionPolicy struct{}

// AddFailure returns true unless the event concerns a single shard.
func (SimpleConvictionPolicy) AddFailure(ev ConvictionEvent) bool {
	return !ev.ShardID.Set
}

// Reset does nothing: the policy keeps no state.
func (SimpleConvictionPolicy) Reset(ConvictionEvent) {}

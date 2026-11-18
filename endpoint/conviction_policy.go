// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

// SimpleConvictionPolicy convicts a host whenever opening its connections
// leaves it with none, or the control connection fails to dial it, whatever
// the error. It ignores single-shard failures.
//
// The zero SimpleConvictionPolicy is ready to use.
type SimpleConvictionPolicy struct{}

func (e *SimpleConvictionPolicy) AddFailure(ev ConvictionEvent) bool {
	return !ev.Shard.Set
}

func (e *SimpleConvictionPolicy) Reset(ConvictionEvent) {}

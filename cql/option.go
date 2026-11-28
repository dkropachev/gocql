// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

// Option is a value that may be absent: Set reports whether Value holds one.
// In a setting, such as an execopt.Options field, it tells an explicit zero
// apart from the inherited default: the zero Option inherits, Some(v)
// applies v. In a result, such as cqlerr.AttemptFailure.ShardID, an unset Option means
// the value is not known. Nullable is the type for a CQL NULL.
type Option[T any] struct {
	// Value is the value when Set is true.
	Value T
	// Set reports that Value holds a value.
	Set bool
}

// Some returns an Option set to v.
func Some[T any](v T) Option[T] { return Option[T]{Value: v, Set: true} }

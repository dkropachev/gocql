// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

// Option distinguishes an explicit zero value from an inherited setting.
// Its zero value inherits the setting; Set=true applies Value.
type Option[T any] struct {
	// Value is the setting's value when Set is true.
	Value T
	// Set reports that Value applies instead of the inherited setting.
	Set bool
}

// Some returns an Option set to v.
func Some[T any](v T) Option[T] { return Option[T]{Value: v, Set: true} }

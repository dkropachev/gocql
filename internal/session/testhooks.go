// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

// Test hooks: accessors for the storage of handles that tests/integration
// builds or inspects directly.

func BoundData[R any](v *Bound[R]) *BoundStorage[R] { return &v.privateStorage }

func OptionsData(v *Options) *OptionsStorage { return &v.privateStorage }

func PreparedData[P any, R any](v *Prepared[P, R]) *PreparedStorage[P, R] { return &v.privateStorage }

func PrepareOptionsData(v *PrepareOptions) *PrepareOptionsStorage { return &v.privateStorage }

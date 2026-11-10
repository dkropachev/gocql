// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import "github.com/scylladb/gocql/v2/internal/transport"

type PrepareOptionsStorage struct {
	InternalConnection *transport.Conn
}

func PrepareOptionsData(v *PrepareOptions) *PrepareOptionsStorage { return &v.privateStorage }

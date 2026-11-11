// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"sync"

	"github.com/scylladb/gocql/v2/internal/wire"
)

type PageLeaseStorage struct {
	owner *wire.Lease
	once  sync.Once
}

func PageLeaseData(v *PageLease) *PageLeaseStorage { return &v.privateStorage }

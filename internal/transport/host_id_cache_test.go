// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"testing"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
)

// TestConnHostIDCachesOnlyAKnownID checks that an empty host ID is not cached,
// so the ID filled in later is observed, and that the cached ID is returned
// afterwards without reading the host.
func TestConnHostIDCachesOnlyAKnownID(t *testing.T) {
	host := new(HostInfo)
	c := &Conn{InternalHost: host}
	if id := c.InternalHostID(); !id.IsEmpty() {
		t.Fatalf("got %v for a host without an ID", id)
	}
	want := UUID{15: 7}
	data := implEndpoint.HostInfoData(host)
	data.InternalMu.Lock()
	data.InternalHostId = want
	data.InternalMu.Unlock()
	if id := c.InternalHostID(); id != want {
		t.Fatalf("got %v, want %v", id, want)
	}
	data.InternalMu.Lock()
	data.InternalHostId = UUID{}
	data.InternalMu.Unlock()
	if id := c.InternalHostID(); id != want {
		t.Fatalf("cached ID not used: got %v", id)
	}
}

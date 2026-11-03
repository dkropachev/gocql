// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

import (
	"net"
	"testing"
)

// TestHostUUIDCachesOnlyAKnownID checks that an empty host ID is not cached,
// so the ID filled in later is observed, and that state reads see updates.
func TestHostUUIDCachesOnlyAKnownID(t *testing.T) {
	h := new(HostInfo)
	h.privateStorage.InternalConnectAddress = net.IPv4(127, 0, 0, 1)
	if id := InternalHostInfoHostUUID(h); !id.IsEmpty() {
		t.Fatal("empty host reported an ID", id)
	}
	from := new(HostInfo)
	from.privateStorage.InternalConnectAddress = h.privateStorage.InternalConnectAddress
	from.privateStorage.InternalHostId = UUID{15: 3}
	InternalHostInfoUpdate(h, from)
	if id := InternalHostInfoHostUUID(h); id != (UUID{15: 3}) {
		t.Fatal("filled-in host ID not observed", id)
	}
	InternalHostInfoSetState(h, NodeUp)
	if !h.IsUp() {
		t.Fatal("state update not observed")
	}
	InternalHostInfoSetState(h, NodeDown)
	if h.IsUp() {
		t.Fatal("state update not observed")
	}
}

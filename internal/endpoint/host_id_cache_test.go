// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

import (
	"net"
	"testing"
)

func TestHostInfo(t *testing.T) {
	newHost := func() *HostInfo {
		h := new(HostInfo)
		h.privateStorage.InternalConnectAddress = net.IPv4(127, 0, 0, 1)
		return h
	}

	// An empty host ID must not be cached, so the ID filled in later is observed.
	t.Run("InternalHostInfoHostUUID", func(t *testing.T) {
		h := newHost()
		if id := InternalHostInfoHostUUID(h); !id.IsEmpty() {
			t.Fatal("empty host reported an ID", id)
		}
		from := newHost()
		from.privateStorage.InternalHostId = UUID{15: 3}
		InternalHostInfoUpdate(h, from)
		if id := InternalHostInfoHostUUID(h); id != (UUID{15: 3}) {
			t.Fatal("filled-in host ID not observed", id)
		}
	})

	t.Run("InternalHostInfoSetState", func(t *testing.T) {
		h := newHost()
		InternalHostInfoSetState(h, NodeUp)
		if !h.IsUp() {
			t.Fatal("NodeUp not observed")
		}
		InternalHostInfoSetState(h, NodeDown)
		if h.IsUp() {
			t.Fatal("NodeDown not observed")
		}
	})
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

import (
	"fmt"
	"net/netip"
	"strconv"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
)

func SnapshotHost(h *HostInfo) (model.HostSnapshot, error) {
	if h == nil {
		return model.HostSnapshot{}, cqltype.TypeValidation("host", "nil host")
	}
	h.privateStorage.InternalMu.RLock()
	address, _ := InternalHostInfoConnectAddressLocked(h)
	addr, _ := netip.AddrFromSlice(address)
	out := model.HostSnapshot{ID: h.privateStorage.InternalHostId, Address: netip.AddrPortFrom(addr.Unmap(), uint16(h.privateStorage.InternalPort)), Datacenter: h.privateStorage.InternalDataCenter, Rack: h.privateStorage.InternalRack, Up: h.privateStorage.InternalState.Load() == NodeUp, Enabled: true}
	var tokenErr error
	for _, token := range h.privateStorage.InternalTokens {
		n, err := strconv.ParseInt(token, 10, 64)
		if err != nil {
			tokenErr = fmt.Errorf("host %s invalid token %q: %w", out.ID, token, err)
			out.Tokens = nil
			break
		}
		out.Tokens = append(out.Tokens, n)
	}
	out.Shards = uint32(h.privateStorage.InternalScyllaFeatures.ShardsCount())
	out.ShardingIgnoreMSB = uint8(h.privateStorage.InternalScyllaFeatures.privateStorage.InternalMsbIgnore)
	h.privateStorage.InternalMu.RUnlock()
	return out, tokenErr
}

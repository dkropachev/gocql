// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

import (
	"fmt"
	"net/netip"
	"strconv"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/metadata"
)

// SnapshotHost returns the snapshot of h. It fails, with the snapshot still
// filled, when a token of h does not parse as an int64.
func SnapshotHost(h *HostInfo) (metadata.HostSnapshot, error) {
	out, _, err := SnapshotHostTokens(h)
	return out, err
}

// SnapshotHostTokens returns the snapshot of h and its tokens, which are nil
// when one of them does not parse as an int64, as the error then reports.
func SnapshotHostTokens(h *HostInfo) (metadata.HostSnapshot, []int64, error) {
	if h == nil {
		return metadata.HostSnapshot{}, nil, cqltype.TypeValidation("host", "nil host")
	}
	h.privateStorage.InternalMu.RLock()
	address, _ := hostInfoConnectAddressLocked(h)
	addr, _ := netip.AddrFromSlice(address)
	out := metadata.HostSnapshot{HostID: h.privateStorage.InternalHostId, Address: netip.AddrPortFrom(addr.Unmap(), uint16(h.privateStorage.InternalPort)), Datacenter: h.privateStorage.InternalDataCenter, Rack: h.privateStorage.InternalRack, Up: h.privateStorage.InternalState.Load() == NodeUp, Enabled: true}
	var tokens []int64
	var tokenErr error
	for _, token := range h.privateStorage.InternalTokens {
		n, err := strconv.ParseInt(token, 10, 64)
		if err != nil {
			tokenErr = fmt.Errorf("host %s invalid token %q: %w", out.HostID, token, err)
			tokens = nil
			break
		}
		tokens = append(tokens, n)
	}
	out.Shards = uint32(h.privateStorage.InternalScyllaFeatures.ShardsCount())
	out.ShardingIgnoreMSB = uint8(h.privateStorage.InternalScyllaFeatures.privateStorage.InternalMsbIgnore)
	h.privateStorage.InternalMu.RUnlock()
	return out, tokens, tokenErr
}

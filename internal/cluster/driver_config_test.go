// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	implProtocol "github.com/scylladb/gocql/v2/internal/protocol"

	"context"
	"testing"
)

// newTestReportSession builds a *Session suitable for exercising the report
// builders directly, without dialing anything. Only the fields the builders
// read (cfg, policy, logger) are populated.
func newTestReportSession(cfg InternalClusterSettings, policy HostSelectionPolicy) *Core {
	return func() *Core {
		opaqueStorageValuex := new(Core)
		SessionData(opaqueStorageValuex).InternalCfg = cfg
		SessionData(opaqueStorageValuex).InternalPolicy = policy
		SessionData(opaqueStorageValuex).InternalLogger = &logging.InternalDefaultLogger{}
		return opaqueStorageValuex
	}()
}

// fakeReconnectionPolicy wraps a ReconnectionPolicy so its dynamic type name
// (used for the "custom" report branch) is distinct from any built-in policy.
type fakeReconnectionPolicy struct {
	ReconnectionPolicy
}

// fakeHostSelectionPolicy wraps a HostSelectionPolicy so its dynamic type
// name is distinct from any built-in policy, without reimplementing the
// (large) HostSelectionPolicy interface by hand.
type fakeHostSelectionPolicy struct {
	HostSelectionPolicy
}

// fakeShardDialer is a HostDialer that can target a shard, like the
// *scyllaDialer the driver substitutes when no HostDialer is configured.
type fakeShardDialer struct{ fakeHostDialer }

func (fakeShardDialer) DialShard(ctx context.Context, host *endpoint.HostInfo, shardID, nrShards int) (*endpoint.DialedHost, error) {
	return nil, nil
}

type fakeHostDialer struct{}

func (fakeHostDialer) DialHost(ctx context.Context, host *endpoint.HostInfo) (*endpoint.DialedHost, error) {
	return nil, nil
}

// readStartupOptions decodes the string map carried by a STARTUP frame body.
// The framer read helpers panic on a truncated buffer and this runs on the fake
// server's read goroutine, so a malformed frame is turned into a test failure
// instead of taking the whole test binary down.
func readStartupOptions(t *testing.T, f *implProtocol.Framer) (opts map[string]string) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("malformed STARTUP frame: %v", r)
			opts = nil
		}
	}()

	opts = make(map[string]string)
	for n := f.ReadShort(); n > 0; n-- {
		key := f.ReadString()
		opts[key] = f.ReadString()
	}
	return opts
}

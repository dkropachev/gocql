// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"context"
	"strings"
	"testing"
	"time"

	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

type negativeReconnect struct{ calls []int }

func (p *negativeReconnect) Delay(attempt int) (time.Duration, bool) {
	p.calls = append(p.calls, attempt)
	return -time.Second, true
}

// TestConnectRejectsNegativeReconnectDelay checks that Connect asks the
// reconnect policy for attempt 1, the first reconnection, and fails before
// dialing when the policy answers with a negative delay.
func TestConnectRejectsNegativeReconnectDelay(t *testing.T) {
	policy := &negativeReconnect{}
	config := implSettings.DefaultConfig("127.0.0.1")
	config.Reconnect = policy
	_, err := InternalConnectCluster(context.Background(), config, nil)
	if err == nil || !strings.Contains(err.Error(), "WithReconnect") {
		t.Fatalf("got %v, want a WithReconnect validation error", err)
	}
	if len(policy.calls) != 1 || policy.calls[0] != 1 {
		t.Fatalf("Delay calls %v, want [1]", policy.calls)
	}
}

// TestProbeReconnectAsksOnce checks that settings whose first delay was
// already asked for, as config.Build does, are not asked again.
func TestProbeReconnectAsksOnce(t *testing.T) {
	policy := &negativeReconnect{}
	config := implSettings.DefaultConfig("127.0.0.1")
	config.Reconnect = policy
	config.ReconnectFirst = implSettings.ReconnectProbe{Delay: time.Second, Again: true, Set: true}
	if err := config.ProbeReconnect(); err != nil || len(policy.calls) != 0 {
		t.Fatalf("ProbeReconnect = %v with calls %v, want the kept answer and no call", err, policy.calls)
	}
}

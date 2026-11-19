// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"time"

	"github.com/scylladb/gocql/v2/cql"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

// startReconnectingDownedHosts starts the loop that reopens the pools of down
// hosts, paced by the session's reconnect policy. config.WithReconnect(nil),
// or a policy whose first Delay declines to retry, starts no loop: down hosts
// then come back only through status and topology events.
func (s *Core) startReconnectingDownedHosts() {
	owner := s.CurrentExecution()
	if owner == nil {
		return
	}
	policy := owner.ConfigSnapshot().Reconnect
	if policy == nil {
		return
	}
	delay, again := policy.Delay(0)
	if !again || delay < 0 {
		return
	}
	// A session worker, so Close joins the loop after cancelling its ctx.
	s.Peripheral().InternalStartWorker(func() { s.reconnectTypedDownedHosts(policy, max(delay, time.Millisecond)) })
}

// reconnectTypedDownedHosts keeps independent bounded backoff state for each host.
func (s *Core) reconnectTypedDownedHosts(policy implSettings.ReconnectPolicy, initial time.Duration) {
	type retryState struct {
		next    time.Time
		attempt int
		stopped bool
	}
	retries := make(map[cql.UUID]retryState)
	tick := min(max(initial, time.Millisecond), time.Second)
	timer := time.NewTicker(tick)
	defer timer.Stop()
	for {
		select {
		case <-s.privateStorage.InternalCtx.Done():
			return
		case now := <-timer.C:
			for _, host := range s.privateStorage.InternalHostSource.InternalGetHostsList() {
				id := implEndpoint.InternalHostInfoHostUUID(host)
				if host.IsUp() {
					delete(retries, id)
					continue
				}
				state := retries[id]
				if state.stopped || now.Before(state.next) {
					continue
				}
				if s.privateStorage.InternalCtx.Err() != nil {
					return
				}
				s.privateStorage.InternalPool.InternalAddHost(host)
				state.attempt++
				delay, again := policy.Delay(state.attempt)
				state.stopped = !again || delay < 0
				state.next = now.Add(max(delay, time.Millisecond))
				retries[id] = state
			}
		}
	}
}

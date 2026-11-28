// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"time"

	"github.com/scylladb/gocql/v2/cql"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	logging "github.com/scylladb/gocql/v2/internal/logging"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

// startReconnectingDownedHosts starts the loop that reopens the pools of down
// hosts, paced by the session's reconnect policy. A policy whose first delay,
// which the configuration asked for once (settings.Config.ProbeReconnect),
// declines to retry starts no loop: down hosts then come back only through
// status and topology events. So does no policy, which only internal
// settings have.
func (s *Core) startReconnectingDownedHosts() {
	owner := s.CurrentExecution()
	if owner == nil {
		return
	}
	config := owner.ConfigSnapshot()
	policy, first := config.Reconnect, config.ReconnectFirst
	if policy == nil || !first.Set || !first.Again {
		return
	}
	delay := first.Delay
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
				_ = s.privateStorage.InternalPool.InternalAddHost(host) // the pool logs its failures
				state.attempt++
				delay, again := policy.Delay(state.attempt + 1)
				if delay < 0 {
					s.logger().Warn("reconnect policy returned a negative delay; not reconnecting host", logging.KeyHost, host.ConnectAddress().String())
				}
				state.stopped = !again || delay < 0
				state.next = now.Add(max(delay, time.Millisecond))
				retries[id] = state
			}
		}
	}
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package event holds the cluster events a session delivers through
// gocql.Session.Subscribe.
package event

import "github.com/scylladb/gocql/v2/internal/link"

// Subscription owns a bounded queue of events. Overflow discards queued
// deltas and emits Overflow, then closes. Subscribe again before reading
// Session.Metadata.
type Subscription struct {
	events <-chan Event
	close  func()
}

// The session creates subscriptions through internal/link.
func init() {
	link.Set(link.NewSubscription, func(events <-chan Event, close func()) *Subscription {
		return &Subscription{events: events, close: close}
	})
}

// Events returns the subscription's queue. It is closed when the
// subscription closes.
func (s *Subscription) Events() <-chan Event {
	if s == nil {
		return nil
	}
	return s.events
}

// Close stops the subscription and closes its queue. It is idempotent.
func (s *Subscription) Close() {
	if s != nil && s.close != nil {
		s.close()
	}
}

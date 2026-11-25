// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package event holds the cluster events a session delivers through
// gocql.Session.Subscribe.
package event

import "github.com/scylladb/gocql/v2/internal/link"

// Subscription owns a bounded queue of events. When the queue overflows,
// the queued events are discarded and one KindOverflow event, whose
// Generation is the highest discarded one, takes their place; the
// subscription stays open and keeps delivering. Read Session.Metadata again
// after an Overflow event to catch up. gocql.Session.Subscribe makes one; a
// nil or zero Subscription is closed: its Events queue is closed and empty.
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
	if s == nil || s.events == nil {
		return closedEvents
	}
	return s.events
}

// closedEvents is the queue of a nil or zero Subscription.
var closedEvents = func() <-chan Event {
	c := make(chan Event)
	close(c)
	return c
}()

// Close stops the subscription and closes its queue. It is idempotent.
func (s *Subscription) Close() {
	if s != nil && s.close != nil {
		s.close()
	}
}

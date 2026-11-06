//go:build unit
// +build unit

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"net"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/events"
	"github.com/scylladb/gocql/v2/internal/eventbus"
	"github.com/scylladb/gocql/v2/internal/logging"
)

func TestSessionEventBusPublishesEvents(t *testing.T) {
	s := func() *Session {
		opaqueStorageValue := new(Session)
		SessionData(opaqueStorageValue).InternalEventBus = eventbus.New[events.Event](eventbus.EventBusConfig{
			InputEventsQueueSize: 1,
		}, nil)
		SessionData(opaqueStorageValue).InternalLogger = &logging.InternalNopLogger{}
		return opaqueStorageValue
	}()

	if err := SessionData(s).InternalEventBus.Start(); err != nil {
		t.Fatalf("starting event bus: %v", err)
	}
	defer SessionData(s).InternalEventBus.Stop()

	sub := InternalSessionSubscribeToEvents(s, "test", 1, nil)
	defer sub.Stop()

	ev := &events.StatusChangeEvent{
		Change: "UP",
		Host:   net.ParseIP("127.0.0.1"),
		Port:   9042,
	}
	InternalSessionPublishEvent(s, ev)

	select {
	case received := <-sub.Events():
		if received != ev {
			t.Fatalf("unexpected event pointer: got %p want %p", received, ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"errors"
	"testing"
)

func TestCallReqReuseStartsWaitingWithAnEmptySlot(t *testing.T) {
	t.Parallel()

	// A caller that stopped after a response was delivered drains it; a reused
	// call must still start waiting with an empty slot.
	oldCall := InternalGetCallReq(1)
	if !oldCall.deliver(InternalCallResp{InternalErr: errors.New("stale")}) {
		t.Fatal("deliver to a waiting call failed")
	}
	oldCall.InternalDone.Done()
	InternalPutCallReq(oldCall)

	newCall := InternalGetCallReq(2)
	defer newCall.InternalDone.Done()
	select {
	case resp := <-newCall.InternalResp:
		t.Fatalf("reused call observed a stale response: %v", resp.InternalErr)
	default:
	}
	if !newCall.deliver(InternalCallResp{}) {
		t.Fatal("reused call did not start waiting")
	}
	if _, owned := newCall.InternalStopWaiting(); !owned {
		t.Fatal("stopping after delivery did not hand over the response")
	}
}

func TestCallReqStopWaitingDecidesResponseOwnership(t *testing.T) {
	t.Parallel()

	stopped := InternalGetCallReq(1)
	defer stopped.InternalDone.Done()
	if _, owned := stopped.InternalStopWaiting(); owned {
		t.Fatal("stop before delivery claimed a response")
	}
	if stopped.deliver(InternalCallResp{}) {
		t.Fatal("deliver succeeded after the caller stopped waiting")
	}

	delivered := InternalGetCallReq(2)
	defer delivered.InternalDone.Done()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if !delivered.deliver(InternalCallResp{removedFromCalls: true}) {
			t.Error("deliver to a waiting call failed")
		}
	}()
	<-done
	resp, owned := delivered.InternalStopWaiting()
	if !owned || !resp.removedFromCalls {
		t.Fatal("stop after delivery did not return the delivered response")
	}
}

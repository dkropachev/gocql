// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"errors"
	"testing"
	"time"
)

func TestCall(t *testing.T) {
	t.Run("Reuse", func(t *testing.T) {
		t.Parallel()

		// A caller that stopped after a response was delivered drains it; a reused
		// call must still start waiting with an empty slot.
		oldCall := getCallReq(1)
		if !oldCall.deliver(InternalCallResp{InternalErr: errors.New("stale")}) {
			t.Fatal("deliver to a waiting call failed")
		}
		oldCall.done.Done()
		putCallReq(oldCall)

		newCall := getCallReq(2)
		defer newCall.done.Done()
		select {
		case resp := <-newCall.InternalResp:
			t.Fatalf("reused call observed a stale response: %v", resp.InternalErr)
		default:
		}
		if !newCall.deliver(InternalCallResp{}) {
			t.Fatal("reused call did not start waiting")
		}
		if _, owned := newCall.stopWaiting(); !owned {
			t.Fatal("stopping after delivery did not hand over the response")
		}
	})

	t.Run("StopWaiting", func(t *testing.T) {
		t.Run("DecidesResponseOwnership", func(t *testing.T) {
			t.Parallel()

			stopped := getCallReq(1)
			defer stopped.done.Done()
			if _, owned := stopped.stopWaiting(); owned {
				t.Fatal("stop before delivery claimed a response")
			}
			if stopped.deliver(InternalCallResp{}) {
				t.Fatal("deliver succeeded after the caller stopped waiting")
			}

			delivered := getCallReq(2)
			defer delivered.done.Done()
			done := make(chan struct{})
			go func() {
				defer close(done)
				if !delivered.deliver(InternalCallResp{removedFromCalls: true}) {
					t.Error("deliver to a waiting call failed")
				}
			}()
			<-done
			resp, owned := delivered.stopWaiting()
			if !owned || !resp.removedFromCalls {
				t.Fatal("stop after delivery did not return the delivered response")
			}
		})
		// Checks that a caller that
		// stops waiting after the reader claimed its call receives the response the
		// reader is building in the caller's storage, so that the storage is never
		// reused while the reader still writes to it.
		t.Run("ReceivesClaimedResponse", func(t *testing.T) {
			t.Parallel()

			dest := new(InternalResponsePage)
			call := getCallReq(1)
			call.dest = dest
			if !call.claim() {
				t.Fatal("reader could not claim a waiting call")
			}
			stopped := make(chan InternalCallResp)
			go func() {
				resp, owned := call.stopWaiting()
				if !owned {
					t.Error("claimed response not handed to the caller")
				}
				stopped <- resp
			}()
			select {
			case <-stopped:
				t.Fatal("caller stopped waiting while the reader still built its response")
			case <-time.After(20 * time.Millisecond):
			}
			if !call.deliver(InternalCallResp{page: dest}) {
				t.Fatal("claimed call refused delivery")
			}
			if resp := <-stopped; resp.page != dest {
				t.Fatal("caller received another response")
			}

			// A caller that stopped first keeps the reader out of its storage.
			late := getCallReq(2)
			late.dest = dest
			if _, owned := late.stopWaiting(); owned {
				t.Fatal("stopped caller owns a response")
			}
			if late.claim() {
				t.Fatal("reader claimed a stopped call")
			}
		})
	})
	t.Run("Table", func(t *testing.T) {
		var nilTable *InternalCallTable
		if nilTable.Get(1) != nil || nilTable.Delete(1) != nil || nilTable.Len() != 0 {
			t.Fatal("a nil table must read as empty")
		}
		for range nilTable.All() {
			t.Fatal("a nil table must yield nothing")
		}

		tbl := InternalNewCallTable()
		a, b, c := &InternalCallReq{}, &InternalCallReq{}, &InternalCallReq{}
		tbl.Set(0, a)
		tbl.Set(32767, b)
		tbl.Set(65, c)
		tbl.Set(65, c) // overwrite keeps the count
		if tbl.Len() != 3 {
			t.Fatalf("Len = %d, want 3", tbl.Len())
		}
		if tbl.Get(0) != a || tbl.Get(32767) != b || tbl.Get(65) != c {
			t.Fatal("Get returned the wrong call")
		}
		if tbl.Get(-1) != nil || tbl.Get(64) != nil || tbl.Get(1<<20) != nil {
			t.Fatal("unset or out-of-range streams must read as nil")
		}
		var order []int
		for stream := range tbl.All() {
			order = append(order, stream)
		}
		if len(order) != 3 || order[0] != 0 || order[1] != 65 || order[2] != 32767 {
			t.Fatalf("All yielded %v", order)
		}
		if tbl.Delete(65) != c || tbl.Delete(65) != nil || tbl.Len() != 2 {
			t.Fatal("Delete must remove once and keep the count")
		}
		tbl.Set(0, nil)
		if tbl.Get(0) != nil || tbl.Len() != 1 {
			t.Fatal("Set(nil) must delete")
		}
	})
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"net"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestTracer(t *testing.T) {
	t.Parallel()

	t.Run("Enhanced", func(t *testing.T) {
		t.Parallel()

		t.Run("Complete", func(t *testing.T) {
			t.Parallel()

			ts := newTraceServer(t, traceConfig{duration: 1500, coordinator: net.IPv4(10, 0, 0, 9), events: traceEvents})
			tracer := NewTracer(ts.session(t))

			tracer.Trace(traceID)
			tracer.Trace([]byte{1})
			if got := tracer.AllTraceIDs(); !reflect.DeepEqual(got, [][]byte{traceID, {1}}) {
				t.Fatalf("AllTraceIDs = %x, want [%x 01]", got, traceID)
			}

			ready, err := tracer.IsReady(traceID)
			if err != nil || !ready {
				t.Fatalf("IsReady = %v, %v; want true", ready, err)
			}

			coordinator, d, err := tracer.GetCoordinatorTime(traceID)
			if err != nil || coordinator != "10.0.0.9" || d != 1500*time.Microsecond {
				t.Fatalf("GetCoordinatorTime = %q, %v, %v; want 10.0.0.9, 1.5ms", coordinator, d, err)
			}

			activities, err := tracer.GetActivities(traceID)
			if err != nil {
				t.Fatal(err)
			}
			want := []TraceEntry{
				{Timestamp: traceStart, Activity: "Parsing statement", Source: "10.0.0.1", Elapsed: 15, Thread: "Native-Transport-Requests-1"},
				{Timestamp: traceStart.Add(time.Millisecond), Activity: "Request complete", Source: "10.0.0.2", Elapsed: 420, Thread: "shard 3"},
			}
			if len(activities) != len(want) {
				t.Fatalf("GetActivities = %+v, want %+v", activities, want)
			}
			for i := range want {
				got := activities[i]
				if !got.Timestamp.Equal(want[i].Timestamp) || got.Activity != want[i].Activity || got.Source != want[i].Source ||
					got.Elapsed != want[i].Elapsed || got.Thread != want[i].Thread {
					t.Fatalf("activity %d = %+v, want %+v", i, got, want[i])
				}
			}
		})

		t.Run("Errors", func(t *testing.T) {
			t.Parallel()

			ts := newTraceServer(t, traceConfig{fail: true})
			tracer := NewTracer(ts.session(t))

			if ready, err := tracer.IsReady(traceID); err == nil || ready {
				t.Fatalf("IsReady = %v, %v; want an error", ready, err)
			}
			if _, _, err := tracer.GetCoordinatorTime(traceID); err == nil {
				t.Fatal("GetCoordinatorTime: want an error")
			}
			if a, err := tracer.GetActivities(traceID); err == nil || a != nil {
				t.Fatalf("GetActivities = %v, %v; want nil and an error", a, err)
			}
		})

		// A session whose duration is still 0 has not finished tracing.
		t.Run("NotReady", func(t *testing.T) {
			t.Parallel()

			ts := newTraceServer(t, traceConfig{readyAfter: 1, duration: 10})
			tracer := NewTracer(ts.session(t))

			ready, err := tracer.IsReady(traceID)
			if err != nil || ready {
				t.Fatalf("first IsReady = %v, %v; want false", ready, err)
			}
			ready, err = tracer.IsReady(traceID)
			if err != nil || !ready {
				t.Fatalf("second IsReady = %v, %v; want true", ready, err)
			}
		})
	})

	t.Run("Writer", func(t *testing.T) {
		t.Parallel()

		// A non-positive attempt limit still makes one lookup.
		t.Run("AttemptFloor", func(t *testing.T) {
			t.Parallel()

			ts := newTraceServer(t, traceConfig{readyAfter: 100, duration: 5, coordinator: net.IPv4(10, 0, 0, 9)})
			var out syncBuffer
			w := NewTraceWriter(ts.session(t), &out)
			w.SetMaxAttempts(0)

			w.Trace(traceID)

			if n := ts.durationLookups.Load(); n != 1 {
				t.Fatalf("duration looked up %d times, want 1", n)
			}
		})

		t.Run("Error", func(t *testing.T) {
			t.Parallel()

			ts := newTraceServer(t, traceConfig{fail: true})
			var out syncBuffer
			w := NewTraceWriter(ts.session(t), &out)

			w.Trace(traceID)

			if got := out.String(); !strings.HasPrefix(got, "Error: traces unavailable") || strings.Contains(got, "Tracing session") {
				t.Fatalf("trace output = %q, want only the error", got)
			}
		})

		// After maxAttempts unfinished lookups the writer says the trace is incomplete
		// and prints what there is.
		t.Run("Incomplete", func(t *testing.T) {
			t.Parallel()

			ts := newTraceServer(t, traceConfig{readyAfter: 100, duration: 5, coordinator: net.IPv4(10, 0, 0, 9)})
			var out syncBuffer
			w := NewTraceWriter(ts.session(t), &out)
			w.SetMaxAttempts(2)
			w.SetSleepInterval(time.Millisecond)

			w.Trace(traceID)

			if !strings.HasPrefix(out.String(), "Error: failed to wait tracing to complete. !!! Tracing is incomplete !!!\nTracing session") {
				t.Fatalf("trace output = %q, want the incomplete warning before the session line", out.String())
			}
			if n := ts.durationLookups.Load(); n != 2 {
				t.Fatalf("duration looked up %d times, want 2", n)
			}
		})

		t.Run("Polls", func(t *testing.T) {
			t.Parallel()

			// Unfinished on the first two lookups: the writer must keep polling.
			ts := newTraceServer(t, traceConfig{readyAfter: 2, duration: 1500, coordinator: net.IPv4(10, 0, 0, 9), events: traceEvents})
			var out syncBuffer
			w := NewTraceWriter(ts.session(t), &out)
			w.SetSleepInterval(time.Millisecond)

			w.Trace(traceID)

			want := "Tracing session deadbeef000010008000000000000001 (coordinator: 10.0.0.9, duration: 1.5ms):\n" +
				"2026/10/13 12:00:00: Parsing statement [Native-Transport-Requests-1] (source: 10.0.0.1, elapsed: 15)\n" +
				"2026/10/13 12:00:00.001: Request complete [shard 3] (source: 10.0.0.2, elapsed: 420)\n"
			got := out.String()
			// event timestamps are formatted in the local time zone
			got = strings.ReplaceAll(got, traceStart.Local().Format("2006/01/02 15:04:05"), "2026/10/13 12:00:00")
			if got != want {
				t.Fatalf("trace output:\n%s\nwant:\n%s", got, want)
			}
			if n := ts.durationLookups.Load(); n != 3 {
				t.Fatalf("duration looked up %d times, want 3", n)
			}
		})
	})
}

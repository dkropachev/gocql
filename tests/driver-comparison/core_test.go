package main

import (
	"context"
	"testing"
	"time"
)

type fakeWorker struct{ delay time.Duration }

func (f fakeWorker) Write(context.Context, int32, int32, []byte) error { return nil }
func (f fakeWorker) Read(ctx context.Context, k int32, c consumer) error {
	select {
	case <-time.After(f.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	return c(&row{K: k, P: payload(false, k, 0)})
}
func TestOwnedValidation(t *testing.T) {
	initialize()
	k := int32(73)
	v := row{K: k, P: append([]byte(nil), payload(false, k, 0)...)}
	if e := validator(false, k)(&v); e != nil {
		t.Fatal(e)
	}
	v.P[987] ^= 1
	if e := validator(false, k)(&v); e == nil {
		t.Fatal("accepted corrupt full payload")
	}
	v.P = payload(false, k, 0)
	v.R = 1
	if e := validator(false, k)(&v); e == nil {
		t.Fatal("accepted wrong row ID")
	}
}
func TestWindowAndDrain(t *testing.T) {
	initialize()
	s, e := phase([]op{scenarioOp(fakeWorker{8 * time.Millisecond}, "read")}, 30*time.Millisecond, 0)
	if e != nil {
		t.Fatal(e)
	}
	if s.Completed != s.Admitted || s.Errors != 0 || s.InWindow+s.Drained != s.Completed || s.Drained < 1 {
		t.Fatalf("bad completion accounting: %+v", s)
	}
	if s.Latency.TotalCount() != s.Completed {
		t.Fatal("histogram omitted operations")
	}
	end, err := time.Parse(time.RFC3339Nano, s.End)
	if err != nil {
		t.Fatal(err)
	}
	drained, err := time.Parse(time.RFC3339Nano, s.DrainEnd)
	if err != nil || drained.Before(end) {
		t.Fatal("diagnostic syscall window omitted the drain", drained, end, err)
	}
}
func TestBoundedScheduledQueue(t *testing.T) {
	oldQueueSize := *queueSize
	*queueSize = 4
	t.Cleanup(func() { *queueSize = oldQueueSize })
	initialize()
	s, e := phase([]op{scenarioOp(fakeWorker{4 * time.Millisecond}, "read")}, 40*time.Millisecond, 2000)
	if e != nil {
		t.Fatal(e)
	}
	if s.Rejected == 0 || s.PeakQueue > 4 || s.Completed != s.Admitted || s.Offered != s.Admitted+s.Rejected {
		t.Fatalf("bad queue accounting: %+v", s)
	}
	if s.Latency.ValueAtQuantile(90) <= s.Dispatch.ValueAtQuantile(90) {
		t.Fatal("scheduled latency omitted queueing")
	}
}

func TestScheduledWindowBoundaryDrainsEveryArrival(t *testing.T) {
	initialize()
	s, err := phaseWithSleep([]op{scenarioOp(fakeWorker{}, "read")}, 20*time.Millisecond, 100,
		func(wait time.Duration) { time.Sleep(wait + 30*time.Millisecond) })
	if err != nil {
		t.Fatal(err)
	}
	if s.Offered != 2 || s.Admitted != 2 || s.Completed != 2 || s.Rejected != 0 ||
		s.QueueRejected != 0 || s.ArrivalDeadlineRejected != 0 || s.LateArrivals == 0 || s.Drained == 0 {
		t.Fatalf("timer overshoot was counted as a capacity rejection: %+v", s)
	}
	if s.InWindow >= s.Completed || s.Schedule.TotalCount() != s.Offered {
		t.Fatal("late work inflated window throughput or lost its schedule latency")
	}
}

func TestRowCheckCountsAndOrders(t *testing.T) {
	initialize()
	var c rowCheck
	c.reset(true, 5, 32)
	for r := int32(32); r < 40; r++ {
		v := row{K: 5, R: r, P: payload(true, 5, r)}
		if err := c.row(&v); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.done(8, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.done(9, nil); err == nil {
		t.Fatal("accepted a short page")
	}
	v := row{K: 5, R: 41, P: payload(true, 5, 41)}
	if err := c.row(&v); err == nil {
		t.Fatal("accepted a skipped row")
	}
}

func TestPageCursorWalksPartitions(t *testing.T) {
	c := pageCursor{k: partKey(0)}
	first := c.k
	for page := 0; page < 4; page++ {
		if c.expect() != pageRows {
			t.Fatalf("page %d expects %d", page, c.expect())
		}
		if err := c.advance(pageRows, []byte{1, 2, 3}); err != nil {
			t.Fatal(err)
		}
	}
	if c.expect() != 0 {
		t.Fatal("terminal page must be empty")
	}
	if err := c.advance(0, nil); err != nil {
		t.Fatal(err)
	}
	if c.k == first || c.next != 0 || len(c.state) != 0 {
		t.Fatalf("cursor did not move to the next partition: %+v", c)
	}
	c.next = 10
	if err := c.advance(0, nil); err == nil {
		t.Fatal("accepted a partition that ended early")
	}
}

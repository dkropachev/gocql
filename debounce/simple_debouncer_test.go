//go:build unit
// +build unit

package debounce

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSimpleDebouncerRace tests SimpleDebouncer for the fact that it does not allow concurrent writing, reading.
func TestSimpleDebouncerRace(t *testing.T) {
	t.Parallel()

	operations := 1000
	runs := 100
	count := 3

	d := NewSimpleDebouncer()
	for r := 0; r < runs; r++ {
		var counter atomic.Int32
		var wg sync.WaitGroup
		wg.Add(count)

		results := make([]bool, count)
		fails := make([]bool, count)
		for c := range results {
			result := &results[c]
			fail := &fails[c]

			go func() {
				*result = d.Debounce(func() {
					for i := 0; i < operations; i++ {
						if counter.Add(1) != 1 {
							*fail = true
						}
						time.Sleep(time.Microsecond)
						counter.Add(-1)
					}
				})
				wg.Done()
			}()
		}
		wg.Wait()

		// check results

		finished := 0
		for i, done := range results {
			if done {
				finished++
			}
			if fails[i] {
				t.Fatalf("Simultaneous execution detected")
			}
		}
		if finished < 2 {
			t.Fatalf("In one run should be finished more than 2 `Debounce` method calls, but finished %d", finished)
		}
	}
}

// TestDebouncerExtreme tests SimpleDebouncer in the conditions  fast multi `Debounce` method calls and fast execution of the `debounced function`.
func TestDebouncerExtreme(t *testing.T) {
	t.Parallel()

	type runResult struct {
		executedN int32
		done      bool
	}

	runs := 10000
	count := 20

	d := NewSimpleDebouncer()
	var wg sync.WaitGroup
	for r := 0; r < runs; r++ {
		var executionsC atomic.Int32
		wg.Add(count)

		results := make([]runResult, count)

		for c := range results {
			result := &results[c]

			go func() {
				result.done = d.Debounce(func() {
					result.executedN = executionsC.Add(1)
				})
				wg.Done()
			}()
		}
		wg.Wait()

		// check results
		finished := 0
		for _, result := range results {
			if result.done {
				if result.executedN == 0 {
					t.Fatalf("Wrong execution detected: \n%#v", result)
				}
				finished++
			}
		}
		if finished < 2 {
			t.Fatalf("In one run should be finished more than 2 `Debounce` method calls, but finished %d", finished)
		}
	}
}

// TestSimpleDebouncerCount tests SimpleDebouncer for the fact that it pended only one function call.
func TestSimpleDebouncerCount(t *testing.T) {
	t.Parallel()

	calls := 10

	// Subtracting a one call that will be performed directly (not through goroutines)
	calls--

	d := NewSimpleDebouncer()
	var prepared, start, done sync.WaitGroup
	prepared.Add(calls)
	start.Add(1)
	done.Add(calls)

	finished := 0
	for c := 0; c < calls; c++ {
		go func() {
			prepared.Done()
			start.Wait()
			d.Debounce(func() {
				finished++
			})
			done.Done()
		}()
	}
	d.Debounce(func() {
		prepared.Wait()
		start.Done()
		finished++
		time.Sleep(time.Second)
	})
	done.Wait()

	// check results
	if finished != 2 {
		t.Fatalf("Should be finished 2 `Debounce` method calls, but finished %d", finished)
	}
}

// TestDebouncer tests that the debouncer allows only one function to execute at a time
func TestSimpleDebouncer(t *testing.T) {
	t.Parallel()

	d := NewSimpleDebouncer()
	var executions int32
	startedCh := make(chan struct{}, 1)
	doneCh := make(chan struct{}, 1)

	// Function to increment executions
	fn := func() {
		<-startedCh // Simulate work
		atomic.AddInt32(&executions, 1)
		<-doneCh // Simulate work
	}
	t.Run("Case 1", func(t *testing.T) {
		// Case 1: Normal single execution
		startedCh <- struct{}{}
		doneCh <- struct{}{}
		d.Debounce(fn)
		// We expect that the function has only executed once due to debouncing
		if atomic.LoadInt32(&executions) != 1 {
			t.Errorf("Expected function to be executed only once, but got %d executions", executions)
		}
	})

	atomic.StoreInt32(&executions, 0)
	t.Run("Case 2", func(t *testing.T) {
		// Case 2: Debounce the function multiple times at row when body is started
		go d.Debounce(fn)
		startedCh <- struct{}{}
		// Wait until first call execution started
		waitTillChannelIsEmpty(startedCh)
		// Call function twice, due to debounce only one should be executed
		go d.Debounce(fn)
		go d.Debounce(fn)
		// Let first call to complete
		doneCh <- struct{}{}
		// Let second call to complete
		startedCh <- struct{}{}
		doneCh <- struct{}{}
		// Make sure second call is completed
		waitTillChannelIsEmpty(doneCh)
		// We expect that the function has only executed once due to debouncing
		if atomic.LoadInt32(&executions) != 2 {
			t.Errorf("Expected function to be executed twice, but got %d executions", executions)
		}
	})
}
func waitTillChannelIsEmpty(ch chan struct{}) {
	for {
		if len(ch) == 0 {
			return
		}
		runtime.Gosched()
	}
}

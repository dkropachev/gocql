// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"errors"
	"testing"
)

// TestAllRowsCloses checks that the iterator behind Rows.All, Page.All and
// CASBatchRows.All closes the rows however the loop ends: run to the end
// (yielding close's error), broken out of, or ended by a panic in its body.
func TestAllRowsCloses(t *testing.T) {
	rows := func(n int, closeErr error) (func(*int) bool, func() error, *int) {
		closes := new(int)
		i := 0
		next := func(dst *int) bool {
			if i == n {
				return false
			}
			i++
			*dst = i
			return true
		}
		return next, func() error { *closes++; return closeErr }, closes
	}

	t.Run("End", func(t *testing.T) {
		broken := errors.New("broken")
		next, close, closes := rows(2, broken)
		var got []int
		var gotErr error
		for row, err := range allRows(nil, next, close) {
			if err != nil {
				gotErr = err
				continue
			}
			got = append(got, *row)
		}
		if len(got) != 2 || got[0] != 1 || got[1] != 2 {
			t.Fatalf("rows %v, want [1 2]", got)
		}
		if !errors.Is(gotErr, broken) {
			t.Fatalf("error %v, want close's error", gotErr)
		}
		if *closes == 0 {
			t.Fatal("rows not closed")
		}
	})

	t.Run("Break", func(t *testing.T) {
		next, close, closes := rows(3, nil)
		for range allRows(new(int), next, close) {
			break
		}
		if *closes == 0 {
			t.Fatal("rows not closed after break")
		}
	})

	t.Run("Panic", func(t *testing.T) {
		next, close, closes := rows(3, nil)
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("the loop body's panic did not propagate")
				}
			}()
			for range allRows(new(int), next, close) {
				panic("loop body")
			}
		}()
		if *closes == 0 {
			t.Fatal("rows not closed after the loop body panicked")
		}
	})
}

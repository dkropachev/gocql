// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package main

import "testing"

func TestValidation(t *testing.T) {
	t.Run("PageCursor", func(t *testing.T) {
		t.Run("WalksPartitions", func(t *testing.T) {
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
		})
	})

	t.Run("RowCheck", func(t *testing.T) {
		t.Run("CountsAndOrders", func(t *testing.T) {
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
		})
	})

	t.Run("Validator", func(t *testing.T) {
		t.Run("Owned", func(t *testing.T) {
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
		})
	})
}

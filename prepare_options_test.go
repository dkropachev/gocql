// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import "testing"

func TestPrepareOptions(t *testing.T) {
	t.Run("BindOptions", func(t *testing.T) {
		t.Run("RequiresSession", func(t *testing.T) {
			if _, err := BindOptions(nil, Options{}); err == nil {
				t.Fatal("options bound without a session")
			}
		})
	})

	t.Run("OnePrepareOptions", func(t *testing.T) {
		t.Run("AcceptsAtMostOne", func(t *testing.T) {
			if _, err := onePrepareOptions(nil); err != nil {
				t.Fatal(err)
			}
			want := PrepareOptions{Keyspace: "ks"}
			got, err := onePrepareOptions([]PrepareOptions{want})
			if err != nil || got.Keyspace != "ks" {
				t.Fatalf("onePrepareOptions = %+v, %v", got, err)
			}
			if _, err := onePrepareOptions([]PrepareOptions{{}, {}}); err == nil {
				t.Fatal("two PrepareOptions accepted")
			}
		})
	})
}

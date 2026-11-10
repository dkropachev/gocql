// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package model

import "testing"

func TestPageState(t *testing.T) {
	t.Run("PageStateFromToken", func(t *testing.T) {
		start := StartPage()
		empty := PageStateFromToken(nil)
		complete := CompletePageState()
		if start.More() || start.Complete() || !empty.More() || empty.Complete() || !complete.Complete() {
			t.Fatal("page state distinctions lost")
		}
		if start.Token() != nil || complete.Token() != nil {
			t.Fatal("initial or complete state exported a continuation token")
		}
		if empty.Token() == nil {
			t.Fatal("empty continuation became absent")
		}
		token := []byte{1}
		state := PageStateFromToken(token)
		token[0] = 2
		copy := state.Token()
		copy[0] = 3
		if state.Token()[0] != 1 {
			t.Fatal("state token aliased")
		}
		for _, state := range []PageState{empty, state} {
			restored := PageStateFromToken(state.Token())
			if !restored.More() || restored.Complete() || string(restored.Token()) != string(state.Token()) {
				t.Fatal("continuation token did not round-trip")
			}
		}
	})
}

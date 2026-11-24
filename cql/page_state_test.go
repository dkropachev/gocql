// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestPageState(t *testing.T) {
	t.Run("PageStateFromToken", func(t *testing.T) {
		start := startPage()
		empty := PageStateFromToken(nil)
		complete := completePageState()
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

	// TestPageState/Marshal checks that the binary and text encodings round
	// trip every state, the empty continuation included, and reject other
	// input.
	t.Run("Marshal", func(t *testing.T) {
		states := map[string]PageState{
			"start":    startPage(),
			"empty":    PageStateFromToken(nil),
			"token":    PageStateFromToken([]byte{0, 1, 2, 0xff}),
			"complete": completePageState(),
		}
		for name, state := range states {
			b, err := state.MarshalBinary()
			if err != nil {
				t.Fatal(name, err)
			}
			var got PageState
			if err := got.UnmarshalBinary(b); err != nil {
				t.Fatal(name, err)
			}
			if got.More() != state.More() || got.Complete() != state.Complete() || string(got.Token()) != string(state.Token()) || (got.Token() == nil) != (state.Token() == nil) {
				t.Errorf("%s: binary round trip gave %+v", name, got)
			}
			if len(b) > 1 {
				b[1] = 7 // the decoded token is a copy
				if got.Token()[0] == 7 {
					t.Errorf("%s: decoded token aliases its input", name)
				}
			}
			text, err := json.Marshal(state)
			if err != nil {
				t.Fatal(name, err)
			}
			got = PageState{}
			if err := json.Unmarshal(text, &got); err != nil {
				t.Fatal(name, err)
			}
			if got.More() != state.More() || got.Complete() != state.Complete() || string(got.Token()) != string(state.Token()) {
				t.Errorf("%s: text round trip through %s gave %+v", name, text, got)
			}
		}
		for name, b := range map[string][]byte{"empty": nil, "kind": {3}, "start token": {0, 1}, "complete token": {2, 1}} {
			var got PageState
			if err := got.UnmarshalBinary(b); !errors.As(err, new(*ValidationError)) {
				t.Errorf("%s: UnmarshalBinary(%v) = %v, want a ValidationError", name, b, err)
			}
		}
		var got PageState
		if err := got.UnmarshalText([]byte("not base64!")); !errors.As(err, new(*ValidationError)) {
			t.Errorf("UnmarshalText of bad base64 = %v, want a ValidationError", err)
		}
	})
}

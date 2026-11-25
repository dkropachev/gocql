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
		for name, b := range map[string][]byte{"kind": {3}, "start token": {0, 1}, "complete token": {2, 1}} {
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

	// TestPageState/EmptyStart checks that the start state encodes as empty
	// binary and text, that empty input decodes to it, whatever state the
	// receiver held, and that "AA", the start state earlier releases
	// encoded, still does.
	t.Run("EmptyStart", func(t *testing.T) {
		if b, err := StartPage().MarshalBinary(); err != nil || b == nil || len(b) != 0 {
			t.Fatalf("start MarshalBinary = %v, %v; want empty", b, err)
		}
		if text, err := StartPage().MarshalText(); err != nil || len(text) != 0 {
			t.Fatalf("start MarshalText = %q, %v; want empty", text, err)
		}
		inputs := map[string]func(*PageState) error{
			"binary nil":   func(p *PageState) error { return p.UnmarshalBinary(nil) },
			"binary empty": func(p *PageState) error { return p.UnmarshalBinary([]byte{}) },
			"binary zero":  func(p *PageState) error { return p.UnmarshalBinary([]byte{0}) },
			"text empty":   func(p *PageState) error { return p.UnmarshalText(nil) },
			"text AA":      func(p *PageState) error { return p.UnmarshalText([]byte("AA")) },
		}
		for name, decode := range inputs {
			got := PageStateFromToken([]byte{1})
			if err := decode(&got); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if got.More() || got.Complete() || got.Token() != nil {
				t.Errorf("%s: decoded %+v, want the start state", name, got)
			}
		}
		var got PageState
		if err := json.Unmarshal([]byte(`""`), &got); err != nil || got.More() || got.Complete() {
			t.Errorf(`json "" = %+v, %v; want the start state`, got, err)
		}
	})
}

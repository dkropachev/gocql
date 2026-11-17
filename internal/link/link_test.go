// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package link

import (
	"strings"
	"testing"
)

func TestSlot(t *testing.T) {
	s := NewSlot("test.double")
	assertPanics(t, "not registered", func() { Get[func(int) int](s) })
	Set(s, func(v int) int { return 2 * v })
	if got := Get[func(int) int](s)(21); got != 42 {
		t.Fatalf("Get()(21) = %d", got)
	}
	assertPanics(t, "holds", func() { Get[func(string) int](s) })
	assertPanics(t, "registered twice", func() { Set(s, func(v int) int { return v }) })
}

func assertPanics(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		msg, _ := r.(string)
		if !strings.Contains(msg, want) {
			t.Fatalf("panic %v, want one containing %q", r, want)
		}
	}()
	fn()
}

type pubInner struct {
	n int64
	b []byte
}

type repInner struct {
	n int64
	b []byte
}

type pubOuter struct {
	inner pubInner
	p     *pubInner
	name  string
}

type repOuter struct {
	inner repInner
	p     *repInner
	name  string
}

type repRenamed struct {
	inner repInner
	q     *repInner
	name  string
}

type repShort struct {
	inner repInner
	p     *repInner
}

type repRetyped struct {
	inner repInner
	p     *repInner
	name  []byte
}

func TestCheckLayout(t *testing.T) {
	if err := CheckLayout[pubOuter, repOuter](); err != nil {
		t.Fatalf("identical layouts: %v", err)
	}
	for name, err := range map[string]error{
		"renamed field": CheckLayout[pubOuter, repRenamed](),
		"missing field": CheckLayout[pubOuter, repShort](),
		"retyped field": CheckLayout[pubOuter, repRetyped](),
	} {
		if err == nil {
			t.Errorf("%s: CheckLayout accepted a different layout", name)
		}
	}
	v := pubOuter{inner: pubInner{n: 7}, name: "x"}
	rep := Mirror[repOuter](&v)
	rep.inner.n = 9
	if v.inner.n != 9 || rep.name != "x" {
		t.Fatalf("Mirror does not alias its argument")
	}
}

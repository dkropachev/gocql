// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package link lets the driver's engine reach the unexported parts of public
// types without exporting them. See docs/package-layout.md, "Engine access to
// public types".
//
// Public packages may import link; it imports nothing of the driver, so it
// creates no cycle. It offers two mechanisms:
//
//   - A Slot holds one function. A public package fills it from init with an
//     unexported function (a constructor, an accessor), and the engine calls
//     it through Get. The slot stores the function as any, because link
//     cannot name the public types in its signature; Get asserts it back.
//   - Mirror converts a pointer to a public type into a pointer to an
//     internal struct of identical layout, for values the engine reads on
//     every request. CheckLayout, called from a test, proves the layouts
//     match.
package link

import "fmt"

// Slot holds one function a public package registers for the engine.
type Slot struct {
	fn   any
	name string
}

// NewSlot returns an empty slot. name identifies it in panics.
func NewSlot(name string) *Slot {
	return &Slot{name: name}
}

// Set fills s with f. A public package calls it from init, once.
func Set[F any](s *Slot, f F) {
	if s.fn != nil {
		panic(fmt.Sprintf("gocql: internal link %s registered twice", s.name))
	}
	s.fn = f
}

// Get returns the function in s. It panics when s is empty or holds another
// type: both are driver bugs, not user errors.
func Get[F any](s *Slot) F {
	f, ok := s.fn.(F)
	if !ok {
		if s.fn == nil {
			panic(fmt.Sprintf("gocql: internal link %s is not registered", s.name))
		}
		panic(fmt.Sprintf("gocql: internal link %s holds %T, not %T", s.name, s.fn, f))
	}
	return f
}

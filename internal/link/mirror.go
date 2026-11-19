// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package link

import (
	"fmt"
	"reflect"
	"unsafe"
)

// Mirror converts p, a pointer to a public type, into a pointer to Rep, an
// internal struct with the same layout. The result aliases p. Each Pub and
// Rep pair must have a test that calls CheckLayout.
//
// unsafe: CheckLayout proves the two types have the same size, alignment,
// field offsets and pointer positions, so the GC and every field access see
// the same memory through either type.
func Mirror[Rep, Pub any](p *Pub) *Rep {
	return (*Rep)(unsafe.Pointer(p))
}

// CheckLayout reports whether Pub and Rep have the same memory layout: the
// same size and alignment, and field by field the same name, offset and
// layout, recursively. Types outside structs, arrays and pointers must be
// identical, apart from named struct types, which are compared by layout.
func CheckLayout[Pub, Rep any]() error {
	return sameLayout(reflect.TypeFor[Pub](), reflect.TypeFor[Rep](), "", 0)
}

func sameLayout(pub, rep reflect.Type, path string, depth int) error {
	if pub == rep {
		return nil
	}
	if depth > 16 {
		return fmt.Errorf("%s: layout nests too deeply", pathOr(path, pub))
	}
	if pub.Size() != rep.Size() || pub.Align() != rep.Align() {
		return fmt.Errorf("%s: size/align %d/%d, mirror %v has %d/%d",
			pathOr(path, pub), pub.Size(), pub.Align(), rep, rep.Size(), rep.Align())
	}
	// An unsafe.Pointer may mirror a typed pointer: the public side holds an
	// engine object it cannot name.
	if isPointer(pub) && isPointer(rep) && (pub.Kind() == reflect.UnsafePointer || rep.Kind() == reflect.UnsafePointer) {
		return nil
	}
	if pub.Kind() != rep.Kind() {
		return fmt.Errorf("%s: kind %v, mirror %v has %v", pathOr(path, pub), pub.Kind(), rep, rep.Kind())
	}
	switch pub.Kind() {
	case reflect.Struct:
		if pub.NumField() != rep.NumField() {
			return fmt.Errorf("%s: %d fields, mirror %v has %d", pathOr(path, pub), pub.NumField(), rep, rep.NumField())
		}
		for i := range pub.NumField() {
			pf, rf := pub.Field(i), rep.Field(i)
			at := path + "." + pf.Name
			if pf.Name != rf.Name || pf.Offset != rf.Offset {
				return fmt.Errorf("%s: field %s at %d, mirror has %s at %d", pathOr(path, pub), pf.Name, pf.Offset, rf.Name, rf.Offset)
			}
			if err := sameLayout(pf.Type, rf.Type, at, depth+1); err != nil {
				return err
			}
		}
		return nil
	case reflect.Array:
		if pub.Len() != rep.Len() {
			return fmt.Errorf("%s: array length %d, mirror has %d", pathOr(path, pub), pub.Len(), rep.Len())
		}
		return sameLayout(pub.Elem(), rep.Elem(), path+"[]", depth+1)
	case reflect.Pointer, reflect.Slice:
		return sameLayout(pub.Elem(), rep.Elem(), path+"*", depth+1)
	}
	if pub.Kind() >= reflect.Bool && pub.Kind() <= reflect.Complex128 || pub.Kind() == reflect.String {
		return nil
	}
	return fmt.Errorf("%s: type %v, mirror has %v", pathOr(path, pub), pub, rep)
}

func pathOr(path string, t reflect.Type) string {
	if path == "" {
		return t.String()
	}
	return t.String() + " at " + path
}

func isPointer(t reflect.Type) bool {
	return t.Kind() == reflect.Pointer || t.Kind() == reflect.UnsafePointer
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

import (
	"errors"
	"fmt"
)

var ErrReleased = errors.New("gocql: borrowed value has been released")

// ValidationError describes an invalid API argument or type descriptor.
type ValidationError struct {
	Err   error
	Field string
}

func (e *ValidationError) Error() string { return fmt.Sprintf("gocql: invalid %s: %v", e.Field, e.Err) }
func (e *ValidationError) Unwrap() error { return e.Err }

// CodecError records the physical column and nested path of a codec failure.
type CodecError struct {
	Err    error
	Column string
	Path   string
	Stage  string
	Index  int
}

func (e *CodecError) Error() string {
	return fmt.Sprintf("gocql: %s column %d (%s)%s: %v", e.Stage, e.Index, e.Column, e.Path, e.Err)
}
func (e *CodecError) Unwrap() error { return e.Err }

// ProtocolError reports that a peer violated the CQL protocol: a frame the
// driver cannot accept, or data that does not decode.
type ProtocolError struct {
	Err error
	// Stage names where the violation was found. A violation of the frame
	// itself, such as an unknown opcode or an unexpected response, has
	// Stage "frame".
	Stage string
	// Offset is the byte offset of the invalid data, or -1 when the
	// violation has no position, as for a frame violation.
	Offset int64
}

// NewFrameError returns the ProtocolError of a frame violation.
func NewFrameError(format string, args ...any) *ProtocolError {
	return &ProtocolError{Stage: "frame", Offset: -1, Err: fmt.Errorf(format, args...)}
}

func (e *ProtocolError) Error() string {
	if e.Offset < 0 {
		return fmt.Sprintf("gocql: %s: %v", e.Stage, e.Err)
	}
	return fmt.Sprintf("gocql: %s at byte %d: %v", e.Stage, e.Offset, e.Err)
}
func (e *ProtocolError) Unwrap() error { return e.Err }

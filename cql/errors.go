// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"errors"
	"fmt"
)

// ErrReleased reports use of a borrowed view, such as Bytes or Text, after the
// response storage it borrows was released.
var ErrReleased = errors.New("gocql: borrowed value has been released")

// ErrNull reports a CQL NULL read into a destination that cannot hold one,
// such as an int64 field or a Value accessor. Decode a nullable column into
// Nullable[T] instead. v1 decoded NULL as the zero value.
var ErrNull = errors.New("gocql: value is null")

// nullCellError is the failure of decoding NULL into a non-nullable value.
type nullCellError struct{ kind Kind }

func (e nullCellError) Error() string {
	return fmt.Sprintf("gocql: nonnullable %s cannot decode cell state null", e.kind)
}

// Unwrap returns ErrNull.
func (nullCellError) Unwrap() error { return ErrNull }

// ValidationError describes an invalid API argument or type descriptor.
//
// The driver returns it filled in; its zero value is an error without detail.
type ValidationError struct {
	// Err says what is invalid.
	Err error
	// Field names the invalid argument or part of the descriptor.
	Field string
}

// Error names the invalid field and the reason.
func (e *ValidationError) Error() string { return fmt.Sprintf("gocql: invalid %s: %v", e.Field, e.Err) }

// Unwrap returns Err.
func (e *ValidationError) Unwrap() error { return e.Err }

// CodecStage is the step a CodecError failed in. Later releases may add
// values, so a switch over them needs a default case.
type CodecStage string

const (
	// CodecStageCompile: the codec rejected the column's type.
	CodecStageCompile CodecStage = "compile"
	// CodecStageEncode: a parameter value did not encode.
	CodecStageEncode CodecStage = "encode"
	// CodecStageDecode: a column value did not decode.
	CodecStageDecode CodecStage = "decode"
	// CodecStageStreamCompile: a streaming decoder rejected the column's
	// type.
	CodecStageStreamCompile CodecStage = "stream compile"
	// CodecStageStreamDecode: a streaming decoder failed on a column value.
	CodecStageStreamDecode CodecStage = "stream decode"
)

// CodecError records the physical column and nested path of a codec failure.
// Every codec failure of a parameter or result column is one: typed and
// struct row decoders, parameter encoders, dynamic rows and streams.
//
// The driver returns it filled in; its zero value is an error without detail.
type CodecError struct {
	// Err is the codec's failure.
	Err error
	// Column is the name of the failing column, or of the parameter's bind
	// marker.
	Column string
	// Path locates a failing tuple or UDT member inside the column's value,
	// such as "[1]" for the second tuple member or ".address.zip" for a
	// nested UDT field. It is empty when the column's value itself failed,
	// and when the member is inside a collection element or decoded by a
	// streaming decoder; the message still names the member then.
	Path string
	// Stage is where the failure happened.
	Stage CodecStage
	// Index is the zero-based position of the column, or of the parameter.
	// DynamicRow.At reports the position in the DynamicRow, which is the
	// right-hand part of the row when it is decoded by PairRows.
	Index int
}

// Error names the stage, the column's index and name, the member's path and
// the failure.
func (e *CodecError) Error() string {
	return fmt.Sprintf("gocql: %s column %d (%s)%s: %v", e.Stage, e.Index, e.Column, e.Path, e.Err)
}

// Unwrap returns Err.
func (e *CodecError) Unwrap() error { return e.Err }

// ProtocolError reports that a peer violated the CQL protocol: a frame the
// driver cannot accept, or data that does not decode.
//
// The driver returns it filled in; its zero value is an error without detail.
type ProtocolError struct {
	// Err is the violation.
	Err error
	// Stage names where the violation was found. A violation of the frame
	// itself, such as an unknown opcode or an unexpected response, has
	// Stage "frame".
	Stage string
	// Offset is the byte offset of the invalid data, or -1 when the
	// violation has no position, as for a frame violation.
	Offset int64
}

// Error names the stage, the byte offset when it is known, and the failure.
func (e *ProtocolError) Error() string {
	if e.Offset < 0 {
		return fmt.Sprintf("gocql: %s: %v", e.Stage, e.Err)
	}
	return fmt.Sprintf("gocql: %s at byte %d: %v", e.Stage, e.Offset, e.Err)
}

// Unwrap returns Err.
func (e *ProtocolError) Unwrap() error { return e.Err }

// columnError names column index of a codec failure at stage. An error that
// already is a CodecError names its column and is returned as it is.
//
// A memberError, the failure of a tuple or UDT member inside the column, is
// reported as the column's CodecError with the member's path.
func columnError(stage CodecStage, index int, name string, err error) error {
	switch e := err.(type) { //nolint:errorlint // only the outermost error names the column or a member path
	case *CodecError:
		return err
	case *memberError:
		if e.stage != "" {
			stage = e.stage
		}
		return &CodecError{Err: e.err, Column: name, Stage: stage, Index: index, Path: e.path}
	}
	return &CodecError{Err: err, Column: name, Stage: stage, Index: index}
}

// memberError is the failure of a member of a tuple or UDT value: its path,
// such as "[1]" or ".street", and the error. The CodecError of the column
// that holds the value reports it with Path set.
type memberError struct {
	err   error
	path  string
	stage CodecStage // the member's stage, such as "compile" for a lazily compiled codec
}

func (e *memberError) Error() string { return fmt.Sprintf("gocql: member %s: %v", e.path, e.err) }

func (e *memberError) Unwrap() error { return e.err }

// recordMemberError turns the CodecError of a tuple (kind KindTuple) or UDT
// member into a memberError, so that the enclosing column names the member
// by its path instead of being hidden by it.
func recordMemberError(kind Kind, err error) error {
	e, ok := err.(*CodecError) //nolint:errorlint // only the member plan's own CodecError names a member; a wrapped one belongs to a deeper value
	if !ok {
		return err
	}
	path := fmt.Sprintf("[%d]", e.Index)
	if kind == KindUDT {
		path = "." + e.Column
	}
	return &memberError{err: e.Err, path: path + e.Path, stage: e.Stage}
}

// rebaseColumnError shifts the column index of a CodecError reported by a
// plan that decoded or encoded a slice of the columns starting at offset.
func rebaseColumnError(err error, offset int) error {
	if e, ok := err.(*CodecError); ok && offset != 0 { //nolint:errorlint // only the sub-plan's own CodecError indexes its columns
		shifted := *e
		shifted.Index += offset
		return &shifted
	}
	return err
}

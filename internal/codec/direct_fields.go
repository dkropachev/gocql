// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"errors"
	"fmt"
	"math"
	"unicode/utf8"
	"unsafe"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// A field of a built-in codec is compiled into a direct op: the row and
// parameter plans switch on its kind and call the codec's code statically,
// instead of calling the field, the codec and the codec's function value
// through interfaces. Any other field keeps its interface.
type directKind uint8

const (
	directNone      directKind = iota
	directInt                  // a built-in signed integer codec of width bytes
	directBlob                 // BlobCodec: a view borrowing the response
	directText                 // TextCodec or ASCIICodec: a validated view borrowing the response
	directOwnedBlob            // ByteSliceCodec: a copy into the field's capacity
	directBytes                // ByteSliceCodec
	directString               // StringCodec or ASCIIStringCodec
)

// funcPointer returns a func value's pointer. A func value is one pointer
// to its closure, and funcs whose parameters and results have the same
// layouts are called the same way, so a direct op can call an accessor
// returning *V as one returning unsafe.Pointer, or a getter returning V as
// one returning the integer of V's width.
func funcPointer[F any](f *F) unsafe.Pointer { return *(*unsafe.Pointer)(unsafe.Pointer(f)) }

// integerWidth returns the width of V when it is a signed integer type, or 0.
func integerWidth[V any]() int64 {
	switch any((*V)(nil)).(type) {
	case *int8, *int16, *int32, *int64, *Counter:
		var v V
		return int64(unsafe.Sizeof(v))
	}
	return 0
}

// isIntegerKind reports whether kind is a CQL type of a built-in integer
// codec, whose width the codec fixes.
func isIntegerKind(kind cqltype.Type, width int64) bool {
	switch kind {
	case cqltype.TypeTinyInt:
		return width == 1
	case cqltype.TypeSmallInt:
		return width == 2
	case cqltype.TypeInt:
		return width == 4
	case cqltype.TypeBigInt, cqltype.TypeCounter:
		return width == 8
	}
	return false
}

// directRowField and directParamField are the fields that may compile into
// direct ops.
type directRowField[R any] interface {
	directRowOp() (rowOp[R], bool)
}
type directParamField[P any] interface {
	directParamOp() (paramOp[P], bool)
}

// rowOps returns the ops of a row plan's compiled fields.
func rowOps[R any](ordered []OutputField[R]) []rowOp[R] {
	ops := make([]rowOp[R], len(ordered))
	for i, field := range ordered {
		ops[i] = rowOp[R]{field: field}
		if d, ok := field.(directRowField[R]); ok {
			if op, ok := d.directRowOp(); ok {
				ops[i] = op
			}
		}
	}
	return ops
}

// paramOps returns the ops of a parameter plan's compiled fields.
func paramOps[P any](ordered []InputField[P]) []paramOp[P] {
	ops := make([]paramOp[P], len(ordered))
	for i, field := range ordered {
		ops[i] = paramOp[P]{field: field}
		if d, ok := field.(directParamField[P]); ok {
			if op, ok := d.directParamOp(); ok {
				ops[i] = op
			}
		}
	}
	return ops
}

// rowOp is one compiled result field.
type rowOp[R any] struct {
	field           OutputField[R] // directNone: the field itself
	access          unsafe.Pointer // the field accessor, a func(*R) *V
	width           int64
	columnKind, alt cqltype.Type
	kind            directKind
	compiled        bool
}

// directRowOp compiles f, already compiled for its column, into a direct op
// when its codec is a built-in one, and reports whether it did.
func (f outputField[R, V]) directRowOp() (rowOp[R], bool) {
	op := rowOp[R]{access: funcPointer(&f.field)}
	switch c := any(f.codec).(type) {
	case scalarCodec[V]:
		if c.decodeRow == nil {
			return rowOp[R]{}, false
		}
		if width := integerWidth[V](); width > 0 && isIntegerKind(c.kind, width) {
			op.kind, op.width = directInt, width
		} else if _, blob := any(f.codec).(scalarCodec[cqltype.Bytes]); blob && c.kind == cqltype.TypeBlob {
			op.kind = directBlob
		} else {
			return rowOp[R]{}, false
		}
		op.columnKind, op.alt, op.compiled = c.kind, c.alternative, c.compiled
	case byteSliceCodec:
		op.kind = directOwnedBlob
	case textCodec:
		// The field stays for columns of another kind, whose type error the
		// codec reports in its own words.
		op.kind, op.field, op.compiled = directText, f, c.compiled
		op.columnKind, op.alt = cqltype.TypeText, cqltype.TypeVarchar
		if c.ascii {
			op.columnKind, op.alt = cqltype.TypeAscii, 0
		}
	default:
		return rowOp[R]{}, false
	}
	return op, true
}

// decodeDirect decodes op's column into dst like outputField.decode, calling
// the codec's row decoder statically.
func (op *rowOp[R]) decodeDirect(reader *RowReader, dst *R) error {
	access := op.access
	ptr := (*(*func(*R) unsafe.Pointer)(unsafe.Pointer(&access)))(dst)
	if ptr == nil {
		return fmt.Errorf("gocql: output field accessor returned nil")
	}
	switch op.kind {
	case directInt:
		v, err := integerColumn(reader, op.columnKind, op.alt, op.compiled, op.width)
		if err != nil {
			return err
		}
		switch op.width {
		case 1:
			*(*int8)(ptr) = int8(v)
		case 2:
			*(*int16)(ptr) = int16(v)
		case 4:
			*(*int32)(ptr) = int32(v)
		default:
			*(*int64)(ptr) = v
		}
		return nil
	case directBlob:
		v, err := blobColumn(reader, op.columnKind, op.alt, op.compiled)
		if err == nil {
			*(*cqltype.Bytes)(ptr) = v
		}
		return err
	case directText:
		return op.decodeText(reader, dst, ptr)
	default: // directOwnedBlob
		return byteSliceCodec{}.decodeColumnInto(reader, (*[]byte)(ptr))
	}
}

// decodeText is decodeDirect's directText case, kept out of line so the
// integer and blob cases keep their code layout.
//
//go:noinline
func (op *rowOp[R]) decodeText(reader *RowReader, dst *R, ptr unsafe.Pointer) error {
	if column, ok := reader.columns.At(reader.index); ok && column.Type.Kind() != op.columnKind && column.Type.Kind() != op.alt {
		return op.field.decode(reader, dst)
	}
	v, err := textColumn(reader, op.columnKind, op.alt, op.compiled)
	if err == nil {
		*(*cqltype.Text)(ptr) = v
	}
	return err
}

// paramOp is one compiled parameter field.
type paramOp[P any] struct {
	field           InputField[P]  // directNone, or a precondition the op does not cover
	get             unsafe.Pointer // the field getter, a func(P) V
	width           int64
	columnKind, alt cqltype.Type // directInt: the codec's types
	kind            directKind
	ascii           bool
}

// directParamOp compiles f, already compiled for its column, into a direct op
// when its codec is a built-in one, and reports whether it did.
func (f inputField[P, V]) directParamOp() (paramOp[P], bool) {
	op := paramOp[P]{get: funcPointer(&f.get), field: f}
	switch c := any(f.codec).(type) {
	case scalarCodec[V]:
		width := integerWidth[V]()
		if width == 0 || !isIntegerKind(c.kind, width) || !c.compiled {
			return paramOp[P]{}, false
		}
		op.kind, op.width, op.columnKind, op.alt = directInt, width, c.kind, c.alternative
	case byteSliceCodec:
		if !c.compiled {
			return paramOp[P]{}, false
		}
		op.kind = directBytes
	case stringCodec:
		if !c.compiled {
			return paramOp[P]{}, false
		}
		op.kind, op.ascii = directString, c.ascii
	default:
		return paramOp[P]{}, false
	}
	return op, true
}

// encodeDirect writes op's parameter like WriteParam with the field's codec,
// writing the cell itself. A writer state the op does not cover, such as an
// invalid column type, goes through WriteParam for its exact errors.
func (op *paramOp[P]) encodeDirect(w *ParamWriter, value P) error {
	column, ok := w.columns.At(w.index)
	if !ok || w.builder == nil || cqltype.ValidateDataType(column.Type) != nil || w.capture != nil && w.index >= len(w.capture) {
		return op.field.encode(w, value)
	}
	get := op.get
	start := w.builder.Len()
	switch op.kind {
	case directInt:
		if checkScalarType(column.Type, op.columnKind, op.alt) != nil {
			return op.field.encode(w, value)
		}
		p, err := w.builder.ScalarBytes(4 + int(op.width))
		if err != nil {
			return op.field.encode(w, value)
		}
		p[0], p[1], p[2], p[3] = 0, 0, 0, byte(op.width)
		switch op.width {
		case 1:
			p[4] = byte((*(*func(P) int8)(unsafe.Pointer(&get)))(value))
		case 2:
			v := uint16((*(*func(P) int16)(unsafe.Pointer(&get)))(value))
			p[4], p[5] = byte(v>>8), byte(v)
		case 4:
			v := uint32((*(*func(P) int32)(unsafe.Pointer(&get)))(value))
			p[4], p[5], p[6], p[7] = byte(v>>24), byte(v>>16), byte(v>>8), byte(v)
		default:
			v := uint64((*(*func(P) int64)(unsafe.Pointer(&get)))(value))
			for i := 0; i < 8; i++ {
				p[4+i] = byte(v >> (56 - 8*i))
			}
		}
	case directBytes:
		if column.Type.Kind() != cqltype.TypeBlob {
			return op.field.encode(w, value)
		}
		b := (*(*func(P) []byte)(unsafe.Pointer(&get)))(value)
		if err := op.writeBody(w, b, ""); err != nil {
			return err
		}
	default: // directString
		if checkTextType(column.Type, op.ascii) != nil {
			return op.field.encode(w, value)
		}
		s := (*(*func(P) string)(unsafe.Pointer(&get)))(value)
		if op.ascii {
			for i := 0; i < len(s); i++ {
				if s[i] >= utf8.RuneSelf {
					return fmt.Errorf("gocql: parameter %d (%s): %w", w.index, column.Name, errors.New("gocql: invalid ASCII"))
				}
			}
		} else if !utf8.ValidString(s) {
			return fmt.Errorf("gocql: parameter %d (%s): %w", w.index, column.Name, errors.New("gocql: invalid UTF-8"))
		}
		if err := op.writeBody(w, nil, s); err != nil {
			return err
		}
	}
	if w.capture != nil {
		w.capture[w.index] = ParamCell{Start: start + 4, End: w.builder.Len(), State: cqltype.Present}
	}
	w.index++
	return nil
}

// writeBody writes a present cell holding b, or s when b is nil, borrowing
// or copying it as ValueWriter.BorrowBytes and BorrowString do.
func (op *paramOp[P]) writeBody(w *ParamWriter, b []byte, s string) error {
	n := len(b)
	if op.kind == directString {
		n = len(s)
	}
	if int64(n) > math.MaxInt32 {
		column, _ := w.columns.At(w.index)
		return fmt.Errorf("gocql: parameter %d (%s): %w", w.index, column.Name, cqltype.TypeValidation("value", "cell length %d is outside signed 32-bit range", n))
	}
	mark := w.builder.Mark()
	if err := w.builder.Uint32(uint32(n)); err != nil {
		w.builder.Rollback(mark)
		return op.wrap(w, err)
	}
	var err error
	switch {
	case op.kind != directString && w.ownBorrowed:
		err = w.builder.Borrow(wire.Borrow(append([]byte(nil), b...)))
	case op.kind != directString:
		err = w.builder.Borrow(wire.Borrow(b))
	case w.ownBorrowed:
		err = w.builder.Borrow(wire.Borrow([]byte(s)))
	default:
		err = w.builder.Borrow(wire.BorrowString(s))
	}
	if err != nil {
		w.builder.Rollback(mark)
		return op.wrap(w, err)
	}
	return nil
}

func (op *paramOp[P]) wrap(w *ParamWriter, err error) error {
	column, _ := w.columns.At(w.index)
	return fmt.Errorf("gocql: parameter %d (%s): %w", w.index, column.Name, err)
}

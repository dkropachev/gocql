// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strings"
	"sync"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/structmap"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// Struct mappings derive a field's codec from its Go type, recursively: a
// leaf type has a built-in codec (the table below), a type with a CQLCodec
// method has its own, and slices, arrays, pointers, maps, Nullable,
// BindValue and Pair are built from the codecs of their parts. cmd/cqlgen
// applies the same rules to source types and spells the codec as nested
// constructor calls, such as cql.SliceCodec(cql.PointerCodec(cql.Int32Codec)).
//
// Reflection cannot instantiate a generic codec for a type it only finds at
// run time, so a composite field is coded by an rcodec: a codec of a Go
// type known by reflection, which reads and writes the value at a pointer.
// A leaf keeps its generic codec, so a scalar field still compiles into a
// direct op.

// rdir selects the directions an rcodec is compiled for.
type rdir uint8

const (
	encDir rdir = 1 << iota
	decDir
)

// rcodec encodes and decodes the value of one Go type at a pointer.
//
// unsafe: p always points to a value of the type the rcodec was derived
// for, or of a type with its layout (sameLayout): a struct field at its
// reflect offset, a slice or array element, or a value allocated with
// reflect.New of that type.
type rcodec interface {
	compile(t DataType, dir rdir) (rcodec, error)
	encode(w *ValueWriter, p unsafe.Pointer) error
	decode(r ValueReader, p unsafe.Pointer) error
	// clone writes a copy of *src that holds no borrowed storage to *dst;
	// dst may be src.
	clone(dst, src unsafe.Pointer)
	// detach makes *p independent of the response in place.
	detach(p unsafe.Pointer)
}

// rEncoder adapts an rcodec to the Encoder of the value's address, for the
// collection and parameter writers. A compiled rcodec keeps a *rEncoder of
// each part it encodes, so that passing one as an Encoder never allocates.
type rEncoder struct{ c rcodec }

// encoderOf returns c's Encoder.
func encoderOf(c rcodec) Encoder[unsafe.Pointer] { return &rEncoder{c: c} }

func (e *rEncoder) CheckType(t DataType) error {
	_, err := e.c.compile(t, encDir)
	return err
}
func (e *rEncoder) Encode(w *ValueWriter, p unsafe.Pointer) error { return e.c.encode(w, p) }

// derived is a field type's derived codec: codec for any position, and entry,
// when set, the field builders of its generic leaf codec, whose fields
// compile into direct ops.
type derived struct {
	entry   erasedEntry
	codec   rcodec
	decodes bool // false for a type that only binds parameters
}

// fieldEntry returns d's field builders.
func (d derived) fieldEntry() erasedEntry {
	if d.entry.param != nil {
		return d.entry
	}
	entry := erasedEntry{param: rParamOf(d.codec)}
	if d.decodes {
		entry.column = rColumnOf(d.codec)
	}
	return entry
}

// deriveCodec returns c as a derived leaf of its own type V.
func deriveCodec[V any](c Codec[V]) derived {
	return derived{
		entry:   erasedEntry{param: erasedParamOf(Encoder[V](c)), column: erasedColumnOf(Decoder[V](c))},
		codec:   leafR[V]{enc: c, dec: c},
		decodes: true,
	}
}

// deriveEncoder returns enc as a derived leaf that only binds parameters.
func deriveEncoder[V any](enc Encoder[V]) derived {
	return derived{entry: erasedEntry{param: erasedParamOf(enc)}, codec: leafR[V]{enc: enc}}
}

// fieldCodec is implemented by the package's codecs, so that a CQLCodec
// method returning one is used without reflection.
type fieldCodec interface{ fieldCodec() derived }

func (c recordCodec[T]) fieldCodec() derived       { return deriveCodec[T](c) }
func (c convertCodec[A, B]) fieldCodec() derived   { return deriveCodec[A](c) }
func (c scalarCodec[T]) fieldCodec() derived       { return deriveCodec[T](c) }
func (c nullableCodec[T]) fieldCodec() derived     { return deriveCodec[Nullable[T]](c) }
func (c pointerCodec[T]) fieldCodec() derived      { return deriveCodec[*T](c) }
func (c sliceCodec[T]) fieldCodec() derived        { return deriveCodec[[]T](c) }
func (c mapSliceCodec[K, V]) fieldCodec() derived  { return deriveCodec[[]Entry[K, V]](c) }
func (c goMapCodec[K, V]) fieldCodec() derived     { return deriveCodec[map[K]V](c) }
func (c emptyCodec[T]) fieldCodec() derived        { return deriveCodec[MaybeEmpty[T]](c) }
func (c stringCodec) fieldCodec() derived          { return deriveCodec[string](c) }
func (c textCodec) fieldCodec() derived            { return deriveCodec[Text](c) }
func (c byteSliceCodec) fieldCodec() derived       { return deriveCodec[[]byte](c) }
func (c varintBigIntCodec) fieldCodec() derived    { return deriveCodec[*big.Int](c) }
func (c dynamicCodec) fieldCodec() derived         { return deriveCodec[Value](c) }
func (c convertEncoder[A, B]) fieldCodec() derived { return deriveEncoder[A](c) }

// leafType is one Go type of the built-in codec table.
type leafType struct {
	derive func() derived
	structmap.BuiltinFieldType
}

func leaf[V any](c Codec[V], typeSrc, name string, aliases ...string) leafType {
	return leafType{
		BuiltinFieldType: structmap.BuiltinFieldType{
			Type: reflect.TypeFor[V](), TypeSrc: typeSrc,
			EncoderSrc: "cql." + name, DecoderSrc: "cql." + name,
			Names: append([]string{name}, aliases...), Default: true,
		},
		derive: sync.OnceValue(func() derived { return deriveCodec(c) }),
	}
}

// leafTypes is the built-in codec table: the Go types a struct mapping maps
// without configuration, each by one codec, which a `codec=` tag names. A
// named type maps like the predeclared type it is defined from, and slices,
// arrays, pointers, maps, Nullable, BindValue and Pair are derived from it.
var leafTypes = sync.OnceValue(func() []leafType {
	return []leafType{
		leaf(BoolCodec, "bool", "BoolCodec"),
		leaf(Int8Codec, "int8", "Int8Codec"),
		leaf(Int16Codec, "int16", "Int16Codec"),
		leaf(Int32Codec, "int32", "Int32Codec"),
		leaf(Int64Codec, "int64", "Int64Codec"),
		leaf(IntCodec, "int", "IntCodec"),
		leaf(Float32Codec, "float32", "Float32Codec"),
		leaf(Float64Codec, "float64", "Float64Codec"),
		// ASCIIStringCodec names StringCodec, which maps ascii too, in a tag.
		leaf(StringCodec, "string", "StringCodec", "ASCIIStringCodec"),
		leaf(TextViewCodec, "cql.Text", "TextViewCodec"),
		leaf(ByteSliceCodec, "[]byte", "ByteSliceCodec"),
		leaf(BytesViewCodec, "cql.Bytes", "BytesViewCodec"),
		leaf(UUIDCodec, "cql.UUID", "UUIDCodec"),
		leaf(TimeUUIDCodec, "cql.TimeUUID", "TimeUUIDCodec"),
		leaf(InetCodec, "netip.Addr", "InetCodec"),
		leaf(VarintCodec, "cql.Varint", "VarintCodec"),
		leaf(VarintBigIntCodec, "*big.Int", "VarintBigIntCodec"),
		leaf(DecimalCodec, "cql.Decimal", "DecimalCodec"),
		leaf(DateCodec, "cql.Date", "DateCodec"),
		leaf(TimeOfDayCodec, "cql.TimeOfDay", "TimeOfDayCodec"),
		leaf(TimestampCodec, "cql.Timestamp", "TimestampCodec"),
		leaf(DurationCodec, "cql.Duration", "DurationCodec"),
		leaf(TimestampTimeCodec, "time.Time", "TimestampTimeCodec"),
		leaf(TimeDurationCodec, "time.Duration", "TimeDurationCodec"),
	}
})

// sequenceTags are the `codec=` tags that choose the collection kind of a
// slice or array field: the outermost one.
var sequenceTags = map[string]Kind{"ListCodec": KindList, "SetCodec": KindSet, "VectorCodec": KindVector}

// leafNames reports whether name is a `codec=` tag of the built-in table.
func leafNames() map[string]bool {
	names := map[string]bool{}
	for _, l := range leafTypes() {
		for _, n := range l.Names {
			names[n] = true
		}
	}
	for n := range sequenceTags {
		names[n] = true
	}
	return names
}

var (
	nullableName = reflect.TypeFor[Nullable[int]]()
	bindName     = reflect.TypeFor[BindValue[int]]()
	pairName     = reflect.TypeFor[Pair[int, int]]()
)

// genericOf reports whether t instantiates the generic type that example
// instantiates, such as Nullable.
func genericOf(t, example reflect.Type) bool {
	base := func(name string) string {
		if i := strings.IndexByte(name, '['); i > 0 {
			return name[:i]
		}
		return ""
	}
	b := base(t.Name())
	return b != "" && b == base(example.Name()) && t.PkgPath() == example.PkgPath() && t.Kind() == reflect.Struct
}

// cqlCodecMethod returns the codec of t's CQLCodec method, declared on t or
// *t as func() cql.Codec[t], or false.
func cqlCodecMethod(t reflect.Type) (any, bool, error) {
	m, ok := reflect.PointerTo(t).MethodByName("CQLCodec")
	if !ok {
		return nil, false, nil
	}
	want := reflect.TypeFor[Codec[int]]()
	ft := m.Type
	if ft.NumIn() != 1 || ft.NumOut() != 1 || ft.Out(0).Kind() != reflect.Interface || !genericInterfaceOf(ft.Out(0), want, t) {
		return nil, false, fmt.Errorf("type %s: method CQLCodec must be func() cql.Codec[%s]", t, t)
	}
	out := reflect.New(t).MethodByName("CQLCodec").Call(nil)[0]
	if out.IsNil() {
		return nil, false, fmt.Errorf("type %s: CQLCodec returned a nil codec", t)
	}
	return out.Interface(), true, nil
}

// genericInterfaceOf reports whether iface is Codec[arg], given example, an
// instance of Codec.
func genericInterfaceOf(iface, example, arg reflect.Type) bool {
	if iface.PkgPath() != example.PkgPath() || !strings.HasPrefix(iface.Name(), "Codec[") {
		return false
	}
	enc, ok := iface.MethodByName("Encode")
	return ok && enc.Type.NumIn() == 2 && enc.Type.In(1) == arg
}

// deriver derives the codecs of one field's type.
type deriver struct {
	options map[reflect.Type]derived
	leafTag string // a built-in codec name from the field's `codec=` tag
	depth   int    // of the type being derived; the field's own type is 1
	tagUsed bool
}

// deriveField derives the codec of a field of type t with `codec=` tag tag,
// as ParamsOf and RowOf map it: a built-in codec the tag names, else an
// option for t (WithCodec), else t's CQLCodec method, else the built-in
// derivation.
func deriveField(t reflect.Type, tag string, options map[reflect.Type]derived) (derived, error) {
	d := &deriver{options: options}
	var forced Kind
	switch kind, ok := sequenceTags[tag]; {
	case ok:
		forced = kind
		d.tagUsed = true
	case tag != "" && leafNames()[tag]:
		d.leafTag = tag
	case tag != "":
		// A Go expression, which only cmd/cqlgen evaluates: reflection maps
		// the field with the codec derived from an option for its type, or
		// for a type it is derived from, or from a CQLCodec method.
		if derivesFrom(t, options) || hasCQLCodec(t) {
			return d.derive(t, 0)
		}
		return derived{}, fmt.Errorf("codec %q is not a built-in codec; pass cql.WithCodec for %s, or give it a CQLCodec method", tag, t)
	}
	out, err := d.derive(t, forced)
	if err != nil {
		return derived{}, err
	}
	if tag != "" && !d.tagUsed {
		return derived{}, fmt.Errorf("codec %q is not a built-in codec for %s", tag, t)
	}
	return out, nil
}

// hasCQLCodec reports whether t, or a type t is derived from, has a CQLCodec
// method.
func hasCQLCodec(t reflect.Type) bool {
	if _, ok := reflect.PointerTo(t).MethodByName("CQLCodec"); ok {
		return true
	}
	switch t.Kind() {
	case reflect.Slice, reflect.Array, reflect.Pointer:
		return hasCQLCodec(t.Elem())
	case reflect.Map:
		return hasCQLCodec(t.Key()) || hasCQLCodec(t.Elem())
	case reflect.Struct:
		if genericOf(t, nullableName) || genericOf(t, bindName) || genericOf(t, pairName) {
			for i := range t.NumField() {
				if hasCQLCodec(t.Field(i).Type) {
					return true
				}
			}
		}
	}
	return false
}

// leafCandidates returns the built-in leaves of t: its own type's, or those
// of the predeclared type a named t is defined from, or for an array of 16
// bytes UUID's.
func leafCandidates(t reflect.Type) []leafType {
	var candidates []leafType
	for _, l := range leafTypes() {
		if l.Type == t {
			candidates = append(candidates, l)
		}
	}
	if len(candidates) == 0 {
		for _, l := range leafTypes() {
			// A predeclared leaf, such as string, or an unnamed one, such
			// as []byte; a named type defined from another named type, such
			// as cql.UUID, does not map like it.
			if l.Type.PkgPath() == "" && sameLayout(t, l.Type) {
				candidates = append(candidates, l)
			}
		}
	}
	if len(candidates) == 0 && t.Kind() == reflect.Array && t.Len() == 16 && t.Elem().Kind() == reflect.Uint8 {
		for _, l := range leafTypes() {
			if l.Type == reflect.TypeFor[UUID]() {
				candidates = append(candidates, l)
			}
		}
	}
	return candidates
}

// leafFor returns t's built-in leaf: the one the field's tag names, or with
// tagged false the default.
func (d *deriver) leafFor(t reflect.Type, tagged bool) (derived, bool) {
	candidates := leafCandidates(t)
	if len(candidates) == 0 {
		return derived{}, false
	}
	types := make([]structmap.BuiltinFieldType, len(candidates))
	for i, c := range candidates {
		types[i] = c.BuiltinFieldType
	}
	if d.leafTag != "" {
		if i, ok := structmap.SelectBuiltin(types, d.leafTag); ok {
			d.tagUsed = true
			return candidates[i].derive(), true
		}
	}
	if tagged {
		return derived{}, false
	}
	i, _ := structmap.SelectBuiltin(types, "")
	return candidates[i].derive(), true
}

func (d *deriver) derive(t reflect.Type, forced Kind) (derived, error) {
	d.depth++
	defer func() { d.depth-- }()
	// A built-in tag takes precedence, then an option, then the table's own
	// types.
	if forced == 0 {
		if out, ok := d.leafFor(t, true); ok {
			return out, nil
		}
	}
	if o, ok := d.options[t]; ok {
		return o, nil
	}
	if forced == 0 {
		if out, ok := d.exactLeaf(t); ok {
			return out, nil
		}
		codec, ok, err := cqlCodecMethod(t)
		if err != nil {
			return derived{}, err
		}
		if ok {
			if f, ok := codec.(fieldCodec); ok {
				return f.fieldCodec(), nil
			}
			return reflectDerived(t, codec), nil
		}
		if out, ok := d.leafFor(t, false); ok {
			return out, nil
		}
	}
	switch t.Kind() {
	case reflect.Slice, reflect.Array:
		elem, err := d.derive(t.Elem(), 0)
		if err != nil {
			return derived{}, err
		}
		kind := forced
		if kind == 0 {
			kind = anySequence
		}
		c := sequenceR{elem: elem.codec, typ: t, esize: t.Elem().Size(), kind: kind, length: -1}
		if t.Kind() == reflect.Array {
			c.length = t.Len()
		}
		return derived{codec: c, decodes: elem.decodes}, nil
	}
	if forced != 0 {
		return derived{}, fmt.Errorf("codec %s needs a slice or array, not %s", sequenceName(forced), t)
	}
	switch t.Kind() {
	case reflect.Pointer:
		elem, err := d.derive(t.Elem(), 0)
		if err != nil {
			return derived{}, err
		}
		return derived{codec: pointerR{elem: elem.codec, typ: t.Elem()}, decodes: elem.decodes}, nil
	case reflect.Map:
		key, err := d.derive(t.Key(), 0)
		if err != nil {
			return derived{}, fmt.Errorf("map key: %w", err)
		}
		value, err := d.derive(t.Elem(), 0)
		if err != nil {
			return derived{}, fmt.Errorf("map value: %w", err)
		}
		return derived{codec: mapR{key: key.codec, value: value.codec, typ: t}, decodes: key.decodes && value.decodes}, nil
	case reflect.Struct:
		switch {
		case genericOf(t, nullableName):
			elem, err := d.derive(t.Field(0).Type, 0)
			if err != nil {
				return derived{}, err
			}
			return derived{codec: nullableR{elem: elem.codec, typ: t, valid: t.Field(1).Offset}, decodes: elem.decodes}, nil
		case genericOf(t, bindName):
			if d.depth > 1 {
				return derived{}, fmt.Errorf("type %s: BindValue maps only a field's own type", t)
			}
			elem, err := d.derive(t.Field(0).Type, 0)
			if err != nil {
				return derived{}, err
			}
			return derived{codec: bindR{elem: elem.codec, state: t.Field(1).Offset}}, nil
		case genericOf(t, pairName):
			a, err := d.derive(t.Field(0).Type, 0)
			if err != nil {
				return derived{}, err
			}
			b, err := d.derive(t.Field(1).Type, 0)
			if err != nil {
				return derived{}, err
			}
			return derived{codec: pairR{a: a.codec, b: b.codec, second: t.Field(1).Offset}, decodes: a.decodes && b.decodes}, nil
		}
	}
	return derived{}, fmt.Errorf("no codec for type %s; give it a CQLCodec method, pass cql.WithCodec, or use a supported type", t)
}

// exactLeaf returns the built-in leaf of exactly t, which takes precedence
// over a CQLCodec method only for the table's own types.
func (d *deriver) exactLeaf(t reflect.Type) (derived, bool) {
	for _, l := range leafTypes() {
		if l.Type == t {
			return d.leafFor(t, false)
		}
	}
	return derived{}, false
}

func sequenceName(k Kind) string {
	for name, kind := range sequenceTags {
		if kind == k {
			return name
		}
	}
	return k.String()
}

// leafR is the rcodec of a generic codec of V.
type leafR[V any] struct {
	enc  Encoder[V]
	dec  Decoder[V]
	into valueIntoDecoder[V]
}

func (c leafR[V]) compile(t DataType, dir rdir) (rcodec, error) {
	var err error
	if dir&encDir != 0 {
		if c.enc == nil {
			return nil, typeValidation("codec", "%s has no encoder", reflect.TypeFor[V]())
		}
		if c.enc, err = compileValueEncoder(c.enc, t); err != nil {
			return nil, err
		}
	}
	if dir&decDir != 0 {
		if c.dec == nil {
			return nil, typeValidation("codec", "type %s only binds parameters", reflect.TypeFor[V]())
		}
		if c.dec, err = compileValueDecoder(c.dec, t); err != nil {
			return nil, err
		}
		c.into, _ = c.dec.(valueIntoDecoder[V])
	}
	return c, nil
}
func (c leafR[V]) encode(w *ValueWriter, p unsafe.Pointer) error { return c.enc.Encode(w, *(*V)(p)) }
func (c leafR[V]) decode(r ValueReader, p unsafe.Pointer) error {
	return decodeResolved(c.dec, c.into, r, (*V)(p))
}
func (c leafR[V]) clone(dst, src unsafe.Pointer) {
	if c.dec == nil {
		*(*V)(dst) = *(*V)(src)
		return
	}
	*(*V)(dst) = c.dec.Clone(*(*V)(src))
}
func (c leafR[V]) detach(p unsafe.Pointer) {
	if c.dec != nil {
		detachValue(c.dec, (*V)(p))
	}
}

// reflectR is the rcodec of an application codec of a type known only by
// reflection, found through its CQLCodec method; it calls the codec's
// methods through reflect.
type reflectR struct {
	codec         any
	typ           reflect.Type
	enc, dec, cln reflect.Value
}

func reflectDerived(t reflect.Type, codec any) derived {
	v := reflect.ValueOf(codec)
	c := reflectR{codec: codec, enc: v.MethodByName("Encode"), dec: v.MethodByName("Decode"), cln: v.MethodByName("Clone"), typ: t}
	return derived{codec: c, decodes: true}
}

func (c reflectR) compile(t DataType, _ rdir) (rcodec, error) {
	checker, ok := c.codec.(interface{ CheckType(DataType) error })
	if !ok {
		return nil, typeValidation("codec", "%T has no CheckType method", c.codec)
	}
	return c, checker.CheckType(t)
}
func (c reflectR) encode(w *ValueWriter, p unsafe.Pointer) error {
	out := c.enc.Call([]reflect.Value{reflect.ValueOf(w), reflect.NewAt(c.typ, p).Elem()})
	err, _ := out[0].Interface().(error)
	return err
}
func (c reflectR) decode(r ValueReader, p unsafe.Pointer) error {
	out := c.dec.Call([]reflect.Value{reflect.ValueOf(r)})
	if err, _ := out[1].Interface().(error); err != nil {
		return err
	}
	reflect.NewAt(c.typ, p).Elem().Set(out[0])
	return nil
}
func (c reflectR) clone(dst, src unsafe.Pointer) {
	out := c.cln.Call([]reflect.Value{reflect.NewAt(c.typ, src).Elem()})
	reflect.NewAt(c.typ, dst).Elem().Set(out[0])
}
func (c reflectR) detach(p unsafe.Pointer) { c.clone(p, p) }

// sequenceR codes a slice, or an array of length elements, as a list, a set
// or a vector: kind, or whichever the column is for anySequence.
type sequenceR struct {
	elem    rcodec
	elemEnc Encoder[unsafe.Pointer] // elem's, set by compile
	typ     reflect.Type            // the slice or array type
	// ctyp, etyp and width are set by compile: the column's type, its
	// element type and a vector element's fixed width.
	ctyp   DataType
	etyp   DataType
	esize  uintptr
	length int // an array's length, or -1 for a slice
	width  int64
	kind   Kind
}

func (c sequenceR) compile(t DataType, dir rdir) (rcodec, error) {
	element, width, err := sequenceElement(t, c.kind)
	if err != nil {
		return nil, err
	}
	elem, err := c.elem.compile(element, dir)
	if err != nil {
		return nil, fmt.Errorf("gocql: element type: %w", err)
	}
	c.elem, c.ctyp, c.etyp, c.width = elem, t, element, width
	if dir&encDir != 0 {
		c.elemEnc = encoderOf(elem)
	}
	return c, nil
}

// elements returns the address and count of the elements at p.
func (c sequenceR) elements(p unsafe.Pointer) (unsafe.Pointer, int) {
	if c.length >= 0 {
		return p, c.length
	}
	// Every slice header has the same layout (see bufferReset).
	s := *(*[]byte)(p)
	return unsafe.Pointer(unsafe.SliceData(s)), len(s)
}

func (c sequenceR) encode(w *ValueWriter, p unsafe.Pointer) error {
	if !c.ctyp.Equal(w.Type()) {
		return typeValidation("type", "sequence metadata changed after compilation")
	}
	data, n := c.elements(p)
	enc := c.elemEnc
	if c.ctyp.Kind() == KindVector {
		dimension, _ := c.ctyp.Dimension()
		if n != int(dimension) {
			return typeValidation("dimension", "vector requires %d elements, got %d", dimension, n)
		}
		for i := range n {
			if err := writeVectorElement(w, c.etyp, c.width, enc, unsafe.Add(data, uintptr(i)*c.esize)); err != nil {
				return fmt.Errorf("gocql: vector element %d: %w", i, err)
			}
		}
		return nil
	}
	if uint64(n) > math.MaxInt32 {
		return typeValidation("count", "collection has too many elements")
	}
	if err := w.Uint32(uint32(n)); err != nil {
		return err
	}
	for i := range n {
		if err := writeCollectionElement(w, c.etyp, enc, unsafe.Add(data, uintptr(i)*c.esize)); err != nil {
			return fmt.Errorf("gocql: element %d: %w", i, err)
		}
	}
	return nil
}

func (c sequenceR) decode(r ValueReader, p unsafe.Pointer) error {
	if !c.ctyp.Equal(r.Type()) {
		return typeValidation("type", "sequence metadata changed after compilation")
	}
	kind := c.ctyp.Kind()
	count := 0
	var body Bytes
	if kind != KindVector && r.err == nil && r.State() == Null {
		if err := checkBytesAlive(r.Bytes()); err != nil {
			return err
		}
	} else {
		var err error
		if body, count, _, err = validateSequenceLayout(r, kind, c.etyp, c.width); err != nil {
			return err
		}
	}
	var data unsafe.Pointer
	if c.length >= 0 {
		if count != c.length {
			return typeValidation("length", "%s has %d elements, %s needs %d", kind, count, c.typ, c.length)
		}
		data = p
	} else {
		v := reflect.NewAt(c.typ, p).Elem()
		if v.Cap() < count {
			// Grow allocates only the backing array, where MakeSlice would
			// allocate a slice header too.
			v.SetZero()
			v.Grow(count)
		}
		v.SetLen(count)
		data = v.UnsafePointer()
	}
	cursor := ValueReader{body: bytesChain(body), lease: bytesLease(body)}.cursorValue()
	for i := range count {
		var child ValueReader
		var err error
		if kind == KindVector {
			child, err = readVectorElement(&cursor, c.etyp, c.width)
		} else {
			child, err = validatedChild(&cursor, c.etyp)
		}
		if err == nil {
			err = c.elem.decode(child, unsafe.Add(data, uintptr(i)*c.esize))
		}
		if err != nil {
			return fmt.Errorf("gocql: element %d: %w", i, err)
		}
	}
	return nil
}

func (c sequenceR) clone(dst, src unsafe.Pointer) {
	if c.length >= 0 {
		for i := range c.length {
			off := uintptr(i) * c.esize
			c.elem.clone(unsafe.Add(dst, off), unsafe.Add(src, off))
		}
		return
	}
	in := reflect.NewAt(c.typ, src).Elem()
	if in.IsNil() {
		reflect.NewAt(c.typ, dst).Elem().SetZero()
		return
	}
	n := in.Len()
	out := reflect.MakeSlice(c.typ, n, n)
	from, to := in.UnsafePointer(), out.UnsafePointer()
	for i := range n {
		off := uintptr(i) * c.esize
		c.elem.clone(unsafe.Add(to, off), unsafe.Add(from, off))
	}
	reflect.NewAt(c.typ, dst).Elem().Set(out)
}

func (c sequenceR) detach(p unsafe.Pointer) {
	data, n := c.elements(p)
	for i := range n {
		c.elem.detach(unsafe.Add(data, uintptr(i)*c.esize))
	}
}

// pointerR codes a *T, with NULL as nil.
type pointerR struct {
	elem rcodec
	typ  reflect.Type // T
}

func (c pointerR) compile(t DataType, dir rdir) (rcodec, error) {
	elem, err := c.elem.compile(t, dir)
	if err != nil {
		return nil, err
	}
	c.elem = elem
	return c, nil
}
func (c pointerR) encode(w *ValueWriter, p unsafe.Pointer) error {
	v := *(*unsafe.Pointer)(p)
	if v == nil {
		return w.Null()
	}
	return c.elem.encode(w, v)
}
func (c pointerR) decode(r ValueReader, p unsafe.Pointer) error {
	if r.err != nil {
		return r.err
	}
	if r.State() == Null {
		*(*unsafe.Pointer)(p) = nil
		return checkBytesAlive(r.Bytes())
	}
	v := reflect.New(c.typ).UnsafePointer()
	if err := c.elem.decode(r, v); err != nil {
		return err
	}
	*(*unsafe.Pointer)(p) = v
	return nil
}
func (c pointerR) clone(dst, src unsafe.Pointer) {
	from := *(*unsafe.Pointer)(src)
	if from == nil {
		*(*unsafe.Pointer)(dst) = nil
		return
	}
	v := reflect.New(c.typ).UnsafePointer()
	c.elem.clone(v, from)
	*(*unsafe.Pointer)(dst) = v
}
func (c pointerR) detach(p unsafe.Pointer) {
	if v := *(*unsafe.Pointer)(p); v != nil {
		c.elem.detach(v)
	}
}

// nullableR codes a Nullable[T], whose Value is at offset 0 and Valid at
// valid.
type nullableR struct {
	elem  rcodec
	typ   reflect.Type
	valid uintptr
}

func (c nullableR) compile(t DataType, dir rdir) (rcodec, error) {
	elem, err := c.elem.compile(t, dir)
	if err != nil {
		return nil, err
	}
	c.elem = elem
	return c, nil
}
func (c nullableR) encode(w *ValueWriter, p unsafe.Pointer) error {
	if !*(*bool)(unsafe.Add(p, c.valid)) {
		return w.Null()
	}
	return c.elem.encode(w, p)
}
func (c nullableR) decode(r ValueReader, p unsafe.Pointer) error {
	if r.err != nil {
		return r.err
	}
	if r.State() == Null {
		reflect.NewAt(c.typ, p).Elem().SetZero()
		return checkBytesAlive(r.Bytes())
	}
	if err := c.elem.decode(r, p); err != nil {
		return err
	}
	*(*bool)(unsafe.Add(p, c.valid)) = true
	return nil
}
func (c nullableR) clone(dst, src unsafe.Pointer) {
	if !*(*bool)(unsafe.Add(src, c.valid)) {
		reflect.NewAt(c.typ, dst).Elem().SetZero()
		return
	}
	c.elem.clone(dst, src)
	*(*bool)(unsafe.Add(dst, c.valid)) = true
}
func (c nullableR) detach(p unsafe.Pointer) {
	if *(*bool)(unsafe.Add(p, c.valid)) {
		c.elem.detach(p)
	}
}

// bindR codes a BindValue[T], whose Value is at offset 0 and State at state;
// it only encodes.
type bindR struct {
	elem  rcodec
	state uintptr
}

func (c bindR) compile(t DataType, dir rdir) (rcodec, error) {
	if dir&decDir != 0 {
		return nil, typeValidation("codec", "BindValue only binds parameters")
	}
	elem, err := c.elem.compile(t, dir)
	if err != nil {
		return nil, err
	}
	c.elem = elem
	return c, nil
}
func (c bindR) encode(w *ValueWriter, p unsafe.Pointer) error {
	switch *(*CellState)(unsafe.Add(p, c.state)) {
	case Present:
		return c.elem.encode(w, p)
	case Null:
		return w.Null()
	case Unset:
		return w.Unset()
	default:
		return errors.New("gocql: invalid bind state")
	}
}
func (c bindR) decode(ValueReader, unsafe.Pointer) error {
	return typeValidation("codec", "BindValue only binds parameters")
}
func (c bindR) clone(dst, src unsafe.Pointer) {}
func (c bindR) detach(unsafe.Pointer)         {}

// mapR codes a Go map, as GoMapCodec does.
type mapR struct {
	key, value       rcodec
	keyEnc, valueEnc Encoder[unsafe.Pointer] // set by compile
	typ              reflect.Type
	// Set by compile.
	ctyp, ktyp, vtyp DataType
}

func (c mapR) compile(t DataType, dir rdir) (rcodec, error) {
	kt, vt, ok := t.KeyValue()
	if !ok {
		return nil, typeValidation("type", "expected map, got %s", t)
	}
	key, err := c.key.compile(kt, dir)
	if err != nil {
		return nil, fmt.Errorf("gocql: map key type: %w", err)
	}
	value, err := c.value.compile(vt, dir)
	if err != nil {
		return nil, fmt.Errorf("gocql: map value type: %w", err)
	}
	c.key, c.value, c.ctyp, c.ktyp, c.vtyp = key, value, t, kt, vt
	if dir&encDir != 0 {
		c.keyEnc, c.valueEnc = encoderOf(key), encoderOf(value)
	}
	return c, nil
}
func (c mapR) encode(w *ValueWriter, p unsafe.Pointer) error {
	if !c.ctyp.Equal(w.Type()) {
		return typeValidation("type", "map metadata changed after compilation")
	}
	m := reflect.NewAt(c.typ, p).Elem()
	if uint64(m.Len()) > math.MaxInt32 {
		return typeValidation("count", "map has too many entries")
	}
	if err := w.Uint32(uint32(m.Len())); err != nil {
		return err
	}
	k, v := reflect.New(c.typ.Key()).Elem(), reflect.New(c.typ.Elem()).Elem()
	for it := m.MapRange(); it.Next(); {
		k.SetIterKey(it)
		v.SetIterValue(it)
		if err := writeCollectionElement(w, c.ktyp, c.keyEnc, k.Addr().UnsafePointer()); err != nil {
			return fmt.Errorf("gocql: map key: %w", err)
		}
		if err := writeCollectionElement(w, c.vtyp, c.valueEnc, v.Addr().UnsafePointer()); err != nil {
			return fmt.Errorf("gocql: map value: %w", err)
		}
	}
	return nil
}
func (c mapR) decode(r ValueReader, p unsafe.Pointer) error {
	if !c.ctyp.Equal(r.Type()) {
		return typeValidation("type", "map metadata changed after compilation")
	}
	m := reflect.NewAt(c.typ, p).Elem()
	if r.err == nil && r.State() == Null {
		if err := checkBytesAlive(r.Bytes()); err != nil {
			return err
		}
		if !m.IsNil() {
			m.Clear()
		}
		return nil
	}
	body, count, err := validateMapBody(r)
	if err != nil {
		return err
	}
	if m.IsNil() {
		m.Set(reflect.MakeMapWithSize(c.typ, count))
	} else {
		m.Clear()
	}
	cursor := ValueReader{body: bytesChain(body), lease: bytesLease(body)}.cursorValue()
	k, v := reflect.New(c.typ.Key()).Elem(), reflect.New(c.typ.Elem()).Elem()
	for i := range count {
		// Zeroed for each entry, so that an entry never shares a decoded
		// slice's storage with the one before.
		k.SetZero()
		v.SetZero()
		kr, err := cursor.Child(c.ktyp)
		if err == nil {
			err = c.key.decode(kr, k.Addr().UnsafePointer())
		}
		if err != nil {
			return fmt.Errorf("gocql: map key %d: %w", i, err)
		}
		vr, err := cursor.Child(c.vtyp)
		if err == nil {
			err = c.value.decode(vr, v.Addr().UnsafePointer())
		}
		if err != nil {
			return fmt.Errorf("gocql: map value %d: %w", i, err)
		}
		m.SetMapIndex(k, v)
		if m.Len() != i+1 {
			return typeValidation("map", "key %d repeats an earlier key", i)
		}
	}
	return nil
}
func (c mapR) clone(dst, src unsafe.Pointer) {
	in := reflect.NewAt(c.typ, src).Elem()
	if in.IsNil() {
		reflect.NewAt(c.typ, dst).Elem().SetZero()
		return
	}
	out := reflect.MakeMapWithSize(c.typ, in.Len())
	k, v := reflect.New(c.typ.Key()).Elem(), reflect.New(c.typ.Elem()).Elem()
	ck, cv := reflect.New(c.typ.Key()).Elem(), reflect.New(c.typ.Elem()).Elem()
	for it := in.MapRange(); it.Next(); {
		k.SetIterKey(it)
		v.SetIterValue(it)
		ck.SetZero()
		cv.SetZero()
		c.key.clone(ck.Addr().UnsafePointer(), k.Addr().UnsafePointer())
		c.value.clone(cv.Addr().UnsafePointer(), v.Addr().UnsafePointer())
		out.SetMapIndex(ck, cv)
	}
	reflect.NewAt(c.typ, dst).Elem().Set(out)
}
func (c mapR) detach(p unsafe.Pointer) {
	// Keys are comparable values decoded into owned storage; values may
	// borrow, and are detached and stored back.
	m := reflect.NewAt(c.typ, p).Elem()
	if m.IsNil() {
		return
	}
	k, v := reflect.New(c.typ.Key()).Elem(), reflect.New(c.typ.Elem()).Elem()
	for it := m.MapRange(); it.Next(); {
		k.SetIterKey(it)
		v.SetIterValue(it)
		c.value.detach(v.Addr().UnsafePointer())
		m.SetMapIndex(k, v)
	}
}

// pairR codes a Pair[A, B] as a tuple of two members, as PairCodec does.
type pairR struct {
	a, b       rcodec
	aEnc, bEnc Encoder[unsafe.Pointer] // set by compile
	// ctyp and columns are set by compile.
	ctyp    DataType
	columns Columns
	second  uintptr // the offset of Second
}

func (c pairR) compile(t DataType, dir rdir) (rcodec, error) {
	columns, err := recordColumns(t, KindTuple)
	if err != nil {
		return nil, err
	}
	if columns.Len() != 2 {
		return nil, typeValidation("type", "a Pair maps a tuple of 2 members, got %s", t)
	}
	first, _ := columns.At(0)
	second, _ := columns.At(1)
	if c.a, err = c.a.compile(first.Type, dir); err != nil {
		return nil, recordMemberError(KindTuple, columnError(CodecStageCompile, 0, first.Name, err))
	}
	if c.b, err = c.b.compile(second.Type, dir); err != nil {
		return nil, recordMemberError(KindTuple, columnError(CodecStageCompile, 1, second.Name, err))
	}
	c.ctyp, c.columns = t, columns
	if dir&encDir != 0 {
		c.aEnc, c.bEnc = encoderOf(c.a), encoderOf(c.b)
	}
	return c, nil
}
func (c pairR) encode(w *ValueWriter, p unsafe.Pointer) error {
	if !c.ctyp.Equal(w.Type()) {
		return typeValidation("type", "tuple/UDT identity or fields changed after compilation")
	}
	if err := w.payload(); err != nil {
		return err
	}
	params := newParamWriter(w.builder, c.columns, false)
	params.ownBorrowed = w.ownBorrowed
	err := params.WriteParam(c.aEnc, p)
	if err == nil {
		err = params.WriteParam(c.bEnc, unsafe.Add(p, c.second))
	}
	if err != nil {
		return w.fail(recordMemberError(KindTuple, err))
	}
	return w.fail(finishParamWriter(params))
}
func (c pairR) decode(r ValueReader, p unsafe.Pointer) error {
	if !c.ctyp.Equal(r.Type()) {
		return typeValidation("type", "tuple/UDT identity or fields changed after compilation")
	}
	if err := requirePresent(r); err != nil {
		return err
	}
	if err := validateRecordBody(r, c.columns); err != nil {
		return recordMemberError(KindTuple, err)
	}
	cursor := wire.NewCursor(r.body)
	reader := RowReader{columns: c.columns, cursor: cursor, lease: r.lease, allowMissing: true}
	for i, member := range [...]struct {
		c rcodec
		p unsafe.Pointer
	}{{c.a, p}, {c.b, unsafe.Add(p, c.second)}} {
		v, err := reader.nextValue()
		if err == nil {
			err = member.c.decode(v, member.p)
		}
		if err != nil {
			column, _ := c.columns.At(i)
			return recordMemberError(KindTuple, columnError(CodecStageDecode, i, column.Name, err))
		}
	}
	if err := finishRowReader(&reader); err != nil {
		return err
	}
	return cursor.RequireEnd()
}
func (c pairR) clone(dst, src unsafe.Pointer) {
	c.a.clone(dst, src)
	c.b.clone(unsafe.Add(dst, c.second), unsafe.Add(src, c.second))
}
func (c pairR) detach(p unsafe.Pointer) {
	c.a.detach(p)
	c.b.detach(unsafe.Add(p, c.second))
}

// rParamOf and rColumnOf build the fields of an rcodec at a struct offset.
func rParamOf(c rcodec) func(string, uintptr) InputField[*erasedBase] {
	return func(name string, offset uintptr) InputField[*erasedBase] {
		return rParam{c: c, loc: fieldLocation{name: name}, offset: offset}
	}
}

func rColumnOf(c rcodec) func(string, uintptr) OutputField[erasedBase] {
	return func(name string, offset uintptr) OutputField[erasedBase] {
		return rColumn{c: c, loc: fieldLocation{name: name}, offset: offset}
	}
}

type rParam struct {
	c        rcodec
	enc      Encoder[unsafe.Pointer] // c's, set by compile
	loc      fieldLocation
	offset   uintptr
	compiled bool
}

func (f rParam) location() fieldLocation { return f.loc }
func (f rParam) compile(typ DataType) (InputField[*erasedBase], error) {
	c, err := f.c.compile(typ, encDir)
	if err != nil {
		return nil, err
	}
	f.c, f.enc, f.compiled = c, encoderOf(c), true
	return f, nil
}
func (f rParam) encode(w *ParamWriter, base *erasedBase) error {
	if !f.compiled {
		return typeValidation("field", "uncompiled struct field")
	}
	return w.WriteParam(f.enc, unsafe.Add(unsafe.Pointer(base), f.offset))
}

type rColumn struct {
	c        rcodec
	loc      fieldLocation
	offset   uintptr
	compiled bool
}

func (f rColumn) location() fieldLocation { return f.loc }
func (f rColumn) compile(typ DataType) (OutputField[erasedBase], error) {
	c, err := f.c.compile(typ, decDir)
	if err != nil {
		return nil, err
	}
	f.c, f.compiled = c, true
	return f, nil
}
func (f rColumn) decode(r *RowReader, base *erasedBase) error {
	if !f.compiled {
		return typeValidation("field", "uncompiled struct field")
	}
	v, err := r.nextValue()
	if err != nil {
		return err
	}
	return f.c.decode(v, unsafe.Add(unsafe.Pointer(base), f.offset))
}
func (f rColumn) clone(dst, source *erasedBase) {
	f.c.clone(unsafe.Add(unsafe.Pointer(dst), f.offset), unsafe.Add(unsafe.Pointer(source), f.offset))
}
func (f rColumn) detachField(row *erasedBase) {
	f.c.detach(unsafe.Add(unsafe.Pointer(row), f.offset))
}

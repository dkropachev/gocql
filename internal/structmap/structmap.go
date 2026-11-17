// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package structmap

import (
	"fmt"
	"reflect"
	"strings"
)

// FieldTag is a struct field's parsed `cql` tag.
type FieldTag struct {
	Column string // column name; the lowercased field name when untagged
	Codec  string // codec override: a built-in codec name, or (cqlgen only) a Go expression
	Stream string // streaming decoder override, like Codec, for StreamRowOf
}

// ParseFieldTag returns the column mapping of a struct field. ok is false for
// a field that maps to no column: an unexported one, or one tagged "-".
func ParseFieldTag(fieldName string, exported bool, tag reflect.StructTag) (FieldTag, bool, error) {
	value, tagged := tag.Lookup("cql")
	if value == "-" || !exported {
		return FieldTag{}, false, nil
	}
	parts := splitTag(value)
	result := FieldTag{Column: parts[0]}
	if !tagged || result.Column == "" {
		result.Column = strings.ToLower(fieldName)
	}
	for _, option := range parts[1:] {
		key, val, _ := strings.Cut(option, "=")
		switch {
		case key == "codec" && val != "":
			result.Codec = val
		case key == "stream" && val != "":
			result.Stream = val
		default:
			return FieldTag{}, false, fmt.Errorf("gocql: field %s: unknown cql tag option %q", fieldName, option)
		}
	}
	return result, true, nil
}

// splitTag splits a tag value at the commas outside brackets, so a codec
// expression such as MapCodec(StringCodec, Int64) stays one option.
func splitTag(value string) []string {
	var parts []string
	depth, start := 0, 0
	for i, r := range value {
		switch r {
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, value[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, value[start:])
}

// BuiltinFieldType is one Go field type mapped without configuration.
type BuiltinFieldType struct {
	Type reflect.Type
	// TypeSrc, EncoderSrc and DecoderSrc spell the type and codecs in Go
	// source, qualified with "cql." or "netip.".
	TypeSrc, EncoderSrc, DecoderSrc string
	// Names are the codec names a `codec=` tag selects this entry by.
	Names []string
	// Default marks the entry used for Type without a `codec=` tag.
	Default bool
}

// SelectBuiltin picks among the built-in entries for one Go type: the one
// named codec, or the default when codec is empty.
func SelectBuiltin(candidates []BuiltinFieldType, codec string) (int, bool) {
	for i, c := range candidates {
		if codec == "" && c.Default {
			return i, true
		}
		for _, name := range c.Names {
			if codec != "" && name == codec {
				return i, true
			}
		}
	}
	return -1, false
}

// Embedding says how a struct mapping treats an embedded field.
type Embedding int

const (
	EmbedField   Embedding = iota // an embedded non-struct: an ordinary field named after its type
	EmbedFlatten                  // a struct embedded by value: its fields are mapped as the outer struct's
	EmbedSkip                     // tagged `cql:"-"`
)

// EmbeddedField decides how to map an embedded field. Pointer embedding is
// rejected: its fields are not at a fixed offset, and decoding would have to
// allocate a nil one. A tag other than "-" is rejected too, since there is no
// column prefix to give.
func EmbeddedField(name string, tag reflect.StructTag, pointer, isStruct bool) (Embedding, error) {
	value, tagged := tag.Lookup("cql")
	switch {
	case value == "-":
		return EmbedSkip, nil
	case pointer:
		return 0, fmt.Errorf("embedded pointer field %s is not supported; embed it by value or tag it `cql:\"-\"`", name)
	case !isStruct:
		return EmbedField, nil
	case tagged:
		return 0, fmt.Errorf("embedded field %s: only the tag `cql:\"-\"` is allowed", name)
	}
	return EmbedFlatten, nil
}

// FieldCandidate is a mapped field found while walking a struct and the
// structs it embeds; Depth counts the embeddings above it.
type FieldCandidate struct {
	Column, Path string
	Depth        int
}

// ResolveFieldConflicts returns, in order, the candidates that map their
// column, like Go's field promotion: a shallower field hides deeper ones with
// the same column, and two at the same depth are an error.
func ResolveFieldConflicts(candidates []FieldCandidate) ([]int, error) {
	best := map[string]int{}
	for i, c := range candidates {
		j, ok := best[c.Column]
		switch {
		case !ok || c.Depth < candidates[j].Depth:
			best[c.Column] = i
		case c.Depth == candidates[j].Depth:
			return nil, fmt.Errorf("fields %s and %s both map column %q", candidates[j].Path, c.Path, c.Column)
		}
	}
	keep := make([]int, 0, len(best))
	for i, c := range candidates {
		if best[c.Column] == i {
			keep = append(keep, i)
		}
	}
	return keep, nil
}

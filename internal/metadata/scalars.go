// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"maps"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
)

func (InternalV2MetadataText) CheckType(t cqltype.DataType) error {
	if t.Kind() == cqltype.TypeAscii {
		return codec.ASCIICodec.CheckType(t)
	}
	return codec.TextCodec.CheckType(t)
}
func (InternalV2MetadataText) Decode(r codec.ValueReader) (string, error) {
	var text cqltype.Text
	var err error
	if r.Type().Kind() == cqltype.TypeAscii {
		text, err = codec.ASCIICodec.Decode(r)
	} else {
		text, err = codec.TextCodec.Decode(r)
	}
	if err != nil {
		return "", err
	}
	return text.String(), nil
}
func (InternalV2MetadataText) Clone(v string) string { return v }

func (InternalV2MetadataUUIDString) CheckType(t cqltype.DataType) error {
	return codec.UUIDCodec.CheckType(t)
}
func (InternalV2MetadataUUIDString) Decode(r codec.ValueReader) (string, error) {
	v, e := codec.UUIDCodec.Decode(r)
	if e != nil {
		return "", e
	}
	return v.String(), nil
}
func (InternalV2MetadataUUIDString) Clone(v string) string { return v }

func (InternalV2MetadataInt) CheckType(t cqltype.DataType) error { return codec.Int32.CheckType(t) }
func (InternalV2MetadataInt) Decode(r codec.ValueReader) (int, error) {
	v, e := codec.Int32.Decode(r)
	return int(v), e
}
func (InternalV2MetadataInt) Clone(v int) int { return v }

func (v2MetadataKind) CheckType(t cqltype.DataType) error {
	return (InternalV2MetadataText{}).CheckType(t)
}
func (v2MetadataKind) Decode(r codec.ValueReader) (ColumnKind, error) {
	v, e := (InternalV2MetadataText{}).Decode(r)
	if e != nil {
		return 0, e
	}
	return columnKindFromSchema(v)
}
func (v2MetadataKind) Clone(v ColumnKind) ColumnKind { return v }

func (v2MetadataStrings) CheckType(t cqltype.DataType) error {
	if t.Kind() == cqltype.TypeSet {
		return codec.SetDecoder[cqltype.Text](codec.TextCodec).CheckType(t)
	}
	return codec.ListDecoder[cqltype.Text](codec.TextCodec).CheckType(t)
}
func (v2MetadataStrings) Decode(r codec.ValueReader) ([]string, error) {
	decoder := codec.ListDecoder[cqltype.Text](codec.TextCodec)
	if r.Type().Kind() == cqltype.TypeSet {
		decoder = codec.SetDecoder[cqltype.Text](codec.TextCodec)
	}
	v, err := decoder.Decode(r)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(v))
	for _, value := range v {
		out = append(out, value.String())
	}
	return out, nil
}
func (v2MetadataStrings) Clone(v []string) []string { return append([]string(nil), v...) }

func (v2MetadataStringMap) CheckType(t cqltype.DataType) error {
	return codec.MapDecoder[cqltype.Text, cqltype.Text](codec.TextCodec, codec.TextCodec).CheckType(t)
}
func (v2MetadataStringMap) Decode(r codec.ValueReader) (map[string]string, error) {
	v, err := codec.MapDecoder[cqltype.Text, cqltype.Text](codec.TextCodec, codec.TextCodec).Decode(r)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(v))
	for _, entry := range v {
		key := entry.Key.String()
		if _, ok := out[key]; ok {
			return nil, cqltype.TypeValidation("schema map", "duplicate key %q", key)
		}
		out[key] = entry.Value.String()
	}
	return out, nil
}
func (v2MetadataStringMap) Clone(v map[string]string) map[string]string { return maps.Clone(v) }

func (v2MetadataExtensions) CheckType(t cqltype.DataType) error {
	return codec.MapDecoder[cqltype.Text, cqltype.Bytes](codec.TextCodec, codec.BlobCodec).CheckType(t)
}
func (v2MetadataExtensions) Decode(r codec.ValueReader) (map[string][]byte, error) {
	v, err := codec.MapDecoder[cqltype.Text, cqltype.Bytes](codec.TextCodec, codec.BlobCodec).Decode(r)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]byte, len(v))
	for _, entry := range v {
		key := entry.Key.String()
		if _, ok := out[key]; ok {
			return nil, cqltype.TypeValidation("schema extensions", "duplicate key %q", key)
		}
		out[key] = entry.Value.OwnedBytes()
	}
	return out, nil
}
func (v2MetadataExtensions) Clone(v map[string][]byte) map[string][]byte {
	return cloneV2Extensions(v)
}

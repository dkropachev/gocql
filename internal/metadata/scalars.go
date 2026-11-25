// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"maps"
	"strings"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/metadata"
)

func (InternalV2MetadataText) CheckType(t cql.DataType) error {
	return cql.TextCodec.CheckType(t)
}
func (InternalV2MetadataText) Decode(r cql.ValueReader) (string, error) {
	text, err := cql.TextCodec.Decode(r)
	if err != nil {
		return "", err
	}
	return text.String(), nil
}
func (InternalV2MetadataText) Clone(v string) string { return v }

func (InternalV2MetadataUUIDString) CheckType(t cql.DataType) error {
	return cql.UUIDCodec.CheckType(t)
}
func (InternalV2MetadataUUIDString) Decode(r cql.ValueReader) (string, error) {
	v, e := cql.UUIDCodec.Decode(r)
	if e != nil {
		return "", e
	}
	return v.String(), nil
}
func (InternalV2MetadataUUIDString) Clone(v string) string { return v }

func (InternalV2MetadataInt) CheckType(t cql.DataType) error { return cql.Int32Codec.CheckType(t) }
func (InternalV2MetadataInt) Decode(r cql.ValueReader) (int, error) {
	v, e := cql.Int32Codec.Decode(r)
	return int(v), e
}
func (InternalV2MetadataInt) Clone(v int) int { return v }

func (v2MetadataKind) CheckType(t cql.DataType) error {
	return (InternalV2MetadataText{}).CheckType(t)
}
func (v2MetadataKind) Decode(r cql.ValueReader) (metadata.ColumnKind, error) {
	v, e := (InternalV2MetadataText{}).Decode(r)
	if e != nil {
		return 0, e
	}
	return columnKindFromSchema(v)
}
func (v2MetadataKind) Clone(v metadata.ColumnKind) metadata.ColumnKind { return v }

func (v2MetadataClusteringOrder) CheckType(t cql.DataType) error {
	return (InternalV2MetadataText{}).CheckType(t)
}

// Decode reads system_schema.columns.clustering_order: "asc" or "desc" for a
// clustering column and "none" for any other.
func (v2MetadataClusteringOrder) Decode(r cql.ValueReader) (metadata.ColumnOrder, error) {
	v, e := (InternalV2MetadataText{}).Decode(r)
	if e != nil {
		return metadata.ColumnOrderNone, e
	}
	return columnOrderFromSchema(v), nil
}
func (v2MetadataClusteringOrder) Clone(v metadata.ColumnOrder) metadata.ColumnOrder { return v }

func columnOrderFromSchema(order string) metadata.ColumnOrder {
	switch strings.ToLower(order) {
	case "asc":
		return metadata.ColumnOrderAsc
	case "desc":
		return metadata.ColumnOrderDesc
	default:
		return metadata.ColumnOrderNone
	}
}

func (v2MetadataStrings) CheckType(t cql.DataType) error {
	if t.Kind() == cql.TypeSet {
		return cql.SetDecoder[cql.Text](cql.TextCodec).CheckType(t)
	}
	return cql.ListDecoder[cql.Text](cql.TextCodec).CheckType(t)
}
func (v2MetadataStrings) Decode(r cql.ValueReader) ([]string, error) {
	decoder := cql.ListDecoder[cql.Text](cql.TextCodec)
	if r.Type().Kind() == cql.TypeSet {
		decoder = cql.SetDecoder[cql.Text](cql.TextCodec)
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

func (v2MetadataStringMap) CheckType(t cql.DataType) error {
	return cql.MapDecoder[cql.Text, cql.Text](cql.TextCodec, cql.TextCodec).CheckType(t)
}
func (v2MetadataStringMap) Decode(r cql.ValueReader) (map[string]string, error) {
	v, err := cql.MapDecoder[cql.Text, cql.Text](cql.TextCodec, cql.TextCodec).Decode(r)
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

func (v2MetadataExtensions) CheckType(t cql.DataType) error {
	return cql.MapDecoder[cql.Text, cql.Bytes](cql.TextCodec, cql.BytesCodec).CheckType(t)
}
func (v2MetadataExtensions) Decode(r cql.ValueReader) (map[string][]byte, error) {
	v, err := cql.MapDecoder[cql.Text, cql.Bytes](cql.TextCodec, cql.BytesCodec).Decode(r)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]byte, len(v))
	for _, entry := range v {
		key, err := entry.Key.Copy()
		if err != nil {
			return nil, err
		}
		if _, ok := out[key]; ok {
			return nil, cqltype.TypeValidation("schema extensions", "duplicate key %q", key)
		}
		if out[key], err = entry.Value.Copy(); err != nil {
			return nil, err
		}
	}
	return out, nil
}
func (v2MetadataExtensions) Clone(v map[string][]byte) map[string][]byte {
	return cloneV2Extensions(v)
}

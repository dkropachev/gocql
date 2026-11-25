/*
 * Copyright (C) 2026 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package metadata_test

import (
	"reflect"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/metadata"
)

// differentValue returns a value of the same type as v that is never equal to
// it. The bool reports whether the kind is handled; callers fail rather than
// skip, so a field this helper cannot mutate surfaces as a test failure
// instead of a silent hole in the drift check.
func differentValue(v reflect.Value) (reflect.Value, bool) {
	switch v.Kind() {
	case reflect.String:
		return reflect.ValueOf(v.String() + "-changed").Convert(v.Type()), true
	case reflect.Bool:
		return reflect.ValueOf(!v.Bool()).Convert(v.Type()), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return reflect.ValueOf(v.Int() + 1).Convert(v.Type()), true
	case reflect.Float32, reflect.Float64:
		return reflect.ValueOf(v.Float() + 1).Convert(v.Type()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return reflect.ValueOf(v.Uint() + 1).Convert(v.Type()), true
	case reflect.Array:
		// A copy whose first element differs, such as a UUID's first byte.
		if v.Len() == 0 {
			return reflect.Value{}, false
		}
		changed := reflect.New(v.Type()).Elem()
		changed.Set(v)
		first, ok := differentValue(changed.Index(0))
		if !ok {
			return reflect.Value{}, false
		}
		changed.Index(0).Set(first)
		return changed, true
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return reflect.Value{}, false
		}
		// A copy with one extra entry. The added key changes the length, which
		// every map comparison in this file notices, and the zero element keeps
		// this independent of what the map holds.
		changed := reflect.MakeMap(v.Type())
		iter := v.MapRange()
		for iter.Next() {
			changed.SetMapIndex(iter.Key(), iter.Value())
		}
		key := reflect.ValueOf("added-key").Convert(v.Type().Key())
		changed.SetMapIndex(key, reflect.New(v.Type().Elem()).Elem())
		return changed, true
	case reflect.Slice:
		// Appending a zero element changes the length, for the same reason.
		return reflect.Append(v, reflect.New(v.Type().Elem()).Elem()), true
	case reflect.Struct:
		// Mutate the first field this helper can handle, so a nested struct is
		// compared through its own Equal rather than skipped.
		changed := reflect.New(v.Type())
		changed.Elem().Set(v)
		for i := 0; i < v.NumField(); i++ {
			if !v.Type().Field(i).IsExported() {
				continue
			}
			field := changed.Elem().Field(i)
			if nested, ok := differentValue(field); ok {
				field.Set(nested)
				return changed.Elem(), true
			}
		}
		return reflect.Value{}, false
	default:
		return reflect.Value{}, false
	}
}

// assertEveryFieldCompared mutates each exported field of *want in turn and
// requires equals to reject the result. There is deliberately no exclusion
// list: every exported field on all four of these structs is compared today,
// so a field that cannot be mutated is a finding rather than something to
// configure away.
func assertEveryFieldCompared(t *testing.T, want any, equals func(a, b any) bool) {
	t.Helper()

	typ := reflect.TypeOf(want).Elem()

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}

		t.Run(field.Name, func(t *testing.T) {
			mutated := reflect.New(typ)
			mutated.Elem().Set(reflect.ValueOf(want).Elem())

			target := mutated.Elem().Field(i)
			changed, ok := differentValue(target)
			if !ok {
				t.Fatalf("differentValue cannot mutate %s (kind %s); extend the helper "+
					"so this field stays covered by the drift check", field.Name, target.Kind())
			}
			target.Set(changed)

			if equals(want, mutated.Interface()) {
				t.Errorf("Equal() = true after changing %s; the field is not compared", field.Name)
			}
		})
	}
}

func populatedTableMetadataOptions() *metadata.TableOptions {
	return &metadata.TableOptions{
		Caching:                 map[string]string{"keys": "ALL"},
		Compaction:              map[string]string{"class": "SizeTieredCompactionStrategy"},
		Compression:             map[string]string{"sstable_compression": "LZ4Compressor"},
		CDC:                     map[string]string{"enabled": "true"},
		SpeculativeRetry:        "99p",
		Comment:                 "a table",
		Version:                 cql.UUID{3},
		Partitioner:             "org.apache.cassandra.dht.Murmur3Partitioner",
		GCGraceSeconds:          864000,
		MaxIndexInterval:        2048,
		MemtableFlushPeriodInMS: 1,
		MinIndexInterval:        128,
		ReadRepairChance:        0.1,
		BloomFilterFPChance:     0.01,
		DefaultTimeToLive:       60,
		DCLocalReadRepairChance: 0.2,
		CRCCheckChance:          0.9,
		InMemory:                true,
	}
}

func populatedColumnMetadata() *metadata.Column {
	return &metadata.Column{
		Keyspace:        "ks",
		Table:           "tbl",
		Name:            "col",
		Type:            "text",
		ClusteringOrder: metadata.ColumnOrderAsc,
		ComponentIndex:  1,
		Kind:            metadata.ColumnKindRegular,
	}
}

func populatedTableMetadata() *metadata.Table {
	partitionKey := populatedColumnMetadata()
	partitionKey.Name = "pk"
	clustering := populatedColumnMetadata()
	clustering.Name = "ck"

	return &metadata.Table{
		Keyspace:          "ks",
		Name:              "tbl",
		PartitionKey:      []*metadata.Column{partitionKey},
		ClusteringColumns: []*metadata.Column{clustering},
		Columns: map[string]*metadata.Column{
			"pk": partitionKey,
			"ck": clustering,
		},
		OrderedColumns: []*metadata.Column{partitionKey, clustering},
		Flags:          []string{"compound"},
		Extensions:     map[string][]byte{"scylla_encryption_options": []byte("blob")},
		Options:        *populatedTableMetadataOptions(),
	}
}

// deepCopyTableMetadata produces an independent value that must compare equal,
// so a test can mutate one side without disturbing the other.
func deepCopyTableMetadata(src *metadata.Table) *metadata.Table {
	dst := *src

	dst.PartitionKey = make([]*metadata.Column, len(src.PartitionKey))
	for i, c := range src.PartitionKey {
		col := *c
		dst.PartitionKey[i] = &col
	}
	dst.ClusteringColumns = make([]*metadata.Column, len(src.ClusteringColumns))
	for i, c := range src.ClusteringColumns {
		col := *c
		dst.ClusteringColumns[i] = &col
	}
	dst.Columns = make(map[string]*metadata.Column, len(src.Columns))
	for k, c := range src.Columns {
		col := *c
		dst.Columns[k] = &col
	}
	dst.OrderedColumns = make([]*metadata.Column, len(src.OrderedColumns))
	for i, c := range src.OrderedColumns {
		col := *c
		dst.OrderedColumns[i] = &col
	}
	dst.Flags = append([]string(nil), src.Flags...)
	dst.Extensions = make(map[string][]byte, len(src.Extensions))
	for k, v := range src.Extensions {
		dst.Extensions[k] = v
	}

	return &dst
}

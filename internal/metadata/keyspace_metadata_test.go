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

package metadata

import (
	"testing"

	"github.com/scylladb/gocql/v2/metadata"
)

func TestKeyspaceMetadata(t *testing.T) {
	t.Run("Compile", func(t *testing.T) {
		t.Parallel()

		keyspace := &metadata.Keyspace{
			Name: "V2Keyspace",
		}
		tables := []metadata.Table{
			{
				Keyspace: "V2Keyspace",
				Name:     "Table1",
			},
			{
				Keyspace: "V2Keyspace",
				Name:     "Table2",
			},
		}
		columns := []metadata.Column{
			{
				Keyspace:       "V2Keyspace",
				Table:          "Table1",
				Name:           "KEY1",
				Kind:           metadata.ColumnPartitionKey,
				ComponentIndex: 0,
				Type:           "text",
			},
			{
				Keyspace:       "V2Keyspace",
				Table:          "Table1",
				Name:           "Key1",
				Kind:           metadata.ColumnPartitionKey,
				ComponentIndex: 0,
				Type:           "text",
			},
			{
				Keyspace:       "V2Keyspace",
				Table:          "Table2",
				Name:           "Column1",
				Kind:           metadata.ColumnPartitionKey,
				ComponentIndex: 0,
				Type:           "text",
			},
			{
				Keyspace:       "V2Keyspace",
				Table:          "Table2",
				Name:           "Column2",
				Kind:           metadata.ColumnClusteringKey,
				ComponentIndex: 0,
				Type:           "text",
			},
			{
				Keyspace:        "V2Keyspace",
				Table:           "Table2",
				Name:            "Column3",
				Kind:            metadata.ColumnClusteringKey,
				ComponentIndex:  1,
				Type:            "text",
				ClusteringOrder: "desc",
			},
			{
				Keyspace: "V2Keyspace",
				Table:    "Table2",
				Name:     "Column4",
				Kind:     metadata.ColumnRegular,
				Type:     "text",
			},
			{
				Keyspace: "V2Keyspace",
				Table:    "view",
				Name:     "ColReg",
				Kind:     metadata.ColumnRegular,
				Type:     "text",
			},
			{
				Keyspace: "V2Keyspace",
				Table:    "view",
				Name:     "ColCK",
				Kind:     metadata.ColumnClusteringKey,
				Type:     "text",
			},
			{
				Keyspace: "V2Keyspace",
				Table:    "view",
				Name:     "ColPK",
				Kind:     metadata.ColumnPartitionKey,
				Type:     "text",
			},
			{
				Keyspace:        "V2Keyspace",
				Table:           "buckets_by_owner_index",
				Name:            "idx_token",
				Kind:            metadata.ColumnClusteringKey,
				ComponentIndex:  0,
				Type:            "bigint",
				ClusteringOrder: "asc",
			},
			{
				Keyspace:        "V2Keyspace",
				Table:           "buckets_by_owner_index",
				Name:            "name",
				Kind:            metadata.ColumnClusteringKey,
				ComponentIndex:  1,
				Type:            "text",
				ClusteringOrder: "asc",
			},
			{
				Keyspace: "V2Keyspace",
				Table:    "buckets_by_owner_index",
				Name:     "owner",
				Kind:     metadata.ColumnPartitionKey,
				Type:     "text",
			},
		}
		// Consider an index by column `owner` on the base table `buckets` with
		// partition key `name`.
		//
		// CREATE INDEX buckets_by_owner ON stg_msk_a.buckets(owner);
		indexes := []metadata.Index{
			{Name: "buckets_by_owner"},
		}
		views := []metadata.View{
			{
				KeyspaceName: "V2Keyspace",
				ViewName:     "view",
			},
			{
				KeyspaceName: "V2Keyspace",
				ViewName:     "buckets_by_owner_index",
			},
		}
		compileMetadata(keyspace, tables, columns, nil, nil, nil, indexes, views, nil)
		assertKeyspaceMetadata(
			t,
			keyspace,
			&metadata.Keyspace{
				Name: "V2Keyspace",
				Views: map[string]*metadata.View{
					"view": {
						PartitionKey: []*metadata.Column{
							{
								Name: "ColPK",
								Type: "text",
							},
						},
						ClusteringColumns: []*metadata.Column{
							{
								Name: "ColCK",
								Type: "text",
							},
						},
						OrderedColumns: []string{
							"ColPK", "ColCK", "ColReg",
						},
						Columns: map[string]*metadata.Column{
							"ColPK": {
								Name: "ColPK",
								Kind: metadata.ColumnPartitionKey,
								Type: "text",
							},
							"ColCK": {
								Name: "ColCK",
								Kind: metadata.ColumnClusteringKey,
								Type: "text",
							},
							"ColReg": {
								Name: "ColReg",
								Kind: metadata.ColumnRegular,
								Type: "text",
							},
						},
					},
				},
				Tables: map[string]*metadata.Table{
					"Table1": {
						PartitionKey: []*metadata.Column{
							{
								Name: "Key1",
								Type: "text",
							},
						},
						ClusteringColumns: []*metadata.Column{},
						Columns: map[string]*metadata.Column{
							"KEY1": {
								Name: "KEY1",
								Type: "text",
								Kind: metadata.ColumnPartitionKey,
							},
							"Key1": {
								Name: "Key1",
								Type: "text",
								Kind: metadata.ColumnPartitionKey,
							},
						},
						OrderedColumns: []string{
							"Key1",
						},
					},
					"Table2": {
						PartitionKey: []*metadata.Column{
							{
								Name: "Column1",
								Type: "text",
							},
						},
						ClusteringColumns: []*metadata.Column{
							{
								Name:  "Column2",
								Type:  "text",
								Order: metadata.ASC,
							},
							{
								Name:  "Column3",
								Type:  "text",
								Order: true,
							},
						},
						Columns: map[string]*metadata.Column{
							"Column1": {
								Name: "Column1",
								Type: "text",
								Kind: metadata.ColumnPartitionKey,
							},
							"Column2": {
								Name:  "Column2",
								Type:  "text",
								Order: metadata.ASC,
								Kind:  metadata.ColumnClusteringKey,
							},
							"Column3": {
								Name:  "Column3",
								Type:  "text",
								Order: true,
								Kind:  metadata.ColumnClusteringKey,
							},
							"Column4": {
								Name: "Column4",
								Type: "text",
								Kind: metadata.ColumnRegular,
							},
						},
						OrderedColumns: []string{
							"Column1", "Column2", "Column3", "Column4",
						},
					},
				},
				Indexes: map[string]*metadata.Index{
					"buckets_by_owner": {
						Name:      "buckets_by_owner",
						TableName: "buckets_by_owner_index",
						PartitionKey: []*metadata.Column{
							{Name: "owner", Type: "text"},
						},
						ClusteringColumns: []*metadata.Column{
							{Name: "idx_token", Type: "bigint"},
							{Name: "name", Type: "text"},
						},
						OrderedColumns: []string{
							"owner", "idx_token", "name",
						},
						Columns: map[string]*metadata.Column{
							"owner": {
								Name: "owner",
								Type: "text",
								Kind: metadata.ColumnPartitionKey,
							},
							"idx_token": {
								Name:  "idx_token",
								Type:  "bigint",
								Order: metadata.ASC,
								Kind:  metadata.ColumnClusteringKey,
							},
							"name": {
								Name:  "name",
								Type:  "text",
								Order: metadata.ASC,
								Kind:  metadata.ColumnClusteringKey,
							},
						},
					},
				},
			},
		)
	})
}

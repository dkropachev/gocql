// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"github.com/scylladb/gocql/v2/internal/link"
	"github.com/scylladb/gocql/v2/metadata"
)

// AggregateMetadataStorage is the state the schema loader keeps on a
// metadata.Aggregate: the names of its functions until they are resolved.
type AggregateMetadataStorage struct {
	InternalStateFunc string
	InternalFinalFunc string
}

// aggregateRep mirrors metadata.Aggregate.
type aggregateRep struct {
	privateStorage AggregateMetadataStorage
	Keyspace       string
	Name           string
	InitCond       string
	ReturnType     string
	StateType      string
	ArgumentTypes  []string
	FinalFunc      metadata.Function
	StateFunc      metadata.Function
}

// AggregateMetadataData returns the loader's state on v.
func AggregateMetadataData(v *metadata.Aggregate) *AggregateMetadataStorage {
	return &link.Mirror[aggregateRep](v).privateStorage
}

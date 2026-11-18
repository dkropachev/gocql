// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

// ScyllaConnectionFeatures is what one connection's SUPPORTED reported: the
// node's features plus the shard the connection landed on.
type ScyllaConnectionFeatures struct {
	ScyllaHostFeatures
	privateStorage ScyllaConnectionFeaturesStorage
}

func (f ScyllaConnectionFeatures) Shard() int {
	return f.privateStorage.InternalShard
}

// ScyllaHostFeatures are the ScyllaDB features a node advertises in SUPPORTED.
type ScyllaHostFeatures struct {
	privateStorage ScyllaHostFeaturesStorage
}

// IsPresent reports whether the host is a ScyllaDB node.
func (f ScyllaHostFeatures) IsPresent() bool {
	return f.privateStorage.InternalIsScylla
}

func (f ScyllaHostFeatures) Partitioner() string {
	return f.privateStorage.InternalPartitioner
}

func (f ScyllaHostFeatures) ShardingAlgorithm() string {
	return f.privateStorage.InternalShardingAlgorithm
}

func (f ScyllaHostFeatures) ShardsCount() int {
	return f.privateStorage.InternalNrShards
}

func (f ScyllaHostFeatures) MSBIgnore() uint64 {
	return f.privateStorage.InternalMsbIgnore
}

func (f ScyllaHostFeatures) LWTFlagMask() int {
	return f.privateStorage.InternalLwtFlagMask
}

func (f ScyllaHostFeatures) ShardAwarePort() uint16 {
	return f.privateStorage.InternalShardAwarePort
}

func (f ScyllaHostFeatures) ShardAwarePortTLS() uint16 {
	return f.privateStorage.InternalShardAwarePortTLS
}

func (f ScyllaHostFeatures) RateLimitErrorCode() int {
	return f.privateStorage.InternalRateLimitErrorCode
}

func (f ScyllaHostFeatures) IsMetadataIDSupported() bool {
	return f.privateStorage.InternalIsMetadataIDSupported
}

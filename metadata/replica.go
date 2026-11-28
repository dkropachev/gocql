// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import "github.com/scylladb/gocql/v2/cql"

// Replica is a replica of a token: its host and, when known, the host's
// shard that owns the token. Snapshot.Replicas, Snapshot.PartitionReplicas
// and TokenRange list them.
//
// The driver fills it; its zero value is empty.
type Replica struct {
	// Host is the replica's host, as the snapshot that listed the replica
	// holds it.
	Host HostSnapshot
	// ShardID is the shard of Host that owns the token: a tablet replica's
	// is its tablet's, and a vnode replica's the shard that owns the token
	// on the host when HostSnapshot.Shards is known. It is unset otherwise.
	ShardID cql.Option[uint32]
}

// Target returns the target that pins a request to the replica's host and
// shard, as routing.SingleTarget takes it.
func (r Replica) Target() Target {
	return Target{HostID: r.Host.HostID, ShardID: r.ShardID}
}

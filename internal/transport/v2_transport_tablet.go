// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"

	"encoding/binary"
	"fmt"

	"github.com/scylladb/gocql/v2/internal/wire"
	"github.com/scylladb/gocql/v2/tablets"
)

// parseTypedTabletHint consumes the tuple<bigint,bigint,list<tuple<uuid,int>>>
// custom payload without materializing a potentially fragmented payload.
func InternalParseTypedTabletHint(value Bytes, keyspace, table string) (tablets.TabletInfo, error) {
	return InternalParseTypedTabletPayload(value, keyspace, table, false)
}

func InternalParseTypedTabletPayload(value Bytes, keyspace, table string, v2 bool) (tablets.TabletInfo, error) {
	if err := cqltype.CheckBytesAlive(value); err != nil {
		return tablets.TabletInfo{}, err
	}
	if raw, ok := cqltype.BytesChain(value).Contiguous(); ok {
		if hint, ok, err := parseContiguousTabletPayload(raw, keyspace, table, v2); ok {
			return hint, err
		}
		// A malformed hint is parsed again below for its error.
	}
	// Cursors are held by value and the first error returns at once, so a
	// hint allocates only its replicas.
	p := wire.CursorOver(cqltype.BytesChain(value))
	first, err := tabletHintBigint(&p)
	if err != nil {
		return tablets.TabletInfo{}, err
	}
	last, err := tabletHintBigint(&p)
	if err != nil {
		return tablets.TabletInfo{}, err
	}
	list, err := tabletHintField(&p)
	if err != nil {
		return tablets.TabletInfo{}, err
	}
	n, err := list.Uint32()
	if err != nil {
		return tablets.TabletInfo{}, err
	}
	count := int(int32(n))
	if count < 0 || int64(count) > list.Remaining()/4 {
		return tablets.TabletInfo{}, fmt.Errorf("gocql: invalid metadata count %d", count)
	}
	if count > 1000 {
		return tablets.TabletInfo{}, fmt.Errorf("gocql: unreasonable tablet replica count %d", count)
	}
	replicas := make([]tablets.ReplicaInfo, count)
	for i := range replicas {
		tuple, err := tabletHintField(&list)
		if err != nil {
			return tablets.TabletInfo{}, err
		}
		id, err := tabletHintField(&tuple)
		if err != nil {
			return tablets.TabletInfo{}, err
		}
		hi, err := id.Uint64()
		if err != nil {
			return tablets.TabletInfo{}, err
		}
		lo, err := id.Uint64()
		if err != nil {
			return tablets.TabletInfo{}, err
		}
		if err := id.RequireEnd(); err != nil {
			return tablets.TabletInfo{}, err
		}
		var uuid [16]byte
		binary.BigEndian.PutUint64(uuid[:8], hi)
		binary.BigEndian.PutUint64(uuid[8:], lo)
		shard, err := tabletHintField(&tuple)
		if err != nil {
			return tablets.TabletInfo{}, err
		}
		v, err := shard.Uint32()
		if err != nil {
			return tablets.TabletInfo{}, err
		}
		if err := shard.RequireEnd(); err != nil {
			return tablets.TabletInfo{}, err
		}
		shardID := int(int32(v))
		if shardID < 0 {
			return tablets.TabletInfo{}, fmt.Errorf("gocql: negative tablet shard %d", shardID)
		}
		if err := tuple.RequireEnd(); err != nil {
			return tablets.TabletInfo{}, err
		}
		replicas[i] = tablets.NewReplicaInfo(tablets.HostUUID(uuid), shardID)
	}
	if err := list.RequireEnd(); err != nil {
		return tablets.TabletInfo{}, err
	}
	var version uint64
	if v2 {
		v, err := tabletHintBigint(&p)
		if err != nil {
			return tablets.TabletInfo{}, err
		}
		version = uint64(v)
	}
	if err := p.RequireEnd(); err != nil {
		return tablets.TabletInfo{}, err
	}
	hint, err := tablets.NewTabletInfo(keyspace, table, first, last, replicas)
	if v2 {
		hint = hint.WithVersion(version)
	}
	return hint, err
}

// tabletHintField reads a [bytes] field of a tablet hint and returns a cursor
// over its body; a null or unset field is an error.
func tabletHintField(c *wire.Cursor) (wire.Cursor, error) {
	n, err := c.Uint32()
	if err != nil {
		return wire.Cursor{}, err
	}
	if int32(n) < 0 {
		return wire.Cursor{}, errTabletHintNullField
	}
	chain, err := c.Take(int64(int32(n)))
	if err != nil {
		return wire.Cursor{}, err
	}
	return wire.CursorOver(chain), nil
}

var errTabletHintNullField = fmt.Errorf("gocql: tablet hint contains null or unset field")

// tabletHintBigint reads a bigint field of a tablet hint.
func tabletHintBigint(c *wire.Cursor) (int64, error) {
	f, err := tabletHintField(c)
	if err != nil {
		return 0, err
	}
	n, err := f.Uint64()
	if err != nil {
		return 0, err
	}
	if err := f.RequireEnd(); err != nil {
		return 0, err
	}
	return int64(n), nil
}

// parseContiguousTabletPayload is InternalParseTypedTabletPayload for a
// payload in one buffer, reading it as plain bytes. It reports false for
// any malformed payload, leaving the error to the cursor parser.
func parseContiguousTabletPayload(b []byte, keyspace, table string, v2 bool) (tablets.TabletInfo, bool, error) {
	first, b, ok := contiguousTabletBigint(b)
	if !ok {
		return tablets.TabletInfo{}, false, nil
	}
	last, b, ok := contiguousTabletBigint(b)
	if !ok {
		return tablets.TabletInfo{}, false, nil
	}
	list, b, ok := contiguousTabletField(b)
	if !ok || len(list) < 4 {
		return tablets.TabletInfo{}, false, nil
	}
	count := int(int32(binary.BigEndian.Uint32(list)))
	list = list[4:]
	if count < 0 || count > len(list)/4 || count > 1000 {
		return tablets.TabletInfo{}, false, nil
	}
	replicas := make([]tablets.ReplicaInfo, count)
	for i := range replicas {
		var tuple, id, shard []byte
		if tuple, list, ok = contiguousTabletField(list); !ok {
			return tablets.TabletInfo{}, false, nil
		}
		if id, tuple, ok = contiguousTabletField(tuple); !ok || len(id) != 16 {
			return tablets.TabletInfo{}, false, nil
		}
		if shard, tuple, ok = contiguousTabletField(tuple); !ok || len(shard) != 4 || len(tuple) != 0 {
			return tablets.TabletInfo{}, false, nil
		}
		shardID := int(int32(binary.BigEndian.Uint32(shard)))
		if shardID < 0 {
			return tablets.TabletInfo{}, false, nil
		}
		replicas[i] = tablets.NewReplicaInfo(tablets.HostUUID(id), shardID)
	}
	if len(list) != 0 {
		return tablets.TabletInfo{}, false, nil
	}
	var version int64
	if v2 {
		if version, b, ok = contiguousTabletBigint(b); !ok {
			return tablets.TabletInfo{}, false, nil
		}
	}
	if len(b) != 0 {
		return tablets.TabletInfo{}, false, nil
	}
	hint, err := tablets.NewTabletInfo(keyspace, table, first, last, replicas)
	if v2 {
		hint = hint.WithVersion(uint64(version))
	}
	return hint, true, err
}

// contiguousTabletField splits a non-null [bytes] field off b.
func contiguousTabletField(b []byte) (field, rest []byte, ok bool) {
	if len(b) < 4 {
		return nil, nil, false
	}
	n := int32(binary.BigEndian.Uint32(b))
	if n < 0 || int(n) > len(b)-4 {
		return nil, nil, false
	}
	return b[4 : 4+n], b[4+n:], true
}

// contiguousTabletBigint splits a bigint field off b.
func contiguousTabletBigint(b []byte) (int64, []byte, bool) {
	f, rest, ok := contiguousTabletField(b)
	if !ok || len(f) != 8 {
		return 0, nil, false
	}
	return int64(binary.BigEndian.Uint64(f)), rest, true
}

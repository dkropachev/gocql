package tablets

import (
	"encoding/binary"
)

// hintBytes encodes one [bytes] value; nil encodes CQL null.
func hintBytes(v []byte) []byte {
	if v == nil {
		return []byte{0xff, 0xff, 0xff, 0xff}
	}
	return append(binary.BigEndian.AppendUint32(nil, uint32(len(v))), v...)
}

func hintBigInt(v int64) []byte {
	return hintBytes(binary.BigEndian.AppendUint64(nil, uint64(v)))
}

func hintReplica(uuid HostUUID, shard int32) []byte {
	return hintBytes(append(hintBytes(uuid[:]), hintBytes(binary.BigEndian.AppendUint32(nil, uint32(shard)))...))
}

// hintPayload encodes tuple<bigint, bigint, list<tuple<uuid, int>>> as the
// tablets-routing-v1 custom payload carries it.
func hintPayload(first, last int64, replicas ...[]byte) []byte {
	list := binary.BigEndian.AppendUint32(nil, uint32(len(replicas)))
	for _, r := range replicas {
		list = append(list, r...)
	}
	out := append(hintBigInt(first), hintBigInt(last)...)
	return append(out, hintBytes(list)...)
}

var (
	hintHost1 = MustParseHostUUID("00000000-0000-0000-0000-000000000001")
	hintHost2 = MustParseHostUUID("deadbeef-cafe-4bad-8000-0123456789ab")
)

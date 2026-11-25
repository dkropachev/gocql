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
	"fmt"
	"net"
	"net/netip"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/cqltype"
)

func fixtureBody[T any](typ cql.DataType, codec cql.Encoder[T], v T) ([]byte, error) {
	encoded, err := cql.EncodeValue(typ, codec, v)
	if err != nil {
		return nil, err
	}
	return encoded.Bytes().OwnedBytes(), nil
}
func encodeFixtureCell(info cqltype.TypeInfo, value any) ([]byte, error) {
	typ, err := cqltype.DataTypeFromInfo(info)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	switch v := value.(type) {
	case string:
		if typ.Kind() == cql.TypeInet {
			ip, err := netip.ParseAddr(v)
			if err != nil {
				return nil, err
			}
			return fixtureBody(typ, cql.InetCodec, ip)
		}
		return fixtureBody(typ, cql.StringCodec, v)
	case net.IP:
		ip, ok := netip.AddrFromSlice(v)
		if !ok {
			return nil, fmt.Errorf("invalid fixture IP")
		}
		return fixtureBody(typ, cql.InetCodec, ip.Unmap())
	case cql.UUID:
		return fixtureBody(typ, cql.UUIDCodec, v)
	case []byte:
		return fixtureBody(typ, cql.BytesCodec, cql.BorrowBytes(v))
	case int8:
		return fixtureBody(typ, cql.Int8Codec, v)
	case int16:
		return fixtureBody(typ, cql.Int16Codec, v)
	case float32:
		return fixtureBody(typ, cql.Float32Codec, v)
	case map[string][]byte:
		entries := make([]cql.Entry[string, cql.Bytes], 0, len(v))
		for k, x := range v {
			entries = append(entries, cql.Entry[string, cql.Bytes]{Key: k, Value: cql.BorrowBytes(x)})
		}
		return fixtureBody(typ, cql.MapEncoder(cql.StringCodec, cql.BytesCodec), entries)
	case bool:
		return fixtureBody(typ, cql.BoolCodec, v)
	case int:
		return fixtureBody(typ, cql.Int32Codec, int32(v))
	case int32:
		return fixtureBody(typ, cql.Int32Codec, v)
	case int64:
		return fixtureBody(typ, cql.Int64Codec, v)
	case float64:
		return fixtureBody(typ, cql.Float64Codec, v)
	case []string:
		if typ.Kind() == cql.TypeSet {
			return fixtureBody(typ, cql.SetEncoder(cql.StringCodec), v)
		}
		return fixtureBody(typ, cql.ListEncoder(cql.StringCodec), v)
	case map[string]string:
		entries := make([]cql.Entry[string, string], 0, len(v))
		for k, x := range v {
			entries = append(entries, cql.Entry[string, string]{Key: k, Value: x})
		}
		return fixtureBody(typ, cql.MapEncoder(cql.StringCodec, cql.StringCodec), entries)
	default:
		return nil, fmt.Errorf("unsupported explicit fixture value %T", value)
	}
}

// routingTestServer is a TestServer answering PREPARE from a fixed table of
// statements and counting how many PREPAREs each one received. Statements not
// in the table get a server error.

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

package session

import (
	"context"
	"fmt"

	implCql "github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implCodec "github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/protocol"

	"net"
	"net/netip"
	"time"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

// nativeResultFixture contains preframed native cells used by metadata peers.
// It has no query, iterator, reflection serialization or scanning behavior.
type nativeResultFixture struct {
	framer  protocol.FramerInterface
	err     error
	meta    protocol.ResultMetadata
	numRows int
}

func fixtureBody[T any](typ cqltype.DataType, codec implCodec.Encoder[T], v T) ([]byte, error) {
	encoded, err := implCodec.EncodeValue(typ, codec, v)
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
		if typ.Kind() == cqltype.TypeInet {
			ip, err := netip.ParseAddr(v)
			if err != nil {
				return nil, err
			}
			return fixtureBody(typ, implCodec.InetCodec, ip)
		}
		return fixtureBody(typ, implCodec.StringEncoder, v)
	case net.IP:
		ip, ok := netip.AddrFromSlice(v)
		if !ok {
			return nil, fmt.Errorf("invalid fixture IP")
		}
		return fixtureBody(typ, implCodec.InetCodec, ip.Unmap())
	case cqltype.UUID:
		return fixtureBody(typ, implCodec.UUIDCodec, v)
	case []byte:
		return fixtureBody(typ, implCodec.BlobCodec, cqltype.BorrowBytes(v))
	case int8:
		return fixtureBody(typ, implCodec.Int8, v)
	case int16:
		return fixtureBody(typ, implCodec.Int16, v)
	case float32:
		return fixtureBody(typ, implCodec.Float32, v)
	case map[string][]byte:
		entries := make([]implCodec.Entry[string, cqltype.Bytes], 0, len(v))
		for k, x := range v {
			entries = append(entries, implCodec.Entry[string, cqltype.Bytes]{Key: k, Value: cqltype.BorrowBytes(x)})
		}
		return fixtureBody(typ, implCodec.MapEncoder(implCodec.StringEncoder, implCodec.BlobCodec), entries)
	case bool:
		return fixtureBody(typ, implCodec.Bool, v)
	case int:
		return fixtureBody(typ, implCodec.Int32, int32(v))
	case int32:
		return fixtureBody(typ, implCodec.Int32, v)
	case int64:
		return fixtureBody(typ, implCodec.Int64, v)
	case float64:
		return fixtureBody(typ, implCodec.Float64, v)
	case []string:
		if typ.Kind() == cqltype.TypeSet {
			return fixtureBody(typ, implCodec.SetEncoder(implCodec.StringEncoder), v)
		}
		return fixtureBody(typ, implCodec.ListEncoder(implCodec.StringEncoder), v)
	case map[string]string:
		entries := make([]implCodec.Entry[string, string], 0, len(v))
		for k, x := range v {
			entries = append(entries, implCodec.Entry[string, string]{Key: k, Value: x})
		}
		return fixtureBody(typ, implCodec.MapEncoder(implCodec.StringEncoder, implCodec.StringEncoder), entries)
	default:
		return nil, fmt.Errorf("unsupported explicit fixture value %T", value)
	}
}

func execTestStatement(ctx context.Context, s *cluster.Core, cql string, opts CompiledOptions) error {
	bound, err := BindUnpreparedOwned(binding.Statement[implCodec.EmptyRecord, implCodec.EmptyRecord]{CQL: cql, Params: implCodec.EmptyParamsEncoder, Rows: implCodec.EmptyRowDecoder}, cqltype.NewColumns(), implCodec.EmptyRecord{})
	if err != nil {
		return err
	}
	defer bound.Close()
	if v := opts.Values(); !v.Timeout.Set {
		v.Timeout = implCql.Option[time.Duration]{Set: true, Value: implCluster.SessionData(s).InternalCfg.Timeout}
		opts = testOptions(v)
	}
	_, err = Exec(ctx, s, bound, opts)
	return err
}

func writeErrorResult(resp *protocol.Framer, stream int, code int, msg string) {
	resp.WriteHeader(0, frm.OpError, stream)
	resp.WriteInt(int32(code))
	resp.WriteString(msg)
}

// routingTestServer is a TestServer answering PREPARE from a fixed table of
// statements and counting how many PREPAREs each one received. Statements not
// in the table get a server error.

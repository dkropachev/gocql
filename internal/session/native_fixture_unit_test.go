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
	"net"
	"net/netip"
	"time"

	implCql "github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/protocol"
)

// nativeResultFixture contains preframed native cells used by metadata peers.
// It has no query, iterator, reflection serialization or scanning behavior.
type nativeResultFixture struct {
	framer  protocol.FramerInterface
	err     error
	meta    protocol.ResultMetadata
	numRows int
}

func fixtureBody[T any](typ implCql.DataType, codec implCql.Encoder[T], v T) ([]byte, error) {
	encoded, err := implCql.EncodeValue(typ, codec, v)
	if err != nil {
		return nil, err
	}
	return ownedBytesForTest(encoded.Bytes()), nil
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
		if typ.Kind() == implCql.KindInet {
			ip, err := netip.ParseAddr(v)
			if err != nil {
				return nil, err
			}
			return fixtureBody(typ, implCql.InetCodec, ip)
		}
		return fixtureBody(typ, implCql.StringCodec, v)
	case net.IP:
		ip, ok := netip.AddrFromSlice(v)
		if !ok {
			return nil, fmt.Errorf("invalid fixture IP")
		}
		return fixtureBody(typ, implCql.InetCodec, ip.Unmap())
	case implCql.UUID:
		return fixtureBody(typ, implCql.UUIDCodec, v)
	case []byte:
		return fixtureBody(typ, implCql.BytesViewCodec, implCql.BorrowBytes(v))
	case int8:
		return fixtureBody(typ, implCql.Int8Codec, v)
	case int16:
		return fixtureBody(typ, implCql.Int16Codec, v)
	case float32:
		return fixtureBody(typ, implCql.Float32Codec, v)
	case map[string][]byte:
		entries := make([]implCql.Entry[string, implCql.Bytes], 0, len(v))
		for k, x := range v {
			entries = append(entries, implCql.Entry[string, implCql.Bytes]{Key: k, Value: implCql.BorrowBytes(x)})
		}
		return fixtureBody(typ, implCql.MapEncoder(implCql.StringCodec, implCql.BytesViewCodec), entries)
	case bool:
		return fixtureBody(typ, implCql.BoolCodec, v)
	case int:
		return fixtureBody(typ, implCql.Int32Codec, int32(v))
	case int32:
		return fixtureBody(typ, implCql.Int32Codec, v)
	case int64:
		return fixtureBody(typ, implCql.Int64Codec, v)
	case float64:
		return fixtureBody(typ, implCql.Float64Codec, v)
	case []string:
		if typ.Kind() == implCql.KindSet {
			return fixtureBody(typ, implCql.SetEncoder(implCql.StringCodec), v)
		}
		return fixtureBody(typ, implCql.ListEncoder(implCql.StringCodec), v)
	case map[string]string:
		entries := make([]implCql.Entry[string, string], 0, len(v))
		for k, x := range v {
			entries = append(entries, implCql.Entry[string, string]{Key: k, Value: x})
		}
		return fixtureBody(typ, implCql.MapEncoder(implCql.StringCodec, implCql.StringCodec), entries)
	default:
		return nil, fmt.Errorf("unsupported explicit fixture value %T", value)
	}
}

func execTestStatement(ctx context.Context, s *cluster.Core, cql string, opts CompiledOptions) error {
	bound, err := BindUnpreparedOwned(binding.Statement[implCql.EmptyRow, implCql.EmptyRow]{CQL: cql, Params: implCql.NoParams}, implCql.NewColumns(), implCql.EmptyRow{})
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

func writeErrorResult(resp *protocol.Framer, stream int, code cqlerr.ErrorCode, msg string) {
	resp.WriteHeader(0, frm.OpError, stream)
	resp.WriteInt(int32(code))
	resp.WriteString(msg)
}

// routingTestServer is a TestServer answering PREPARE from a fixed table of
// statements and counting how many PREPAREs each one received. Statements not
// in the table get a server error.

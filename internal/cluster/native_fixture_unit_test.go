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

package cluster

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"net/netip"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/transport"
)

// nativeResultFixture contains preframed native cells used by metadata peers.
// It has no query, iterator, reflection serialization or scanning behavior.
type nativeResultFixture struct {
	framer  protocol.FramerInterface
	err     error
	meta    protocol.ResultMetadata
	numRows int
}

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
		return fixtureBody(typ, cql.BlobCodec, cql.BorrowBytes(v))
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
		return fixtureBody(typ, cql.MapEncoder(cql.StringCodec, cql.BlobCodec), entries)
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

// hostInfoFromFixture exercises the same typed topology decoder as control I/O.
func hostInfoFromFixture(f *nativeResultFixture, port int) (*endpoint.HostInfo, error) {
	rows, err := readV2SystemRowsVia(context.Background(), 4, 0, func(_ context.Context, _ *model.AttemptRequest) (*transport.InternalResponsePage, error) {
		return v2SchemaMockResponse(transport.InternalQrySystemLocal, f)
	}, binding.Statement[cql.EmptyRow, metadata.InternalV2TopologyRow]{CQL: transport.InternalQrySystemLocal, Params: cql.EmptyParamsEncoder, Rows: metadata.InternalV2DecodeTopology()}, cql.NewColumns(), cql.EmptyRow{})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("query returned 0 rows")
	}
	return rows[0].InternalHost(port)
}

func writeErrorResult(resp *protocol.Framer, stream int, code protocol.ErrorCode, msg string) {
	resp.WriteHeader(0, frm.OpError, stream)
	resp.WriteInt(int32(code))
	resp.WriteString(msg)
}

type testPreparedCol struct {
	keyspace, table, name string
	typ                   cql.Type
}

// testPrepared describes a RESULT/Prepared reply. keyspace/table set means a
// global table spec; otherwise each column carries its own.
type testPrepared struct {
	id              uint64
	flags           int
	keyspace, table string
	cols            []testPreparedCol
	pkIndexes       []uint16
}

func (p testPrepared) write(resp *protocol.Framer, stream int, writeResultMetadataID func(uint64)) {
	resp.WriteHeader(0, frm.OpResult, stream)
	resp.WriteInt(frm.ResultKindPrepared)
	resp.WriteShortBytes(binary.BigEndian.AppendUint64(nil, p.id))
	writeResultMetadataID(p.id)

	global := p.keyspace != ""
	flags := p.flags
	if global {
		flags |= frm.FlagGlobalTableSpec
	}
	resp.WriteInt(int32(flags))
	resp.WriteInt(int32(len(p.cols)))
	if resp.Proto >= protocol.ProtoVersion4 {
		resp.WriteInt(int32(len(p.pkIndexes)))
		for _, idx := range p.pkIndexes {
			resp.WriteShort(idx)
		}
	}
	if global {
		resp.WriteString(p.keyspace)
		resp.WriteString(p.table)
	}
	for _, c := range p.cols {
		if !global {
			resp.WriteString(c.keyspace)
			resp.WriteString(c.table)
		}
		resp.WriteString(c.name)
		resp.WriteShort(uint16(c.typ))
	}

	// <result_metadata>
	resp.WriteInt(int32(frm.FlagNoMetaData))
	resp.WriteInt(0)
}

// routingTestServer is a TestServer answering PREPARE from a fixed table of
// statements and counting how many PREPAREs each one received. Statements not
// in the table get a server error.

func int32Bytes(v int32) []byte { return binary.BigEndian.AppendUint32(nil, uint32(v)) }

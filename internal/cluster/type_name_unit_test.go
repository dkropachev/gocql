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
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/cqltype"
)

type MyPointerMarshaler struct{}

var typeLookupTest = []struct {
	TypeName     string
	ExpectedType cql.Kind
}{
	{"AsciiType", cql.KindASCII},
	{"LongType", cql.KindBigInt},
	{"BytesType", cql.KindBlob},
	{"BooleanType", cql.KindBoolean},
	{"CounterColumnType", cql.KindCounter},
	{"DecimalType", cql.KindDecimal},
	{"DoubleType", cql.KindDouble},
	{"FloatType", cql.KindFloat},
	{"Int32Type", cql.KindInt},
	{"DateType", cql.KindTimestamp},
	{"TimestampType", cql.KindTimestamp},
	{"UUIDType", cql.KindUUID},
	{"UTF8Type", cql.KindText},
	{"IntegerType", cql.KindVarint},
	{"TimeUUIDType", cql.KindTimeUUID},
	{"InetAddressType", cql.KindInet},
	{"MapType", cql.KindMap},
	{"ListType", cql.KindList},
	{"SetType", cql.KindSet},
	{"unknown", cql.KindCustom},
	{"ShortType", cql.KindSmallInt},
	{"ByteType", cql.KindTinyInt},
}

func testType(t *testing.T, cassType string, expectedType cql.Kind) {
	if computedType := getApacheCassandraType(cqltype.ApacheCassandraTypePrefix + cassType); computedType != expectedType {
		t.Errorf("Cassandra custom type lookup for %s failed. Expected %s, got %s.", cassType, expectedType.String(), computedType.String())
	}
}

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
	ExpectedType cql.Type
}{
	{"AsciiType", cql.TypeASCII},
	{"LongType", cql.TypeBigInt},
	{"BytesType", cql.TypeBlob},
	{"BooleanType", cql.TypeBoolean},
	{"CounterColumnType", cql.TypeCounter},
	{"DecimalType", cql.TypeDecimal},
	{"DoubleType", cql.TypeDouble},
	{"FloatType", cql.TypeFloat},
	{"Int32Type", cql.TypeInt},
	{"DateType", cql.TypeTimestamp},
	{"TimestampType", cql.TypeTimestamp},
	{"UUIDType", cql.TypeUUID},
	{"UTF8Type", cql.TypeText},
	{"IntegerType", cql.TypeVarint},
	{"TimeUUIDType", cql.TypeTimeUUID},
	{"InetAddressType", cql.TypeInet},
	{"MapType", cql.TypeMap},
	{"ListType", cql.TypeList},
	{"SetType", cql.TypeSet},
	{"unknown", cql.TypeCustom},
	{"ShortType", cql.TypeSmallInt},
	{"ByteType", cql.TypeTinyInt},
}

func testType(t *testing.T, cassType string, expectedType cql.Type) {
	if computedType := getApacheCassandraType(cqltype.ApacheCassandraTypePrefix + cassType); computedType != expectedType {
		t.Errorf("Cassandra custom type lookup for %s failed. Expected %s, got %s.", cassType, expectedType.String(), computedType.String())
	}
}

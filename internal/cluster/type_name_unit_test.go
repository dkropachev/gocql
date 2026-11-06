//go:build unit

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

	"github.com/scylladb/gocql/v2/internal/cqltype"
)

func TestLookupCassType(t *testing.T) {
	t.Parallel()

	for _, lookupTest := range typeLookupTest {
		testType(t, lookupTest.TypeName, lookupTest.ExpectedType)
	}
}

type MyPointerMarshaler struct{}

var typeLookupTest = []struct {
	TypeName     string
	ExpectedType cqltype.Type
}{
	{"AsciiType", cqltype.TypeAscii},
	{"LongType", cqltype.TypeBigInt},
	{"BytesType", cqltype.TypeBlob},
	{"BooleanType", cqltype.TypeBoolean},
	{"CounterColumnType", cqltype.TypeCounter},
	{"DecimalType", cqltype.TypeDecimal},
	{"DoubleType", cqltype.TypeDouble},
	{"FloatType", cqltype.TypeFloat},
	{"Int32Type", cqltype.TypeInt},
	{"DateType", cqltype.TypeTimestamp},
	{"TimestampType", cqltype.TypeTimestamp},
	{"UUIDType", cqltype.TypeUUID},
	{"UTF8Type", cqltype.TypeVarchar},
	{"IntegerType", cqltype.TypeVarint},
	{"TimeUUIDType", cqltype.TypeTimeUUID},
	{"InetAddressType", cqltype.TypeInet},
	{"MapType", cqltype.TypeMap},
	{"ListType", cqltype.TypeList},
	{"SetType", cqltype.TypeSet},
	{"unknown", cqltype.TypeCustom},
	{"ShortType", cqltype.TypeSmallInt},
	{"ByteType", cqltype.TypeTinyInt},
}

func testType(t *testing.T, cassType string, expectedType cqltype.Type) {
	if computedType := InternalGetApacheCassandraType(cqltype.ApacheCassandraTypePrefix + cassType); computedType != expectedType {
		t.Errorf("Cassandra custom type lookup for %s failed. Expected %s, got %s.", cassType, expectedType.String(), computedType.String())
	}
}

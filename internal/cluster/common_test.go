/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package cluster

import (
	"fmt"
	"hash/fnv"

	"net"
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/internal/endpoint"
)

// extractKeyspaceTableFromDDL extracts the keyspace and table names from a DDL
// statement like "CREATE TABLE gocql_test.table_name (...)".
// Returns ("", "") for non-TABLE DDL or when keyspace is not qualified.
func extractKeyspaceTableFromDDL(ddl string) (keyspace, table string) {
	upper := strings.ToUpper(ddl)
	idx := strings.Index(upper, "TABLE")
	if idx < 0 {
		return "", ""
	}
	rest := strings.TrimSpace(ddl[idx+len("TABLE"):])
	// Skip optional "IF [NOT] EXISTS" between TABLE and the name.
	upperRest := strings.ToUpper(rest)
	if strings.HasPrefix(upperRest, "IF NOT EXISTS") {
		rest = strings.TrimSpace(rest[len("IF NOT EXISTS"):])
	} else if strings.HasPrefix(upperRest, "IF EXISTS") {
		rest = strings.TrimSpace(rest[len("IF EXISTS"):])
	}
	// Extract keyspace.table
	dot := strings.Index(rest, ".")
	if dot < 0 {
		return "", ""
	}
	ks := rest[:dot]
	// Extract table name: everything after the dot until whitespace or '('
	nameRest := rest[dot+1:]
	end := strings.IndexAny(nameRest, " \t\n(")
	if end < 0 {
		return ks, nameRest
	}
	return ks, nameRest[:end]
}

const maxCQLIdentifierLen = 48
const testTableNameHashLen = 16

// testTableName builds a CQL-safe table name from t.Name() and optional parts.
// Truncates to 48 chars (CQL limit) using <first-n>_<fnv64a hash>_<last-n>
// when needed.
func testTableName(t testing.TB, parts ...string) string {
	name := strings.ToLower(t.Name())
	for _, p := range parts {
		name += "_" + strings.ToLower(p)
	}

	var b strings.Builder
	prevUnderscore := false
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			prevUnderscore = false
		} else if !prevUnderscore {
			b.WriteByte('_')
			prevUnderscore = true
		}
	}
	name = strings.Trim(b.String(), "_")

	if len(name) > maxCQLIdentifierLen {
		h := fnv.New64a()
		h.Write([]byte(name))
		hash := fmt.Sprintf("%016x", h.Sum64()) // 16 hex chars for better collision resistance
		remaining := maxCQLIdentifierLen - testTableNameHashLen - 2
		prefixLen := remaining / 2
		suffixLen := remaining - prefixLen
		name = name[:prefixLen] + "_" + hash + "_" + name[len(name)-suffixLen:]
	}
	return name
}

func staticAddressTranslator(newAddr net.IP, newPort int) endpoint.AddressTranslator {
	return endpoint.AddressTranslatorFunc(func(_ endpoint.AddressTranslatorHostInfo, _ endpoint.AddressPort) (endpoint.AddressPort, error) {
		return endpoint.AddressPort{Address: newAddr, Port: uint16(newPort)}, nil
	})
}

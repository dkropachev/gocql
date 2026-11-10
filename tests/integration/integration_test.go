//go:build integration

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

package integration

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	implExecution "github.com/scylladb/gocql/v2/internal/execution"

	gocql "github.com/scylladb/gocql/v2"

	"github.com/scylladb/gocql/v2/cql"
)

func assertV2CustomPayload(t testing.TB, want, got []cql.PayloadEntry) {
	t.Helper()
	decode := func(entries []cql.PayloadEntry) map[string][]byte {
		if entries == nil {
			return nil
		}
		result := make(map[string][]byte, len(entries))
		for _, entry := range entries {
			if _, exists := result[entry.Key]; exists {
				t.Fatalf("duplicate custom-payload key %q", entry.Key)
			}
			if entry.Value.Valid {
				result[entry.Key] = entry.Value.Value.OwnedBytes()
			} else {
				result[entry.Key] = nil
			}
		}
		return result
	}
	if !reflect.DeepEqual(decode(want), decode(got)) {
		t.Fatalf("received custom payload differs: want %v, got %v", decode(want), decode(got))
	}
}

func requireCassandraFixtures(t *testing.T) {
	t.Helper()
	if *flagDistribution != "cassandra" || !*flagCassandraFixtures {
		t.Skip("requires Cassandra write-failure/custom-payload fixtures and -runcassandrafixtures")
	}
}

// readV2ClientOptions keeps node-local startup reports on the control connection.
func readV2ClientOptions(ctx context.Context, session *gocql.Session, stmt string) ([]map[string]string, error) {
	handle := (*implExecution.Session)(session).Data().InternalControl.InternalGetConn()
	if handle == nil {
		return nil, errNoControl
	}
	conn, ok := handle.InternalConn.(*driverConn)
	if !ok {
		return nil, fmt.Errorf("control connection does not support typed queries")
	}
	statement := cql.Statement[cql.EmptyParams, []cql.Entry[cql.Text, cql.Text]]{CQL: stmt, Params: cql.EmptyParamsEncoder, Rows: cql.OneColumn(cql.MapDecoder(cql.TextCodec, cql.TextCodec))}
	rows, err := readV2SystemRows(ctx, conn, statement, cql.NewColumns(), cql.EmptyParams{})
	if err != nil {
		return nil, err
	}
	result := make([]map[string]string, 0, len(rows))
	for _, row := range rows {
		values := make(map[string]string)
		for _, entry := range row {
			values[entry.Key.String()] = entry.Value.String()
		}
		result = append(result, values)
	}
	return result, nil
}

func queryV2ControlKey(ctx context.Context, session *gocql.Session) error {
	handle := (*implExecution.Session)(session).Data().InternalControl.InternalGetConn()
	if handle == nil {
		return errNoControl
	}
	conn, ok := handle.InternalConn.(*driverConn)
	if !ok {
		return fmt.Errorf("control connection does not support typed queries")
	}
	statement := cql.Statement[cql.EmptyParams, cql.Text]{CQL: "SELECT key FROM system.local WHERE key='local'", Params: cql.EmptyParamsEncoder, Rows: cql.OneColumn(cql.TextCodec)}
	_, err := readV2SystemRows(ctx, conn, statement, cql.NewColumns(), cql.EmptyParams{})
	return err
}

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
	"flag"
	"fmt"
	"hash/fnv"

	"log"

	"os"

	"strings"
	"sync"
	"testing"
	"time"
)

var (
	flagCluster           = flag.String("cluster", "127.0.0.1", "a comma-separated list of host:port tuples")
	flagProto             = flag.Int("proto", 0, "protcol version")
	flagCQL               = flag.String("cql", "3.0.0", "CQL version")
	flagRF                = flag.Int("rf", 1, "replication factor for test keyspace")
	clusterSize           = flag.Int("clusterSize", 1, "the expected size of the cluster")
	flagRetry             = flag.Int("retries", 5, "number of times to retry queries")
	flagAutoWait          = flag.Duration("autowait", 1000*time.Millisecond, "time to wait for autodiscovery to fill the hosts poll")
	flagRunSslTest        = flag.Bool("runssl", false, "Set to true to run ssl test")
	flagRunAuthTest       = flag.Bool("runauth", false, "Set to true to run authentication test")
	flagCassandraFixtures = flag.Bool("runcassandrafixtures", false, "Run tests requiring Cassandra custom-payload mirroring and test-keyspace write failures")
	flagCompressTest      = flag.String("compressor", "no-compression", "compressor to use")
	flagTimeout           = flag.Duration("gocql.timeout", 5*time.Second, "sets the connection `timeout` for all operations")
	flagClusterSocket     = flag.String("cluster-socket", "", "nodes socket files separated by comma")
	flagDistribution      = flag.String("distribution", "scylla", "database distribution - scylla or cassandra")
	flagCassVersion       cassVersion
)

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

func getClusterHosts() []string {
	return strings.Split(*flagCluster, ",")
}

type OnceManager struct {
	keyspaces map[string]*sync.Once
	mu        sync.Mutex
}

func NewOnceManager() *OnceManager {
	return &OnceManager{
		keyspaces: make(map[string]*sync.Once),
	}
}

func (o *OnceManager) GetOnce(key string) *sync.Once {
	o.mu.Lock()
	defer o.mu.Unlock()

	if once, exists := o.keyspaces[key]; exists {
		return once
	}
	o.keyspaces[key] = &sync.Once{}
	return o.keyspaces[key]
}

var initKeyspaceOnce = NewOnceManager()

type testKeyspaceOpts struct {
	tabletsDisabled bool
}

func (o *testKeyspaceOpts) KeyspaceName() string {
	if o.tabletsDisabled {
		return fixtureKeyspace("_td")
	}
	return fixtureKeyspace("")
}

func getClusterSocketFile() []string {
	var res []string
	for _, socketFile := range strings.Split(*flagClusterSocket, ",") {
		if socketFile != "" {
			res = append(res, socketFile)
		}
	}
	return res
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

// testKeyspaceName builds a CQL-safe keyspace name from t.Name() and optional parts.
// Analogous to testTableName but intended for CREATE/DROP KEYSPACE statements.
func testKeyspaceName(t testing.TB, parts ...string) string {
	if os.Getenv("GOCQL_TEST_RUN") != "" {
		h := fnv.New64a()
		h.Write([]byte(testTableName(t, parts...)))
		return fmt.Sprintf("%s_%016x", fixtureKeyspace(""), h.Sum64())
	}
	return testTableName(t, parts...)
}

func init() {
	flag.Var(&flagCassVersion, "gocql.cversion", "the cassandra version being tested against")

	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

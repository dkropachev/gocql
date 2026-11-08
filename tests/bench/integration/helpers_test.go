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
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	inf "gopkg.in/inf.v0"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/auth"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	protocol "github.com/scylladb/gocql/v2/internal/protocol"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/testkit"
	"github.com/scylladb/gocql/v2/internal/testkit/ccm"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/retry"
	routing "github.com/scylladb/gocql/v2/routing"
)

var (
	flagCluster = flag.String("cluster", "127.0.0.1", "a comma-separated list of host:port tuples")
	flagProto   = flag.Int("proto", 0, "protcol version")
	flagCQL     = flag.String("cql", "3.0.0", "CQL version")
	flagRF      = flag.Int("rf", 1, "replication factor for test keyspace")

	flagRetry = flag.Int("retries", 5, "number of times to retry queries")

	flagRunSslTest  = flag.Bool("runssl", false, "Set to true to run ssl test")
	flagRunAuthTest = flag.Bool("runauth", false, "Set to true to run authentication test")

	flagCompressTest = flag.String("compressor", "no-compression", "compressor to use")
	flagTimeout      = flag.Duration("gocql.timeout", 5*time.Second, "sets the connection `timeout` for all operations")

	flagDistribution = flag.String("distribution", "scylla", "database distribution - scylla or cassandra")
	flagCassVersion  cassVersion
)

// Accepted so the benchmarks take the same command line as the integration
// suite; the benchmarks themselves do not read them.
var (
	_ = flag.Int("clusterSize", 1, "the expected size of the cluster")
	_ = flag.Duration("autowait", 1000*time.Millisecond, "time to wait for autodiscovery to fill the hosts poll")
	_ = flag.Bool("runcassandrafixtures", false, "Run tests requiring Cassandra custom-payload mirroring and test-keyspace write failures")
	_ = flag.String("cluster-socket", "", "nodes socket files separated by comma")
)

func TestMain(m *testing.M) {
	flag.Parse()
	useSharedCluster()
	os.Exit(m.Run())
}

// useSharedCluster points the benchmarks at the cluster make scylla-start or
// make cassandra-start created, read through ccm, unless -cluster is given.
// It also takes -gocql.cversion from the running nodes when not given.
func useSharedCluster() {
	given := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { given[f.Name] = true })
	if given["cluster"] || ccm.Available() != nil {
		return
	}
	dist, err := ccm.ParseDistribution(*flagDistribution)
	if err != nil {
		return
	}
	c, v, err := ccm.RunningShared(dist, 5*time.Second)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bench: no running ccm cluster, using -cluster default:", err)
		return
	}
	defer c.Close()
	*flagCluster = strings.Join(c.IPs(), ",")
	if !given["gocql.cversion"] {
		_ = flagCassVersion.Set(fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch))
	}
	fmt.Fprintf(os.Stderr, "bench: using ccm cluster %s (%s, version %s)\n", c.Name(), *flagCluster, v.Raw)
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

func init() {
	flag.Var(&flagCassVersion, "gocql.cversion", "the cassandra version being tested against")

	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

type Bound[R any] = implExecution.Bound[R]

func BindBorrowed[P any, R any](a0 *gocql.Prepared[P, R], a1 P) (*Bound[R], error) {
	return implExecution.BindBorrowed[P, R](a0, a1)
}

func BindOwned[P any, R any](a0 *gocql.Prepared[P, R], a1 P) (*Bound[R], error) {
	return implExecution.BindOwned[P, R](a0, a1)
}

func BindUnpreparedOwned[P any, R any](a0 cql.Statement[P, R], a1 cql.Columns, a2 P) (*Bound[R], error) {
	return implExecution.BindUnpreparedOwned[P, R](a0, a1, a2)
}

func Exec[R any](a0 context.Context, a1 *gocql.Session, a2 *Bound[R], a3 gocql.SessionOptions) (gocql.Result, error) {
	return implExecution.Exec[R](a0, a1, a2, a3)
}

func Run[R any](a0 context.Context, a1 *gocql.Session, a2 *Bound[R], a3 gocql.SessionOptions) (gocql.Rows[R], error) {
	return implExecution.Run[R](a0, a1, a2, a3)
}

type cassandraObserver struct {
	mu       sync.Mutex
	requests []gocql.RequestEvent
	attempts []gocql.AttemptEvent
}

func (o *cassandraObserver) ObserveRequest(e gocql.RequestEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.requests = append(o.requests, e)
}
func (o *cassandraObserver) ObserveAttempt(e gocql.AttemptEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.attempts = append(o.attempts, e)
}
func (o *cassandraObserver) reset() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.requests = nil
	o.attempts = nil
}
func (o *cassandraObserver) snapshot() ([]gocql.RequestEvent, []gocql.AttemptEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]gocql.RequestEvent(nil), o.requests...), append([]gocql.AttemptEvent(nil), o.attempts...)
}

type cassandraRetry struct{}

func (cassandraRetry) Decide(a retry.Attempt) retry.Decision {
	if a.Idempotent && a.Number < 6 {
		return retry.Decision{Action: retry.Same}
	}
	return retry.Decision{Action: retry.Stop}
}

// Custom values use explicit codecs; field discovery and reflection are absent.
type fullNameCodec struct{}

func (fullNameCodec) CheckType(t cql.DataType) error { return cql.TextCodec.CheckType(t) }
func (fullNameCodec) Encode(w *cql.ValueWriter, n FullName) error {
	return cql.StringEncoder.Encode(w, n.FirstName+" "+n.LastName)
}
func (fullNameCodec) Decode(r cql.ValueReader) (FullName, error) {
	text, err := cql.TextCodec.Decode(r)
	if err != nil {
		return FullName{}, err
	}
	names := strings.SplitN(text.String(), " ", 2)
	if len(names) != 2 {
		return FullName{}, errors.New("full name requires two components")
	}
	return FullName{FirstName: names[0], LastName: names[1]}, nil
}
func (fullNameCodec) Clone(n FullName) FullName { return n }

type cassandraVarintInt64 struct{}

func (cassandraVarintInt64) CheckType(t cql.DataType) error { return cql.VarintCodec.CheckType(t) }
func (cassandraVarintInt64) Decode(r cql.ValueReader) (int64, error) {
	v, err := cql.VarintCodec.Decode(r)
	if err != nil {
		return 0, err
	}
	n := v.BigInt()
	if !n.IsInt64() {
		return 0, fmt.Errorf("varint is outside int64 range")
	}
	return n.Int64(), nil
}
func (cassandraVarintInt64) Clone(v int64) int64 { return v }

type cassandraReconnect struct{}

func (cassandraReconnect) Delay(attempt int) (time.Duration, bool) { return time.Second, true }

type FullName struct {
	FirstName string
	LastName  string
}

// unixSocketDialer is a special dialer which connects only to the maintenance_socket.
type unixSocketDialer struct {
	dialer     net.Dialer
	socketPath string
}

func (d unixSocketDialer) DialContext(ctx context.Context, target endpoint.DialTarget) (net.Conn, error) {
	return d.dialer.DialContext(ctx, target.Network, target.Address)
}

type unixSocketNoReconnect struct{}

func (unixSocketNoReconnect) Delay(int) (time.Duration, bool) { return 0, false }

type mockDNSResolver struct {
	lock sync.RWMutex
	data map[string][]net.IP
}

func (r *mockDNSResolver) LookupIP(host string) ([]net.IP, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	ips, _ := r.data[host]
	if len(ips) == 0 {
		return nil, &net.DNSError{Err: errors.New("no IP addresses").Error(), Name: host}
	}
	return ips, nil
}

func (r *mockDNSResolver) Update(host string, ips ...net.IP) {
	r.lock.Lock()
	r.data[host] = ips
	defer r.lock.Unlock()
}

func (r *mockDNSResolver) Delete(hosts ...string) {
	r.lock.Lock()
	for _, host := range hosts {
		delete(r.data, host)
	}
	defer r.lock.Unlock()
}

type framer = protocol.Framer

const headSize = protocol.HeadSize

const protoVersion4 = protocol.ProtoVersion4

func validateShortBytes(a0 string, a1 []byte) error { return protocol.ValidateShortBytes(a0, a1) }

func retiredScyllaGetSourcePort(a0 context.Context) uint16 {
	return implEndpoint.ScyllaGetSourcePort(a0)
}

type retiredScyllaShardAwareDialer = implEndpoint.ScyllaShardAwareDialer

type cassVersion = implEndpoint.InternalCassVersion

type scyllaSourcePortCtx = implEndpoint.InternalScyllaSourcePortCtx

func fixtureKeyspace(suffix string) string {
	prefix := os.Getenv("GOCQL_TEST_RUN")
	if prefix == "" {
		prefix = "gocql_test"
	}
	if len(prefix) > 24 {
		panic("GOCQL_TEST_RUN exceeds 24 characters")
	}
	for _, c := range prefix {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_') {
			panic("invalid GOCQL_TEST_RUN identifier")
		}
	}
	if prefix[0] < 'a' || prefix[0] > 'z' {
		panic("GOCQL_TEST_RUN must start with a lowercase letter")
	}
	return prefix + suffix
}

func codecTestMust[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// bindOptions binds v to session, failing loudly on values a test meant to
// be valid.
func bindOptions(session *gocql.Session, v gocql.Options) gocql.SessionOptions {
	o, err := gocql.BindOptions(session, v)
	if err != nil {
		panic(err)
	}
	return o
}

// fixtureParts names the settings the shared fixture chooses from the
// integration flags. A test that chooses one of them itself leaves that part
// out, because a setting can be chosen only once.
type fixtureParts uint

const (
	fixtureProtocol    fixtureParts = 1 << iota // ProtocolVersion and CQLVersion
	fixtureCompression                          // Compression
	fixtureAuth                                 // Auth
	fixtureTLS                                  // TLS; the port is still chosen
	fixtureAgreement                            // SchemaAgreement
	fixtureConsistency                          // default profile Consistency
	fixtureTimeout                              // default profile Timeout
	fixtureRetry                                // default profile Retry
)

// createV2Cluster starts a fixture config for the integration cluster.
func createV2Cluster(opts ...func(*gocql.ConfigBuilder)) *gocql.ConfigBuilder {
	return newFixtureConfig(getClusterHosts(), 0, opts...)
}

// createV2ClusterWithout starts a fixture config that leaves out skip, for a
// test that chooses those settings itself.
func createV2ClusterWithout(skip fixtureParts, opts ...func(*gocql.ConfigBuilder)) *gocql.ConfigBuilder {
	return newFixtureConfig(getClusterHosts(), skip, opts...)
}

// newFixtureConfig applies the integration flags, except skip, to a config
// for hosts, then opts.
func newFixtureConfig(hosts []string, skip fixtureParts, opts ...func(*gocql.ConfigBuilder)) *gocql.ConfigBuilder {
	b := gocql.NewConfig(hosts...)
	if skip&fixtureProtocol == 0 {
		b.ProtocolVersion(gocql.ProtocolVersion(*flagProto)).CQLVersion(*flagCQL)
	}
	if skip&fixtureAgreement == 0 {
		b.SchemaAgreement(2*time.Minute, 200*time.Millisecond)
	}
	b.DefaultProfile(func(p *gocql.ProfileBuilder) { fixtureProfileExcept(p, skip) })
	if *flagRunAuthTest && skip&fixtureAuth == 0 {
		b.Auth(auth.Password("cassandra", "cassandra"))
	}
	if skip&fixtureCompression == 0 {
		switch *flagCompressTest {
		case "snappy":
			b.Compression(gocql.SnappyCompression)
		case "lz4":
			b.Compression(gocql.LZ4Compression)
		case "no-compression", "":
			b.Compression(gocql.NoCompression)
		default:
			panic("invalid compressor: " + *flagCompressTest)
		}
	}
	if *flagRunSslTest {
		if *flagDistribution == "cassandra" {
			b.Port(9042)
		} else {
			b.Port(9142)
		}
		if skip&fixtureTLS == 0 {
			b.TLS(fixtureTLSConfig(fixtureClientTLS()))
		}
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// fixtureProfileExcept applies the fixture's profile choices, except skip.
func fixtureProfileExcept(p *gocql.ProfileBuilder, skip fixtureParts) {
	if skip&fixtureConsistency == 0 {
		p.Consistency(cql.Quorum)
	}
	if skip&fixtureTimeout == 0 {
		p.Timeout(*flagTimeout)
	}
	if skip&fixtureRetry == 0 {
		p.Retry(retry.Safe{MaxAttempts: max(1, *flagRetry+1)})
	}
}

// fixtureProfile applies the integration flags to a profile. createV2Cluster
// uses it for the default profile; tests use it for per-query profiles.
func fixtureProfile(p *gocql.ProfileBuilder) { fixtureProfileExcept(p, 0) }

// fixtureClientTLS returns the integration client's certificate and CA.
func fixtureClientTLS() *tls.Config {
	certificate, err := tls.LoadX509KeyPair("../../../testdata/pki/gocql.crt", "../../../testdata/pki/gocql.key")
	if err != nil {
		panic(fmt.Errorf("load integration client certificate: %w", err))
	}
	pem, err := os.ReadFile("../../../testdata/pki/ca.crt")
	if err != nil {
		panic(fmt.Errorf("load integration CA: %w", err))
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		panic("invalid integration CA certificate")
	}
	return &tls.Config{
		Certificates: []tls.Certificate{certificate},
		RootCAs:      roots,
		MinVersion:   tls.VersionTLS12,
	}
}

// fixtureTLSConfig verifies the fixture server, which identifies itself with
// a SPIFFE URI rather than an IP SAN. It keeps base's client certificates,
// session cache and callbacks, but replaces the default IP check with chain
// and URI verification; base's VerifyConnection runs after those checks.
func fixtureTLSConfig(base *tls.Config) *tls.Config {
	fixtureTLS := base.Clone()
	verifyConnection := fixtureTLS.VerifyConnection
	fixtureTLS.InsecureSkipVerify = true
	fixtureTLS.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			return fmt.Errorf("integration TLS server supplied no certificate")
		}
		intermediates := x509.NewCertPool()
		for _, certificate := range state.PeerCertificates[1:] {
			intermediates.AddCert(certificate)
		}
		now := time.Now()
		if fixtureTLS.Time != nil {
			now = fixtureTLS.Time()
		}
		chains, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{
			Roots:         fixtureTLS.RootCAs,
			Intermediates: intermediates,
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			CurrentTime:   now,
		})
		if err != nil {
			return fmt.Errorf("verify integration TLS server chain: %w", err)
		}
		const expectedURI = "spiffe://test.cassandra.apache.org/cassandra-gocql-driver/integrationTest/cassandra"
		matched := false
		for _, identity := range state.PeerCertificates[0].URIs {
			if identity.String() == expectedURI {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("integration TLS server certificate has no expected URI SAN %q", expectedURI)
		}
		if verifyConnection != nil {
			state.VerifiedChains = chains
			return verifyConnection(state)
		}
		return nil
	}
	return fixtureTLS
}

func createV2Session(t testing.TB, opts ...func(*gocql.ConfigBuilder)) *gocql.Session {
	t.Helper()
	return createV2SessionFromConfig(createV2Cluster(opts...), t, false)
}

// mustBuild builds b or fails the test.
func mustBuild(t testing.TB, b *gocql.ConfigBuilder) gocql.Config {
	t.Helper()
	config, err := b.Build()
	if err != nil {
		t.Fatalf("build integration config: %v", err)
	}
	return config
}

func createV2SessionFromConfig(b *gocql.ConfigBuilder, t testing.TB, tabletsDisabled bool) *gocql.Session {
	t.Helper()
	keyspace := fixtureKeyspace("")
	if tabletsDisabled {
		keyspace = fixtureKeyspace("_td")
	}
	// Use a separate once key while both fixture implementations coexist. Creating
	// an existing keyspace is harmless; dropping it would race other test sessions.
	initKeyspaceOnce.GetOnce("v2:" + keyspace).Do(func() {
		// Keyspace creation uses a fresh fixture config: the test's own
		// choices may not reach a working cluster.
		bootstrap := mustBuild(t, createV2ClusterWithout(fixtureTimeout).Keyspace("system").
			DefaultProfile(func(p *gocql.ProfileBuilder) { p.Timeout(30 * time.Second) }))
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		session, err := gocql.Connect(ctx, bootstrap)
		if err != nil {
			t.Fatalf("connect to create typed integration keyspace: %v", err)
		}
		defer session.Close()
		supported, err := v2TabletsSupported(ctx, session)
		if err != nil {
			t.Fatalf("probe tablet support: %v", err)
		}
		stmt := fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': %d}", keyspace, *flagRF)
		if supported {
			stmt += fmt.Sprintf(" AND tablets = {'enabled': %t}", !tabletsDisabled)
		}
		if err := v2CreateTable(ctx, session, stmt); err != nil {
			t.Fatalf("create typed integration keyspace: %v", err)
		}
	})
	config := mustBuild(t, b.Keyspace(keyspace))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	session, err := gocql.Connect(ctx, config)
	if err != nil {
		t.Fatalf("create typed integration session: %v", err)
	}
	if err := session.AwaitSchemaAgreement(ctx); err != nil {
		session.Close()
		t.Fatalf("wait for typed integration schema agreement: %v", err)
	}
	if err := session.RefreshMetadata(ctx); err != nil {
		session.Close()
		t.Fatalf("refresh typed integration metadata: %v", err)
	}
	return session
}

func v2TabletsSupported(ctx context.Context, session *gocql.Session) (bool, error) {
	row, err := v2ReadOne(ctx, session, "SELECT * FROM system.local")
	if err != nil {
		return false, err
	}
	// Cassandra does not expose supported_features. Check the schema before
	// looking up the value so a missing column is distinct from a decode failure.
	for i := 0; i < row.Columns().Len(); i++ {
		column, _ := row.Columns().At(i)
		if column.Name != "supported_features" {
			continue
		}
		value, err := row.At(i)
		if err != nil {
			return false, err
		}
		if value.State() == cql.Null {
			return false, nil
		}
		features, err := value.Text()
		if err != nil {
			return false, err
		}
		for _, feature := range strings.Split(features.String(), ",") {
			if feature == "TABLETS" {
				return true, nil
			}
		}
		return false, nil
	}
	return false, nil
}

func v2Query(ctx context.Context, session *gocql.Session, stmt string, values ...cql.Value) (gocql.Rows[cql.DynamicRow], error) {
	statement := cql.Statement[[]cql.Value, cql.DynamicRow]{CQL: stmt, Params: cql.DynamicParams, Rows: cql.DynamicRows}
	var bound *Bound[cql.DynamicRow]
	var err error
	if len(values) == 0 {
		bound, err = BindUnpreparedOwned(statement, cql.NewColumns(), values)
	} else {
		var prepared *gocql.Prepared[[]cql.Value, cql.DynamicRow]
		prepared, err = gocql.Prepare(ctx, session, statement, gocql.PrepareOptions{})
		if err == nil {
			bound, err = BindOwned(prepared, values)
		}
	}
	if err != nil {
		return gocql.Rows[cql.DynamicRow]{}, err
	}
	// Run admits the request before returning. Rows owns the execution reference
	// until it is exhausted or closed, even after closing the binding here.
	defer bound.Close()
	return Run(ctx, session, bound, gocql.SessionOptions{})
}

func v2ReadOne(ctx context.Context, session *gocql.Session, stmt string, values ...cql.Value) (cql.DynamicRow, error) {
	rows, err := v2Query(ctx, session, stmt, values...)
	if err != nil {
		return cql.DynamicRow{}, err
	}
	defer rows.Close()
	var current cql.DynamicRow
	if !rows.NextInto(&current) {
		if err := rows.Err(); err != nil {
			return cql.DynamicRow{}, err
		}
		return cql.DynamicRow{}, cqlerr.ErrNoRows
	}
	row, err := rows.Clone()
	if err != nil {
		return cql.DynamicRow{}, err
	}
	if rows.NextInto(&current) {
		return cql.DynamicRow{}, cqlerr.ErrMultipleRows
	}
	if err := rows.Err(); err != nil {
		return cql.DynamicRow{}, err
	}
	return row, rows.Close()
}

func v2CreateTable(ctx context.Context, session *gocql.Session, stmt string) error {
	bound, err := BindUnpreparedOwned(cql.Command(stmt), cql.NewColumns(), cql.EmptyParams{})
	if err != nil {
		return err
	}
	defer bound.Close()
	if _, err := Exec(ctx, session, bound, bindOptions(session, gocql.Options{Idempotent: true, Retry: retry.Safe{MaxAttempts: 4}})); err != nil {
		return err
	}
	if err := session.AwaitSchemaAgreement(ctx); err != nil {
		return err
	}
	return session.RefreshMetadata(ctx)
}

// incompletePeerDialer appends an incomplete peer to real database responses.
// It exercises discovery through the transport without modifying system tables.
// This fixture uses uncompressed protocol v4 against Cassandra so its framing
// is independent of Scylla extensions and v5 transport segments.
type incompletePeerDialer struct {
	net.Dialer
	injected atomic.Int64
}

func (d *incompletePeerDialer) DialContext(ctx context.Context, target endpoint.DialTarget) (net.Conn, error) {
	network, address := target.Network, target.Address
	conn, err := d.Dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return &incompletePeerConn{
		Conn: conn, injected: &d.injected,
		requests: make(map[uint16]peerFixtureRequest), prepared: make(map[string]string),
	}, nil
}

type peerFixtureRequest struct {
	op    frm.Op
	query string
}

type incompletePeerConn struct {
	net.Conn
	injected *atomic.Int64
	writeMu  sync.Mutex
	readMu   sync.Mutex
	mu       sync.Mutex
	requests map[uint16]peerFixtureRequest
	prepared map[string]string
	written  []byte
	pending  []byte
}

func (c *incompletePeerConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.written = append(c.written, p...)
	for len(c.written) >= headSize {
		length := int(binary.BigEndian.Uint32(c.written[5:9]))
		if length > frm.MaxFrameSize {
			return 0, fmt.Errorf("peer fixture: oversized request frame")
		}
		if len(c.written) < headSize+length {
			break
		}
		header, body := c.written[:headSize], c.written[headSize:headSize+length]
		request := peerFixtureRequest{op: frm.Op(header[4])}
		c.mu.Lock()
		switch request.op {
		case frm.OpQuery, frm.OpPrepare:
			if len(body) >= 4 {
				n := int(binary.BigEndian.Uint32(body[:4]))
				if n <= len(body)-4 {
					request.query = strings.ToLower(string(body[4 : 4+n]))
				}
			}
		case frm.OpExecute:
			if len(body) >= 2 {
				n := int(binary.BigEndian.Uint16(body[:2]))
				if n <= len(body)-2 {
					request.query = c.prepared[string(body[2:2+n])]
				}
			}
		}
		if strings.Contains(request.query, "system.peers") {
			c.requests[binary.BigEndian.Uint16(header[2:4])] = request
		}
		c.mu.Unlock()
		c.written = c.written[headSize+length:]
	}
	return c.Conn.Write(p)
}

func (c *incompletePeerConn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if len(p) == 0 {
		return 0, nil
	}
	if len(c.pending) == 0 {
		var header [headSize]byte
		if _, err := io.ReadFull(c.Conn, header[:]); err != nil {
			return 0, err
		}
		length := int(binary.BigEndian.Uint32(header[5:9]))
		if length > frm.MaxFrameSize {
			return 0, fmt.Errorf("peer fixture: oversized response frame")
		}
		body := make([]byte, length)
		if _, err := io.ReadFull(c.Conn, body); err != nil {
			return 0, err
		}
		stream := binary.BigEndian.Uint16(header[2:4])
		c.mu.Lock()
		request, tracked := c.requests[stream]
		delete(c.requests, stream)
		c.mu.Unlock()
		if tracked && frm.Op(header[4]) == frm.OpResult {
			if header[1] != 0 || len(body) < 4 {
				return 0, fmt.Errorf("peer fixture requires ordinary v4 result envelope")
			}
			switch binary.BigEndian.Uint32(body[:4]) {
			case 4: // RESULT/PREPARED: short prepared ID precedes all metadata.
				if len(body) < 6 {
					return 0, io.ErrUnexpectedEOF
				}
				n := int(binary.BigEndian.Uint16(body[4:6]))
				if n > len(body)-6 {
					return 0, io.ErrUnexpectedEOF
				}
				c.mu.Lock()
				c.prepared[string(body[6:6+n])] = request.query
				c.mu.Unlock()
			case 2:
				var err error
				body, err = testkit.AppendIncompletePeer(body)
				if err != nil {
					return 0, err
				}
				c.injected.Add(1)
			}
		}

		binary.BigEndian.PutUint32(header[5:9], uint32(len(body)))
		c.pending = append(header[:], body...)
	}
	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

func (c *incompletePeerConn) SetNoDelay(v bool) error   { return c.Conn.(*net.TCPConn).SetNoDelay(v) }
func (c *incompletePeerConn) SetKeepAlive(v bool) error { return c.Conn.(*net.TCPConn).SetKeepAlive(v) }
func (c *incompletePeerConn) SetKeepAlivePeriod(v time.Duration) error {
	return c.Conn.(*net.TCPConn).SetKeepAlivePeriod(v)
}
func (c *incompletePeerConn) SetReadBuffer(v int) error {
	return c.Conn.(*net.TCPConn).SetReadBuffer(v)
}
func (c *incompletePeerConn) SetWriteBuffer(v int) error {
	return c.Conn.(*net.TCPConn).SetWriteBuffer(v)
}
func (c *incompletePeerConn) SetLinger(v int) error { return c.Conn.(*net.TCPConn).SetLinger(v) }

// A test-only metadata input models down/removal events while retaining actual
// query admission and routing. Session metadata snapshots are immutable, so this
// fault injection cannot change shared HostInfo objects used by other sessions.
type v2UnavailableHostsRouting struct {
	routing.Policy
	oneHost bool
}

func (p v2UnavailableHostsRouting) Pick(info routing.Info, metadata *metadata.Snapshot) (routing.Route, bool) {
	return p.Policy.Pick(info, p.unavailable(metadata))
}

func (p v2UnavailableHostsRouting) Fallback(info routing.Info, metadata *metadata.Snapshot) routing.Plan {
	return p.Policy.Fallback(info, p.unavailable(metadata))
}

// unavailable returns metadata with every host down.
func (p v2UnavailableHostsRouting) unavailable(metadata *metadata.Snapshot) *metadata.Snapshot {
	snapshot := *metadata
	implRouting.SnapshotData(&snapshot).InternalHostIndex = nil
	implRouting. // rebuild indices after test-only membership changes
			SnapshotData(&snapshot).InternalHosts = metadata.Hosts()
	for i := range implRouting.SnapshotData(&snapshot).InternalHosts {
		implRouting.SnapshotData(&snapshot).InternalHosts[i].Up = false
	}
	if p.oneHost && len(implRouting.SnapshotData(&snapshot).InternalHosts) > 1 {
		implRouting.SnapshotData(&snapshot).InternalHosts = implRouting.SnapshotData(&snapshot).InternalHosts[:1]
	}
	return &snapshot
}

type liveDestination struct {
	Value   int32
	Payload cql.Bytes
}
type liveDecoder struct {
	calls *atomic.Int32
	base  cql.RowDecoder[liveDestination]
}

func (d liveDecoder) Compile(c cql.Columns) (cql.RowPlan[liveDestination], error) {
	d.calls.Add(1)
	return d.base.Compile(c)
}
func (d liveDecoder) Clone(v liveDestination) liveDestination {
	return d.base.Clone(v)
}

type countingDecoder struct {
	calls *atomic.Int32
	hook  func()
}

func (d countingDecoder) Compile(c cql.Columns) (cql.RowPlan[cql.Bytes], error) {
	d.calls.Add(1)
	if d.hook != nil {
		d.hook()
	}
	return cql.OneColumn(cql.BlobCodec).Compile(c)
}
func (d countingDecoder) Clone(v cql.Bytes) cql.Bytes { return v.Clone() }

// Independent native request fixtures produce byte oracles for the canonical
// span writer. These own already-encoded test bytes only; no production request
// can enter them. Oracle self-tests are distinct from typed writer regressions.
type fixtureQueryValue struct {
	name    string
	value   []byte
	isUnset bool
}

type fixtureQueryParams struct {
	nowInSeconds          *int
	keyspace              string
	values                []fixtureQueryValue
	pagingState           []byte
	pageSize              int
	defaultTimestampValue int64
	consistency           cql.Consistency
	serialConsistency     cql.Consistency
	skipMeta              bool
	defaultTimestamp      bool
}

func (q fixtureQueryParams) String() string {
	return fmt.Sprintf("[query_params consistency=%v skip_meta=%v page_size=%d paging_state=%q serial_consistency=%v default_timestamp=%v values=%v keyspace=%s now_in_seconds=%v]",
		q.consistency, q.skipMeta, q.pageSize, q.pagingState, q.serialConsistency, q.defaultTimestamp, q.values, q.keyspace, q.nowInSeconds)
}

func fixtureWriteQueryParams(f *framer, opts *fixtureQueryParams) error {
	// Validated again here, not only in the callers' BuildFrame: this function is
	// package-internal and nothing else would enforce the precondition.
	if err := f.ValidateV5Options(opts.keyspace, opts.nowInSeconds); err != nil {
		return err
	}

	f.WriteConsistency(opts.consistency)

	var flags uint32
	names := false

	if len(opts.values) > 0 {
		flags |= frm.FlagValues
	}
	if opts.skipMeta {
		flags |= frm.FlagSkipMetaData
	}
	if opts.pageSize > 0 {
		flags |= frm.FlagPageSize
	}
	if len(opts.pagingState) > 0 {
		flags |= frm.FlagWithPagingState
	}
	if opts.serialConsistency > 0 {
		flags |= frm.FlagWithSerialConsistency
	}

	// protoV3 specific things
	if opts.defaultTimestamp {
		flags |= frm.FlagDefaultTimestamp
	}

	if len(opts.values) > 0 && opts.values[0].name != "" {
		flags |= frm.FlagWithNameValues
		names = true
	}

	if opts.keyspace != "" {
		flags |= frm.FlagWithKeyspace
	}

	if opts.nowInSeconds != nil {
		flags |= frm.FlagWithNowInSeconds
	}

	if f.Proto > protoVersion4 {
		f.WriteUint(flags)
	} else {
		f.WriteByteValue(byte(flags))
	}

	if n := len(opts.values); n > 0 {
		f.WriteShort(uint16(n))

		for i := 0; i < n; i++ {
			if names {
				f.WriteString(opts.values[i].name)
			}
			if opts.values[i].isUnset {
				f.WriteUnset()
			} else {
				f.WriteBytes(opts.values[i].value)
			}
		}
	}

	if opts.pageSize > 0 {
		f.WriteInt(int32(opts.pageSize))
	}

	if len(opts.pagingState) > 0 {
		f.WriteBytes(opts.pagingState)
	}

	if opts.serialConsistency > 0 {
		f.WriteConsistency(opts.serialConsistency)
	}

	if opts.defaultTimestamp {
		// timestamp in microseconds
		var ts int64
		if opts.defaultTimestampValue != 0 {
			ts = opts.defaultTimestampValue
		} else {
			ts = time.Now().UnixNano() / 1000
		}
		f.WriteLong(ts)
	}

	if opts.keyspace != "" {
		f.WriteString(opts.keyspace)
	}

	if opts.nowInSeconds != nil {
		// Bounds already validated at the top of this function.
		f.WriteInt(int32(*opts.nowInSeconds))
	}
	return nil
}

type fixtureExecuteFrame struct {
	customPayload    map[string][]byte
	preparedID       []byte
	resultMetadataID []byte
	params           fixtureQueryParams
}

func (e *fixtureExecuteFrame) String() string {
	return fmt.Sprintf("[execute id=% X params=%v]", e.preparedID, &e.params)
}

func (e *fixtureExecuteFrame) BuildFrame(fr *framer, streamID int) error {
	return fixtureWriteExecute(fr, streamID, e.preparedID, e.resultMetadataID, &e.params, &e.customPayload)
}

func fixtureWriteExecute(f *framer, streamID int, preparedID, resultMetadataID []byte, params *fixtureQueryParams, customPayload *map[string][]byte) error {
	// Validate first, as in fixtureQueryFrame: the prepared id (and, on v5, the
	// result metadata id) are written before writeFixtureQueryParams runs.
	if err := f.ValidateV5Options(params.keyspace, params.nowInSeconds); err != nil {
		return err
	}
	writeResultMetadataID := f.Proto > protoVersion4 || f.ScyllaUseMetadataID
	if err := validateShortBytes("prepared id", preparedID); err != nil {
		return err
	}
	if writeResultMetadataID {
		if err := validateShortBytes("result metadata id", resultMetadataID); err != nil {
			return err
		}
	}

	if len(*customPayload) > 0 {
		f.Payload()
	}
	f.WriteHeader(f.Flags, frm.OpExecute, streamID)
	f.WriteCustomPayload(customPayload)
	if err := f.WriteShortBytes(preparedID); err != nil {
		return err
	}

	if writeResultMetadataID {
		if err := f.WriteShortBytes(resultMetadataID); err != nil {
			return err
		}
	}

	if err := fixtureWriteQueryParams(f, params); err != nil {
		return err
	}

	return f.Finish()
}

type testConviction struct {
	mu      sync.Mutex
	cause   error
	hosts   []routing.HostSnapshot
	targets []routing.HostSnapshot
	verdict bool
	resets  int
}

func (p *testConviction) AddFailure(err error, host routing.HostSnapshot) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !errors.Is(err, p.cause) {
		panic("conviction lost original failure")
	}
	host.Tokens = append([]int64(nil), host.Tokens...)
	p.hosts = append(p.hosts, host)
	return p.verdict
}
func (p *testConviction) Reset(host routing.HostSnapshot) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.hosts) == 0 || p.hosts[0].ID != host.ID {
		panic("reset lost host identity")
	}
	p.resets++
}

type noReconnect struct{}

func (noReconnect) Delay(int) (time.Duration, bool) { return 0, false }

var ScyllaGetSourcePort = retiredScyllaGetSourcePort

type ScyllaShardAwareDialer = retiredScyllaShardAwareDialer

type sourcePortOffByOneTestDialer struct {
	ScyllaShardAwareDialer
}

func (spobo *sourcePortOffByOneTestDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	// Simulate a NAT that always increases the source port by 1
	// This should always result in wrong shard being assigned if host
	// has more than one shard.

	sourcePort := ScyllaGetSourcePort(ctx)
	if sourcePort > 0 {
		sourcePort++
	}
	newCtx := context.WithValue(ctx, scyllaSourcePortCtx{}, sourcePort)
	return spobo.ScyllaShardAwareDialer.DialContext(newCtx, network, addr)
}

type allowOnlyNonShardAwarePortDialer struct {
	net.Dialer

	allowedPort uint16
}

func (aonsa *allowOnlyNonShardAwarePortDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	// Simulate a network configuration which allows connections to the
	// non-shard-aware port, but not to the shard-aware one.

	_, targetPort, _ := net.SplitHostPort(addr)
	if targetPort != strconv.Itoa(int(aonsa.allowedPort)) {
		return nil, fmt.Errorf("allowOnlyNonShardAwarePortDialer: tried to connect to port %s, but only %d is allowed", targetPort, aonsa.allowedPort)
	}

	return aonsa.Dialer.DialContext(ctx, network, addr)
}

type loggingTestDialer struct {
	ScyllaShardAwareDialer

	events map[string][]testConnectionEvent
	mu     sync.Mutex
}

type testConnectionEvent struct {
	sourcePort, destinationPort uint16
}

func (ltd *loggingTestDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	ipaddr, destinationPortStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	destinationPort, err := strconv.ParseUint(destinationPortStr, 10, 16)
	if err != nil {
		return nil, err
	}

	conn, err := ltd.ScyllaShardAwareDialer.DialContext(ctx, network, addr)

	if err == nil {
		ltd.mu.Lock()
		defer ltd.mu.Unlock()

		evt := testConnectionEvent{
			sourcePort:      uint16(conn.LocalAddr().(*net.TCPAddr).Port),
			destinationPort: uint16(destinationPort),
		}
		ltd.events[ipaddr] = append(ltd.events[ipaddr], evt)
	}

	return conn, err
}

func (ltd *loggingTestDialer) connectionEvents(address string) []testConnectionEvent {
	ltd.mu.Lock()
	defer ltd.mu.Unlock()
	return append([]testConnectionEvent(nil), ltd.events[address]...)
}

// These fixture helpers compose the public codecs; the integration values never
// pass through reflected parameter binding or result destination discovery.
func serializationExec[P any](s *gocql.Session, stmt string, encoder cql.ParamEncoder[P], value P) error {
	ctx := context.Background()
	p, err := gocql.Prepare(ctx, s, cql.Statement[P, cql.EmptyRow]{CQL: stmt, Params: encoder, Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
	if err != nil {
		return err
	}
	b, err := BindOwned(p, value)
	if err != nil {
		return err
	}
	defer b.Close()
	_, err = Exec(ctx, s, b, gocql.SessionOptions{})
	return err
}

func serializationRows[R any](s *gocql.Session, stmt string, decoder cql.RowDecoder[R], values ...cql.Value) (gocql.Rows[R], error) {
	ctx := context.Background()
	p, err := gocql.Prepare(ctx, s, cql.Statement[[]cql.Value, R]{CQL: stmt, Params: cql.DynamicParams, Rows: decoder}, gocql.PrepareOptions{})
	if err != nil {
		return gocql.Rows[R]{}, err
	}
	b, err := BindOwned(p, values)
	if err != nil {
		return gocql.Rows[R]{}, err
	}
	defer b.Close()
	return Run(ctx, s, b, gocql.SessionOptions{})
}

func serializationOne[R any](s *gocql.Session, stmt string, decoder cql.RowDecoder[R], values ...cql.Value) (R, error) {
	var zero R
	rows, err := serializationRows(s, stmt, decoder, values...)
	if err != nil {
		return zero, err
	}
	defer rows.Close()
	var current R
	if !rows.NextInto(&current) {
		if err := rows.Err(); err != nil {
			return zero, err
		}
		return zero, cqlerr.ErrNoRows
	}
	row, err := rows.Clone()
	if err != nil {
		return zero, err
	}
	if rows.NextInto(&current) {
		return zero, cqlerr.ErrMultipleRows
	}
	if err := rows.Err(); err != nil {
		return zero, err
	}
	return row, rows.Close()
}

// Owned application representations are explicit, opt-in conversions in tests.
type serializationStringCodec struct{}

func (serializationStringCodec) CheckType(t cql.DataType) error {
	return cql.TextCodec.CheckType(t)
}
func (serializationStringCodec) Encode(w *cql.ValueWriter, s string) error {
	return cql.StringEncoder.Encode(w, s)
}
func (serializationStringCodec) Decode(r cql.ValueReader) (string, error) {
	v, e := cql.TextCodec.Decode(r)
	if e != nil {
		return "", e
	}
	return v.String(), nil
}
func (serializationStringCodec) Clone(s string) string { return s }

var serializationString cql.Codec[string] = serializationStringCodec{}

type serializationSliceCodec[T any] struct {
	encoder cql.Encoder[[]T]
	decoder cql.Decoder[cql.Sequence[T]]
	element cql.Decoder[T]
}

func (c serializationSliceCodec[T]) CheckType(t cql.DataType) error {
	if err := c.encoder.CheckType(t); err != nil {
		return err
	}
	return c.decoder.CheckType(t)
}
func (c serializationSliceCodec[T]) Encode(w *cql.ValueWriter, v []T) error {
	return c.encoder.Encode(w, v)
}
func (c serializationSliceCodec[T]) Decode(r cql.ValueReader) ([]T, error) {
	s, e := c.decoder.Decode(r)
	if e != nil {
		return nil, e
	}
	return cql.Collect(s.Iterator())
}
func (c serializationSliceCodec[T]) Clone(v []T) []T {
	if v == nil {
		return nil
	}
	out := make([]T, len(v))
	for i := range v {
		out[i] = c.element.Clone(v[i])
	}
	return out
}

func serializationSet[T any](c cql.Codec[T]) cql.Codec[[]T] {
	return serializationSliceCodec[T]{cql.SetEncoder(c), cql.SetDecoder(c), c}
}

type serializationCodec[T any] struct {
	cql.Encoder[T]
	cql.Decoder[T]
}

func (c serializationCodec[T]) CheckType(t cql.DataType) error {
	if err := c.Encoder.CheckType(t); err != nil {
		return err
	}
	return c.Decoder.CheckType(t)
}

type serializationMapCodec[K comparable, V any] struct {
	key   cql.Codec[K]
	value cql.Codec[V]
}

func (c serializationMapCodec[K, V]) CheckType(t cql.DataType) error {
	if e := cql.MapEncoder(c.key, c.value).CheckType(t); e != nil {
		return e
	}
	return cql.MapDecoder(c.key, c.value).CheckType(t)
}
func (c serializationMapCodec[K, V]) Encode(w *cql.ValueWriter, v map[K]V) error {
	entries := make([]cql.Entry[K, V], 0, len(v))
	for k, x := range v {
		entries = append(entries, cql.Entry[K, V]{Key: k, Value: x})
	}
	return cql.MapEncoder(c.key, c.value).Encode(w, entries)
}
func (c serializationMapCodec[K, V]) Decode(r cql.ValueReader) (map[K]V, error) {
	m, e := cql.MapDecoder(c.key, c.value).Decode(r)
	if e != nil {
		return nil, e
	}
	out := make(map[K]V, m.Len())
	it := m.Iterator()
	for it.Next() {
		v := it.Value()
		out[v.Key] = v.Value
	}
	return out, it.Err()
}
func (c serializationMapCodec[K, V]) Clone(v map[K]V) map[K]V {
	if v == nil {
		return nil
	}
	o := make(map[K]V, len(v))
	for k, x := range v {
		o[c.key.Clone(k)] = c.value.Clone(x)
	}
	return o
}
func serializationMap[K comparable, V any](k cql.Codec[K], v cql.Codec[V]) cql.Codec[map[K]V] {
	return serializationMapCodec[K, V]{k, v}
}

type serializationRawDecoder struct{}

func (serializationRawDecoder) CheckType(t cql.DataType) error {
	return cql.DynamicCodec.CheckType(t)
}
func (serializationRawDecoder) Decode(r cql.ValueReader) (cql.EncodedValue, error) {
	return cql.NewEncodedValue(r.Type(), r.State(), r.Bytes())
}
func (serializationRawDecoder) Clone(v cql.EncodedValue) cql.EncodedValue { return v.Clone() }

// The shared typed session intentionally preserves its keyspace. Names include
// a process nonce so repeated runs remain isolated from previous run data/schema.
var serializationFixtureNamespace = cql.NewTimeUUID().String()

func serializationTableName(t testing.TB, parts ...string) string {
	parts = append(parts, "v2", serializationFixtureNamespace)
	return testTableName(t, parts...)
}

type OneConnTestServer struct {
	Err        error
	listener   net.Listener
	acceptChan chan struct{}
	Addr       net.IP
	Port       int
	mu         sync.Mutex
	closed     bool
}

func (c *OneConnTestServer) Accepted() chan struct{} {
	return c.acceptChan
}

func (c *OneConnTestServer) Close() {
	c.lockedClose()
}

func (c *OneConnTestServer) Serve() {
	conn, err := c.listener.Accept()
	c.Err = err
	if conn != nil {
		conn.Close()
	}
	c.lockedClose()
}

func (c *OneConnTestServer) lockedClose() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		close(c.acceptChan)
		c.listener.Close()
		c.closed = true
	}
}

type sessionTestObserver struct {
	mu       sync.Mutex
	requests []gocql.RequestEvent
	attempts []gocql.AttemptEvent
}

func (o *sessionTestObserver) ObserveRequest(e gocql.RequestEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.requests = append(o.requests, e)
}
func (o *sessionTestObserver) ObserveAttempt(e gocql.AttemptEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.attempts = append(o.attempts, e)
}
func (o *sessionTestObserver) snapshot() ([]gocql.RequestEvent, []gocql.AttemptEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]gocql.RequestEvent(nil), o.requests...), append([]gocql.AttemptEvent(nil), o.attempts...)
}

type sessionTestRetry struct {
	action      retry.Action
	maxAttempts int
}

func (p sessionTestRetry) Decide(e retry.Attempt) retry.Decision {
	if e.Number >= p.maxAttempts {
		return retry.Decision{Action: retry.Stop}
	}
	return retry.Decision{Action: p.action}
}

type sessionCache struct {
	orig       tls.ClientSessionCache
	values     map[string][][]byte
	caches     map[string][]int64
	valuesLock sync.Mutex
}

func (c *sessionCache) Get(sessionKey string) (session *tls.ClientSessionState, ok bool) {
	return c.orig.Get(sessionKey)
}

func (c *sessionCache) Put(sessionKey string, cs *tls.ClientSessionState) {
	if cs == nil {
		c.orig.Put(sessionKey, nil)
		return
	}
	ticket, _, err := cs.ResumptionState()
	if err != nil {
		panic(err)
	}
	if len(ticket) == 0 {
		panic("ticket should not be empty")
	}
	c.valuesLock.Lock()
	c.values[sessionKey] = append(c.values[sessionKey], ticket)
	c.valuesLock.Unlock()
	c.orig.Put(sessionKey, cs)
}

func (c *sessionCache) NumberOfTickets() int {
	c.valuesLock.Lock()
	defer c.valuesLock.Unlock()
	total := 0
	for _, tickets := range c.values {
		total += len(tickets)
	}
	return total
}

// Keep native sockets unwrapped so net.Buffers retains the OS writev path.
// TCP_INFO counters are kernel-accounted bytes for exactly these client sockets.
// /proc/self/io syscr/syscw are whole-process syscall counts (including background
// control work and the counter read); they are labeled separately from Read calls.
type testIO struct {
	mu    sync.Mutex
	conns []*net.TCPConn
}

func (s *testIO) add(c net.Conn) {
	if tcp, ok := c.(*net.TCPConn); ok {
		s.mu.Lock()
		s.conns = append(s.conns, tcp)
		s.mu.Unlock()
	}
}
func (s *testIO) snapshot() (reads, writes, readBytes, writeBytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, conn := range s.conns {
		raw, e := conn.SyscallConn()
		if e != nil {
			continue
		}
		raw.Control(func(fd uintptr) {
			var info [256]byte
			length := uint32(len(info))
			_, _, err := syscall.Syscall6(syscall.SYS_GETSOCKOPT, fd, syscall.IPPROTO_TCP, syscall.TCP_INFO, uintptr(unsafe.Pointer(&info[0])), uintptr(unsafe.Pointer(&length)), 0)
			if err == 0 && length >= 208 {
				readBytes += int64(binary.NativeEndian.Uint64(info[128:136]))
				writeBytes += int64(binary.NativeEndian.Uint64(info[200:208]))
			}
		})
	}
	data, e := os.ReadFile("/proc/self/io")
	if e == nil {
		for _, line := range strings.Split(string(data), "\n") {
			parts := strings.Fields(line)
			if len(parts) == 2 {
				n, _ := strconv.ParseInt(parts[1], 10, 64)
				if parts[0] == "syscr:" {
					reads = n
				}
				if parts[0] == "syscw:" {
					writes = n
				}
			}
		}
	}
	return
}

type v2TabletAttemptObserver struct {
	mu      sync.Mutex
	targets []routing.Target
}

func (*v2TabletAttemptObserver) ObserveRequest(gocql.RequestEvent) {}
func (o *v2TabletAttemptObserver) ObserveAttempt(event gocql.AttemptEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if event.Err == nil {
		o.targets = append(o.targets, event.Target)
	}
}
func (o *v2TabletAttemptObserver) snapshot() []routing.Target {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]routing.Target(nil), o.targets...)
}

type position struct {
	Lat, Lon int32
	Padding  string
}

// The original per-member hooks become an application-owned typed Codec.
// Clone is valid before any metadata has been compiled.
type positionCodec struct{ fields cql.Codec[position] }

func (c positionCodec) CheckType(t cql.DataType) error              { return c.fields.CheckType(t) }
func (c positionCodec) Encode(w *cql.ValueWriter, p position) error { return c.fields.Encode(w, p) }
func (c positionCodec) Decode(r cql.ValueReader) (position, error)  { return c.fields.Decode(r) }
func (positionCodec) Clone(p position) position                     { return p }

type person struct {
	FirstName, LastName string
	Age                 int32
}

func (p person) String() string {
	return fmt.Sprintf("Person{firstName: %s, lastName: %s, Age: %d}", p.FirstName, p.LastName, p.Age)
}

var serializationBlob = cql.NamedScalar(cql.BlobCodec, func(v []byte) cql.Bytes { return cql.BorrowBytes(v) }, func(v cql.Bytes) []byte { return v.OwnedBytes() })
var serializationTimestamp = cql.NamedScalar(cql.TimestampCodec, func(v time.Time) cql.Timestamp {
	n, e := cql.TimestampFromTime(v)
	if e != nil {
		panic(e)
	}
	return n
}, func(v cql.Timestamp) time.Time {
	n, e := v.Time()
	if e != nil {
		panic(e)
	}
	return n.UTC()
})

var serializationDecimal = cql.NamedScalar(cql.DecimalCodec, func(v inf.Dec) cql.Decimal {
	return cql.Decimal{Scale: int32(v.Scale()), Unscaled: cql.VarintFromBigInt(v.UnscaledBig())}
}, func(v cql.Decimal) inf.Dec { return *inf.NewDecBig(v.Unscaled.BigInt(), inf.Scale(v.Scale)) })

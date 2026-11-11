//go:build integration

package integration

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	gocql "github.com/scylladb/gocql/v2"

	"github.com/scylladb/gocql/v2/auth"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/retry"
)

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
		b.WithCQLVersion(*flagCQL)
	}
	if skip&fixtureAgreement == 0 {
		b.WithSchemaAgreement(2*time.Minute, 200*time.Millisecond)
	}
	b.ConfigureDefaultProfile(func(p *gocql.ProfileBuilder) { fixtureProfileExcept(p, skip) })
	if *flagRunAuthTest && skip&fixtureAuth == 0 {
		b.WithAuth(auth.Password("cassandra", "cassandra"))
	}
	if skip&fixtureCompression == 0 {
		switch *flagCompressTest {
		case "snappy":
			b.WithCompression(gocql.SnappyCompression)
		case "lz4":
			b.WithCompression(gocql.LZ4Compression)
		case "no-compression", "":
			b.WithCompression(gocql.NoCompression)
		default:
			panic("invalid compressor: " + *flagCompressTest)
		}
	}
	if *flagRunSslTest {
		if *flagDistribution == "cassandra" {
			b.WithPort(9042)
		} else {
			b.WithPort(9142)
		}
		if skip&fixtureTLS == 0 {
			b.WithTLS(gocql.StaticTLS(fixtureTLSConfig(fixtureClientTLS())))
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
		p.WithConsistency(cql.Quorum)
	}
	if skip&fixtureTimeout == 0 {
		p.WithTimeout(*flagTimeout)
	}
	if skip&fixtureRetry == 0 {
		p.WithRetry(retry.Safe{MaxAttempts: max(1, *flagRetry+1)})
	}
}

// fixtureProfile applies the integration flags to a profile. createV2Cluster
// uses it for the default profile; tests use it for per-query profiles.
func fixtureProfile(p *gocql.ProfileBuilder) { fixtureProfileExcept(p, 0) }

// fixtureClientTLS returns the integration client's certificate and CA.
func fixtureClientTLS() *tls.Config {
	certificate, err := tls.LoadX509KeyPair("../../testdata/pki/gocql.crt", "../../testdata/pki/gocql.key")
	if err != nil {
		panic(fmt.Errorf("load integration client certificate: %w", err))
	}
	pem, err := os.ReadFile("../../testdata/pki/ca.crt")
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

func createV2Session(t testing.TB, opts ...func(*gocql.ConfigBuilder)) *gocql.Session[testDialect] {
	t.Helper()
	return createV2SessionFromConfig(createV2Cluster(opts...), t, false)
}

// createV2SessionWithout opens a fixture session that leaves out skip, for a
// test whose options choose those settings.
func createV2SessionWithout(t testing.TB, skip fixtureParts, opts ...func(*gocql.ConfigBuilder)) *gocql.Session[testDialect] {
	t.Helper()
	return createV2SessionFromConfig(createV2ClusterWithout(skip, opts...), t, false)
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

func createV2SessionFromConfig(b *gocql.ConfigBuilder, t testing.TB, tabletsDisabled bool) *gocql.Session[testDialect] {
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
		bootstrap := mustBuild(t, createV2ClusterWithout(fixtureTimeout).WithKeyspace("system").
			ConfigureDefaultProfile(func(p *gocql.ProfileBuilder) { p.WithTimeout(30 * time.Second) }))
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		session, err := gocql.Connect[testDialect](ctx, bootstrap)
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
	config := mustBuild(t, b.WithKeyspace(keyspace))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	session, err := gocql.Connect[testDialect](ctx, config)
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

func v2TabletsSupported(ctx context.Context, session *gocql.Session[testDialect]) (bool, error) {
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

// v2Exec and v2Query accept only explicitly typed dynamic values. Owned binding
// lets callers release or change their inputs as soon as these helpers return.
func v2Exec(ctx context.Context, session *gocql.Session[testDialect], stmt string, values ...cql.Value) (gocql.Result, error) {
	statement := cql.Statement[[]cql.Value, cql.EmptyRow]{CQL: stmt, Params: cql.DynamicParams, Rows: cql.EmptyRowDecoder}
	var bound *Bound[cql.EmptyRow]
	var err error
	if len(values) == 0 {
		bound, err = BindUnpreparedOwned(statement, cql.NewColumns(), values)
	} else {
		var prepared *gocql.Prepared[[]cql.Value, cql.EmptyRow]
		prepared, err = session.Prepare(ctx, statement, gocql.PrepareOptions{})
		if err == nil {
			bound, err = BindOwned(prepared, values)
		}
	}
	if err != nil {
		return gocql.Result{}, err
	}
	defer bound.Close()
	return Exec(ctx, session, bound, gocql.CompiledOptions{})
}

func v2Query(ctx context.Context, session *gocql.Session[testDialect], stmt string, values ...cql.Value) (gocql.Rows[cql.DynamicRow], error) {
	statement := cql.Statement[[]cql.Value, cql.DynamicRow]{CQL: stmt, Params: cql.DynamicParams, Rows: cql.DynamicRows}
	var bound *Bound[cql.DynamicRow]
	var err error
	if len(values) == 0 {
		bound, err = BindUnpreparedOwned(statement, cql.NewColumns(), values)
	} else {
		var prepared *gocql.Prepared[[]cql.Value, cql.DynamicRow]
		prepared, err = session.Prepare(ctx, statement, gocql.PrepareOptions{})
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
	return Run(ctx, session, bound, gocql.CompiledOptions{})
}

func v2ReadOne(ctx context.Context, session *gocql.Session[testDialect], stmt string, values ...cql.Value) (cql.DynamicRow, error) {
	rows, err := v2Query(ctx, session, stmt, values...)
	if err != nil {
		return cql.DynamicRow{}, err
	}
	defer rows.Close()
	var cur cql.DynamicRow
	if !rows.NextInto(&cur) {
		if err := rows.Err(); err != nil {
			return cql.DynamicRow{}, err
		}
		return cql.DynamicRow{}, cqlerr.ErrNoRows
	}
	row, err := rows.Clone()
	if err != nil {
		return cql.DynamicRow{}, err
	}
	if rows.NextInto(&cur) {
		return cql.DynamicRow{}, cqlerr.ErrMultipleRows
	}
	if err := rows.Err(); err != nil {
		return cql.DynamicRow{}, err
	}
	return row, rows.Close()
}

func v2CreateTable(ctx context.Context, session *gocql.Session[testDialect], stmt string) error {
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

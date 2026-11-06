//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	gocql "github.com/scylladb/gocql/v2"
)

func TestV2RuntimeDatabaseSmoke(t *testing.T) {
	config := gocql.DefaultConfig(strings.Split(v2SmokeOption("cluster", "127.0.20.1,127.0.20.2,127.0.20.3"), ",")...)
	config.Protocol, _ = strconv.Atoi(v2SmokeOption("proto", "4"))
	config.ReportConfiguration = false
	config.Metadata.AutomaticAgreement = true
	config.Metadata.RefreshAfterAgreement = true
	profile := *config.DefaultProfile
	profile.Timeout = 30 * time.Second
	profile.Consistency = gocql.LocalQuorum
	config.DefaultProfile = &profile
	switch v2SmokeOption("compressor", "no-compression") {
	case "lz4":
		config.Compression = gocql.LZ4Compression
	case "snappy":
		config.Compression = gocql.SnappyCompression
	}
	if v2SmokeOption("runauth", "false") == "true" {
		config.Auth = gocql.PasswordAuth("cassandra", "cassandra")
	}
	if v2SmokeOption("runssl", "false") == "true" {
		if v2SmokeOption("distribution", "scylla") != "cassandra" {
			config.Port = 9142
		}
		cert, err := tls.LoadX509KeyPair("../../testdata/pki/gocql.crt", "../../testdata/pki/gocql.key")
		if err != nil {
			t.Fatal(err)
		}
		ca, err := os.ReadFile("../../testdata/pki/ca.crt")
		if err != nil {
			t.Fatal(err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(ca) {
			t.Fatal("invalid root CA")
		}
		config.TLS = &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12, InsecureSkipVerify: true, VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return fmt.Errorf("missing fixture server certificate")
			}
			intermediates := x509.NewCertPool()
			for _, certificate := range state.PeerCertificates[1:] {
				intermediates.AddCert(certificate)
			}
			if _, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
				return err
			}
			for _, identity := range state.PeerCertificates[0].URIs {
				if identity.String() == "spiffe://test.cassandra.apache.org/cassandra-gocql-driver/integrationTest/cassandra" {
					return nil
				}
			}
			return fmt.Errorf("unexpected fixture server identity")
		}}
		// The existing fixture identifies nodes with a verified SPIFFE URI rather than IP SANs.
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	session, err := gocql.Connect(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	keyspace := fmt.Sprintf("v2_runtime_%x", time.Now().UnixNano())
	exec := func(cql string) {
		t.Helper()
		bound, err := BindUnpreparedOwned(gocql.Statement[gocql.Unit, gocql.Unit]{CQL: cql, Params: gocql.NoParams, Rows: gocql.NoRows}, gocql.NewColumns(), gocql.Unit{})
		if err != nil {
			t.Fatal(err)
		}
		defer bound.Close()
		if _, err = Exec(ctx, session, bound, gocql.Options{}); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE KEYSPACE " + keyspace + " WITH replication = {'class':'NetworkTopologyStrategy','replication_factor':1}")
	defer func() {
		bound, err := BindUnpreparedOwned(gocql.Statement[gocql.Unit, gocql.Unit]{CQL: "DROP KEYSPACE " + keyspace, Params: gocql.NoParams, Rows: gocql.NoRows}, gocql.NewColumns(), gocql.Unit{})
		if err == nil {
			defer bound.Close()
			if _, err = Exec(context.Background(), session, bound, gocql.Options{}); err != nil {
				t.Error(err)
			}
		}
	}()
	exec("CREATE TABLE " + keyspace + ".values (id int PRIMARY KEY, value blob)")
	type input struct {
		ID    int32
		Value []byte
	}
	type output struct {
		ID    int32
		Value gocql.Bytes
	}
	inputs := gocql.Params(gocql.Param("id", gocql.Int32, func(v input) int32 { return v.ID }), gocql.Param("value", gocql.BytesEncoder, func(v input) []byte { return v.Value }))
	rows := gocql.Row(gocql.Column("id", gocql.Int32, func(v *output) *int32 { return &v.ID }), gocql.Column("value", gocql.BlobCodec, func(v *output) *gocql.Bytes { return &v.Value }))
	prepared, err := gocql.Prepare(ctx, session, gocql.Statement[input, gocql.Unit]{CQL: "INSERT INTO " + keyspace + ".values (id,value) VALUES (?,?)", Params: inputs, Rows: gocql.NoRows}, gocql.PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("typed-large-segment-payload"), 24000)
	for id := int32(1); id <= 3; id++ {
		bound, err := BindBorrowed(prepared, input{ID: id, Value: payload})
		if err != nil {
			t.Fatal(err)
		}
		if !bound.Routing().Token.Set {
			t.Fatal("prepared partition key had no routing token")
		}
		if _, err = Exec(ctx, session, bound, gocql.Options{}); err != nil {
			t.Fatal(err)
		}
		bound.Close()
		if err = bound.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	selectBound, err := BindUnpreparedOwned(gocql.Statement[gocql.Unit, output]{CQL: "SELECT id,value FROM " + keyspace + ".values", Params: gocql.NoParams, Rows: rows}, gocql.NewColumns(), gocql.Unit{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := Run(ctx, session, selectBound, testOptions(gocql.OptionValues{PageSize: gocql.Option[int32]{Value: 1, Set: true}}))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	var retained *gocql.PageLease
	var view gocql.Bytes
	var detached output
	for result.Next() {
		row := result.Row()
		if !bytes.Equal(row.Value.OwnedBytes(), payload) {
			t.Fatal("large payload changed")
		}
		if count == 0 {
			retained, err = result.Retain()
			if err != nil {
				t.Fatal(err)
			}
			view = row.Value
			detached, err = result.Clone()
			if err != nil {
				t.Fatal(err)
			}
		}
		count++
	}
	if err = result.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("rows %d", count)
	}
	if result.Info().Rows != 3 {
		t.Fatalf("reported rows %d", result.Info().Rows)
	}
	selectBound.Close()
	if err = selectBound.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(view.OwnedBytes(), payload) {
		t.Fatal("retained page changed")
	}
	retained.Close()
	if !bytes.Equal(detached.Value.OwnedBytes(), payload) {
		t.Fatal("clone changed")
	}
}

func v2SmokeOption(name, fallback string) string {
	if value := os.Getenv("GOCQL_V2_SMOKE_" + strings.ToUpper(name)); value != "" {
		return value
	}
	if f := flag.Lookup(name); f != nil {
		return f.Value.String()
	}
	return fallback
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Command streaming demonstrates typed execution into reusable caller storage.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/auth"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cql/stream"
	"github.com/scylladb/gocql/v2/cqlerr"
)

type record struct {
	ApplicationState string
	Payload          []byte
	Pieces           [][]byte
	ID               int32
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() (resultErr error) {
	hosts := flag.String("hosts", "127.0.0.1:9042", "comma-separated native endpoints")
	incremental := flag.Bool("incremental", false, "use bounded incremental receive instead of page buffering")
	dialect := flag.String("dialect", "scylla-v4", "scylla-v4, cassandra-v4 or cassandra-v5")
	compression := flag.String("compression", "none", "none, lz4 or snappy")
	username := flag.String("username", "", "authentication username")
	password := flag.String("password", "", "authentication password (prefer GOCQL_PASSWORD)")
	ca := flag.String("ca", "", "TLS CA PEM file")
	cert := flag.String("cert", "", "TLS client certificate PEM file")
	key := flag.String("key", "", "TLS client private key PEM file")
	serverName := flag.String("server-name", "", "TLS server DNS identity")
	uri := flag.String("tls-uri", "", "explicit TLS URI identity instead of DNS")
	flag.Parse()
	builder := gocql.NewConfig(strings.Split(*hosts, ",")...).
		WithPoolSize(gocql.PerHost(1)).
		NoAdvancedShardAwareness().
		WithHeartbeat(0, 0).
		WithMetadataRefresh(0)
	if _, port, err := net.SplitHostPort(strings.Split(*hosts, ",")[0]); err == nil {
		n, err := strconv.ParseUint(port, 10, 16)
		if err != nil {
			return err
		}
		builder.WithPort(uint16(n))
	}
	frameCompression := gocql.NoCompression
	switch *compression {
	case "none":
	case "lz4":
		frameCompression = gocql.LZ4Compression
	case "snappy":
		frameCompression = gocql.SnappyCompression
	default:
		return fmt.Errorf("unknown compression %q", *compression)
	}
	if *password == "" {
		*password = os.Getenv("GOCQL_PASSWORD")
	}
	builder.WithCompression(frameCompression)
	if *username != "" {
		builder.WithAuth(auth.Password(*username, *password))
	}
	if *ca != "" {
		pem, err := os.ReadFile(*ca)
		if err != nil {
			return err
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return fmt.Errorf("invalid CA PEM")
		}
		tlsConfig := &tls.Config{RootCAs: roots, ServerName: *serverName, MinVersion: tls.VersionTLS12}
		if *cert != "" {
			pair, err := tls.LoadX509KeyPair(*cert, *key)
			if err != nil {
				return err
			}
			tlsConfig.Certificates = []tls.Certificate{pair}
		}
		if *uri != "" {
			// Explicit URI identity verification also verifies the complete CA chain.
			tlsConfig.InsecureSkipVerify = true
			tlsConfig.VerifyConnection = func(state tls.ConnectionState) error {
				if len(state.PeerCertificates) == 0 {
					return fmt.Errorf("missing peer certificate")
				}
				intermediates := x509.NewCertPool()
				for _, c := range state.PeerCertificates[1:] {
					intermediates.AddCert(c)
				}
				if _, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
					return err
				}
				for _, id := range state.PeerCertificates[0].URIs {
					if id.String() == *uri {
						return nil
					}
				}
				return fmt.Errorf("missing expected TLS URI %q", *uri)
			}
		}
		builder.WithTLS(gocql.StaticTLS(tlsConfig))
	}
	cfg, err := builder.Build()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// The dialect is a type: the program picks it once and runs the rest
	// generically.
	switch *dialect {
	case "scylla-v4":
		return streamTable[gocql.ScyllaV4](ctx, cfg, *incremental, false, frameCompression)
	case "cassandra-v4":
		return streamTable[gocql.CassandraV4](ctx, cfg, *incremental, false, frameCompression)
	case "cassandra-v5":
		return streamTable[gocql.CassandraV5](ctx, cfg, *incremental, true, frameCompression)
	}
	return fmt.Errorf("unknown dialect %q", *dialect)
}

// streamTable connects with dialect D and streams the example table. segments
// reports that the dialect's protocol compresses transport segments rather
// than frames.
func streamTable[D gocql.Dialect](ctx context.Context, cfg gocql.Config, incremental, segments bool, frameCompression gocql.Compression) (resultErr error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s, err := gocql.Connect[D](ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, s.Close()) }()
	prefix := os.Getenv("GOCQL_TEST_RUN")
	if prefix == "" {
		prefix = "gocql"
	}
	id := cql.RandomUUID()
	namespace := prefix + "_stream_" + strings.ReplaceAll(id.String(), "-", "")[:16]
	if !regexp.MustCompile(`^[a-z][a-z0-9_]{0,47}$`).MatchString(namespace) {
		return fmt.Errorf("invalid owned namespace %q", namespace)
	}
	execContext := func(operation context.Context, text string) error {
		_, err := s.UnpreparedExec(operation, cql.Command(text), cql.EmptyParams{})
		return err
	}
	exec := func(text string) error { return execContext(ctx, text) }
	owned := true
	defer func() {
		if owned {
			cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			err := execContext(cleanup, "DROP KEYSPACE IF EXISTS "+namespace)
			resultErr = errors.Join(resultErr, err)
			if err == nil {
				fmt.Println("CLEANED", namespace)
			}
		}
	}()
	if err := exec("CREATE KEYSPACE " + namespace + " WITH replication = {'class':'NetworkTopologyStrategy','replication_factor':1}"); err != nil {
		if _, ok := errors.AsType[*cqlerr.AlreadyExistsError](err); ok {
			owned = false
		}
		return err
	}
	fmt.Println("READY", namespace)
	switch os.Getenv("GOCQL_EXAMPLE_FAILURE") {
	case "error":
		return errors.New("injected example failure after create")
	case "cancel":
		cancel()
		return ctx.Err()
	case "pause":
		<-ctx.Done()
		return ctx.Err()
	}
	table := namespace + ".records"
	if err := exec("CREATE TABLE " + table + " (id int PRIMARY KEY,payload blob,pieces list<blob>)"); err != nil {
		return err
	}
	insert, err := s.Prepare(ctx, cql.Statement[record, cql.EmptyRow]{CQL: "INSERT INTO " + table + " (id,payload,pieces) VALUES (?,?,?)", Params: cql.Params(
		cql.Param("id", cql.Int32Codec, func(v record) int32 { return v.ID }),
		cql.Param("payload", cql.ByteSliceCodec, func(v record) []byte { return v.Payload }),
		cql.Param("pieces", cql.ListEncoder(cql.ByteSliceCodec), func(v record) [][]byte { return v.Pieces }),
	), Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
	if err != nil {
		return err
	}
	writeQuery, err := insert.Query(gocql.Options{})
	if err != nil {
		return err
	}
	defer writeQuery.Close()
	payload := bytes.Repeat([]byte("typed-stream-"), 16384)
	for i := int32(0); i < 3; i++ {
		if _, err := writeQuery.Exec(ctx, record{ID: i, Payload: payload, Pieces: [][]byte{[]byte("nested"), []byte("storage")}}); err != nil {
			return err
		}
	}
	decoder := stream.RowInto(func(r *record) { r.ApplicationState = "" },
		stream.Column("id", stream.Int32, func(r *record) *int32 { return &r.ID }),
		stream.Column("payload", stream.Blob, func(r *record) *[]byte { return &r.Payload }),
		stream.Column("pieces", stream.List(stream.Blob), func(r *record) *[][]byte { return &r.Pieces }),
	)
	prepared, err := s.PrepareStream(ctx, stream.Statement[cql.EmptyParams, record]{
		CQL:  "SELECT id,payload,pieces FROM " + table,
		Rows: decoder,
	})
	if err != nil {
		return err
	}
	hostsSnapshot := s.Metadata().Hosts()
	if len(hostsSnapshot) == 0 {
		return fmt.Errorf("missing discovered hosts")
	}
	pinned := gocql.Options{PageSize: cql.Some[int32](2), HostID: cql.Some(hostsSnapshot[0].ID)}
	options := gocql.StreamOptions{Incremental: incremental, Options: pinned}
	query, err := prepared.Query(options)
	if err != nil {
		return err
	}
	defer query.Close()
	stream, err := query.Run(ctx, cql.EmptyParams{})
	if incremental && !segments && frameCompression != gocql.NoCompression {
		if _, ok := errors.AsType[*cqlerr.StreamCapabilityError](err); !ok {
			return fmt.Errorf("expected frame compression capability error, got %w", err)
		}
		fmt.Println("frame compression explicitly rejected before streaming submission")
		return nil
	}
	if err != nil {
		return err
	}
	defer stream.Close()
	// One worker owns one query; overlapping use is rejected immediately.
	blockedCtx, blockedCancel := context.WithTimeout(ctx, 20*time.Millisecond)
	_, blockedErr := query.Run(blockedCtx, cql.EmptyParams{})
	blockedCancel()
	if !errors.Is(blockedErr, cqlerr.ErrBusy) {
		return fmt.Errorf("query admission: %w", blockedErr)
	}
	// Seed both the outer slots and every nested payload before the first NextInto.
	row := record{Payload: make([]byte, 0, len(payload)), Pieces: [][]byte{make([]byte, 0, 16), make([]byte, 0, 16)}}
	backing := &row.Payload[:cap(row.Payload)][0]
	nested := &row.Pieces[0][:cap(row.Pieces[0])][0]
	seen := map[int32]bool{}
	for stream.NextInto(&row) {
		if seen[row.ID] || !bytes.Equal(row.Payload, payload) || len(row.Pieces) != 2 || string(row.Pieces[0]) != "nested" || string(row.Pieces[1]) != "storage" || row.ApplicationState != "" {
			return fmt.Errorf("incorrect streamed row %d", row.ID)
		}
		if &row.Payload[0] != backing || &row.Pieces[0][0] != nested {
			return fmt.Errorf("seeded storage replaced")
		}
		seen[row.ID] = true
		row.ApplicationState = "consumed" // Reset clears only this unmapped field.
	}
	if err := stream.Err(); err != nil {
		return err
	}
	if err := stream.Close(); err != nil {
		return err
	}
	query.Close()
	if err := query.Wait(ctx); err != nil {
		return err
	}
	if len(seen) != 3 || !bytes.Equal(row.Payload, payload) {
		return fmt.Errorf("incomplete stream or output lost on Close")
	}
	fmt.Println("3 rows; seeded nested storage reused; query admission and drain verified")
	return nil
}

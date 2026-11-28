// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Command streaming demonstrates typed execution into reusable caller storage.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/auth"
	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/endpoint"
	"github.com/scylladb/gocql/v2/execopt"
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
	var d config.Dialect
	flag.TextVar(&d, "dialect", config.ScyllaV4, "ScyllaV4, CassandraV4 or CassandraV5")
	var frameCompression cql.Compression
	flag.TextVar(&frameCompression, "compression", cql.NoCompression, "none, lz4 or snappy")
	username := flag.String("username", "", "authentication username")
	password := flag.String("password", "", "authentication password (prefer GOCQL_PASSWORD)")
	ca := flag.String("ca", "", "TLS CA PEM file")
	cert := flag.String("cert", "", "TLS client certificate PEM file")
	key := flag.String("key", "", "TLS client private key PEM file")
	serverName := flag.String("server-name", "", "TLS server DNS identity")
	uri := flag.String("tls-uri", "", "explicit TLS URI identity instead of DNS")
	flag.Parse()
	builder := config.New(d, strings.Split(*hosts, ",")...).
		WithPool(config.PerHost{Connections: 1}).
		WithHeartbeat(0, 0).
		WithMetadataRefresh(0)
	if *password == "" {
		*password = os.Getenv("GOCQL_PASSWORD")
	}
	builder.WithCompression(frameCompression)
	if *username != "" {
		builder.WithAuth(auth.Password(*username, *password))
	}
	if *ca != "" {
		tlsConfig, err := config.LoadTLSFiles(&tls.Config{ServerName: *serverName, MinVersion: tls.VersionTLS12},
			config.TLSFiles{CertFile: *cert, KeyFile: *key, CAFile: *ca})
		if err != nil {
			return err
		}
		builder.WithTLS(tlsConfig)
		if *uri != "" {
			// The driver verifies the certificate chain against the CA; the
			// node is identified by a URI in its certificate, not a name.
			want := *uri
			builder.WithTLSIdentity(func(endpoint.DialTarget) endpoint.TLSIdentity {
				return endpoint.TLSIdentity{ChainOnly: true, Verify: func(state tls.ConnectionState) error {
					for _, id := range state.PeerCertificates[0].URIs {
						if id.String() == want {
							return nil
						}
					}
					return fmt.Errorf("missing expected TLS URI %q", want)
				}}
			})
		}
	}
	cfg, err := builder.Build()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// Protocol 5 compresses transport segments rather than frames.
	return streamTable(ctx, cfg, *incremental, d == config.CassandraV5, frameCompression)
}

// streamTable connects with cfg and streams the example table. segments
// reports that the dialect's protocol compresses transport segments rather
// than frames.
func streamTable(ctx context.Context, cfg config.Config, incremental, segments bool, frameCompression cql.Compression) (resultErr error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s, err := gocql.Connect(ctx, cfg)
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
		_, err := s.UnpreparedCommand(operation, text)
		return err
	}
	runDDL := func(text string) error { return execContext(ctx, text) }
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
	if err := runDDL("CREATE KEYSPACE " + namespace + " WITH replication = {'class':'NetworkTopologyStrategy','replication_factor':1}"); err != nil {
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
	if err := runDDL("CREATE TABLE " + table + " (id int PRIMARY KEY,payload blob,pieces list<blob>)"); err != nil {
		return err
	}
	insert, err := s.Prepare(ctx, cql.Write("INSERT INTO "+table+" (id,payload,pieces) VALUES (?,?,?)", cql.Params(
		cql.Param("id", cql.Int32Codec, func(v record) int32 { return v.ID }),
		cql.Param("payload", cql.ByteSliceCodec, func(v record) []byte { return v.Payload }),
		cql.Param("pieces", cql.ListEncoder(cql.ByteSliceCodec), func(v record) [][]byte { return v.Pieces }),
	)))
	if err != nil {
		return err
	}
	writer, err := insert.Worker()
	if err != nil {
		return err
	}
	defer writer.Close()
	payload := bytes.Repeat([]byte("typed-stream-"), 16384)
	for i := int32(0); i < 3; i++ {
		if _, err := writer.Exec(ctx, record{ID: i, Payload: payload, Pieces: [][]byte{[]byte("nested"), []byte("storage")}}); err != nil {
			return err
		}
	}
	decoder := cql.StreamRowInto(func(r *record) { r.ApplicationState = "" },
		cql.StreamColumn("id", cql.StreamInt32, func(r *record) *int32 { return &r.ID }),
		cql.StreamColumn("payload", cql.StreamByteSlice, func(r *record) *[]byte { return &r.Payload }),
		cql.StreamColumn("pieces", cql.StreamList(cql.StreamByteSlice), func(r *record) *[][]byte { return &r.Pieces }),
	)
	prepared, err := s.PrepareStream(ctx, cql.NewStreamStatement("SELECT id,payload,pieces FROM "+table, cql.NoParams, decoder))
	if err != nil {
		return err
	}
	hostsSnapshot := s.Metadata().Hosts()
	if len(hostsSnapshot) == 0 {
		return fmt.Errorf("missing discovered hosts")
	}
	pinned, err := s.CompileOptions(execopt.Options{PageSize: 2, HostID: cql.Some(hostsSnapshot[0].HostID)})
	if err != nil {
		return err
	}
	worker, err := prepared.With(pinned).Worker(execopt.StreamOptions{Incremental: incremental})
	if err != nil {
		return err
	}
	defer worker.Close()
	stream, err := worker.Run(ctx, cql.EmptyParams{})
	if incremental && !segments && frameCompression != cql.NoCompression {
		if _, ok := errors.AsType[*cql.StreamCapabilityError](err); !ok {
			return fmt.Errorf("expected frame compression capability error, got %w", err)
		}
		fmt.Println("frame compression explicitly rejected before streaming submission")
		return nil
	}
	if err != nil {
		return err
	}
	defer stream.Close()
	// One goroutine owns one worker; overlapping use is rejected immediately.
	blockedCtx, blockedCancel := context.WithTimeout(ctx, 20*time.Millisecond)
	_, blockedErr := worker.Run(blockedCtx, cql.EmptyParams{})
	blockedCancel()
	if !errors.Is(blockedErr, cqlerr.ErrBusy) {
		return fmt.Errorf("worker admission: %w", blockedErr)
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
	worker.Close()
	if err := worker.Wait(ctx); err != nil {
		return err
	}
	if len(seen) != 3 || !bytes.Equal(row.Payload, payload) {
		return fmt.Errorf("incomplete stream or output lost on Close")
	}
	fmt.Println("3 rows; seeded nested storage reused; worker admission and drain verified")
	return nil
}

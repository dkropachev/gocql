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

package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/auth"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/dialer/recorder"
	"github.com/scylladb/gocql/v2/dialer/replayer"
	"github.com/scylladb/gocql/v2/endpoint"
)

type corpusConfig struct {
	Namespace string `json:"namespace"`
	Address   string `json:"address"`
	Protocol  int    `json:"protocol"`
}

var corpus corpusConfig
var corpusRoot = "."
var identifier = regexp.MustCompile(`^[a-z][a-z0-9_]{0,47}$`)

func freshNamespace() (string, error) {
	prefix := os.Getenv("GOCQL_TEST_RUN")
	if prefix == "" {
		prefix = "gocql"
	}
	id, err := cql.RandomUUID()
	if err != nil {
		return "", err
	}
	name := prefix + "_bench_" + strings.ReplaceAll(id.String(), "-", "")[:16]
	if !identifier.MatchString(name) {
		return "", fmt.Errorf("invalid owned benchmark namespace %q", name)
	}
	return name, nil
}
func corpusPath(name string) string { return filepath.Join(corpusRoot, name) }
func benchConfig(d endpoint.Dialer) gocql.Config {
	address := os.Getenv("GOCQL_BENCH_ADDRESS")
	if address == "" {
		address = corpus.Address
		if address == "" {
			address = "127.0.20.1:9042"
		}
	}
	c, err := gocql.NewConfig(address).
		WithAuth(auth.Password("cassandra", "cassandra")).
		NoHeartbeat().
		NoMetadataRefresh().
		NoConfigReport().
		WithConnectionsPerHost(1).
		NoDiscovery().
		WithDialer(d).
		Build()
	if err != nil {
		panic(err)
	}
	return c
}

// benchDialect is the dialect of the recorded corpus and the bench cluster.
type benchDialect = gocql.ScyllaV4

func benchExec(ctx context.Context, s *gocql.Session[benchDialect], text string) error {
	_, e := s.UnpreparedExec(ctx, cql.Command(text), cql.EmptyParams{})
	return e
}
func InitializeCluster(ctx context.Context, s *gocql.Session[benchDialect]) error {
	for _, text := range []string{"CREATE TABLE " + corpus.Namespace + ".table1(pk int PRIMARY KEY,ck int,v text)"} {
		if err := benchExec(ctx, s, text); err != nil {
			return err
		}
	}
	return nil
}
func cleanupNamespace(s *gocql.Session[benchDialect]) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := benchExec(ctx, s, "DROP KEYSPACE IF EXISTS "+corpus.Namespace); err != nil {
		return err
	}
	p, err := s.Prepare(ctx, cql.Statement[string, cql.Text]{CQL: "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?", Params: cql.OneParam(cql.StringEncoder), Rows: cql.OneColumn(cql.TextCodec)}, gocql.PrepareOptions{})
	if err != nil {
		return err
	}
	b, err := p.Query(gocql.Options{})
	if err != nil {
		return err
	}
	defer b.Close()
	rows, err := b.Run(ctx, corpus.Namespace)
	if err != nil {
		return err
	}
	defer rows.Close()
	var name cql.Text
	if rows.NextInto(&name) {
		return fmt.Errorf("owned benchmark namespace survived cleanup: %s", corpus.Namespace)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	fmt.Println("CLEANED", corpus.Namespace)
	return nil
}
func generateCaptures(ctx context.Context) (err error) {
	operation, cancel := context.WithCancel(ctx)
	defer cancel()
	ctx = operation
	namespace, err := freshNamespace()
	if err != nil {
		return err
	}
	corpus = corpusConfig{Namespace: namespace, Address: os.Getenv("GOCQL_BENCH_ADDRESS"), Protocol: 4}
	if corpus.Address == "" {
		return fmt.Errorf("capture generation requires explicit GOCQL_BENCH_ADDRESS")
	}
	s, err := gocql.Connect[benchDialect](ctx, benchConfig(nil))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, s.Close()) }()
	owned := true
	defer func() {
		if owned {
			err = errors.Join(err, cleanupNamespace(s))
		}
	}()
	// A namespace is fresh and exclusively generated here. An AlreadyExists
	// response relinquishes ownership instead of deleting a pre-existing name.
	if err = benchExec(ctx, s, "CREATE KEYSPACE "+corpus.Namespace+" WITH replication = {'class':'NetworkTopologyStrategy','replication_factor':1}"); err != nil {
		if _, ok := errors.AsType[*cqlerr.RequestErrAlreadyExists](err); ok {
			owned = false
		}
		return err
	}
	if err = InitializeCluster(ctx, s); err != nil {
		return err
	}
	fmt.Println("READY", corpus.Namespace)
	switch os.Getenv("GOCQL_BENCH_FAILURE") {
	case "error":
		return errors.New("injected benchmark failure after create")
	case "cancel":
		cancel()
		return ctx.Err()
	case "pause":
		<-ctx.Done()
		return ctx.Err()
	}
	for _, name := range []string{"rec_insert", "rec_select"} {
		dir := corpusPath(name)
		if err = os.RemoveAll(dir); err != nil {
			return err
		}
		if err = os.MkdirAll(dir, 0700); err != nil {
			return err
		}
	}
	if err = RecordInsertTraffic(ctx, 10, corpusPath("rec_insert")); err != nil {
		return err
	}
	if err = RecordSelectTraffic(ctx, 10, corpusPath("rec_select")); err != nil {
		return err
	}
	data, err := json.MarshalIndent(corpus, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(corpusPath("corpus.json"), append(data, '\n'), 0600)
}
func benchRead(s *gocql.Session[benchDialect], id int32) error {
	return benchReadContext(context.Background(), s, id)
}
func benchReadContext(ctx context.Context, s *gocql.Session[benchDialect], id int32) error {
	p, e := s.Prepare(ctx, cql.Statement[int32, cql.Text]{CQL: "SELECT v FROM " + corpus.Namespace + ".table1 WHERE pk = ?;", Params: cql.OneParam(cql.Int32Codec), Rows: cql.OneColumn(cql.TextCodec)}, gocql.PrepareOptions{})
	if e != nil {
		return e
	}
	b, e := p.Query(gocql.Options{})
	if e != nil {
		return e
	}
	defer b.Close()
	rows, e := b.Run(ctx, id)
	if e != nil {
		return e
	}
	defer rows.Close()
	count := 0
	var name cql.Text
	for rows.NextInto(&name) {
		count++
		if got, want := name.String(), fmt.Sprintf("Name_%d", id); got != want {
			return fmt.Errorf("recorded name %q, want %q", got, want)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("recorded row count %d, want 1", count)
	}
	return nil
}

type benchInsert struct {
	PK, CK int32
	Value  string
}

func benchWrite(s *gocql.Session[benchDialect], id int32) error {
	return benchWriteContext(context.Background(), s, id)
}
func benchWriteContext(ctx context.Context, s *gocql.Session[benchDialect], id int32) error {
	p, e := s.Prepare(ctx, cql.Statement[benchInsert, cql.EmptyRow]{CQL: "INSERT INTO " + corpus.Namespace + ".table1 (pk, ck, v) VALUES (?, ?, ?);", Params: cql.Params(cql.Param("pk", cql.Int32Codec, func(v benchInsert) int32 { return v.PK }), cql.Param("ck", cql.Int32Codec, func(v benchInsert) int32 { return v.CK }), cql.Param("v", cql.StringEncoder, func(v benchInsert) string { return v.Value })), Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
	if e != nil {
		return e
	}
	b, e := p.Query(gocql.Options{})
	if e != nil {
		return e
	}
	defer b.Close()
	_, e = b.Exec(ctx, benchInsert{id, id % 5, fmt.Sprintf("Name_%d", id)})
	return e
}
func RecordSelectTraffic(ctx context.Context, size int, dir string) error {
	d := newCaptureDialer(dir, false)
	s, e := gocql.Connect[benchDialect](ctx, benchConfig(d))
	if e != nil {
		return e
	}
	defer s.Close()
	for i := 0; i < size; i++ {
		if e := benchReadContext(ctx, s, int32(i)); e != nil {
			return e
		}
	}
	return finishCapture(s, d)
}
func RecordInsertTraffic(ctx context.Context, size int, dir string) error {
	d := newCaptureDialer(dir, false)
	s, e := gocql.Connect[benchDialect](ctx, benchConfig(d))
	if e != nil {
		return e
	}
	defer s.Close()
	for i := 0; i < size; i++ {
		if e := benchWriteContext(ctx, s, int32(i)); e != nil {
			return e
		}
	}
	return finishCapture(s, d)
}

// TestMain regenerates captures only with -update-golden against an explicitly
// owned native fixture. See README.md in this directory for the complete recipe.
func TestMain(m *testing.M) {
	update := flag.Bool("update-golden", false, "Update golden files")
	flag.Parse()
	if root := os.Getenv("GOCQL_BENCH_CORPUS_DIR"); root != "" {
		corpusRoot = root
	}
	if *update {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		err := generateCaptures(ctx)
		cancel()
		if err != nil {
			fmt.Printf("capture generation: %v\n", err)
			os.Exit(1)
		}
	} else {
		data, err := os.ReadFile(corpusPath("corpus.json"))
		if err == nil {
			err = json.Unmarshal(data, &corpus)
		}
		if err != nil || !identifier.MatchString(corpus.Namespace) || corpus.Protocol != 4 {
			fmt.Printf("invalid replay corpus configuration: %v\n", err)
			os.Exit(1)
		}
	}
	os.Exit(m.Run())
}

// Each connection gets its own frame pair. Control is dialed before the one
// configured data connection; reconnects must fail rather than merge captures.
// Atomic ordinal selection occurs only at connection setup, outside measurements.
type captureDialer struct {
	dir         string
	replay      bool
	ordinal     atomic.Int32
	mu          sync.Mutex
	connections []net.Conn
}

func newCaptureDialer(dir string, replay bool) *captureDialer {
	return &captureDialer{dir: dir, replay: replay}
}
func (d *captureDialer) DialContext(ctx context.Context, target endpoint.DialTarget) (net.Conn, error) {
	n := d.ordinal.Add(1) - 1
	base := filepath.Join(d.dir, fmt.Sprintf("connection-%d", n))
	if d.replay {
		conn, err := replayer.NewConnectionReplayer(base, nil)
		if err == nil {
			d.track(conn)
		}
		return conn, err
	}
	conn, err := (endpoint.DefaultDialer{}).DialContext(ctx, target)
	if err != nil {
		return nil, err
	}
	recorded, err := recorder.NewConnectionRecorder(base, conn, nil)
	if err != nil {
		conn.Close()
	}
	if err == nil {
		d.track(recorded)
	}
	if err != nil {
		return nil, err
	}
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		recorded.Close()
		return nil, fmt.Errorf("recording requires TCP, got %T", conn)
	}
	return &captureTCP{Conn: recorded, socket: tcp}, nil
}

func (d *captureDialer) track(conn net.Conn) {
	d.mu.Lock()
	d.connections = append(d.connections, conn)
	d.mu.Unlock()
}
func finishCapture(s *gocql.Session[benchDialect], d *captureDialer) error {
	// Session.Close joins all driver I/O before checking every recorder's cached
	// Close result, including descriptor flush/close errors hidden by net.Conn I/O.
	err := s.Close()
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, conn := range d.connections {
		err = errors.Join(err, conn.Close())
	}
	if len(d.connections) != 2 {
		err = errors.Join(err, fmt.Errorf("capture used %d connections, want control plus one data connection", len(d.connections)))
	}
	return err
}

// The recording wrapper forwards requested socket controls to its actual socket.
// Replay exposes a synthetic address and has no physical TCP settings.
type captureTCP struct {
	net.Conn
	socket *net.TCPConn
}

func (c *captureTCP) SetNoDelay(v bool) error                  { return c.socket.SetNoDelay(v) }
func (c *captureTCP) SetKeepAlive(v bool) error                { return c.socket.SetKeepAlive(v) }
func (c *captureTCP) SetKeepAlivePeriod(v time.Duration) error { return c.socket.SetKeepAlivePeriod(v) }
func (c *captureTCP) SetReadBuffer(v int) error                { return c.socket.SetReadBuffer(v) }
func (c *captureTCP) SetWriteBuffer(v int) error               { return c.socket.SetWriteBuffer(v) }
func (c *captureTCP) SetLinger(v int) error                    { return c.socket.SetLinger(v) }

func BenchmarkSingleConnection(b *testing.B) {
	b.Run("Insert", func(b *testing.B) {
		d := newCaptureDialer(corpusPath("rec_insert"), true)
		s, e := gocql.Connect[benchDialect](context.Background(), benchConfig(d))
		if e != nil {
			b.Fatal(e)
		}
		defer func() {
			if err := finishCapture(s, d); err != nil {
				b.Fatal(err)
			}
		}()
		for i := 0; i < 10; i++ {
			b.Run("Case"+strconv.Itoa(i), func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					if e := benchWrite(s, int32(i)); e != nil {
						b.Fatal(e)
					}
				}
			})
		}
	})

	b.Run("Select", func(b *testing.B) {
		d := newCaptureDialer(corpusPath("rec_select"), true)
		s, e := gocql.Connect[benchDialect](context.Background(), benchConfig(d))
		if e != nil {
			b.Fatal(e)
		}
		defer func() {
			if err := finishCapture(s, d); err != nil {
				b.Fatal(err)
			}
		}()
		for i := 0; i < 10; i++ {
			b.Run("Case"+strconv.Itoa(i), func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					if e := benchRead(s, int32(i)); e != nil {
						b.Fatal(e)
					}
				}
			})
		}
	})
}

func TestSingleConnection(t *testing.T) {
	t.Run("Replay", func(t *testing.T) {
		for _, workload := range []struct {
			name, dir string
			operation func(*gocql.Session[benchDialect], int32) error
		}{
			{"select", "rec_select", benchRead}, {"insert", "rec_insert", benchWrite},
		} {
			t.Run(workload.name, func(t *testing.T) {
				d := newCaptureDialer(corpusPath(workload.dir), true)
				s, err := gocql.Connect[benchDialect](context.Background(), benchConfig(d))
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := finishCapture(s, d); err != nil {
						t.Fatal(err)
					}
				}()
				for i := int32(0); i < 10; i++ {
					t.Run(fmt.Sprintf("Case%d", i), func(t *testing.T) {
						if err := workload.operation(s, i); err != nil {
							t.Fatal(err)
						}
					})
				}
			})
		}
	})
}

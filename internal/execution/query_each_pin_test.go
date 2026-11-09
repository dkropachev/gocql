// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"

	"github.com/scylladb/gocql/v2/retry"
)

// pinServe answers every request on every peer with answer(peer, token),
// which returns a whole response frame, and records which peer saw which
// paging token ("" for the first page).
type pinServe struct {
	mu   sync.Mutex
	seen []string
}

func (p *pinServe) start(t *testing.T, peers []net.Conn, answer func(peer int, token string, stream int) []byte) {
	for i, peer := range peers {
		go func() {
			for {
				_, stream, body, err := readRequest(peer)
				if err != nil {
					return
				}
				token := string(loopbackPagingState(body))
				p.mu.Lock()
				p.seen = append(p.seen, fmt.Sprintf("%d:%s", i, token))
				p.mu.Unlock()
				if _, err := peer.Write(answer(i, token, stream)); err != nil {
					return
				}
			}
		}()
	}
}

func (p *pinServe) take() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	seen := p.seen
	p.seen = nil
	return seen
}

func pinTestQuery(t *testing.T, s *cluster.Core, options BoundOptions) *Query[codec.EmptyRecord, []byte] {
	statement := binding.Statement[codec.EmptyRecord, []byte]{Kind: cqltype.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: codec.EmptyParamsEncoder, Rows: codec.RowInto(func(*[]byte) {}, codec.Column("value", codec.OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
	q := codecTestMust(NewUnpreparedQuery(s, statement, options.Values()))
	t.Cleanup(func() { _ = q.Close(context.Background()) })
	return q
}

func pinPages(t *testing.T, stream int, token string) []byte {
	switch token {
	case "":
		return typedTestResponse(t, 4, stream, runtimeRowsBody([]byte("b"), "a"))
	case "b":
		return typedTestResponse(t, 4, stream, runtimeRowsBody([]byte("c"), "b"))
	default:
		return typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "c"))
	}
}

func pinEach(t *testing.T, q *Query[codec.EmptyRecord, []byte]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var got string
	dst := make([]byte, 0, 8)
	if _, err := q.Each(ctx, codec.EmptyRecord{}, &dst, func(row *[]byte) error { got += string(*row); return nil }); err != nil || got != "abc" {
		t.Fatal(got, err)
	}
}

type pinRetryNext struct{}

func (pinRetryNext) Decide(e retry.Attempt) retry.Decision {
	if e.Number == 1 {
		return retry.Decision{Action: retry.Next}
	}
	return retry.Decision{}
}

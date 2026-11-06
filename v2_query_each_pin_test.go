// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

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
				_, stream, body, err := step03ReadRequest(peer)
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

func pinTestQuery(t *testing.T, s *Session, options Options) *Query[Unit, []byte] {
	statement := Statement[Unit, []byte]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: NoParams, Rows: RowInto(func(*[]byte) {}, Column("value", OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
	q := codecTestMust(NewUnpreparedQuery(s, statement, NewColumns(), options))
	t.Cleanup(q.Close)
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

func pinEach(t *testing.T, q *Query[Unit, []byte]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var got string
	dst := make([]byte, 0, 8)
	if err := q.Each(ctx, Unit{}, &dst, func(row *[]byte) error { got += string(*row); return nil }); err != nil || got != "abc" {
		t.Fatal(got, err)
	}
}

// Continuation pages go to the connection that served the first page, while
// first pages still follow the routing plan across hosts.
func TestQueryEachPinsContinuationPages(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 2)
	var serve pinServe
	serve.start(t, peers, func(_ int, token string, stream int) []byte { return pinPages(t, stream, token) })
	q := pinTestQuery(t, s, Options{})
	firsts := map[byte]bool{}
	for range 8 {
		pinEach(t, q)
		seen := serve.take()
		if len(seen) != 3 || seen[1][0] != seen[0][0] || seen[2][0] != seen[0][0] {
			t.Fatal("continuation left the first page's connection", seen)
		}
		firsts[seen[0][0]] = true
	}
	if len(firsts) != 2 {
		t.Fatal("first pages did not follow the plan across hosts", firsts)
	}
}

// When the pinned host goes down between pages, the next page is planned
// again and served elsewhere.
func TestQueryEachRePlansWhenPinnedHostIsDown(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 2)
	var serve pinServe
	serve.start(t, peers, func(peer int, token string, stream int) []byte {
		if token == "b" {
			implEndpoint.HostInfoData(conns[peer].InternalHost).InternalState.Store(NodeDown)
		}
		return pinPages(t, stream, token)
	})
	q := pinTestQuery(t, s, Options{})
	pinEach(t, q)
	seen := serve.take()
	if len(seen) != 3 || seen[1][0] != seen[0][0] || seen[2][0] == seen[1][0] {
		t.Fatal("page after the pinned host went down did not move", seen)
	}
}

type pinRetryNext struct{}

func (pinRetryNext) Decide(e retry.Attempt) retry.Decision {
	if e.Number == 1 {
		return retry.Decision{Action: retry.Next}
	}
	return retry.Decision{}
}

// retry.Next on a pinned continuation moves to another host, as it would
// have moved past the plan's first target, and the page after it pins to
// the new host. Each run fails its first continuation attempt.
// TestSkipHostPlanOmitsPinnedHost covers the skip itself, which round-robin
// plans alone do not expose.
func TestQueryEachPinnedRetryNextMovesHost(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 2)
	var serve pinServe
	var mu sync.Mutex
	failNext := false
	serve.start(t, peers, func(_ int, token string, stream int) []byte {
		mu.Lock()
		fail := token == "b" && failNext
		if fail {
			failNext = false
		}
		mu.Unlock()
		if fail {
			return step03ErrorResponse(t, stream, ErrCodeServer, nil)
		}
		return pinPages(t, stream, token)
	})
	profile := defaultProfile()
	profile.Retry = pinRetryNext{}
	q := pinTestQuery(t, s, testOptions(OptionValues{Profile: profileOf(profile)}))
	for range 16 {
		mu.Lock()
		failNext = true
		mu.Unlock()
		pinEach(t, q)
		seen := serve.take()
		if len(seen) != 4 || seen[1][0] != seen[0][0] || seen[2][0] == seen[1][0] || seen[3][0] != seen[2][0] {
			t.Fatal("retry did not move off the pinned host, or the next page did not pin to the new one", seen)
		}
	}
}

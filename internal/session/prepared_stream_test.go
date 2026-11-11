// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
)

func TestPreparedStream(t *testing.T) {
	t.Run("UnpreparedStatementBindMarkers", func(t *testing.T) {
		s, conns, peers := runtimeTestSession(t, 1)
		session := InternalWrapSession(s)
		statement := StreamStatement[int32, []byte]{
			CQL:    "SELECT value FROM ks.tbl WHERE k = ?",
			Params: codec.OneParam(codec.Int32),
			Rows:   codec.StreamOneColumn(codec.StreamBlob),
		}
		if _, err := session.UnpreparedStreamQuery(statement, StreamOptions{}); err == nil {
			t.Fatal("missing bind metadata accepted")
		}
		statement.BindMarkers = cqltype.NewColumns(cqltype.ColumnSpec{Name: "k", Type: cqltype.IntType})
		q := codecTestMust(session.UnpreparedStreamQuery(statement, StreamOptions{}))
		defer func() { _ = q.Close(context.Background()) }()
		served := make(chan struct{})
		go func() {
			defer close(served)
			stream := typedTestReadRequest(t, conns[0], peers[0])
			if _, err := peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "value"))); err != nil {
				t.Error(err)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cursor := codecTestMust(q.Run(ctx, int32(7)))
		defer cursor.Close()
		dst := make([]byte, 0, 8)
		if !cursor.NextInto(&dst) || string(dst) != "value" || cursor.NextInto(&dst) || cursor.Err() != nil {
			t.Fatalf("unprepared stream: row %q, error %v", dst, cursor.Err())
		}
		<-served
	})

	t.Run("ConditionalMetadataDeferred", func(t *testing.T) {
		for _, incremental := range []bool{false, true} {
			t.Run(fmt.Sprintf("incremental=%t", incremental), func(t *testing.T) {
				s, _, peer := rowStreamSession(t, 4, nil)
				defer peer.Close()
				defer s.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				served := make(chan error, 1)
				go func() {
					op, stream, _, err := readRequest(peer)
					if err == nil && op != 9 {
						err = fmt.Errorf("expected PREPARE, got %d", op)
					}
					if err == nil {
						_, err = peer.Write(typedTestResponse(t, 4, stream, preparedBody([]byte("conditional"), cqltype.NewColumns(), cqltype.NewColumns())))
					}
					if err == nil {
						op, stream, _, err = readRequest(peer)
					}
					if err == nil && op != 10 {
						err = fmt.Errorf("expected EXECUTE, got %d", op)
					}
					if err == nil {
						_, err = peer.Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "result")))
					}
					served <- err
				}()
				decoder := new(countStreamDecoder)
				p, err := PrepareStream(ctx, s, StreamStatement[codec.EmptyRecord, []byte]{CQL: "UPDATE ks.tbl SET value='x' WHERE k=1 IF EXISTS", Kind: cqltype.ConditionalStatement, Rows: decoder}, PrepareOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if decoder.compiles.Load() != 0 {
					t.Fatal("missing conditional metadata compiled eagerly")
				}
				q, err := p.Query(StreamOptions{Incremental: incremental})
				if err != nil {
					t.Fatal(err)
				}
				defer q.Close(context.Background())
				rows, err := q.Run(ctx, codec.EmptyRecord{})
				if err != nil {
					t.Fatal(err)
				}
				dst := make([]byte, 0, 8)
				if !rows.NextInto(&dst) || string(dst) != "result" {
					t.Fatalf("conditional result %q: %v", dst, rows.Err())
				}
				if err := rows.Close(); err != nil {
					t.Fatal(err)
				}
				if err := <-served; err != nil {
					t.Fatal(err)
				}
				if decoder.compiles.Load() != 1 {
					t.Fatalf("conditional actual metadata compiled %d times", decoder.compiles.Load())
				}
			})
		}
	})

	t.Run("IncrementalReusesInitialPlan", func(t *testing.T) {
		s, _, peer := rowStreamSession(t, 4, nil)
		defer peer.Close()
		defer s.Close()
		requests := make(chan oneShotRequest, 1)
		oneShotServer(t, peer, requests)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		decoder := new(countStreamDecoder)
		p, err := PrepareStream(ctx, s, StreamStatement[int32, []byte]{CQL: oneShotStatement().CQL, Params: codec.OneParam(codec.Int32), Rows: decoder}, PrepareOptions{})
		if err != nil {
			t.Fatal(err)
		}
		q, err := p.Query(StreamOptions{Incremental: true})
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close(context.Background())
		for range 2 {
			go func() {
				r := <-requests
				_, _ = peer.Write(typedTestResponse(t, 4, r.stream, runtimeRowsBody(nil, "value")))
			}()
			rows, err := q.Run(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			dst := make([]byte, 0, 8)
			if !rows.NextInto(&dst) || string(dst) != "value" {
				t.Fatalf("stream result %q: %v", dst, rows.Err())
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if decoder.compiles.Load() != 1 {
			t.Fatalf("initial plan compiled %d times", decoder.compiles.Load())
		}
	})
	t.Run("EagerMetadataValidation", func(t *testing.T) {
		s, _, peers := runtimeTestSession(t, 1)
		defer peers[0].Close()
		requests := make(chan oneShotRequest, 1)
		oneShotServer(t, peers[0], requests)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		statement := StreamStatement[int32, int32]{CQL: oneShotStatement().CQL, Params: codec.OneParam(codec.Int32), Rows: codec.StreamOneColumn(codec.StreamInt32)}
		if _, err := PrepareStream(ctx, s, statement, PrepareOptions{}); err == nil {
			t.Fatal("blob metadata accepted by integer stream decoder")
		}
		select {
		case <-requests:
			t.Fatal("invalid metadata submitted an execution")
		default:
		}
	})

	t.Run("SharesInitialPlanBetweenWorkers", func(t *testing.T) {
		s := loopbackTestSession(t, "pone")
		defer s.Close()
		decoder := new(countStreamDecoder)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		p, err := PrepareStream(ctx, s, StreamStatement[int32, []byte]{CQL: oneShotStatement().CQL, Params: codec.OneParam(codec.Int32), Rows: decoder}, PrepareOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if decoder.compiles.Load() != 1 || p.Parameters().Len() != 1 || p.Columns().Len() != 1 {
			t.Fatal("preparation did not validate and expose metadata")
		}
		var workers sync.WaitGroup
		failures := make(chan error, 4)
		for worker := range 4 {
			workers.Go(func() {
				q, err := p.Query(StreamOptions{})
				if err != nil {
					failures <- err
					return
				}
				defer q.Close(context.Background())
				dst := make([]byte, 0, len(loopbackValue))
				for range 3 {
					rows, err := q.Run(ctx, int32(worker))
					if err != nil {
						failures <- err
						return
					}
					if !rows.NextInto(&dst) || string(dst) != loopbackValue {
						failures <- fmt.Errorf("unexpected stream value %q: %v", dst, rows.Err())
						rows.Close()
						return
					}
					if rows.NextInto(&dst) || rows.Err() != nil {
						failures <- fmt.Errorf("unexpected stream termination: %v", rows.Err())
						rows.Close()
						return
					}
					if err := rows.Close(); err != nil {
						failures <- err
						return
					}
				}
			})
		}
		workers.Wait()
		close(failures)
		for err := range failures {
			t.Error(err)
		}
		if got := decoder.compiles.Load(); got != 1 {
			t.Fatalf("unchanged metadata compiled %d times", got)
		}
	})

	t.Run("RevalidatesChangedMetadata", func(t *testing.T) {
		s, _, peers := runtimeTestSession(t, 1)
		defer peers[0].Close()
		requests := make(chan oneShotRequest, 2)
		oneShotServer(t, peers[0], requests)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		decoder := new(countStreamDecoder)
		p, err := PrepareStream(ctx, s, StreamStatement[int32, []byte]{CQL: oneShotStatement().CQL, Params: codec.OneParam(codec.Int32), Rows: decoder}, PrepareOptions{})
		if err != nil {
			t.Fatal(err)
		}
		q, err := p.Query(StreamOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close(context.Background())
		// The response supplies a column with another name. The one-column
		// decoder accepts its type, but must compile the changed metadata.
		body := runtimeRowsBody(nil, "changed")
		for i := 0; i+5 <= len(body); i++ {
			if string(body[i:i+5]) == "value" {
				copy(body[i:i+5], "other")
				break
			}
		}
		go func() { r := <-requests; _, _ = peers[0].Write(typedTestResponse(t, 4, r.stream, body)) }()
		rows, err := q.Run(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		dst := make([]byte, 0, 16)
		if !rows.NextInto(&dst) || string(dst) != "changed" {
			t.Fatalf("changed metadata result %q: %v", dst, rows.Err())
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if decoder.compiles.Load() != 2 {
			t.Fatal("actual metadata did not recompile stream plan")
		}
		// An incompatible actual type is rejected before any destination access.
		invalid := runtimeRowsBody(nil, "1234")
		typeAt := bytes.Index(invalid, []byte("value")) + len("value")
		binary.BigEndian.PutUint16(invalid[typeAt:], uint16(cqltype.TypeInt))
		go func() { r := <-requests; _, _ = peers[0].Write(typedTestResponse(t, 4, r.stream, invalid)) }()
		dst = append(dst[:0], "untouched"...)
		if rows, err := q.Run(ctx, 1); err == nil || rows != nil {
			t.Fatalf("incompatible actual metadata accepted: %v", err)
		}
		if string(dst) != "untouched" || decoder.compiles.Load() != 3 {
			t.Fatal("incompatible metadata bypassed validation or touched output")
		}
		if err := q.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if q.query.state.runtime.inflight.Load() != 0 {
			t.Fatal("metadata failure left inflight execution")
		}
	})

	t.Run("RejectsNilCompiledPlan", func(t *testing.T) {
		s, _, peers := runtimeTestSession(t, 1)
		defer peers[0].Close()
		requests := make(chan oneShotRequest, 1)
		oneShotServer(t, peers[0], requests)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := PrepareStream(ctx, s, StreamStatement[int32, []byte]{CQL: oneShotStatement().CQL, Params: codec.OneParam(codec.Int32), Rows: nilPreparedStreamPlan{}}, PrepareOptions{})
		if err == nil {
			t.Fatal("nil streaming plan accepted")
		}
	})

	t.Run("RejectsNilDecoderAndHandle", func(t *testing.T) {
		if _, err := PrepareStream(context.Background(), nil, StreamStatement[codec.EmptyRecord, []byte]{CQL: "SELECT value FROM ks.tbl"}, PrepareOptions{}); err == nil {
			t.Fatal("nil decoder accepted")
		}
		var p *PreparedStream[int32, []byte]
		if _, err := p.Query(StreamOptions{}); err == nil {
			t.Fatal("nil prepared stream accepted")
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		s, _ := attachmentSession()
		_, err := PrepareStream(ctx, s, StreamStatement[int32, []byte]{CQL: "SELECT value FROM ks.tbl", Params: codec.OneParam(codec.Int32), Rows: codec.StreamOneColumn(codec.StreamBlob)}, PrepareOptions{})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled preparation: %v", err)
		}
	})
}

type nilPreparedStreamPlan struct{}

func (nilPreparedStreamPlan) Compile(cqltype.Columns) (codec.StreamRowPlan[[]byte], error) {
	return nil, nil
}

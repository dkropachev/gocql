package session

import (
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	implCqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/schemafence"

	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

func streamReconcile(t *testing.T, failContinuation bool) {
	s, c, peer := rowStreamSession(t, 4, nil)
	defer peer.Close()
	defer s.Close()
	implCluster.SessionData(s).InternalCfg.Keyspace = "ks"
	blob := codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))
	columns := implCqltype.NewColumns(implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: blob})
	state, _ := preparedState(t, s, columns)
	changed := implCqltype.NewColumns(cqltype.ColumnSlice(columns)[0], implCqltype.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "added", Type: blob})
	bound := func() *Bound[implCqltype.Bytes] {
		opaqueStorageValue := new(Bound[implCqltype.Bytes])
		BoundData(opaqueStorageValue).InternalRequest = InternalExecutionRequest{statement: state.InternalStatement, InternalSource: state.preparedSource(), InternalValues: model.EncodedValues{Columns: implCqltype.NewColumns()}, InternalOwner: InternalNewInputOwner(), routing: model.RoutingInfo{Kind: implCqltype.OrdinaryStatement}}
		BoundData(opaqueStorageValue).InternalRows = codec.OneColumn(codec.BlobCodec)
		return opaqueStorageValue
	}()
	defer bound.Close()
	done := make(chan error, 1)
	go func() {
		for step := 0; step < 3; step++ {
			op, id, body, err := readRequest(peer)
			if err != nil {
				done <- err
				return
			}
			if step == 1 {
				if op != 9 {
					done <- fmt.Errorf("continuation preflight must PREPARE, got %d", op)
					return
				}
				_, err = peer.Write(typedTestResponse(t, 4, id, preparedBody([]byte("changed"), implCqltype.NewColumns(), changed)))
			} else {
				if op != 10 {
					done <- fmt.Errorf("expected EXECUTE, got %d", op)
					return
				}
				if step == 0 {
					if bytes.Contains(body, []byte("next")) {
						done <- fmt.Errorf("first execution has continuation")
						return
					}
					_, err = peer.Write(testResponse(t, c, id, runtimeRowsBody([]byte("next"), "first")))
				} else {
					if !bytes.Contains(body, []byte("next")) {
						done <- fmt.Errorf("continuation was lost or first page replayed")
						return
					}
					if failContinuation {
						_, err = peer.Write(errorResponse(t, id, protocol.CodeServer, nil))
						done <- err
						return
					}
					reply := binary.BigEndian.AppendUint32(nil, 2)
					reply = binary.BigEndian.AppendUint32(reply, 1)
					reply = binary.BigEndian.AppendUint32(reply, 2)
					reply = binary.BigEndian.AppendUint16(reply, 2)
					reply = append(reply, 'k', 's')
					reply = binary.BigEndian.AppendUint16(reply, 3)
					reply = append(reply, 't', 'b', 'l')
					for _, name := range []string{"value", "added"} {
						reply = binary.BigEndian.AppendUint16(reply, uint16(len(name)))
						reply = append(reply, name...)
						reply = binary.BigEndian.AppendUint16(reply, uint16(implCqltype.TypeBlob))
					}
					reply = binary.BigEndian.AppendUint32(reply, 1)
					reply = binary.BigEndian.AppendUint32(reply, 4)
					reply = append(reply, "last"...)
					reply = binary.BigEndian.AppendUint32(reply, 0xffffffff)
					_, err = peer.Write(testResponse(t, c, id, reply))
				}
			}
			if err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	resets := 0
	decoder := codec.StreamRowInto(func(*[]byte) { resets++ }, codec.StreamColumn("value", codec.StreamBlob, func(v *[]byte) *[]byte { return v }))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cursor, err := RunStream(ctx, s, bound, decoder, StreamOptions{Incremental: true, Options: Options{Idempotent: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	dst := make([]byte, 0, 8)
	backing := &dst[:cap(dst)][0]
	if resets != 0 {
		t.Fatal("RunStream accessed caller destination")
	}
	if !cursor.NextInto(&dst) || string(dst) != "first" {
		t.Fatal(cursor.Err())
	}
	state.InternalDependencies.InternalTracker.InternalBump(schemafence.InternalPreparedDependencyKey{InternalKind: 'b', InternalKeyspace: "ks", InternalName: "tbl"})
	if resets != 1 || string(dst) != "first" {
		t.Fatal("schema event accessed caller destination")
	}
	if failContinuation {
		if cursor.NextInto(&dst) {
			t.Fatal("failed continuation delivered output")
		}
		if server, ok := errors.AsType[protocol.ServerError](cursor.Err()); !ok || server.Code() != protocol.CodeServer {
			t.Fatal(cursor.Err())
		}
		info := cursor.Info()
		if info.DeliveredRows != 1 || info.Result.Attempts != 2 || resets != 1 || string(dst) != "first" {
			t.Fatal("continuation error replayed or accessed destination", info, resets, dst)
		}
		cursor.Close()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		return
	}
	if !cursor.NextInto(&dst) || string(dst) != "last" || &dst[0] != backing {
		t.Fatal("next-page reconciliation", dst, cursor.Err())
	}
	if cursor.NextInto(&dst) || cursor.Close() != nil {
		t.Fatal(cursor.Err())
	}
	info := cursor.Info()
	if info.DeliveredRows != 2 || info.Pages != 2 || info.Result.Attempts != 2 || cursor.Columns().Len() != 2 || resets != 2 {
		t.Fatal("replayed rows/pages or failed shape change", info, cursor.Columns())
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

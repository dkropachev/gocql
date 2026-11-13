//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/exec"
)

// TestLWTDialect pins what each server returns for conditional statements and
// batches, as docs/server-differences.md describes, and that the driver
// decodes both with one decoder.
func TestLWTDialect(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(k int, c int, v int, w text, PRIMARY KEY(k, c))", table))
	scylla := testDialectDistribution == "scylla"

	type row struct {
		K, C, V int32
		W       string
	}
	type update struct{ K, C, V, Expect int32 }
	// One struct-mapped decoder for every outcome and both servers: columns
	// it has no field for are skipped, fields without a column stay zero.
	decoder := cql.CASRows(cql.MustRowOf[row]())
	// The server names the IF marker after its column, v, like the SET
	// marker, so the parameters bind by position.
	updateParams := cql.Params(
		cql.ParamAt(0, cql.Int32Codec, func(u update) int32 { return u.V }),
		cql.ParamAt(1, cql.Int32Codec, func(u update) int32 { return u.K }),
		cql.ParamAt(2, cql.Int32Codec, func(u update) int32 { return u.C }),
		cql.ParamAt(3, cql.Int32Codec, func(u update) int32 { return u.Expect }),
	)
	updateCQL := fmt.Sprintf("UPDATE %s SET v = ? WHERE k = ? AND c = ? IF v = ?", table)
	conditional := cassandraPrepared(t, session, cql.Statement[update, cql.CASResult[row]]{
		Kind:   cql.ConditionalStatement,
		CQL:    updateCQL,
		Params: updateParams,
		Rows:   decoder,
	}, exec.PrepareOptions{})
	insert := cassandraPrepared(t, session, cql.Statement[row, cql.EmptyRow]{
		CQL: fmt.Sprintf("INSERT INTO %s(k, c, v, w) VALUES(?, ?, ?, ?)", table),
		Params: cql.Params(
			cql.Param("k", cql.Int32Codec, func(r row) int32 { return r.K }),
			cql.Param("c", cql.Int32Codec, func(r row) int32 { return r.C }),
			cql.Param("v", cql.Int32Codec, func(r row) int32 { return r.V }),
			cql.Param("w", cql.StringCodec, func(r row) string { return r.W }),
		),
	}, exec.PrepareOptions{})
	_, err := insert.Exec(ctx, row{K: 1, C: 1, V: 10, W: "x"})
	require.NoError(t, err)
	_, err = insert.Exec(ctx, row{K: 1, C: 2, V: 20, W: "y"})
	require.NoError(t, err)

	t.Run("ExecRejected", func(t *testing.T) {
		// Declared conditional: rejected before anything is sent.
		conditional := cassandraPrepared(t, session, cql.Statement[update, cql.EmptyRow]{
			Kind: cql.ConditionalStatement, CQL: updateCQL, Params: updateParams,
		}, exec.PrepareOptions{})
		_, err := conditional.Exec(ctx, update{K: 1, C: 1, V: 11, Expect: 10})
		require.True(t, errors.Is(err, cqlerr.ErrConditionalExec), "%v", err)
		require.Equal(t, cqlerr.NotExecuted, cqlerr.ExecutionStatusOf(err))
	})

	t.Run("Single", func(t *testing.T) {
		var result cql.CASResult[row]
		// Applied: only ScyllaDB returns the previous values.
		found, _, err := conditional.One(ctx, update{K: 1, C: 1, V: 11, Expect: 10}, &result)
		require.NoError(t, err)
		require.True(t, found && result.Applied)
		require.Equal(t, scylla, result.Previous.Set)
		if scylla {
			require.Equal(t, int32(10), result.Previous.Value.V)
		}
		// Not applied: both return the current value of the condition column.
		found, _, err = conditional.One(ctx, update{K: 1, C: 1, V: 12, Expect: 10}, &result)
		require.NoError(t, err)
		require.True(t, found && !result.Applied && result.Previous.Set)
		require.Equal(t, int32(11), result.Previous.Value.V)
		// Not applied to a missing row: no previous row on either server.
		found, _, err = conditional.One(ctx, update{K: 9, C: 9, V: 1, Expect: 1}, &result)
		require.NoError(t, err)
		require.True(t, found && !result.Applied && !result.Previous.Set)
	})

	t.Run("Batch", func(t *testing.T) {
		run := func(updates ...update) []cql.CASResult[row] {
			var batch gocql.Batch
			for _, u := range updates {
				require.NoError(t, conditional.AddToBatch(&batch, u))
			}
			// An ordinary member contributes no row on ScyllaDB.
			require.NoError(t, insert.AddToBatch(&batch, row{K: 1, C: 3, V: 30}))
			_, err := session.ExecBatch(ctx, &batch)
			require.True(t, errors.Is(err, cqlerr.ErrConditionalExec), "%v", err)
			rows, err := session.RunBatch(ctx, &batch, decoder)
			require.NoError(t, err)
			defer rows.Close()
			var out []cql.CASResult[row]
			var r cql.CASResult[row]
			for rows.NextInto(&r) {
				out = append(out, r)
			}
			require.NoError(t, rows.Err())
			return out
		}
		applied := func(rows []cql.CASResult[row]) bool {
			for _, r := range rows {
				if !r.Applied {
					return false
				}
			}
			return len(rows) > 0
		}
		// Applied: ScyllaDB returns a row per conditional statement, in
		// statement order; Cassandra one row.
		rows := run(update{K: 1, C: 1, V: 13, Expect: 11}, update{K: 1, C: 2, V: 23, Expect: 20})
		require.True(t, applied(rows))
		if scylla {
			require.Len(t, rows, 2)
			require.Equal(t, int32(11), rows[0].Previous.Value.V)
			require.Equal(t, int32(20), rows[1].Previous.Value.V)
		} else {
			require.Len(t, rows, 1)
		}
		// Not applied: the batch applied iff every row did, on both servers.
		rows = run(update{K: 1, C: 1, V: 14, Expect: 13}, update{K: 1, C: 2, V: 24, Expect: 0})
		require.False(t, applied(rows))
		if scylla {
			require.Len(t, rows, 2)
		} else {
			require.NotEmpty(t, rows)
		}
	})
}

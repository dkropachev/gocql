// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package binding

import (
	"fmt"
	"strings"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/syncx"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// paramWriters reuses parameter writers. The writer is cql's and reached
// through internal/link, so it escapes to the heap; reusing it keeps binding
// a statement free of a writer allocation.
var paramWriters = syncx.NewPool(func() *cql.ParamWriter { return new(cql.ParamWriter) })

func InternalEncodeParameters[P any](plan cql.CompiledParams[P], columns cql.Columns, value P, owned, allowUnset bool) (model.EncodedValues, error) {
	if columns.Len() > 65535 {
		return model.EncodedValues{}, cqltype.TypeValidation("parameters", "maximum count is 65535")
	}
	// Parameters usually contain a few scalar fields and borrowed payload spans.
	// Avoid retaining a 4 KiB slab just to frame a small partition key. The builder
	// allocates larger slabs when an individual encoded scalar needs them.
	b := wire.NewBuilder(256)
	writer := paramWriters.Get()
	codec.ResetParamWriter(writer, b, columns, allowUnset)
	codec.SetParamOwnership(writer, owned)
	err := plan.Encode(writer, value)
	if err == nil {
		err = codec.FinishParamWriter(writer)
	}
	// Drop the builder and columns before the writer goes back to the pool.
	codec.ResetParamWriter(writer, nil, cql.Columns{}, false)
	paramWriters.Put(writer)
	if err != nil {
		return model.EncodedValues{}, err
	}
	framed, err := b.Freeze()
	if err != nil {
		return model.EncodedValues{}, err
	}
	cursor := wire.NewCursor(framed)
	cells := make([]model.EncodedCell, columns.Len())
	for i := range cells {
		column, _ := columns.At(i)
		v, err := codec.ReadValue(cursor, column.Type, nil, allowUnset)
		if err != nil {
			return model.EncodedValues{}, err
		}
		cells[i] = model.EncodedCell{Typ: column.Type, State: v.State(), Body: cqltype.BytesChain(v.Bytes())}
	}
	if err := cursor.RequireEnd(); err != nil {
		return model.EncodedValues{}, fmt.Errorf("gocql: parameter framing: %w", err)
	}
	return model.EncodedValues{Columns: columns, Cells: cells, Framed: framed}, nil
}

// Statement is CQL text with its typed parameter encoder and row decoder.
// A nil Params is allowed when P is EmptyParams, and a nil Rows when R is
// EmptyRow. BindMarkers describes the bind markers of a statement executed
// without preparation; prepared statements take them from the server, so
// Prepare ignores it.
type Statement[P, R any] struct {
	Params      cql.ParamEncoder[P]
	Rows        cql.RowDecoder[R]
	CQL         string
	BindMarkers cql.Columns
	Kind        cql.StatementKind
	Idempotent  bool
}

// emptyParams and emptyRows fill a nil Params or Rows of a statement without
// bind markers or result columns. They are the driver's own, so a package
// variable an application reassigns cannot change them.
var (
	emptyParams = cql.Params[cql.EmptyParams]()
	emptyRows   = cql.Row[cql.EmptyRow]()
)

// statementDefaults fills a nil Params or Rows that has an empty
// default.
func statementDefaults[P, R any](s *Statement[P, R]) {
	if s.Params == nil {
		if e, ok := any(emptyParams).(cql.ParamEncoder[P]); ok {
			s.Params = e
		}
	}
	if s.Rows == nil {
		if d, ok := any(emptyRows).(cql.RowDecoder[R]); ok {
			s.Rows = d
		}
	}
}

// ValidateStatement fills s's empty defaults, validates it and resolves its
// Kind; see ResolveKind.
func ValidateStatement[P, R any](s *Statement[P, R]) error {
	statementDefaults(s)
	if err := validateStatement(*s); err != nil {
		return err
	}
	kind, err := ResolveKind(s.Kind, s.CQL, s.Rows)
	if err != nil {
		return err
	}
	s.Kind = kind
	return nil
}

// ResolveKind returns the kind a statement of kind, text and row decoder
// executes as. A statement decoded with cql.CASRows, or whose text is an
// INSERT, UPDATE, DELETE or BATCH with an IF outside quotes and comments, is
// conditional; anything else is ordinary. An UnknownStatement takes that
// inference; a ConditionalStatement stays one; an OrdinaryStatement the
// inference finds conditional is rejected with a ValidationError, before
// anything is sent, since it would run with a client timestamp, shuffled
// replicas and speculation a conditional statement must not have.
// It scans the text once, so a prepared statement resolves at Prepare and an
// unprepared one per call.
func ResolveKind(kind cql.StatementKind, text string, rows any) (cql.StatementKind, error) {
	if kind == cql.ConditionalStatement {
		return kind, nil
	}
	conditional := codec.ConditionalRows(rows) || InternalConditionalCQL(text)
	switch {
	case !conditional:
		return cql.OrdinaryStatement, nil
	case kind == cql.OrdinaryStatement:
		return 0, cqltype.TypeValidation("statement kind", "an OrdinaryStatement with a CASRows decoder or an IF condition is conditional; declare it ConditionalStatement or leave it UnknownStatement")
	default:
		return cql.ConditionalStatement, nil
	}
}

func validateStatement[P, R any](s Statement[P, R]) error {
	if s.Kind > cql.ConditionalStatement {
		return cqltype.TypeValidation("statement kind", "unknown classification")
	}
	if strings.TrimSpace(s.CQL) == "" {
		return cqltype.TypeValidation("statement", "empty CQL")
	}
	if strings.EqualFold(firstCQLWord(s.CQL), "USE") {
		return cqlerr.ErrUseStmt
	}
	if s.Params == nil || s.Rows == nil {
		return cqltype.TypeValidation("statement", "parameter and row codecs are required")
	}
	return nil
}

// InternalFirstCQLWord returns the first unquoted word of cql, upper-cased.
func InternalFirstCQLWord(cql string) string {
	return strings.ToUpper(firstCQLWord(cql))
}

// firstCQLWord returns the first unquoted word of cql as written.
func firstCQLWord(cql string) string {
	word := ""
	visitCQLWords(cql, func(next string) bool { word = next; return false })
	return word
}

// InternalConditionalCQL reports whether cql is an INSERT, UPDATE, DELETE or
// BATCH (BEGIN ... BATCH) with an IF word outside string literals, quoted
// identifiers and comments: a lightweight transaction. IF is a reserved word,
// so it cannot be an unquoted identifier; DDL such as CREATE TABLE IF NOT
// EXISTS is not conditional.
func InternalConditionalCQL(cql string) bool {
	conditional, first := false, true
	visitCQLWords(cql, func(word string) bool {
		if first {
			first = false
			for _, dml := range [...]string{"INSERT", "UPDATE", "DELETE", "BEGIN"} {
				if strings.EqualFold(word, dml) {
					return true
				}
			}
			return false
		}
		if strings.EqualFold(word, "IF") {
			conditional = true
			return false
		}
		return true
	})
	return conditional
}

// visitCQLWords calls visit with each unquoted word of cql, as written,
// until visit returns false. It skips string literals, single-quoted with
// a doubled quote as escape or between $$ pairs, double-quoted identifiers,
// and --, // and /* */ comments, so a word inside one is never visited. It
// does not allocate.
func visitCQLWords(cql string, visit func(string) bool) {
	for i := 0; i < len(cql); {
		if cql[i] == '\'' || cql[i] == '"' {
			quote := cql[i]
			i++
			for i < len(cql) {
				if cql[i] == quote {
					i++
					if i < len(cql) && cql[i] == quote {
						i++
						continue
					}
					break
				}
				i++
			}
			continue
		}
		if strings.HasPrefix(cql[i:], "$$") {
			end := strings.Index(cql[i+2:], "$$")
			if end < 0 {
				return
			}
			i += 2 + end + 2
			continue
		}
		if strings.HasPrefix(cql[i:], "--") || strings.HasPrefix(cql[i:], "//") {
			i += 2
			for i < len(cql) && cql[i] != '\n' && cql[i] != '\r' {
				i++
			}
			continue
		}
		if strings.HasPrefix(cql[i:], "/*") {
			end := strings.Index(cql[i+2:], "*/")
			if end < 0 {
				return
			}
			i += 2 + end + 2
			continue
		}
		if cql[i] >= 'a' && cql[i] <= 'z' || cql[i] >= 'A' && cql[i] <= 'Z' || cql[i] == '_' {
			start := i
			i++
			for i < len(cql) && (cql[i] >= 'a' && cql[i] <= 'z' || cql[i] >= 'A' && cql[i] <= 'Z' || cql[i] >= '0' && cql[i] <= '9' || cql[i] == '_') {
				i++
			}
			if !visit(cql[start:i]) {
				return
			}
			continue
		}
		i++
	}
}

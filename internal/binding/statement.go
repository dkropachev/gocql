// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package binding

import (
	"fmt"
	"strings"

	"github.com/scylladb/gocql/v2/internal/codec"
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func InternalEncodeParameters[P any](plan codec.ParamPlan[P], columns cqltype.Columns, value P, owned, allowUnset bool) (model.EncodedValues, error) {
	if columns.Len() > 65535 {
		return model.EncodedValues{}, cqltype.TypeValidation("parameters", "maximum count is 65535")
	}
	// Parameters usually contain a few scalar fields and borrowed payload spans.
	// Avoid retaining a 4 KiB slab just to frame a small partition key. The builder
	// allocates larger slabs when an individual encoded scalar needs them.
	b := wire.NewBuilder(256)
	writer := codec.NewParamWriter(b, columns, allowUnset)
	codec.SetParamOwnership(writer, owned)
	if err := plan.Encode(writer, value); err != nil {
		return model.EncodedValues{}, err
	}
	if err := codec.FinishParamWriter(writer); err != nil {
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
	Params      codec.ParamEncoder[P]
	Rows        codec.RowDecoder[R]
	CQL         string
	BindMarkers cqltype.Columns
	Kind        cqltype.StatementKind
}

// InternalStatementDefaults fills a nil Params or Rows that has an empty
// default.
func statementDefaults[P, R any](s *Statement[P, R]) {
	if s.Params == nil {
		if e, ok := any(codec.EmptyParamsEncoder).(codec.ParamEncoder[P]); ok {
			s.Params = e
		}
	}
	if s.Rows == nil {
		if d, ok := any(codec.EmptyRowDecoder).(codec.RowDecoder[R]); ok {
			s.Rows = d
		}
	}
}

// ValidateStatement fills s's empty defaults and validates it.
func ValidateStatement[P, R any](s *Statement[P, R]) error {
	statementDefaults(s)
	return validateStatement(*s)
}

func validateStatement[P, R any](s Statement[P, R]) error {
	if s.Kind > cqltype.ConditionalStatement {
		return cqltype.TypeValidation("statement kind", "unknown classification")
	}
	if strings.TrimSpace(s.CQL) == "" {
		return cqltype.TypeValidation("statement", "empty CQL")
	}
	if InternalFirstCQLWord(s.CQL) == "USE" {
		return runtimeerrors.ErrUseStmt
	}
	if s.Params == nil || s.Rows == nil {
		return cqltype.TypeValidation("statement", "parameter and row codecs are required")
	}
	return nil
}

func InternalFirstCQLWord(cql string) string {
	word := ""
	visitCQLWords(cql, func(next string) bool { word = next; return false })
	return word
}
func InternalConditionalCQL(cql string) bool {
	conditional := false
	visitCQLWords(cql, func(word string) bool {
		if word == "IF" {
			conditional = true
			return false
		}
		return true
	})
	return conditional
}
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
		if i+1 < len(cql) && cql[i] == '-' && cql[i+1] == '-' {
			i += 2
			for i < len(cql) && cql[i] != '\n' {
				i++
			}
			continue
		}
		if i+1 < len(cql) && cql[i] == '/' && cql[i+1] == '*' {
			i += 2
			for i+1 < len(cql) && (cql[i] != '*' || cql[i+1] != '/') {
				i++
			}
			i += 2
			continue
		}
		if cql[i] >= 'a' && cql[i] <= 'z' || cql[i] >= 'A' && cql[i] <= 'Z' || cql[i] == '_' {
			start := i
			i++
			for i < len(cql) && (cql[i] >= 'a' && cql[i] <= 'z' || cql[i] >= 'A' && cql[i] <= 'Z' || cql[i] >= '0' && cql[i] <= '9' || cql[i] == '_') {
				i++
			}
			if !visit(strings.ToUpper(cql[start:i])) {
				return
			}
			continue
		}
		i++
	}
}

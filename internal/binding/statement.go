// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package binding

import (
	"fmt"
	"strings"

	"github.com/scylladb/gocql/v2/internal/codec"
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func InternalEncodeParameters[P any](plan ParamPlan[P], columns Columns, value P, owned, allowUnset bool) (encodedValues, error) {
	if columns.Len() > 65535 {
		return encodedValues{}, typeValidation("parameters", "maximum count is 65535")
	}
	// Parameters usually contain a few scalar fields and borrowed payload spans.
	// Avoid retaining a 4 KiB slab just to frame a small partition key. The builder
	// allocates larger slabs when an individual encoded scalar needs them.
	b := wire.NewBuilder(256)
	writer := newParamWriter(b, columns, allowUnset)
	codec.SetParamOwnership(writer, owned)
	if err := plan.Encode(writer, value); err != nil {
		return encodedValues{}, err
	}
	if err := finishParamWriter(writer); err != nil {
		return encodedValues{}, err
	}
	framed, err := b.Freeze()
	if err != nil {
		return encodedValues{}, err
	}
	cursor := wire.NewCursor(framed)
	cells := make([]encodedCell, columns.Len())
	for i := range cells {
		column, _ := columns.At(i)
		v, err := readValue(cursor, column.Type, nil, allowUnset)
		if err != nil {
			return encodedValues{}, err
		}
		cells[i] = encodedCell{Typ: column.Type, State: v.State(), Body: cqltype.BytesChain(v.Bytes())}
	}
	if err := cursor.RequireEnd(); err != nil {
		return encodedValues{}, fmt.Errorf("gocql: parameter framing: %w", err)
	}
	return encodedValues{Columns: columns, Cells: cells, Framed: framed}, nil
}

// Statement is CQL text with its typed parameter encoder and row decoder.
// A nil Params is allowed when P is EmptyParams, and a nil Rows when R is
// EmptyRow. BindMarkers describes the bind markers of a statement executed
// without preparation; prepared statements take them from the server, so
// Prepare ignores it.
type Statement[P, R any] struct {
	Params      ParamEncoder[P]
	Rows        RowDecoder[R]
	CQL         string
	BindMarkers Columns
	Kind        StatementKind
}

// InternalStatementDefaults fills a nil Params or Rows that has an empty
// default.
func InternalStatementDefaults[P, R any](s *Statement[P, R]) {
	if s.Params == nil {
		if e, ok := any(codec.EmptyParamsEncoder).(ParamEncoder[P]); ok {
			s.Params = e
		}
	}
	if s.Rows == nil {
		if d, ok := any(codec.EmptyRowDecoder).(RowDecoder[R]); ok {
			s.Rows = d
		}
	}
}

func InternalValidateStatement[P, R any](s Statement[P, R]) error {
	if s.Kind > ConditionalStatement {
		return typeValidation("statement kind", "unknown classification")
	}
	if strings.TrimSpace(s.CQL) == "" {
		return typeValidation("statement", "empty CQL")
	}
	if InternalFirstCQLWord(s.CQL) == "USE" {
		return ErrUseStmt
	}
	if s.Params == nil || s.Rows == nil {
		return typeValidation("statement", "parameter and row codecs are required")
	}
	return nil
}

func InternalFirstCQLWord(cql string) string {
	word := ""
	InternalVisitCQLWords(cql, func(next string) bool { word = next; return false })
	return word
}
func InternalConditionalCQL(cql string) bool {
	conditional := false
	InternalVisitCQLWords(cql, func(word string) bool {
		if word == "IF" {
			conditional = true
			return false
		}
		return true
	})
	return conditional
}
func InternalVisitCQLWords(cql string, visit func(string) bool) {
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

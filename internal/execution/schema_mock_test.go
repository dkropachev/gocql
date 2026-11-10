package execution

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// These adapters retain the existing schema fixture factories and their failure
// injection/counting hooks. Production metadata code receives only canonical
// typed request values and owned response pages through v2SystemQueryer.
func (m *schemaDataMock) InternalQueryV2System(ctx context.Context, request *model.AttemptRequest) (*transport.InternalResponsePage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	values, err := v2SchemaMockArguments(request)
	if err != nil {
		return nil, err
	}
	return v2SchemaMockResponse(request.Statement, m.querySystem(request.Statement, values...))
}

// Schema predicates are text values. Decode their wire framing explicitly; the
// []any is used only to call the pre-existing test fixture factory, never a
// production execution or serialization API.
func v2SchemaMockArguments(request *model.AttemptRequest) ([]any, error) {
	cursor := wire.NewCursor(request.Values)
	values := make([]any, request.ValueCount)
	for i := range values {
		size, err := cursor.Uint32()
		if err != nil {
			return nil, err
		}
		if int32(size) < 0 {
			return nil, fmt.Errorf("schema mock argument %d is null or unset", i)
		}
		body, err := cursor.Take(int64(size))
		if err != nil {
			return nil, err
		}
		var value strings.Builder
		if err := body.Each(func(span wire.Span) error {
			_, err := value.Write(span.Bytes())
			return err
		}); err != nil {
			return nil, err
		}
		values[i] = value.String()
	}
	return values, cursor.RequireEnd()
}

func v2SchemaMockResponse(statement string, fixture *nativeResultFixture) (*transport.InternalResponsePage, error) {
	if fixture == nil {
		return nil, metadata.InternalErrNoControl
	}
	release := func() {
		if fixture.framer != nil {
			fixture.framer.Release()
		}
	}
	if fixture.err != nil {
		release()
		return nil, fixture.err
	}
	columns, err := cqltype.ColumnsFromInfo(fixture.meta.Columns)
	if err != nil {
		release()
		return nil, err
	}
	if columns.Len() == 0 && fixture.numRows == 0 {
		// An empty native result still carries metadata. The old fixture format
		// omitted it, so supply the actual schema of its selected system table.
		columns, err = v2SchemaMockEmptyColumns(statement)
		if err != nil {
			release()
			return nil, err
		}
	}
	var framed []byte
	for row := 0; row < fixture.numRows; row++ {
		if fixture.framer == nil {
			release()
			return nil, fmt.Errorf("schema mock row %d has no framer", row)
		}
		for column := 0; column < columns.Len(); column++ {
			body, err := fixture.framer.ReadBytesInternal()
			if err != nil {
				release()
				return nil, err
			}
			if body == nil {
				framed = binary.BigEndian.AppendUint32(framed, ^uint32(0))
			} else {
				framed = binary.BigEndian.AppendUint32(framed, uint32(len(body)))
				framed = append(framed, body...)
			}
		}
	}
	chain, err := wire.NewChain(wire.Borrow(framed))
	if err != nil {
		release()
		return nil, err
	}
	return &transport.InternalResponsePage{ResponseData: model.ResponseData{Kind: 2,
		Columns:     columns,
		ColumnCount: columns.Len(),
		Rows:        chain,
		RowCount:    int32(fixture.numRows),
		Owner:       wire.NewLease(chain, release),
		State:       model.CompletePageState()},
	}, nil
}

func v2SchemaMockEmptyColumns(statement string) (cqltype.Columns, error) {
	textType := cqltype.VarcharType
	boolType := cqltype.BooleanType
	uuidType := cqltype.UUIDType
	listText, _ := cqltype.ListType(textType)
	mapText, _ := cqltype.MapType(textType, textType)
	textColumn := func(name string) cqltype.ColumnSpec { return cqltype.ColumnSpec{Name: name, Type: textType} }
	switch {
	case strings.Contains(statement, "FROM system_schema.keyspaces"):
		return cqltype.NewColumns(textColumn("keyspace_name"), cqltype.ColumnSpec{Name: "durable_writes", Type: boolType}, cqltype.ColumnSpec{Name: "replication", Type: mapText}), nil
	case strings.Contains(statement, "FROM system_schema.tables"):
		return cqltype.ColumnsFromInfo(tableMeta.Columns)
	case strings.Contains(statement, "FROM system_schema.columns"):
		return cqltype.ColumnsFromInfo(columnMeta.Columns)
	case strings.Contains(statement, "FROM system_schema.scylla_tables"):
		return cqltype.NewColumns(textColumn("table_name")), nil
	case strings.Contains(statement, "FROM system_schema.types"):
		return cqltype.NewColumns(textColumn("type_name"), cqltype.ColumnSpec{Name: "field_names", Type: listText}, cqltype.ColumnSpec{Name: "field_types", Type: listText}), nil
	case strings.Contains(statement, "FROM system_schema.functions"):
		return cqltype.NewColumns(textColumn("function_name"), cqltype.ColumnSpec{Name: "argument_types", Type: listText}, cqltype.ColumnSpec{Name: "argument_names", Type: listText}, textColumn("body"), cqltype.ColumnSpec{Name: "called_on_null_input", Type: boolType}, textColumn("language"), textColumn("return_type")), nil
	case strings.Contains(statement, "FROM system_schema.aggregates"):
		return cqltype.NewColumns(textColumn("aggregate_name"), cqltype.ColumnSpec{Name: "argument_types", Type: listText}, textColumn("final_func"), textColumn("initcond"), textColumn("return_type"), textColumn("state_func"), textColumn("state_type")), nil
	case strings.Contains(statement, "FROM system_schema.indexes"):
		return cqltype.NewColumns(textColumn("index_name"), textColumn("keyspace_name"), textColumn("table_name"), textColumn("kind"), cqltype.ColumnSpec{Name: "options", Type: mapText}), nil
	case strings.Contains(statement, "FROM system_schema.views"):
		return cqltype.NewColumns(cqltype.ColumnSpec{Name: "id", Type: uuidType}, textColumn("view_name"), cqltype.ColumnSpec{Name: "base_table_id", Type: uuidType}, textColumn("base_table_name"), cqltype.ColumnSpec{Name: "include_all_columns", Type: boolType}, textColumn("where_clause")), nil
	case strings.HasPrefix(statement, "DESCRIBE KEYSPACE "):
		return cqltype.NewColumns(textColumn("create_statement")), nil
	default:
		return cqltype.Columns{}, fmt.Errorf("schema mock has no result metadata for %q", statement)
	}
}

func (f *fakeControlConn) InternalQueryV2System(ctx context.Context, r *model.AttemptRequest) (*transport.InternalResponsePage, error) {
	f.statement = r.Statement
	f.values = nil
	cursor := wire.NewCursor(r.Values)
	for i := 0; i < r.ValueCount; i++ {
		size, err := cursor.Uint32()
		if err != nil {
			return nil, err
		}
		body, err := cursor.Take(int64(size))
		if err != nil {
			return nil, err
		}
		view := wire.NewCursor(body)
		n, err := view.Uint32()
		if err != nil {
			return nil, err
		}
		vs := make([]string, 0, n)
		for j := uint32(0); j < n; j++ {
			size, err := view.Uint32()
			if err != nil {
				return nil, err
			}
			v, err := view.Take(int64(size))
			if err != nil {
				return nil, err
			}
			var value strings.Builder
			v.Each(func(span wire.Span) error { value.Write(span.Bytes()); return nil })
			vs = append(vs, value.String())
		}
		f.values = append(f.values, vs)
	}
	if f.iter != nil {
		return v2SchemaMockResponse(r.Statement, f.iter)
	}
	text := cqltype.VarcharType
	integer := cqltype.IntType
	cols := cqltype.NewColumns(cqltype.ColumnSpec{Name: "connection_id", Type: text}, cqltype.ColumnSpec{Name: "host_id", Type: text}, cqltype.ColumnSpec{Name: "address", Type: text}, cqltype.ColumnSpec{Name: "port", Type: integer}, cqltype.ColumnSpec{Name: "tls_port", Type: integer})
	chain, _ := wire.NewChain()
	return &transport.InternalResponsePage{ResponseData: model.ResponseData{Columns: cols, ColumnCount: 5, Rows: chain, Owner: wire.NewLease(chain, nil)}}, nil
}

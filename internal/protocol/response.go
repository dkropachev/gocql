// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/logging"

	"bytes"
	"fmt"
	. "github.com/scylladb/gocql/v2/internal/cqltype"
	"net"
	"strconv"
	"strings"

	frm "github.com/scylladb/gocql/v2/internal/frame"

	model "github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// typedParser folds scalar fields directly across immutable receive spans.
// Its sticky error makes every metadata read checked, without panic/recover.

type TypedParser struct {
	Cursor typedMetadataCursor
	Err    error
	reuse  *MetadataReuse
	owner  *wire.Lease
	// expect is the result columns the request expects, typically a prepared
	// statement's; equal parsed columns are returned as expect itself.
	expect     Columns
	MaxDepth   int
	Proto      byte
	MetadataID bool
}

// MetadataReuse belongs to the serialized receive path of one connection.
// It retains up to metadataReuseEntries fully validated column specifications,
// each with a 64 KiB encoded ceiling, so that statements interleaved on the
// connection keep theirs. Exact byte identity permits sharing immutable names
// and types; differing bytes always take the complete checked parser path.
type MetadataReuse struct {
	entries [metadataReuseEntries]metadataReuseEntry
	// next is the entry a response with no entry of its own replaces.
	next uint8
}

const metadataReuseEntries = 4

type metadataReuseEntry struct {
	encoded []byte
	columns Columns
	// owner is the expected columns of the request that filled the entry, or
	// last matched it; a request expecting them probes this entry first, and
	// replaces it when its result metadata changes.
	owner Columns
	// hit is what a byte-identical response returns to owner's requests:
	// owner when it equals columns, so that checking the response against
	// the expected columns compares identities, otherwise columns.
	hit    Columns
	depth  int32
	global bool
}

// match reports whether the entry holds metadata of count columns encoded
// exactly as the next bytes of cursor, advancing cursor past them if so; on a
// mismatch the caller restores cursor.
func (e *metadataReuseEntry) match(cursor *wire.Cursor, count int, global bool, depth int) bool {
	if e.columns.Len() != count || e.global != global || int(e.depth) != depth || len(e.encoded) == 0 {
		return false
	}
	candidate, err := cursor.Take(int64(len(e.encoded)))
	if err != nil {
		return false
	}
	if b, ok := candidate.Contiguous(); ok {
		return bytes.Equal(b, e.encoded)
	}
	offset, same := 0, true
	_ = candidate.Each(func(span wire.Span) error {
		n := span.Len()
		if !bytes.Equal(span.Bytes(), e.encoded[offset:offset+n]) {
			same = false
		}
		offset += n
		return nil
	})
	return same
}

// lookup returns the remembered columns for metadata encoded exactly as the
// next bytes of cursor, probing first the entry of the request's expected
// columns. On a hit it advances cursor past the metadata.
func (r *MetadataReuse) lookup(cursor *wire.Cursor, expect Columns, count int, global bool, depth int) (Columns, bool) {
	start := *cursor
	tried := -1
	if expect.Len() > 0 {
		for i := range r.entries {
			e := &r.entries[i]
			if owned := e.owner.Same(expect); owned || e.columns.Same(expect) {
				if e.match(cursor, count, global, depth) {
					if !owned {
						// The statement adopted these parsed columns.
						return expect, true
					}
					return e.hit, true
				}
				*cursor = start
				tried = i
				break
			}
		}
	}
	for i := range r.entries {
		e := &r.entries[i]
		if i == tried {
			continue
		}
		if !e.match(cursor, count, global, depth) {
			*cursor = start
			continue
		}
		if expect.Len() == count && e.columns.Equal(expect) {
			if e.owner.Len() == 0 {
				e.owner, e.hit = expect, expect
			}
			return expect, true
		}
		return e.columns, true
	}
	return Columns{}, false
}

// store remembers parsed columns for the encoded metadata, replacing the
// entry of the request's expected columns, or else the next one in turn. It
// returns what the response carries: expect when equal to columns.
func (r *MetadataReuse) store(encoded wire.Chain, columns, expect Columns, global bool, depth int) Columns {
	hit := columns
	if expect.Len() == columns.Len() && expect.Len() > 0 && columns.Equal(expect) {
		hit = expect
	}
	slot := -1
	if expect.Len() > 0 {
		for i := range r.entries {
			if e := &r.entries[i]; e.owner.Same(expect) || e.columns.Same(expect) {
				slot = i
				break
			}
		}
	}
	if slot < 0 {
		slot = int(r.next)
		r.next = (r.next + 1) % metadataReuseEntries
	}
	e := &r.entries[slot]
	n := encoded.Len()
	if cap(e.encoded) < int(n) {
		e.encoded = make([]byte, int(n))
	} else {
		e.encoded = e.encoded[:n]
	}
	_, _ = encoded.CopyTo(e.encoded)
	e.columns, e.owner, e.hit, e.global, e.depth = columns, expect, hit, global, int32(depth)
	return hit
}

func (p *TypedParser) Check(err error) {
	if p.Err == nil {
		p.Err = err
	}
}
func (p *TypedParser) U8() byte {
	if p.Err != nil {
		return 0
	}
	v, e := p.Cursor.Uint8()
	p.Check(e)
	return v
}
func (p *TypedParser) U16() uint16 {
	if p.Err != nil {
		return 0
	}
	v, e := p.Cursor.Uint16()
	p.Check(e)
	return v
}
func (p *TypedParser) I32() int {
	if p.Err != nil {
		return 0
	}
	v, e := p.Cursor.Uint32()
	p.Check(e)
	return int(int32(v))
}
func (p *TypedParser) Take(n int64) wire.Chain {
	if p.Err != nil {
		return wire.Chain{}
	}
	v, e := p.Cursor.Take(n)
	p.Check(e)
	return v
}
func (p *TypedParser) Copy(n int) []byte {
	if v, ok := p.Cursor.(budgetedMetadataCursor); ok {
		if p.Err != nil {
			return nil
		}
		b, err := v.CopyMetadata(int64(n))
		p.Check(err)
		return b
	}
	c := p.Take(int64(n))
	if p.Err != nil {
		return nil
	}
	out := make([]byte, n)
	_, err := c.CopyTo(out)
	p.Check(err)
	return out
}
func (p *TypedParser) Str() string {
	n := int(p.U16())
	if v, ok := p.Cursor.(budgetedMetadataCursor); ok {
		if p.Err != nil {
			return ""
		}
		p.ReserveAllocation(int64(n))
		if p.Err != nil {
			return ""
		}
		b, err := v.CopyMetadata(int64(n))
		p.Check(err)
		return string(b)
	}
	c := p.Take(int64(n))
	if p.Err != nil {
		return ""
	}
	var b strings.Builder
	b.Grow(n)
	_ = c.Each(func(s wire.Span) error { _, err := b.Write(s.Bytes()); return err })
	return b.String()
}
func (p *TypedParser) shortBytes() []byte { return p.Copy(int(p.U16())) }
func (p *TypedParser) Bytes() []byte {
	n := p.I32()
	if n == -1 {
		return nil
	}
	if n < 0 {
		p.Check(fmt.Errorf("gocql: invalid bytes length %d", n))
		return nil
	}
	return p.Copy(n)
}
func (p *TypedParser) Count(minBytes int) int {
	n := p.I32()
	if n < 0 || int64(n) > p.Cursor.Remaining()/int64(minBytes) {
		p.Check(fmt.Errorf("gocql: invalid metadata count %d", n))
		return 0
	}
	return n
}
func (p *TypedParser) Strings() []string {
	n := int(p.U16())
	if int64(n) > p.Cursor.Remaining()/2 {
		p.Check(fmt.Errorf("gocql: invalid string list count"))
		return nil
	}
	p.ReserveAllocation(int64(n) * 16)
	if p.Err != nil {
		return nil
	}
	out := make([]string, n)
	for i := range out {
		out[i] = p.Str()
	}
	return out
}
func (p *TypedParser) dataType(depth int) DataType {
	maxDepth := p.MaxDepth
	if maxDepth == 0 {
		maxDepth = 64
	}
	if depth > maxDepth {
		p.Check(fmt.Errorf("gocql: metadata nesting exceeds %d", maxDepth))
		return DataType{}
	}
	p.ReserveAllocation(160)
	kind := Type(p.U16())
	if p.Err != nil {
		return DataType{}
	}
	var result DataType
	var err error
	switch kind {
	case TypeList, TypeSet:
		element := p.dataType(depth + 1)
		if kind == TypeList {
			result, err = ListType(element)
		} else {
			result, err = SetType(element)
		}
	case TypeMap:
		key := p.dataType(depth + 1)
		element := p.dataType(depth + 1)
		result, err = MapType(key, element)
	case TypeTuple:
		n := int(p.U16())
		if int64(n) > p.Cursor.Remaining()/2 {
			p.Check(fmt.Errorf("gocql: truncated tuple metadata"))
			return DataType{}
		}
		p.ReserveAllocation(int64(n) * 8)
		if p.Err != nil {
			return DataType{}
		}
		fields := make([]DataType, n)
		for i := range fields {
			fields[i] = p.dataType(depth + 1)
		}
		result, err = TupleType(fields...)
	case TypeUDT:
		keyspace, name := p.Str(), p.Str()
		n := int(p.U16())
		if int64(n) > p.Cursor.Remaining()/4 {
			p.Check(fmt.Errorf("gocql: truncated UDT metadata"))
			return DataType{}
		}
		p.ReserveAllocation(int64(n) * 96)
		if p.Err != nil {
			return DataType{}
		}
		fields := make([]ColumnSpec, n)
		for i := range fields {
			fields[i] = ColumnSpec{Name: p.Str(), Type: p.dataType(depth + 1)}
		}
		result, err = UDTType(keyspace, name, fields...)
	case TypeCustom:
		name := p.Str()
		if scalar := GetApacheCassandraType(name); scalar != TypeCustom {
			result, err = ScalarType(scalar)
			break
		}
		prefix := ApacheCassandraTypePrefix + "VectorType("
		if strings.HasPrefix(name, prefix) && strings.HasSuffix(name, ")") {
			args := name[len(prefix) : len(name)-1]
			split := strings.LastIndex(args, ",")
			if split < 0 {
				err = fmt.Errorf("gocql: malformed vector type %q", name)
				break
			}
			dim, parseErr := strconv.ParseUint(strings.TrimSpace(args[split+1:]), 10, 16)
			if parseErr != nil || dim == 0 {
				err = fmt.Errorf("gocql: malformed vector dimensions %q", name)
				break
			}
			// This legacy adapter translates schema descriptions only, never values.
			element, parseErr := DataTypeFromInfo(GetCassandraLongType(strings.TrimSpace(args[:split]), p.Proto, logging.Nop()))
			if parseErr != nil {
				err = parseErr
				break
			}
			result, err = VectorDataType(element, uint16(dim))
		} else {
			err = fmt.Errorf("gocql: unsupported custom protocol type %q", name)
		}
	default:
		result, err = ScalarType(kind)
	}
	p.Check(err)
	return result
}

type typedMetadataCursor interface {
	Uint8() (uint8, error)
	Uint16() (uint16, error)
	Uint32() (uint32, error)
	Uint64() (uint64, error)
	Take(int64) (wire.Chain, error)
	Remaining() int64
	RequireEnd() error
}
type ParsedMetadata struct {
	PagingState, MetadataID          []byte
	pkIndexes                        []int
	lwt, NoMetadata, MetadataChanged bool
}

func (p *TypedParser) ReserveAllocation(n int64) {
	if v, ok := p.Cursor.(budgetedMetadataCursor); ok {
		p.Check(v.ReserveMetadata(n))
	}
}
func (p *TypedParser) metadata(page *model.ResponseData, prepared bool, flagLWT int) (Columns, int) {
	m := ParsedMetadata{}
	cols, count := p.MetadataFields(&m, prepared, flagLWT)
	if prepared {
		page.Extra().PkIndexes = m.pkIndexes
		page.Lwt = m.lwt
	} else {
		page.NoMetadata = m.NoMetadata
		page.MetadataChanged = m.MetadataChanged
	}
	if m.PagingState != nil {
		page.PagingState = m.PagingState
	}
	if m.MetadataID != nil {
		page.MetadataID = m.MetadataID
	}
	return cols, count
}
func (p *TypedParser) MetadataFields(page *ParsedMetadata, prepared bool, flagLWT int) (Columns, int) {
	flags := uint32(p.I32())
	allowed := uint32(frm.FlagGlobalTableSpec | frm.FlagHasMorePages | frm.FlagNoMetaData | frm.FlagMetaDataChanged)
	if prepared {
		allowed |= uint32(flagLWT)
	}
	if flags & ^allowed != 0 {
		p.Check(fmt.Errorf("gocql: unknown metadata flags 0x%x", flags))
	}
	if !prepared {
		page.MetadataChanged = flags&uint32(frm.FlagMetaDataChanged) != 0
	}
	count := p.I32()
	if count < 0 {
		p.Check(fmt.Errorf("gocql: negative column count %d", count))
		return Columns{}, 0
	}
	if prepared {
		n := p.Count(2)
		page.pkIndexes = make([]int, n)
		for i := range page.pkIndexes {
			page.pkIndexes[i] = int(p.U16())
			if page.pkIndexes[i] >= count {
				p.Check(fmt.Errorf("gocql: partition key index exceeds column count"))
			}
		}
	}
	if prepared {
		page.lwt = flagLWT != 0 && flags&uint32(flagLWT) != 0
	}
	if flags&uint32(frm.FlagHasMorePages) != 0 {
		page.PagingState = p.Bytes()
		if page.PagingState == nil {
			p.Check(fmt.Errorf("gocql: continuation flag has null paging state"))
		}
	}
	if !prepared && p.MetadataID && flags&uint32(frm.FlagMetaDataChanged) != 0 {
		page.MetadataID = p.shortBytes()
	}
	if flags&uint32(frm.FlagNoMetaData) != 0 {
		if !prepared && flags&uint32(frm.FlagMetaDataChanged) != 0 {
			p.Check(fmt.Errorf("gocql: metadata changed response omitted its column metadata"))
		}
		if !prepared {
			page.NoMetadata = true
		}
		return Columns{}, count
	}
	global := flags&uint32(frm.FlagGlobalTableSpec) != 0
	var checkpoint wire.Cursor
	cursor, buffered := p.Cursor.(*wire.Cursor)
	if buffered && !prepared && p.reuse != nil && p.Err == nil {
		checkpoint = *cursor
		if columns, ok := p.reuse.lookup(cursor, p.expect, count, global, p.MaxDepth); ok {
			return columns, count
		}
	}
	keyspace, table := "", ""
	if global {
		keyspace, table = p.Str(), p.Str()
	}
	if int64(count) > p.Cursor.Remaining()/4 {
		p.Check(fmt.Errorf("gocql: column count %d exceeds remaining metadata", count))
		return Columns{}, 0
	}
	p.ReserveAllocation(int64(count) * 96)
	if p.Err != nil {
		return Columns{}, 0
	}
	columns := make([]ColumnSpec, count)
	for i := range columns {
		if !global {
			keyspace, table = p.Str(), p.Str()
		}
		columns[i] = ColumnSpec{Keyspace: keyspace, Table: table, Name: p.Str(), Type: p.dataType(0)}
	}
	result := cqltype.ColumnsOwned(columns)
	if buffered && !prepared && p.reuse != nil && p.Err == nil {
		n := checkpoint.Remaining() - cursor.Remaining()
		if n > 0 && n <= 64<<10 {
			if encoded, err := checkpoint.Take(n); err == nil {
				return p.reuse.store(encoded, result, p.expect, global, p.MaxDepth), count
			}
		}
	}
	return result, count
}

func (c *Settings) ParseTypedResponseInto(head frm.FrameHeader, body wire.Chain, owner *wire.Lease, page *model.ResponseData) (*model.ResponseData, error) {

	if body.Len() > c.MaxDecompressedBytes {
		return nil, ErrFrameTooBig
	}
	// A caller's page arrives with the result columns it expects.
	expect := page.Columns
	*page = model.ResponseData{Owner: owner, InternalParseCursor: wire.CursorOver(body)}
	p := &TypedParser{expect: expect, reuse: c.MetadataReuse, Cursor: &page.InternalParseCursor, Proto: c.Version.Number, MetadataID: c.Version.ResultMetadataID || c.MetadataID, owner: owner, MaxDepth: c.MaxMetadataDepth}
	if head.Flags&frm.FlagTracing != 0 {
		page.Extra().TraceID = p.Copy(16)
	}
	if head.Flags&frm.FlagWarning != 0 {
		page.Extra().Warnings = p.Strings()
	}
	if head.Flags&frm.FlagCustomPayload != 0 {
		count := int(p.U16())
		if int64(count) > p.Cursor.Remaining()/6 {
			p.Check(fmt.Errorf("gocql: payload count exceeds remaining body"))
		} else {
			payload := make([]model.PayloadEntry, count)
			page.Extra().Payload = payload
			for i := range payload {
				entry := &payload[i]
				entry.Key = p.Str()
				length := p.I32()
				if length == -1 {
					continue
				}
				if length < 0 {
					p.Check(fmt.Errorf("gocql: invalid payload length %d", length))
					break
				}
				value := p.Take(int64(length))
				entry.Value = Nullable[Bytes]{Valid: true, Value: cqltype.BytesView(value, wire.RefOf(owner))}
			}
		}
	}
	if p.Err != nil {
		return nil, p.Err
	}
	switch head.Op {
	case frm.OpError:
		serverErr := p.ServerError(head, c.RateLimitingErrorCode)
		if p.Err != nil {
			return nil, p.Err
		}
		if err := p.Cursor.RequireEnd(); err != nil {
			return nil, err
		}
		return page, serverErr
	case frm.OpResult:
		page.Kind = p.I32()
		switch page.Kind {
		case frm.ResultKindVoid:
		case frm.ResultKindRows:
			page.Columns, page.ColumnCount = p.metadata(page, false, 0)
			page.RowCount = int32(p.I32())
			if page.RowCount < 0 {
				p.Check(fmt.Errorf("gocql: negative row count"))
			}
			if page.ColumnCount > 0 && int64(page.RowCount) > p.Cursor.Remaining()/int64(page.ColumnCount)/4 {
				p.Check(fmt.Errorf("gocql: rows exceed remaining response body"))
			}
			page.Rows = p.Take(p.Cursor.Remaining())
		case frm.ResultKindPrepared:
			page.Extra().PreparedID = p.shortBytes()
			if p.MetadataID {
				page.MetadataID = p.shortBytes()
			}
			page.Extra().BindColumns, _ = p.metadata(page, true, c.FlagLWT)
			page.Columns, page.ColumnCount = p.metadata(page, false, 0)
		case frm.ResultKindKeyspace:
			page.Extra().Keyspace = p.Str()
		case frm.ResultKindSchemaChanged:
			page.Extra().SchemaChange = true
			_ = p.Str()
			target := p.Str()
			_ = p.Str()
			switch target {
			case "KEYSPACE":
			case "TABLE", "TYPE":
				_ = p.Str()
			case "FUNCTION", "AGGREGATE":
				_ = p.Str()
				_ = p.Strings()
			default:
				p.Check(fmt.Errorf("gocql: unknown schema change target %q", target))
			}
		default:
			p.Check(fmt.Errorf("gocql: unknown result kind %d", page.Kind))
		}
	default:
		p.Check(fmt.Errorf("gocql: unexpected typed response opcode %v", head.Op))
	}
	if p.Err != nil {
		return nil, p.Err
	}
	if err := p.Cursor.RequireEnd(); err != nil {
		return nil, err
	}
	return page, nil
}

func (p *TypedParser) errorMap() ErrorMap {
	n := p.Count(4)
	p.ReserveAllocation(int64(n) * 64)
	if p.Err != nil {
		return nil
	}
	out := make(ErrorMap, n)
	for i := 0; i < n; i++ {
		length := int(p.U8())
		if length != 4 && length != 16 {
			p.Check(fmt.Errorf("gocql: invalid error map address size %d", length))
			break
		}
		ip := net.IP(p.Copy(length))
		out[ip.String()] = p.U16()
	}
	return out
}
func (p *TypedParser) ServerError(head frm.FrameHeader, rateLimitCode int) error {
	// The decoder reads through an interface, which would move every parser
	// to the heap; only an error response pays for a heap copy.
	q := new(TypedParser)
	*q = *p
	err := decodeServerError(q, head.Warnings, p.Proto, rateLimitCode)
	*p = *q
	return err
}

// budgetedMetadataCursor accounts all incremental metadata allocations before reading.
type budgetedMetadataCursor interface {
	CopyMetadata(int64) ([]byte, error)
	ReserveMetadata(int64) error
}

func (c *Settings) ParseTypedResponse(head frm.FrameHeader, body wire.Chain, owner *wire.Lease) (*model.ResponseData, error) {
	return c.ParseTypedResponseInto(head, body, owner, new(model.ResponseData))
}

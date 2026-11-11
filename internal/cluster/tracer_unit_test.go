package cluster

import (
	"bytes"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/protocol"
)

var (
	traceSessionDurationCols = []rowsCol{{"duration", optNative(cqltype.TypeInt)}}
	traceSessionCols         = []rowsCol{{"coordinator", optNative(cqltype.TypeInet)}, {"duration", optNative(cqltype.TypeInt)}}
	traceEventCols           = []rowsCol{
		{"event_id", optNative(cqltype.TypeTimeUUID)},
		{"activity", optNative(cqltype.TypeVarchar)},
		{"source", optNative(cqltype.TypeInet)},
		{"source_elapsed", optNative(cqltype.TypeInt)},
		{"thread", optNative(cqltype.TypeVarchar)},
	}
)

type traceEvent struct {
	at       time.Time
	activity string
	source   net.IP
	elapsed  int32
	thread   string
}

// traceServer answers the system_traces queries the tracers make, as
// described by a traceConfig, and counts the duration lookups.
type traceServer struct {
	*controlTestServer
	durationLookups atomic.Int32
}

// traceConfig describes one traced session. readyAfter is how many duration
// lookups report it unfinished (duration 0) before reporting duration; fail
// makes every trace query fail.
type traceConfig struct {
	readyAfter  int32
	duration    int32
	coordinator net.IP
	events      []traceEvent
	fail        bool
}

func newTraceServer(t *testing.T, cfg traceConfig) *traceServer {
	t.Helper()
	ts := &traceServer{}
	ts.controlTestServer = newControlTestServer(t, nil)

	// The trace queries bind the session id, so the control connection
	// prepares them: answer PREPARE with one of these ids and EXECUTE with
	// the matching rows.
	const (
		durationID = 40 + iota
		coordinatorID
		eventsID
	)
	ts.setPrepareHook(func(query string, stream int, resp *protocol.Framer, writeID func(uint64)) bool {
		var id uint64
		switch {
		case strings.Contains(query, "system_traces.events"):
			id = eventsID
		case strings.Contains(query, "system_traces.sessions") && strings.Contains(query, "coordinator"):
			id = coordinatorID
		case strings.Contains(query, "system_traces.sessions"):
			id = durationID
		default:
			return false
		}
		testPrepared{
			id:       id,
			keyspace: "system_traces", table: "sessions",
			cols:      []testPreparedCol{{name: "session_id", typ: cqltype.TypeUUID}},
			pkIndexes: []uint16{0},
		}.write(resp, stream, writeID)
		return true
	})
	respond := func(id uint64, stream int, resp *protocol.Framer) bool {
		if id < durationID || id > eventsID {
			return false
		}
		if cfg.fail {
			writeErrorResult(resp, stream, protocol.CodeServer, "traces unavailable")
			return true
		}
		switch id {
		case eventsID:
			var rows [][][]byte
			for _, e := range cfg.events {
				u := cqltype.TimeUUIDFromTime(e.at)
				rows = append(rows, [][]byte{u[:], []byte(e.activity), e.source.To4(), int32Bytes(e.elapsed), []byte(e.thread)})
			}
			writeRows(resp, stream, traceEventCols, rows)
		case coordinatorID:
			writeRows(resp, stream, traceSessionCols, [][][]byte{{cfg.coordinator.To4(), int32Bytes(cfg.duration)}})
		case durationID:
			d := int32(0)
			if ts.durationLookups.Add(1) > cfg.readyAfter {
				d = cfg.duration
			}
			writeRows(resp, stream, traceSessionDurationCols, [][][]byte{{int32Bytes(d)}})
		}
		return true
	}
	ts.setExecuteHook(respond)
	ts.controlTestServer.queries = func(query string, stream int, resp *protocol.Framer) bool {
		switch {
		case strings.Contains(query, "system_traces.events"):
			return respond(eventsID, stream, resp)
		case strings.Contains(query, "system_traces.sessions") && strings.Contains(query, "coordinator"):
			return respond(coordinatorID, stream, resp)
		case strings.Contains(query, "system_traces.sessions"):
			return respond(durationID, stream, resp)
		default:
			return false
		}
	}

	return ts
}

func (ts *traceServer) session(t *testing.T) *Core {
	t.Helper()
	s, err := ts.cluster().Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

var (
	traceID     = []byte{0xde, 0xad, 0xbe, 0xef, 0, 0, 0x10, 0, 0x80, 0, 0, 0, 0, 0, 0, 1}
	traceStart  = time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	traceEvents = []traceEvent{
		{traceStart, "Parsing statement", net.IPv4(10, 0, 0, 1), 15, "Native-Transport-Requests-1"},
		{traceStart.Add(time.Millisecond), "Request complete", net.IPv4(10, 0, 0, 2), 420, "shard 3"},
	}
)

// syncBuffer lets a test read what TraceWriter wrote.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

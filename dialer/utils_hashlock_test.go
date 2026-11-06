package dialer

import (
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/murmur"
)

// This file pins GetFrameHash's output for a spread of real frame shapes, as a
// blast-radius detector for changes to the offset math.
//
// It is deliberately not a claim that these numbers may never change. The
// recordings under tests/bench/e2e store raw frames and are rehashed at load time
// (loadResponseFramesFromFiles) with the same function applied to the live request,
// so the hash function itself can change freely. What a recording actually depends
// on is narrower, and neither half is visible in a single hash value:
//
//   - the hash must not cover bytes that differ between record time and replay
//     time — the default timestamp is time.Now() at send, and the STARTUP option
//     map grows as the driver gains options — or a recording stops matching itself;
//   - the hash must cover enough of a request to tell it apart from the other
//     requests in the same recording, or the replayer's first-match scan serves the
//     wrong response.
//
// Those two properties are asserted directly elsewhere in this package. What this
// table adds is that any edit to the walk has to state which frame shapes it moved:
// a diff here is the change describing itself, which is how the QUERY collision in
// scylladb/gocql#1000 was found.
//
// So a moved value is not automatically a bug — but it is never incidental, and an
// unexplained one is.

// murmur3OfStreamBlanked is the raw-bytes fallback the arms of the opcode switch
// produce. They run after the stream id has been blanked and before the deferred
// restore, so the bytes they hash are the frame with 0x30 0x30 at offsets 2 and 3 —
// not the frame as the caller passed it, which is what the two guards ahead of the
// switch hash. The two are not interchangeable.
func murmur3OfStreamBlanked(frame []byte) int64 {
	normalised := append([]byte(nil), frame...)
	normalised[2], normalised[3] = byte('0'), byte('0')
	return murmur.Murmur3H1(normalised)
}

// longString encodes a CQL [long string]: a 4-byte length followed by the bytes.
func longString(s string) []byte {
	return append([]byte{byte(len(s) >> 24), byte(len(s) >> 16), byte(len(s) >> 8), byte(len(s))}, s...)
}

// shortString encodes a CQL [string]: a 2-byte length followed by the bytes.
func shortString(s string) []byte {
	return append([]byte{byte(len(s) >> 8), byte(len(s))}, s...)
}

// bytesValue encodes a CQL [bytes]: a 4-byte signed length followed by the bytes.
func bytesValue(b []byte) []byte {
	return append([]byte{byte(len(b) >> 24), byte(len(b) >> 16), byte(len(b) >> 8), byte(len(b))}, b...)
}

// queryBody assembles a QUERY body: the query as a [long string], then the query
// parameters — consistency, a one-byte flags field (v4 and below), and whatever
// optional fields those flags announce.
func queryBody(query string, flags byte, optional ...[]byte) []byte {
	body := longString(query)
	body = append(body, 0x00, 0x01) // consistency ONE
	body = append(body, flags)
	for _, o := range optional {
		body = append(body, o...)
	}
	return body
}

// batchBody assembles a minimal BATCH body: type, one query count, one simple
// query with no values, consistency, and a flags byte plus whatever it announces.
func batchBody(flags byte, optional ...[]byte) []byte {
	body := []byte{0x00}            // type: LOGGED
	body = append(body, 0x00, 0x01) // one query
	body = append(body, 0x00)       // kind: query string
	body = append(body, longString("INSERT INTO t (pk) VALUES (1)")...)
	body = append(body, 0x00, 0x00) // zero values
	body = append(body, 0x00, 0x01) // consistency ONE
	body = append(body, flags)
	for _, o := range optional {
		body = append(body, o...)
	}
	return body
}

type hashLockCase struct {
	name          string
	frame         []byte
	useMetadataID bool
}

// hashLockCases is the frame spread the lock covers. Every arm of the
// GetFrameHash opcode switch is represented, along with the query-parameter
// flags that move the extracted range and the truncation paths that fall back to
// hashing raw bytes.
func hashLockCases() []hashLockCase {
	// customPayload prefixes a body with the [bytes map] a CUSTOM_PAYLOAD frame opens
	// with. The map sits between the header and the first body field, so every arm
	// that walks a body has to step over it -- and all three are pinned below,
	// because the range this commit moved starts before it.
	customPayload := func(body []byte) []byte {
		out := []byte{0x00, 0x01} // one entry
		out = append(out, shortString("k")...)
		out = append(out, bytesValue([]byte{0xAB, 0xCD})...)
		return append(out, body...)
	}

	return []hashLockCase{
		{name: "options", frame: frameV4(opOptions, 0x00, nil)},
		{name: "startup-short", frame: startupFrameV4([]byte{0x00, 0x01})},
		{name: "startup-long", frame: startupFrameV4(make([]byte, 512))},
		{name: "prepare", frame: frameV4(opPrepare, 0x00, longString("SELECT v FROM t WHERE pk = ?"))},
		{name: "register", frame: frameV4(opRegister, 0x00, append([]byte{0x00, 0x01}, shortString("TOPOLOGY_CHANGE")...))},
		{name: "auth-response", frame: frameV4(opAuthResponse, 0x00, bytesValue([]byte{0xDE, 0xAD, 0xBE, 0xEF}))},

		{name: "query-bare", frame: frameV4(opQuery, 0x00, queryBody("SELECT * FROM system.local", 0x00))},
		{name: "query-values", frame: frameV4(opQuery, 0x00, queryBody("SELECT v FROM t WHERE pk = ?", 0x01,
			[]byte{0x00, 0x01}, bytesValue([]byte{0x00, 0x00, 0x00, 0x2A})))},
		{name: "query-null-value", frame: frameV4(opQuery, 0x00, queryBody("SELECT v FROM t WHERE pk = ?", 0x01,
			[]byte{0x00, 0x01}, []byte{0xFF, 0xFF, 0xFF, 0xFF}))},
		{name: "query-page-size", frame: frameV4(opQuery, 0x00, queryBody("SELECT * FROM t", 0x04,
			[]byte{0x00, 0x00, 0x13, 0x88}))},
		{name: "query-paging-state", frame: frameV4(opQuery, 0x00, queryBody("SELECT * FROM t", 0x08,
			bytesValue([]byte{0x01, 0x02, 0x03})))},
		{name: "query-serial-consistency", frame: frameV4(opQuery, 0x00, queryBody("SELECT * FROM t", 0x10,
			[]byte{0x00, 0x09}))},
		{name: "query-default-timestamp", frame: frameV4(opQuery, 0x00, queryBody("SELECT * FROM t", 0x20,
			[]byte{0x00, 0x06, 0x26, 0x52, 0xBF, 0xD6, 0xE5, 0x3F}))},
		{name: "query-values-page-size-serial", frame: frameV4(opQuery, 0x00, queryBody("SELECT v FROM t WHERE pk = ?", 0x01|0x04|0x10,
			[]byte{0x00, 0x01}, bytesValue([]byte{0x2A}), []byte{0x00, 0x00, 0x13, 0x88}, []byte{0x00, 0x09}))},
		{name: "query-named-values", frame: frameV4(opQuery, 0x00, queryBody("SELECT v FROM t WHERE pk = :pk", 0x01|0x40,
			[]byte{0x00, 0x01}, shortString("pk"), bytesValue([]byte{0x2A})))},
		{name: "query-custom-payload", frame: frameV4(opQuery, frm.FlagCustomPayload,
			customPayload(queryBody("SELECT * FROM system.local", 0x00)))},
		{name: "query-truncated-text-length", frame: frameV4(opQuery, 0x00, []byte{0x00, 0x00, 0x00})},
		{name: "query-overstated-text-length", frame: frameV4(opQuery, 0x00,
			append([]byte{0x7F, 0xFF, 0xFF, 0xFF}, "SELECT * FROM t"...))},

		{name: "v5-query", frame: v5QueryFrame("SELECT * FROM t", 0)},
		{name: "v5-query-values", frame: v5QueryFrame("SELECT v FROM t WHERE pk = ?", frm.FlagValues,
			[]byte{0x00, 0x01}, bytesValue([]byte{0x2A}))},
		{name: "v5-query-timestamp-and-keyspace", frame: v5QueryFrame("SELECT * FROM t",
			frm.FlagDefaultTimestamp|frm.FlagWithKeyspace, timestamp8(111), shortString("ks"))},

		{name: "execute-no-metadata-id", frame: v4ExecuteFrame(false)},
		{name: "execute-metadata-id", frame: v4ExecuteFrame(true), useMetadataID: true},
		{name: "execute-metadata-id-flag-off", frame: v4ExecuteFrame(true)},
		{name: "execute-custom-payload", frame: frameV4(opExecute, frm.FlagCustomPayload,
			customPayload(v4ExecuteFrame(false)[FrameHeaderLen:]))},

		{name: "batch", frame: frameV4(opBatch, 0x00, batchBody(0x00))},
		{name: "batch-custom-payload", frame: frameV4(opBatch, frm.FlagCustomPayload,
			customPayload(batchBody(0x00)))},
		{name: "batch-default-timestamp", frame: frameV4(opBatch, 0x00, batchBody(0x20,
			[]byte{0x00, 0x06, 0x26, 0x52, 0xBF, 0xD6, 0xE5, 0x3F}))},

		{name: "truncated-execute-prepared-id-len", frame: v4ExecuteFrame(true)[:10], useMetadataID: true},
		{name: "truncated-execute-metadata-id-len", frame: v4ExecuteFrame(true)[:15], useMetadataID: true},
		{name: "short-header", frame: frameV4(opQuery, 0x00, nil)[:5]},
		{name: "empty", frame: nil},
		{name: "unknown-opcode", frame: frameV4(frameOp(0x7F), 0x00, []byte{0x01, 0x02, 0x03})},
	}
}

// hashLockWant pins the hash of every case above.
//
// Every v4 value was captured against the implementation as it stood before protocol
// v5 query-parameter support was added, and is unchanged by it: the v5 tail walk is
// gated on v5, so nothing below v5 could reach it. The v5 cases at the bottom of the
// map are the ones that support added.
//
// They are also unchanged by the package settling on a protocol v3 floor. Two v2 cases
// used to sit here pinning the older header layout; deleting the v1/v2 walks moved
// those two and nothing else, which is what says the fixed offsets land where the
// derived ones did.
var hashLockWant = map[string]int64{
	"auth-response":                     -5612286604398787175,
	"empty":                             0,
	"execute-metadata-id":               8953623736212654883,
	"execute-metadata-id-flag-off":      -8761464023806847249,
	"execute-custom-payload":            301853343717256017,
	"execute-no-metadata-id":            -7853075121273079524,
	"options":                           359591853454385582,
	"prepare":                           7009575819196046835,
	"register":                          2208904296610557021,
	"short-header":                      -4915946318166194245,
	"startup-long":                      -4554951338151526776,
	"startup-short":                     -4554951338151526776,
	"truncated-execute-metadata-id-len": 1641695519491433123,
	"truncated-execute-prepared-id-len": -2990148397469877668,
	"unknown-opcode":                    -8681368666721584811,

	// Both BATCH values moved when the arm stopped hashing the frame whole and
	// started walking the body to the end of the parameter block. That is the point
	// of the change: hashing it whole covered the default timestamp, which
	// writeBatchFrame fills with time.Now(), so a recorded BATCH never matched its
	// own replay. No recording had to be regenerated for it — the checked-in
	// fixtures contain no BATCH frame.
	"batch":                   -8625060010961602230,
	"batch-custom-payload":    -134920547368094710,
	"batch-default-timestamp": -8987577148391256339,

	// Every QUERY value here moved when scylladb/gocql#1000 was fixed. Before it, all
	// ten were Murmur3H1({0x00, 0x00, 0x00}) = 8779008611884021576: the parameter
	// walk was handed the body start rather than the position past the query text, so
	// it hashed the top three bytes of the text's length — zero for any query under
	// 16 MiB — and stopped. The checked-in recordings were regenerated in the same
	// change, because their control-connection query text had been stale since
	// b6a9682 and only the collision hid it.
	//
	// query-custom-payload carries query-bare's body behind a [bytes map] and differs
	// in nothing else. The two used to hash alike, because the range began past the
	// payload — and the replayer serves the first hash it matches, so one of them got
	// the other's response. Query.CustomPayload is public, so both are requests a
	// caller can send. The range then started at the body.
	//
	// execute-custom-payload and batch-custom-payload are here because the same range
	// moved in those two arms as well, and without a payload-bearing case each, either
	// could be reverted with every pinned value in this table still matching.
	//
	// All three then moved once more, when the header flags were folded into the hash
	// to cover tracing. They are the only cases in this table with a nonzero flags
	// byte, and the fold is skipped when that byte is zero, so they are also the only
	// values that moved: every other value here is byte-identical across it, which is
	// what let the checked-in recordings stand.
	//
	// The three moved a fourth time when the range stopped covering the payload's raw
	// bytes and started folding in a canonical hash of its entries instead (see
	// addCustomPayload): writeBytesMap (frame.go) ranges over a Go map to encode the
	// payload, so those raw bytes land in a different order on every call once a
	// payload holds two or more entries, and hashing them directly made a recorded
	// request and its own replay hash differently. The three payload-bearing cases here
	// still carry only one entry, so this move changed their values without changing
	// what they cover; TestFrameHash/CustomPayload/OrderIndependent is what actually
	// exercises two or more.
	"query-bare":                    669594534933358966,
	"query-custom-payload":          6969718323714752801,
	"query-default-timestamp":       -3958206948791604622,
	"query-named-values":            1354821534699525939,
	"query-null-value":              -7046978697175198898,
	"query-page-size":               7395467639336959025,
	"query-paging-state":            -7793657441845994096,
	"query-serial-consistency":      -1672825478938872525,
	"query-values":                  -2567259529754147462,
	"query-values-page-size-serial": -6333038150028030013,

	// A QUERY whose [long string] cannot be walked — its length field truncated, or
	// announcing more bytes than the frame holds — is a damaged recording, so it falls
	// back to the raw bytes with the stream id blanked, as the arm's other bounds
	// failures do. Before #1000 neither reached a bound: the walk never looked at the
	// length as a length.
	"query-overstated-text-length": 7220702958000182124,
	"query-truncated-text-length":  -9031940561743828789,

	// The protocol v5 QUERY cases. Their whole point is that they parse at all: the
	// misaligned walk read the v5 flags field across the text's length and the first
	// two characters of the text, failed a bound, and fell back to hashing the frame
	// whole — default timestamp included — so a v5 QUERY could never match its own
	// replay. That was the v5 half of scylladb/gocql#1000.
	"v5-query":                        -1459322207164818650,
	"v5-query-timestamp-and-keyspace": 666526662632016432,
	"v5-query-values":                 7314799553312446526,
}

// frameV5 builds a protocol v5 request frame. The header layout is v3+'s, so only
// the version byte differs from frameV4 — what changes on v5 is the body: the query
// flags become a 4-byte field and EXECUTE always carries a resultMetadataID.
func frameV5(op frameOp, headerFlags byte, body []byte) []byte {
	frame := []byte{
		0x05,
		headerFlags,
		0x00, 0x7B, // stream id
		byte(op),
		byte(len(body) >> 24), byte(len(body) >> 16), byte(len(body) >> 8), byte(len(body)),
	}
	return append(frame, body...)
}

// v5ExecuteFrame builds a protocol v5 EXECUTE frame whose query parameters carry
// exactly the fields flags announces, in the order writeQueryParams emits them.
func v5ExecuteFrame(flags uint32, optional ...[]byte) []byte {
	body := []byte{0x00, 0x03, 0xAA, 0xBB, 0xCC} // preparedID
	body = append(body, 0x00, 0x02, 0xDE, 0xAD)  // resultMetadataID, always present on v5
	body = append(body, 0x00, 0x01)              // consistency ONE
	body = append(body, byte(flags>>24), byte(flags>>16), byte(flags>>8), byte(flags))
	for _, o := range optional {
		body = append(body, o...)
	}
	return frameV5(opExecute, 0x00, body)
}

// v5QueryFrame builds a protocol v5 QUERY frame whose query parameters carry exactly
// the fields flags announces, in the order writeQueryParams emits them. The flags
// field is four bytes wide on v5, which is what the misaligned walk used to trip on.
func v5QueryFrame(query string, flags uint32, optional ...[]byte) []byte {
	body := longString(query)
	body = append(body, 0x00, 0x01) // consistency ONE
	body = append(body, byte(flags>>24), byte(flags>>16), byte(flags>>8), byte(flags))
	for _, o := range optional {
		body = append(body, o...)
	}
	return frameV5(opQuery, 0x00, body)
}

// timestamp8 is an 8-byte default-timestamp field.
func timestamp8(v int64) []byte {
	return []byte{byte(v >> 56), byte(v >> 48), byte(v >> 40), byte(v >> 32),
		byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}

// nowInSeconds4 is a 4-byte now_in_seconds field.
func nowInSeconds4(v int32) []byte {
	return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}

// batchBodyV5 assembles a BATCH body with a 4-byte flags field, as protocol v5
// writes it, carrying exactly the fields flags announces.
func batchBodyV5(statement string, flags uint32, optional ...[]byte) []byte {
	body := []byte{0x00}            // type: LOGGED
	body = append(body, 0x00, 0x01) // one query
	body = append(body, 0x00)       // kind: query string
	body = append(body, longString(statement)...)
	body = append(body, 0x00, 0x00) // zero values
	body = append(body, 0x00, 0x01) // consistency ONE
	body = append(body, byte(flags>>24), byte(flags>>16), byte(flags>>8), byte(flags))
	for _, o := range optional {
		body = append(body, o...)
	}
	return body
}

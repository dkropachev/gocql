package protocol

import (
	"net"
	"strings"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
)

// errBody builds an ERROR frame body: <code><message> followed by whatever the
// code adds.
func errBody(proto byte, code ErrorCode, msg string, rest func(f *Framer)) []byte {
	f := NewFramer(nil, proto)
	f.WriteInt(int32(code))
	f.WriteString(msg)
	if rest != nil {
		rest(f)
	}
	return f.Buf
}

// writeInetOnly writes an [inetaddr]: a one-byte length and the address.
func writeInetOnly(f *Framer, ip net.IP) {
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	f.WriteByteValue(byte(len(ip)))
	f.Buf = append(f.Buf, ip...)
}

type errMapEntry struct {
	ip   string
	code uint16
}

func writeErrMap(f *Framer, entries ...errMapEntry) {
	f.WriteInt(int32(len(entries)))
	for _, e := range entries {
		writeInetOnly(f, net.ParseIP(e.ip))
		f.WriteShort(e.code)
	}
}

func parseErrorBody(proto byte, body []byte, rateLimitCode int) (Frame, error) {
	f := NewFramer(nil, proto)
	f.RateLimitingErrorCode = rateLimitCode
	f.Header = &frm.FrameHeader{Version: frm.ProtoVersion(proto) | ProtoDirectionMask, Op: frm.OpError, Stream: 7}
	f.Buf = body
	return f.ParseFrame()
}

const scyllaRateLimitCode = 0xF000

type errorFrameCase struct {
	name  string
	proto byte
	body  []byte
	want  Frame
	// rateLimitCode is the code the connection negotiated for Scylla's rate
	// limit error, or 0 when the extension is off.
	rateLimitCode int
	// unread is how many trailing bytes of body the parser is expected to
	// ignore; truncating only those is not malformed.
	unread int
}

func errorFrameCases() []errorFrameCase {
	hdr := func(proto byte) frm.FrameHeader {
		return frm.FrameHeader{Version: frm.ProtoVersion(proto) | ProtoDirectionMask, Op: frm.OpError, Stream: 7}
	}
	base := func(msg string) serverError { return serverError{message: msg} }
	response := func(proto byte, err ServerError) Frame {
		return &ErrorResponseFrame{Err: err, FrameHeader: hdr(proto)}
	}

	var cases []errorFrameCase

	for _, code := range []ErrorCode{
		CodeServer, CodeProtocol, CodeBadCredentials, CodeOverloaded, CodeBootstrapping,
		CodeTruncate, CodeSyntax, CodeUnauthorized, CodeInvalid, CodeConfig,
	} {
		cases = append(cases, errorFrameCase{
			name:  "Plain" + strings.ToUpper(errCodeName(code)),
			proto: ProtoVersion4,
			body:  errBody(ProtoVersion4, code, "boom", nil),
			want:  response(ProtoVersion4, &OtherServerError{serverError: base("boom"), code: code}),
		})
	}

	cases = append(cases,
		errorFrameCase{
			name: "Unavailable", proto: ProtoVersion4,
			body: errBody(ProtoVersion4, CodeUnavailable, "unavail", func(f *Framer) {
				f.WriteConsistency(cqltype.LocalQuorum)
				f.WriteInt(3)
				f.WriteInt(1)
			}),
			want: response(ProtoVersion4, &UnavailableError{serverError: base("unavail"),
				Consistency: cqltype.LocalQuorum, Required: 3, Alive: 1}),
		},
		errorFrameCase{
			name: "WriteTimeout", proto: ProtoVersion4,
			body: errBody(ProtoVersion4, CodeWriteTimeout, "wt", func(f *Framer) {
				f.WriteConsistency(cqltype.Quorum)
				f.WriteInt(1)
				f.WriteInt(2)
				f.WriteString("BATCH_LOG")
			}),
			want: response(ProtoVersion4, &WriteTimeoutError{serverError: base("wt"),
				Consistency: cqltype.Quorum, Received: 1, BlockFor: 2, WriteType: "BATCH_LOG"}),
		},
		errorFrameCase{
			name: "ReadTimeout", proto: ProtoVersion4,
			body: errBody(ProtoVersion4, CodeReadTimeout, "rt", func(f *Framer) {
				f.WriteConsistency(cqltype.One)
				f.WriteInt(0)
				f.WriteInt(1)
				f.WriteByteValue(1)
			}),
			want: response(ProtoVersion4, &ReadTimeoutError{serverError: base("rt"),
				Consistency: cqltype.One, Received: 0, BlockFor: 1, DataPresent: true}),
		},
		errorFrameCase{
			name: "AlreadyExists", proto: ProtoVersion4,
			body: errBody(ProtoVersion4, CodeAlreadyExists, "exists", func(f *Framer) {
				f.WriteString("ks")
				f.WriteString("tbl")
			}),
			want: response(ProtoVersion4, &AlreadyExistsError{serverError: base("exists"),
				Keyspace: "ks", Table: "tbl"}),
		},
		errorFrameCase{
			name: "Unprepared", proto: ProtoVersion4,
			body: errBody(ProtoVersion4, CodeUnprepared, "unprep", func(f *Framer) {
				f.WriteShortBytes([]byte{0xca, 0xfe})
			}),
			want: response(ProtoVersion4, &UnpreparedError{serverError: base("unprep"),
				StatementID: []byte{0xca, 0xfe}}),
		},
		// v4 carries only a failure count; v5 replaces it with a per-replica
		// reason map and derives the count from it.
		errorFrameCase{
			name: "ReadFailureV4", proto: ProtoVersion4,
			body: errBody(ProtoVersion4, CodeReadFailure, "rf", func(f *Framer) {
				f.WriteConsistency(cqltype.All)
				f.WriteInt(1)
				f.WriteInt(3)
				f.WriteInt(2)
				f.WriteByteValue(0)
			}),
			want: response(ProtoVersion4, &ReadFailureError{serverError: base("rf"),
				Consistency: cqltype.All, Received: 1, BlockFor: 3, NumFailures: 2, DataPresent: false}),
		},
		errorFrameCase{
			name: "ReadFailureV5", proto: ProtoVersion5,
			body: errBody(ProtoVersion5, CodeReadFailure, "rf", func(f *Framer) {
				f.WriteConsistency(cqltype.All)
				f.WriteInt(1)
				f.WriteInt(3)
				writeErrMap(f, errMapEntry{"10.0.0.1", 1}, errMapEntry{"2001:db8::1", 2})
				f.WriteByteValue(1)
			}),
			want: response(ProtoVersion5, &ReadFailureError{serverError: base("rf"),
				Consistency: cqltype.All, Received: 1, BlockFor: 3, NumFailures: 2, DataPresent: true,
				ErrorMap: ErrorMap{"10.0.0.1": 1, "2001:db8::1": 2}}),
		},
		errorFrameCase{
			name: "WriteFailureV4", proto: ProtoVersion4,
			body: errBody(ProtoVersion4, CodeWriteFailure, "wf", func(f *Framer) {
				f.WriteConsistency(cqltype.Two)
				f.WriteInt(0)
				f.WriteInt(2)
				f.WriteInt(1)
				f.WriteString("SIMPLE")
			}),
			want: response(ProtoVersion4, &WriteFailureError{serverError: base("wf"),
				Consistency: cqltype.Two, Received: 0, BlockFor: 2, NumFailures: 1, WriteType: "SIMPLE"}),
		},
		errorFrameCase{
			name: "WriteFailureV5", proto: ProtoVersion5,
			body: errBody(ProtoVersion5, CodeWriteFailure, "wf", func(f *Framer) {
				f.WriteConsistency(cqltype.Two)
				f.WriteInt(0)
				f.WriteInt(2)
				writeErrMap(f, errMapEntry{"10.0.0.2", 3})
				f.WriteString("SIMPLE")
			}),
			want: response(ProtoVersion5, &WriteFailureError{serverError: base("wf"),
				Consistency: cqltype.Two, Received: 0, BlockFor: 2, NumFailures: 1, WriteType: "SIMPLE",
				ErrorMap: ErrorMap{"10.0.0.2": 3}}),
		},
		errorFrameCase{
			name: "WriteFailureV5EmptyMap", proto: ProtoVersion5,
			body: errBody(ProtoVersion5, CodeWriteFailure, "wf", func(f *Framer) {
				f.WriteConsistency(cqltype.Two)
				f.WriteInt(0)
				f.WriteInt(2)
				writeErrMap(f)
				f.WriteString("SIMPLE")
			}),
			want: response(ProtoVersion5, &WriteFailureError{serverError: base("wf"),
				Consistency: cqltype.Two, BlockFor: 2, WriteType: "SIMPLE", ErrorMap: ErrorMap{}}),
		},
		errorFrameCase{
			name: "FunctionFailure", proto: ProtoVersion4,
			body: errBody(ProtoVersion4, CodeFunctionFailure, "ff", func(f *Framer) {
				f.WriteString("ks")
				f.WriteString("fn")
				f.WriteStringList([]string{"int", "text"})
			}),
			want: response(ProtoVersion4, &FunctionFailureError{serverError: base("ff"),
				Keyspace: "ks", Function: "fn", ArgTypes: []string{"int", "text"}}),
		},
		errorFrameCase{
			name: "CDCWriteFailure", proto: ProtoVersion4,
			body: errBody(ProtoVersion4, CodeCDCWriteFailure, "cdc", nil),
			want: response(ProtoVersion4, &CDCWriteFailureError{serverError: base("cdc")}),
		},
		errorFrameCase{
			name: "CASWriteUnknown", proto: ProtoVersion4,
			body: errBody(ProtoVersion4, CodeCASWriteUnknown, "cas", func(f *Framer) {
				f.WriteConsistency(cqltype.Serial)
				f.WriteInt(1)
				f.WriteInt(2)
			}),
			want: response(ProtoVersion4, &CASWriteUnknownError{serverError: base("cas"),
				Consistency: cqltype.Serial, Received: 1, BlockFor: 2}),
		},
		errorFrameCase{
			name: "RateLimitReached", proto: ProtoVersion4, rateLimitCode: scyllaRateLimitCode,
			body: errBody(ProtoVersion4, scyllaRateLimitCode, "slow down", func(f *Framer) {
				f.WriteByteValue(byte(OpTypeWrite))
				f.WriteByteValue(1)
			}),
			want: response(ProtoVersion4, &RateLimitReachedError{serverError: base("slow down"),
				OpType: OpTypeWrite, RejectedByCoordinator: true}),
		},
		// Without the extension the same code is just unknown, and its extra
		// bytes are not read.
		errorFrameCase{
			name: "RateLimitCodeWithoutExtension", proto: ProtoVersion4, unread: 2,
			body: errBody(ProtoVersion4, scyllaRateLimitCode, "slow down", func(f *Framer) {
				f.WriteByteValue(byte(OpTypeWrite))
				f.WriteByteValue(1)
			}),
			want: response(ProtoVersion4, &OtherServerError{serverError: base("slow down"), code: scyllaRateLimitCode}),
		},
		errorFrameCase{
			name: "Unknown", proto: ProtoVersion4,
			body: errBody(ProtoVersion4, 0x7777, "what", nil),
			want: response(ProtoVersion4, &OtherServerError{serverError: base("what"), code: 0x7777}),
		},
	)
	return cases
}

func errCodeName(code ErrorCode) string {
	switch code {
	case CodeServer:
		return "server"
	case CodeProtocol:
		return "protocol"
	case CodeBadCredentials:
		return "credentials"
	case CodeOverloaded:
		return "overloaded"
	case CodeBootstrapping:
		return "bootstrapping"
	case CodeTruncate:
		return "truncate"
	case CodeSyntax:
		return "syntax"
	case CodeUnauthorized:
		return "unauthorized"
	case CodeInvalid:
		return "invalid"
	case CodeConfig:
		return "config"
	}
	return "other"
}

// messageOf reads the server message off a parsed ERROR response.
func messageOf(f Frame) string {
	return f.(*ErrorResponseFrame).Err.Message()
}

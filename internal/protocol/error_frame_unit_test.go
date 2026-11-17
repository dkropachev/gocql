package protocol

import (
	"net"
	"strings"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

// errBody builds an ERROR frame body: <code><message> followed by whatever the
// code adds.
func errBody(proto byte, code cqlerr.ErrorCode, msg string, rest func(f *Framer)) []byte {
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
	response := func(proto byte, err cqlerr.ServerError) Frame {
		return &ErrorResponseFrame{Err: err, FrameHeader: hdr(proto)}
	}

	var cases []errorFrameCase

	for _, code := range []cqlerr.ErrorCode{
		cqlerr.CodeServer, cqlerr.CodeProtocol, cqlerr.CodeBadCredentials, cqlerr.CodeOverloaded, cqlerr.CodeBootstrapping,
		cqlerr.CodeTruncate, cqlerr.CodeSyntax, cqlerr.CodeUnauthorized, cqlerr.CodeInvalid, cqlerr.CodeConfig,
	} {
		cases = append(cases, errorFrameCase{
			name:  "Plain" + strings.ToUpper(errCodeName(code)),
			proto: ProtoVersion4,
			body:  errBody(ProtoVersion4, code, "boom", nil),
			want:  response(ProtoVersion4, cqlerr.NewOtherServerError(code, "boom")),
		})
	}

	cases = append(cases,
		errorFrameCase{
			name: "Unavailable", proto: ProtoVersion4,
			body: errBody(ProtoVersion4, cqlerr.CodeUnavailable, "unavail", func(f *Framer) {
				f.WriteConsistency(cql.LocalQuorum)
				f.WriteInt(3)
				f.WriteInt(1)
			}),
			want: response(ProtoVersion4, runtimeerrors.WithMessage(&cqlerr.UnavailableError{Consistency: cql.LocalQuorum, Required: 3, Alive: 1}, "unavail")),
		},
		errorFrameCase{
			name: "WriteTimeout", proto: ProtoVersion4,
			body: errBody(ProtoVersion4, cqlerr.CodeWriteTimeout, "wt", func(f *Framer) {
				f.WriteConsistency(cql.Quorum)
				f.WriteInt(1)
				f.WriteInt(2)
				f.WriteString("BATCH_LOG")
			}),
			want: response(ProtoVersion4, runtimeerrors.WithMessage(&cqlerr.WriteTimeoutError{Consistency: cql.Quorum, Received: 1, BlockFor: 2, WriteType: "BATCH_LOG"}, "wt")),
		},
		errorFrameCase{
			name: "ReadTimeout", proto: ProtoVersion4,
			body: errBody(ProtoVersion4, cqlerr.CodeReadTimeout, "rt", func(f *Framer) {
				f.WriteConsistency(cql.One)
				f.WriteInt(0)
				f.WriteInt(1)
				f.WriteByteValue(1)
			}),
			want: response(ProtoVersion4, runtimeerrors.WithMessage(&cqlerr.ReadTimeoutError{Consistency: cql.One, Received: 0, BlockFor: 1, DataPresent: true}, "rt")),
		},
		errorFrameCase{
			name: "AlreadyExists", proto: ProtoVersion4,
			body: errBody(ProtoVersion4, cqlerr.CodeAlreadyExists, "exists", func(f *Framer) {
				f.WriteString("ks")
				f.WriteString("tbl")
			}),
			want: response(ProtoVersion4, runtimeerrors.WithMessage(&cqlerr.AlreadyExistsError{Keyspace: "ks", Table: "tbl"}, "exists")),
		},
		errorFrameCase{
			name: "Unprepared", proto: ProtoVersion4,
			body: errBody(ProtoVersion4, cqlerr.CodeUnprepared, "unprep", func(f *Framer) {
				f.WriteShortBytes([]byte{0xca, 0xfe})
			}),
			want: response(ProtoVersion4, runtimeerrors.WithMessage(&cqlerr.UnpreparedError{StatementID: []byte{0xca, 0xfe}}, "unprep")),
		},
		// v4 carries only a failure count; v5 replaces it with a per-replica
		// reason map and derives the count from it.
		errorFrameCase{
			name: "ReadFailureV4", proto: ProtoVersion4,
			body: errBody(ProtoVersion4, cqlerr.CodeReadFailure, "rf", func(f *Framer) {
				f.WriteConsistency(cql.All)
				f.WriteInt(1)
				f.WriteInt(3)
				f.WriteInt(2)
				f.WriteByteValue(0)
			}),
			want: response(ProtoVersion4, runtimeerrors.WithMessage(&cqlerr.ReadFailureError{Consistency: cql.All, Received: 1, BlockFor: 3, NumFailures: 2, DataPresent: false}, "rf")),
		},
		errorFrameCase{
			name: "ReadFailureV5", proto: ProtoVersion5,
			body: errBody(ProtoVersion5, cqlerr.CodeReadFailure, "rf", func(f *Framer) {
				f.WriteConsistency(cql.All)
				f.WriteInt(1)
				f.WriteInt(3)
				writeErrMap(f, errMapEntry{"10.0.0.1", 1}, errMapEntry{"2001:db8::1", 2})
				f.WriteByteValue(1)
			}),
			want: response(ProtoVersion5, runtimeerrors.WithMessage(&cqlerr.ReadFailureError{Consistency: cql.All, Received: 1, BlockFor: 3, NumFailures: 2, DataPresent: true,
				ErrorMap: cqlerr.ErrorMap{"10.0.0.1": 1, "2001:db8::1": 2}}, "rf")),
		},
		errorFrameCase{
			name: "WriteFailureV4", proto: ProtoVersion4,
			body: errBody(ProtoVersion4, cqlerr.CodeWriteFailure, "wf", func(f *Framer) {
				f.WriteConsistency(cql.Two)
				f.WriteInt(0)
				f.WriteInt(2)
				f.WriteInt(1)
				f.WriteString("SIMPLE")
			}),
			want: response(ProtoVersion4, runtimeerrors.WithMessage(&cqlerr.WriteFailureError{Consistency: cql.Two, Received: 0, BlockFor: 2, NumFailures: 1, WriteType: "SIMPLE"}, "wf")),
		},
		errorFrameCase{
			name: "WriteFailureV5", proto: ProtoVersion5,
			body: errBody(ProtoVersion5, cqlerr.CodeWriteFailure, "wf", func(f *Framer) {
				f.WriteConsistency(cql.Two)
				f.WriteInt(0)
				f.WriteInt(2)
				writeErrMap(f, errMapEntry{"10.0.0.2", 3})
				f.WriteString("SIMPLE")
			}),
			want: response(ProtoVersion5, runtimeerrors.WithMessage(&cqlerr.WriteFailureError{Consistency: cql.Two, Received: 0, BlockFor: 2, NumFailures: 1, WriteType: "SIMPLE",
				ErrorMap: cqlerr.ErrorMap{"10.0.0.2": 3}}, "wf")),
		},
		errorFrameCase{
			name: "WriteFailureV5EmptyMap", proto: ProtoVersion5,
			body: errBody(ProtoVersion5, cqlerr.CodeWriteFailure, "wf", func(f *Framer) {
				f.WriteConsistency(cql.Two)
				f.WriteInt(0)
				f.WriteInt(2)
				writeErrMap(f)
				f.WriteString("SIMPLE")
			}),
			want: response(ProtoVersion5, runtimeerrors.WithMessage(&cqlerr.WriteFailureError{Consistency: cql.Two, BlockFor: 2, WriteType: "SIMPLE", ErrorMap: cqlerr.ErrorMap{}}, "wf")),
		},
		errorFrameCase{
			name: "FunctionFailure", proto: ProtoVersion4,
			body: errBody(ProtoVersion4, cqlerr.CodeFunctionFailure, "ff", func(f *Framer) {
				f.WriteString("ks")
				f.WriteString("fn")
				f.WriteStringList([]string{"int", "text"})
			}),
			want: response(ProtoVersion4, runtimeerrors.WithMessage(&cqlerr.FunctionFailureError{Keyspace: "ks", Function: "fn", ArgTypes: []string{"int", "text"}}, "ff")),
		},
		errorFrameCase{
			name: "CDCWriteFailure", proto: ProtoVersion4,
			body: errBody(ProtoVersion4, cqlerr.CodeCDCWriteFailure, "cdc", nil),
			want: response(ProtoVersion4, runtimeerrors.WithMessage(&cqlerr.CDCWriteFailureError{}, "cdc")),
		},
		errorFrameCase{
			name: "CASWriteUnknown", proto: ProtoVersion4,
			body: errBody(ProtoVersion4, cqlerr.CodeCASWriteUnknown, "cas", func(f *Framer) {
				f.WriteConsistency(cql.Serial)
				f.WriteInt(1)
				f.WriteInt(2)
			}),
			want: response(ProtoVersion4, runtimeerrors.WithMessage(&cqlerr.CASWriteUnknownError{Consistency: cql.Serial, Received: 1, BlockFor: 2}, "cas")),
		},
		errorFrameCase{
			name: "RateLimitReached", proto: ProtoVersion4, rateLimitCode: scyllaRateLimitCode,
			body: errBody(ProtoVersion4, scyllaRateLimitCode, "slow down", func(f *Framer) {
				f.WriteByteValue(byte(cqlerr.OpTypeWrite))
				f.WriteByteValue(1)
			}),
			want: response(ProtoVersion4, runtimeerrors.WithMessage(&cqlerr.RateLimitReachedError{OpType: cqlerr.OpTypeWrite, RejectedByCoordinator: true}, "slow down")),
		},
		// Without the extension the same code is just unknown, and its extra
		// bytes are not read.
		errorFrameCase{
			name: "RateLimitCodeWithoutExtension", proto: ProtoVersion4, unread: 2,
			body: errBody(ProtoVersion4, scyllaRateLimitCode, "slow down", func(f *Framer) {
				f.WriteByteValue(byte(cqlerr.OpTypeWrite))
				f.WriteByteValue(1)
			}),
			want: response(ProtoVersion4, cqlerr.NewOtherServerError(scyllaRateLimitCode, "slow down")),
		},
		errorFrameCase{
			name: "Unknown", proto: ProtoVersion4,
			body: errBody(ProtoVersion4, 0x7777, "what", nil),
			want: response(ProtoVersion4, cqlerr.NewOtherServerError(0x7777, "what")),
		},
	)
	return cases
}

func errCodeName(code cqlerr.ErrorCode) string {
	switch code {
	case cqlerr.CodeServer:
		return "server"
	case cqlerr.CodeProtocol:
		return "protocol"
	case cqlerr.CodeBadCredentials:
		return "credentials"
	case cqlerr.CodeOverloaded:
		return "overloaded"
	case cqlerr.CodeBootstrapping:
		return "bootstrapping"
	case cqlerr.CodeTruncate:
		return "truncate"
	case cqlerr.CodeSyntax:
		return "syntax"
	case cqlerr.CodeUnauthorized:
		return "unauthorized"
	case cqlerr.CodeInvalid:
		return "invalid"
	case cqlerr.CodeConfig:
		return "config"
	}
	return "other"
}

// messageOf reads the server message off a parsed ERROR response.
func messageOf(f Frame) string {
	return f.(*ErrorResponseFrame).Err.Message()
}

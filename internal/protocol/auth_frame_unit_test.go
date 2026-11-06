package protocol

import (
	frm "github.com/scylladb/gocql/v2/internal/frame"
)

func parseOpBody(op frm.Op, body []byte) (frame, error) {
	f := newFramer(nil, protoVersion4)
	f.Header = &frm.FrameHeader{Version: protoVersion4 | protoDirectionMask, Op: op, Stream: 3}
	f.Buf = body
	return f.ParseFrame()
}

func bodyOf(write func(f *framer)) []byte {
	f := newFramer(nil, protoVersion4)
	write(f)
	return f.Buf
}

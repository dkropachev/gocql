package wirerecord

// frameV4 builds a protocol v4 request frame: version, header flags, a 2-byte
// stream id, the opcode and the 4-byte body length, followed by body.
func frameV4(op frameOp, headerFlags byte, body []byte) []byte {
	frame := []byte{
		0x04,
		headerFlags,
		0x00, 0x7B, // stream id
		byte(op),
		byte(len(body) >> 24), byte(len(body) >> 16), byte(len(body) >> 8), byte(len(body)),
	}

	return append(frame, body...)
}

// frameV2 builds the same frame on protocol v2, whose stream id is one byte, so the
// opcode sits at index 3 and the body at index 8 -- a byte earlier than anything in
// this package reads. It exists to pin that such a frame is not understood.
func frameV2(op frameOp, body []byte) []byte {
	frame := []byte{
		0x02,
		0x00, // header flags
		0x00, // stream id (1 byte)
		byte(op),
		byte(len(body) >> 24), byte(len(body) >> 16), byte(len(body) >> 8), byte(len(body)),
	}

	return append(frame, body...)
}

// startupFrameV4 builds a protocol v4 STARTUP request frame carrying body as
// its options blob. The contents of body are irrelevant to the hash; only its
// length is varied by the caller.
func startupFrameV4(body []byte) []byte {
	return frameV4(opStartup, 0x00, body)
}

// v4ExecuteFrame builds a minimal protocol-v4 EXECUTE request frame. When
// withMetadataID is true it inserts a resultMetadataID short-bytes field between
// the preparedID and the query params, as SCYLLA_USE_METADATA_ID does on v4.
// The query params are a bare consistency + zero flags, so a correct parse walks
// to exactly len(frame).
//
//	header: version, flags, stream(2), opcode, length(4)   -> body starts at 9
//	body:   preparedID [short bytes], (resultMetadataID [short bytes],)
//	        consistency [short], query flags [byte]
func v4ExecuteFrame(withMetadataID bool) []byte {
	body := []byte{
		0x00, 0x03, 0xAA, 0xBB, 0xCC, // preparedID: len 3
	}
	if withMetadataID {
		body = append(body, 0x00, 0x02, 0xDE, 0xAD) // resultMetadataID: len 2
	}
	body = append(body, 0x00, 0x01, 0x00) // consistency (2) + query flags (1) = 0
	header := []byte{
		0x04,       // version v4 (request)
		0x00,       // flags
		0x00, 0x01, // stream id
		byte(opExecute),                   // opcode
		0x00, 0x00, 0x00, byte(len(body)), // body length
	}
	return append(header, body...)
}

// stringMap encodes opts as a CQL [string map]: a short count, then that many
// [string] key / [string] value pairs, each length-prefixed with a short.
func stringMap(opts ...[2]string) []byte {
	body := []byte{byte(len(opts) >> 8), byte(len(opts))}
	appendString := func(s string) {
		body = append(body, byte(len(s)>>8), byte(len(s)))
		body = append(body, s...)
	}
	for _, kv := range opts {
		appendString(kv[0])
		appendString(kv[1])
	}
	return body
}

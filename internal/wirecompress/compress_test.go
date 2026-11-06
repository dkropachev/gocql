/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package wirecompress

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/klauspost/compress/snappy"

	"github.com/pierrec/lz4/v4"

	"github.com/scylladb/gocql/v2/internal/wire"
)

type blockCodec struct {
	name   string
	append func([]byte, wire.Chain) ([]byte, error)
	decode func([]byte, int) ([]byte, error)
}

var codecs = []blockCodec{
	{name: "lz4", append: AppendLZ4, decode: func(encoded []byte, size int) ([]byte, error) {
		decoded := make([]byte, size)
		n, err := lz4.UncompressBlock(encoded, decoded)
		if err != nil {
			return nil, err
		}
		if n != size {
			return nil, fmt.Errorf("decoded %d bytes, want %d", n, size)
		}
		return decoded, nil
	}},
	{name: "snappy", append: AppendSnappy, decode: func(encoded []byte, size int) ([]byte, error) {
		// DecodeStrict rejects S2 repeat-offset extensions that the ordinary
		// klauspost decoder deliberately accepts in addition to Snappy.
		return snappy.DecodeStrict(nil, encoded)
	}},
}

func splitChain(t testing.TB, data []byte, step int) wire.Chain {
	t.Helper()
	var spans []wire.Span
	for len(data) != 0 {
		n := min(len(data), step)
		spans = append(spans, wire.Borrow(data[:n]))
		data = data[n:]
	}
	chain, err := wire.NewChain(spans...)
	if err != nil {
		t.Fatal(err)
	}
	return chain
}

func checkRoundTrip(t testing.TB, codec blockCodec, chain wire.Chain, want []byte) []byte {
	t.Helper()
	prefix := []byte{0xfa, 0xce, 0xb0, 0x0c}
	dst := append(make([]byte, 0, len(want)+len(want)/6+64), prefix...)
	encoded, err := codec.append(dst, chain)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded[:len(prefix)], prefix) {
		t.Fatalf("%s changed destination prefix", codec.name)
	}
	encoded = encoded[len(prefix):]
	got, err := codec.decode(encoded, len(want))
	if err != nil {
		t.Fatalf("%s decoder: %v; input length %d", codec.name, err, len(want))
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s round trip mismatch: got %d bytes, want %d", codec.name, len(got), len(want))
	}
	return encoded
}

func FuzzSpanCompression(f *testing.F) {
	f.Add([]byte{}, uint16(1))
	f.Add([]byte("borrowed spans borrowed spans borrowed spans"), uint16(3))
	f.Add(bytes.Repeat([]byte{'x'}, 65537), uint16(257))
	f.Fuzz(func(t *testing.T, data []byte, chunk uint16) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		chain := splitChain(t, data, int(chunk)+1)
		for _, codec := range codecs {
			checkRoundTrip(t, codec, chain, data)
		}
	})
}

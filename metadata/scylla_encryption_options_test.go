/*
 * Copyright (C) 2026 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package metadata

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestScyllaEncryptionOptions(t *testing.T) {
	t.Parallel()
	t.Run("UnmarshalBinary", func(t *testing.T) {
		t.Parallel()

		// Guards against
		// the bounds checks rejecting a blob they should read, which the golden-file
		// test below would not catch for values it does not carry.
		t.Run("AcceptsWellFormed", func(t *testing.T) {
			t.Parallel()

			le32 := func(v uint32) []byte {
				b := make([]byte, 4)
				binary.LittleEndian.PutUint32(b, v)
				return b
			}
			var blob []byte
			blob = append(blob, le32(2)...)
			for _, kv := range [][2]string{{"cipher_algorithm", "AES/ECB/PKCS5Padding"}, {"secret_key_strength", "128"}} {
				blob = append(blob, le32(uint32(len(kv[0])))...)
				blob = append(blob, kv[0]...)
				blob = append(blob, le32(uint32(len(kv[1])))...)
				blob = append(blob, kv[1]...)
			}

			got := &scyllaEncryptionOptions{}
			if err := got.UnmarshalBinary(blob); err != nil {
				t.Fatalf("UnmarshalBinary: %v", err)
			}
			want := &scyllaEncryptionOptions{CipherAlgorithm: "AES/ECB/PKCS5Padding", SecretKeyStrength: 128}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("decoded blob differs (-want +got):\n%s", diff)
			}
		})
		t.Run("Golden", func(t *testing.T) {
			t.Parallel()

			blob, err := os.ReadFile("testdata/recreate/scylla_encryption_options.bin")
			if err != nil {
				t.Fatal(err)
			}
			goldenBuf, err := os.ReadFile("testdata/recreate/scylla_encryption_options_golden.json")
			if err != nil {
				t.Fatal(err)
			}

			want := &scyllaEncryptionOptions{}
			if err := json.Unmarshal(goldenBuf, want); err != nil {
				t.Fatal(err)
			}

			got := &scyllaEncryptionOptions{}
			if err := got.UnmarshalBinary(blob); err != nil {
				t.Fatalf("UnmarshalBinary: %v", err)
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("decoded blob differs from the golden (-want +got):\n%s", diff)
			}
		})
		// Pins the bounds
		// checks. Each of these used to panic with a slice-bounds error rather than
		// return, on a blob the server supplied.
		t.Run("RejectsShortBlobs", func(t *testing.T) {
			t.Parallel()

			// le32 is the little-endian length prefix the format uses.
			le32 := func(v uint32) []byte {
				b := make([]byte, 4)
				binary.LittleEndian.PutUint32(b, v)
				return b
			}
			concat := func(parts ...[]byte) []byte {
				var out []byte
				for _, p := range parts {
					out = append(out, p...)
				}
				return out
			}

			for _, tc := range []struct {
				name string
				blob []byte
				// wantErr, when set, must appear in the error. Only the entry-count
				// guard can produce it, and that guard cannot be pinned by the error
				// merely being non-nil: without it the loop would still fail on the
				// second read, after make() had already been asked for a map of
				// 1<<30 entries.
				wantErr string
			}{
				{name: "no blob at all", blob: nil},
				{name: "shorter than the entry count", blob: []byte{1, 2}},
				{name: "an entry count and nothing else", blob: le32(1)},
				{name: "a key length past the end", blob: concat(le32(1), le32(100))},
				{name: "a key that stops short", blob: concat(le32(1), le32(10), []byte("abc"))},
				{name: "a key but no value length", blob: concat(le32(1), le32(3), []byte("abc"))},
				{name: "a value length past the end", blob: concat(le32(1), le32(3), []byte("abc"), le32(100))},
				{name: "a value that stops short", blob: concat(le32(1), le32(3), []byte("abc"), le32(10), []byte("xy"))},
				// An entry needs at least 8 bytes, so this count cannot be honest.
				// It has to be rejected before it reaches make(): a blob claiming
				// 1<<30 entries would otherwise ask for roughly a gigabyte.
				{
					name:    "an entry count larger than the blob",
					blob:    concat(le32(1<<30), le32(3), []byte("abc")),
					wantErr: "claims 1073741824 entries",
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()

					enc := &scyllaEncryptionOptions{}
					err := enc.UnmarshalBinary(tc.blob)
					if err == nil {
						t.Fatalf("UnmarshalBinary(%v) = nil, want an error", tc.blob)
					}
					if tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr) {
						t.Errorf("UnmarshalBinary(%v) = %q, want it to mention %q", tc.blob, err, tc.wantErr)
					}
				})
			}
		})
	})
}

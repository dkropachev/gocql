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

package execution

import (
	"reflect"
	"testing"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"

	"github.com/scylladb/gocql/v2/retry"
)

type customRetry struct{}

func (*customRetry) Decide(retry.Attempt) retry.Decision {
	return retry.Decision{Action: retry.Stop}
}

// scalarParity checks canonical validation against incremental conversion
// across every short read and every truncated scalar body.
func scalarParity[T any](t *testing.T, kind cqltype.Type, buffered codec.Decoder[T], incremental codec.StreamDecoder[T], bodies ...[]byte) {
	t.Helper()
	typ := foundationScalar(t, kind)
	for _, body := range bodies {
		for length := 0; length <= len(body); length++ {
			prefix := body[:length]
			encoded, err := codec.NewEncodedValue(typ, cqltype.Present, cqltype.BorrowBytes(prefix))
			if err != nil {
				t.Fatal(err)
			}
			want, wantErr := codec.DecodeValue(buffered, encoded)
			for step := 1; step <= len(prefix)+4; step++ {
				var got T
				err := streamDecodeFixture(t, typ, incremental, streamCell(prefix), &got, step)
				if (err == nil) != (wantErr == nil) {
					t.Fatalf("%s bytes %x split %d: stream %v buffered %v", kind, prefix, step, err, wantErr)
				}
				if err == nil && !reflect.DeepEqual(got, want) {
					t.Fatalf("%s bytes %x split %d: got %#v want %#v", kind, prefix, step, got, want)
				}
			}
		}
	}
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
)

func TestTimeUUIDValueAPI(t *testing.T) {
	when := time.Date(2026, 11, 9, 12, 34, 56, 123456700, time.UTC)
	for name, id := range map[string]cql.TimeUUID{
		"new":      cql.NewTimeUUID(),
		"minimum":  cql.MinTimeUUID(when),
		"maximum":  cql.MaxTimeUUID(when),
		"explicit": cql.TimeUUIDWith(cql.UUIDFromTime(when).Timestamp(), 42, []byte{1, 2, 3, 4, 5, 6}),
	} {
		t.Run(name, func(t *testing.T) {
			uuid := cql.UUID(id)
			if id.Version() != 1 || id.IsEmpty() || id.String() != uuid.String() || id.Variant() != uuid.Variant() ||
				id.Clock() != uuid.Clock() || id.Timestamp() != uuid.Timestamp() || !id.Time().Equal(uuid.Time()) ||
				!bytes.Equal(id.Node(), uuid.Node()) || !bytes.Equal(id.Bytes(), uuid.Bytes()) {
				t.Fatalf("value methods differ from UUID: %v", id)
			}
			if _, err := cql.TimeUUIDValue(id); err != nil {
				t.Fatal(err)
			}
			encoded, err := cql.EncodeValue(cql.TimeUUIDType, cql.TimeUUIDCodec, id)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := cql.DecodeValue(cql.TimeUUIDCodec, encoded)
			if err != nil || decoded != id {
				t.Fatalf("codec round trip: %v, %v", decoded, err)
			}
			data, err := json.Marshal(id)
			if err != nil || string(data) != `"`+id.String()+`"` {
				t.Fatalf("JSON representation: %s, %v", data, err)
			}
			var parsed cql.TimeUUID
			if err := json.Unmarshal(data, &parsed); err != nil || parsed != id {
				t.Fatalf("JSON round trip: %v, %v", parsed, err)
			}
			text, err := id.MarshalText()
			if err != nil {
				t.Fatal(err)
			}
			if err := parsed.UnmarshalText(text); err != nil || parsed != id {
				t.Fatalf("text round trip: %v, %v", parsed, err)
			}
			if cql.MustParseTimeUUID(id.String()) != id {
				t.Fatal("MustParseTimeUUID changed the identifier")
			}
		})
	}
	if !cql.MinTimeUUID(when).Time().Equal(when) || !cql.MaxTimeUUID(when).Time().Equal(when) {
		t.Fatal("range helpers changed timestamp")
	}
}

func TestTimeUUIDCheckedInput(t *testing.T) {
	valid := cql.NewTimeUUID()
	for _, text := range []string{
		"malformed", "00000000-0000-0000-0000-000000000000",
		"00112233-4455-4677-8899-aabbccddeeff", "00112233-4455-6677-8899-aabbccddeeff", "00112233-4455-7677-8899-aabbccddeeff",
	} {
		if _, err := cql.ParseTimeUUID(text); err == nil {
			t.Fatalf("parsed non-v1 input %q", text)
		}
		dst := valid
		if err := dst.UnmarshalText([]byte(text)); err == nil || dst != valid {
			t.Fatalf("text input changed receiver: %v, %v", dst, err)
		}
		if err := json.Unmarshal([]byte(`"`+text+`"`), &dst); err == nil || dst != valid {
			t.Fatalf("JSON input changed receiver: %v, %v", dst, err)
		}
	}
	if _, err := cql.TimeUUIDFromUUID(cql.MustRandomUUID()); err == nil {
		t.Fatal("converted a random UUID to timeuuid")
	}
	// Match existing CQL codec policy: version 1 is checked independently of variant.
	unusual := cql.MustParseUUID("00112233-4455-1677-0899-aabbccddeeff")
	if _, err := cql.TimeUUIDFromUUID(unusual); err != nil {
		t.Fatal(err)
	}
	var zero cql.TimeUUID
	if !zero.IsEmpty() {
		t.Fatal("zero timeuuid is not empty")
	}
	if zero.String() != "00000000-0000-0000-0000-000000000000" {
		t.Fatal("zero diagnostic formatting changed")
	}
	for _, invalid := range []cql.TimeUUID{zero, cql.TimeUUID(cql.MustRandomUUID())} {
		if data, err := json.Marshal(invalid); err == nil {
			t.Fatalf("serialized non-v1 timeuuid to JSON: %s", data)
		}
		if data, err := invalid.MarshalText(); err == nil {
			t.Fatalf("serialized non-v1 timeuuid to text: %s", data)
		}
	}
}

func TestTimeUUIDJSONRequiresString(t *testing.T) {
	valid := cql.MustParseTimeUUID("00112233-4455-1677-8899-aabbccddeeff")
	for _, input := range []string{
		"12345678901212345678901234567890",
		"null",
		"[]",
		`["00112233-4455-1677-8899-aabbccddeeff"]`,
		`{"id":"00112233-4455-1677-8899-aabbccddeeff"}`,
	} {
		dst := valid
		if err := json.Unmarshal([]byte(input), &dst); err == nil || dst != valid {
			t.Fatalf("non-string JSON changed receiver: input=%s dst=%v err=%v", input, dst, err)
		}
	}
	var dst cql.TimeUUID
	escaped := []byte(`"\u0030` + valid.String()[1:] + `"`)
	if err := json.Unmarshal(escaped, &dst); err != nil || dst != valid {
		t.Fatalf("escaped UUID string rejected: %v, %v", dst, err)
	}
}

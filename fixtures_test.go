// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

func TestFixtures(t *testing.T) {
	t.Run("BenchRecordings", func(t *testing.T) {
		// Current typed-control captures also run through TestSingleConnection/Replay.
		t.Run("CurrentControlCapture", func(t *testing.T) {
			const regenerate = "\nRegenerate the recordings: see the recipe above TestMain in tests/bench/e2e/single_conn_bench_test.go."

			want := benchControlQuery()
			wantParams := benchControlParams(t)

			for _, name := range benchRecordings {
				t.Run(name, func(t *testing.T) {
					var (
						texts   []string
						params  []byte
						found   bool
						skipped int
					)
					for i, record := range readRecording(t, name) {
						text, tail, ok := recordedQueryText(t, i, record.Data)
						if !ok {
							if recordedIsQuery(record.Data) {
								skipped++
							}
							continue
						}
						texts = append(texts, text)
						if text == want && !found {
							params, found = tail, true
						}
					}

					if !found {
						switch {
						case skipped > 0:
							// Not a stale recording: the frames are there and this parser
							// cannot read them. Reporting "no QUERY frame" here would send
							// the reader off to regenerate a recording that is fine.
							t.Fatalf("all %d of the recording's QUERY frames carry a custom payload, which this guard does not read past", skipped)
						case len(texts) == 0:
							t.Fatal("the recording holds no QUERY frame, so it cannot serve the control connection's system.local lookup")
						default:
							t.Fatalf("no recorded query is the one the control connection sends\n\twant %q\n\tgot  %q"+regenerate, want, texts)
						}
					}

					// The statement matches. The parameters behind it are inside the hash as
					// well, so the recording is only current if they match too.
					if !bytes.Equal(params, wantParams) {
						t.Fatalf("recorded control query parameters differ from current typed metadata execution\nwant % x\ngot  % x%s", wantParams, params, regenerate)
					}
				})
			}
		})
	})

	t.Run("SplitFixtureCQL", func(t *testing.T) {
		source := "CREATE FUNCTION f() AS $$ int x=1; return x; $$; SELECT ';'';'; SELECT \"a;\";"
		queries := splitFixtureCQL(source)
		if len(queries) != 4 || !strings.Contains(queries[0], "return x;") || strings.TrimSpace(queries[3]) != "" {
			t.Fatalf("split: %#v", queries)
		}
	})
}

// splitFixtureCQL respects quoted literals, identifiers and $$ Java/Lua bodies.
// The fixtures intentionally have no alternate dollar tags or line comments.
func splitFixtureCQL(source string) []string {
	var result []string
	start := 0
	var quote byte
	dollar := false
	for i := 0; i < len(source); i++ {
		ch := source[i]
		if quote != 0 {
			if ch == quote {
				if i+1 < len(source) && source[i+1] == quote {
					i++
				} else {
					quote = 0
				}
			}
			continue
		}
		if i+1 < len(source) && source[i:i+2] == "$$" {
			dollar = !dollar
			i++
			continue
		}
		if dollar {
			continue
		}
		if ch == '\'' || ch == '"' {
			quote = ch
			continue
		}
		if ch == ';' {
			result = append(result, source[start:i])
			start = i + 1
		}
	}
	return append(result, source[start:])
}

func fixtureKeyspace(suffix string) string {
	prefix := os.Getenv("GOCQL_TEST_RUN")
	if prefix == "" {
		prefix = "gocql_test"
	}
	if len(prefix) > 24 {
		panic("GOCQL_TEST_RUN exceeds 24 characters")
	}
	for _, c := range prefix {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_') {
			panic("invalid GOCQL_TEST_RUN identifier")
		}
	}
	if prefix[0] < 'a' || prefix[0] > 'z' {
		panic("GOCQL_TEST_RUN must start with a lowercase letter")
	}
	return prefix + suffix
}

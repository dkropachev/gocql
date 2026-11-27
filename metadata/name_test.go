// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata_test

import (
	"fmt"
	"testing"

	"github.com/scylladb/gocql/v2/metadata"
)

func TestName(t *testing.T) {
	for in, want := range map[string]string{
		"users":         "users",
		"Users":         "users",
		`"Users"`:       "Users",
		`"a""b"`:        `a"b`,
		`""`:            "",
		`"`:             `"`,
		`"unterminated`: `"unterminated`,
		"":              "",
	} {
		if got := metadata.Name(in); got != want {
			t.Errorf("Name(%q) = %q, want %q", in, got, want)
		}
	}
}

func ExampleName() {
	// A table created as CREATE TABLE app."UserEvents" (...) is looked up
	// as UserEvents, and one created as CREATE TABLE app.UserEvents (...)
	// as userevents.
	fmt.Println(metadata.Name(`"UserEvents"`), metadata.Name("UserEvents"))
	// Output:
	// UserEvents userevents
}

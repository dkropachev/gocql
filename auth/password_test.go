// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package auth

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

// TestPasswordIsNotPrinted prints a Password provider every way a
// configuration might be printed or logged, alone and inside a struct.
func TestPasswordIsNotPrinted(t *testing.T) {
	const secret = "s3cr3t-P@ssw0rd-1188"
	p := Password("cassandra", secret)
	holder := struct{ Auth Provider }{p}
	var out bytes.Buffer
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
		fmt.Fprintf(&out, format+"\n", p)
		fmt.Fprintf(&out, format+"\n", holder)
	}
	for _, h := range []slog.Handler{slog.NewTextHandler(&out, nil), slog.NewJSONHandler(&out, nil)} {
		slog.New(h).Info("config", "auth", p, "holder", holder)
	}
	if strings.Contains(out.String(), secret) {
		t.Fatalf("the password was printed:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "cassandra") {
		t.Fatalf("the username was not printed:\n%s", out.String())
	}
}

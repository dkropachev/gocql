// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"os/exec"
	"strings"
	"testing"
)

// TestNoCopyVet runs go vet on testdata/vetcopy, which copies a Batch, a
// Worker and a StreamWorker, and requires copylocks to report each copy
// through the noCopy field they hold.
func TestNoCopyVet(t *testing.T) {
	if testing.Short() {
		t.Skip("runs go vet")
	}
	// go ./... skips testdata, so the package is vetted here.
	out, err := exec.Command("go", "vet", "./testdata/vetcopy").CombinedOutput()
	if err == nil {
		t.Fatal("go vet accepted copies of Batch, Worker and StreamWorker")
	}
	for _, typ := range []string{"v2.Batch", "v2.Worker[int, int]", "v2.StreamWorker[int, int]"} {
		if !strings.Contains(string(out), typ+" contains github.com/scylladb/gocql/v2/internal/session.noCopy") {
			t.Errorf("go vet did not report a copy of %s through noCopy:\n%s", typ, out)
		}
	}
}

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package connectivity

import (
	"testing"
	"time"
)

func TestDefaultMetadataServerTimeout(t *testing.T) {
	for client, want := range map[time.Duration]time.Duration{
		31 * time.Second:        30 * time.Second,
		10 * time.Second:        9 * time.Second,
		2 * time.Second:         time.Second,
		time.Second:             500 * time.Millisecond,
		1500 * time.Microsecond: 0,
		0:                       30 * time.Second,
	} {
		if got := DefaultMetadataServerTimeout(client); got != want {
			t.Errorf("DefaultMetadataServerTimeout(%v) = %v, want %v", client, got, want)
		}
	}
}

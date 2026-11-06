// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package main

import (
	"strings"
	"testing"
)

func TestVersion(t *testing.T) {
	t.Run("Compare", func(t *testing.T) {
		ordered := []string{"2.0.0-alpha", "2.0.0-alpha.1", "2.0.0-alpha.beta", "2.0.0-beta", "2.0.0-beta.2", "2.0.0-beta.11", "2.0.0-rc.1", "2.0.0", "2.1.0"}
		for i := 1; i < len(ordered); i++ {
			a, _ := parseVersion(ordered[i-1])
			b, _ := parseVersion(ordered[i])
			if compareVersions(a, b) >= 0 {
				t.Fatalf("%s !< %s", ordered[i-1], ordered[i])
			}
		}
	})

	t.Run("HigherStableRootTag", func(t *testing.T) {
		t.Parallel()
		c, _ := newCandidate("root", "2.0.0")
		tests := []struct {
			tag  string
			want bool
		}{
			{"v2.1.0", true},
			{"v2.0.1", true},
			{"v2.0.0", false},
			{"v1.19.9", false},
			{"v2.1.0-rc.1", false},
			{"lz4/v2.1.0", false},
			{"v2.0.0", false},
			{"not-a-version", false},
		}
		for _, tt := range tests {
			if got := isHigherStableRootTag(tt.tag, c); got != tt.want {
				t.Errorf("isHigherStableRootTag(%q) = %t, want %t", tt.tag, got, tt.want)
			}
		}
	})

	t.Run("Parse", func(t *testing.T) {
		t.Parallel()
		for _, version := range []string{"2.0.0", "2.0.0", "2.0.0-rc.1", "2.0.0-alpha-beta.2", "2.0.0-999999999999999999999999999"} {
			version := version
			t.Run("valid/"+version, func(t *testing.T) {
				t.Parallel()
				if _, err := parseVersion(version); err != nil {
					t.Fatal(err)
				}
			})
		}
		for _, version := range []string{"", "v2.0.0", "1.20.0", "0.20.0", "1.2", "1.2.3.4", "1.02.3", "1.2.03", "1.2.3+build.1", "1.2.3-rc.01", "1.2.3-", "1.2.3-rc/1", "1.2.3;touch-pwned", "1.2.3-$(id)", "1.2.3-rc\nnext=value"} {
			version := version
			t.Run("invalid/"+strings.ReplaceAll(version, "/", "_"), func(t *testing.T) {
				t.Parallel()
				if _, err := parseVersion(version); err == nil {
					t.Fatalf("%q accepted", version)
				}
			})
		}
	})
}

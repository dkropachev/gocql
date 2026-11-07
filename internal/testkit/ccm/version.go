// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package ccm

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Distribution is the database a cluster runs.
type Distribution string

const (
	Scylla    Distribution = "scylla"
	Cassandra Distribution = "cassandra"
)

// ParseDistribution accepts "scylla" or "cassandra" in any case.
func ParseDistribution(s string) (Distribution, error) {
	switch d := Distribution(strings.ToLower(strings.TrimSpace(s))); d {
	case Scylla, Cassandra:
		return d, nil
	}
	return "", fmt.Errorf("ccm: unknown distribution %q", s)
}

// VersionSpec is the version a caller asks ccm to install.
type VersionSpec struct {
	Distribution Distribution
	// Raw is the version as given.
	Raw string
	// CCMArg is the value passed to ccm create -v.
	CCMArg string
}

func (v VersionSpec) String() string { return v.CCMArg }

var (
	// A release number: 6.2, 6.2.0, 2024.1, 2025.1.0, and numbered
	// candidates such as 6.2.0-rc1 or 2025.1~rc2.
	releaseRe = regexp.MustCompile(`^[0-9]+\.[0-9]+(\.[0-9]+)?([-.~](alpha|beta|rc)[0-9]+)?$`)
	// The build part of an unstable or hotfix reference, the same set
	// ci/resolve-scylla-ccm-version.sh accepts.
	mutableBuildRe = regexp.MustCompile(`^(latest|[0-9]+|[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z)$`)
	cassandraRe    = regexp.MustCompile(`^([0-9]+\.[0-9]+(\.[0-9]+)?([-.~][0-9A-Za-z.-]+)?|git:[^\s]+|github:[^\s]+|binary:[^\s]+|alias:[^\s]+)$`)
)

// ParseVersion validates a version and converts it to the form ccm create
// takes.
//
// ScyllaDB accepts plain releases ("6.2.0", "2024.1"), ccm release references
// ("release:6.1", "release:2025.1.0:debug") and unstable or hotfix builds
// ("unstable/master:latest", "unstable/master:2020-02-15T03:02:19Z",
// "hotfix/branch-2025.1:12"). "unstable:master/latest" is accepted as a
// spelling of "unstable/master:latest". Cassandra accepts a release number or
// a ccm source reference (git:, github:, binary:, alias:).
func ParseVersion(d Distribution, s string) (VersionSpec, error) {
	raw := s
	s = strings.TrimSpace(s)
	spec := VersionSpec{Distribution: d, Raw: raw}
	if s == "" || strings.ContainsAny(s, " \t\n") {
		return spec, fmt.Errorf("ccm: invalid %s version %q", d, raw)
	}
	switch d {
	case Cassandra:
		if !cassandraRe.MatchString(s) {
			return spec, fmt.Errorf("ccm: invalid cassandra version %q", raw)
		}
		spec.CCMArg = s
		return spec, nil
	case Scylla:
		arg, err := scyllaCCMArg(s)
		if err != nil {
			return spec, fmt.Errorf("ccm: invalid scylla version %q: %w", raw, err)
		}
		spec.CCMArg = arg
		return spec, nil
	}
	return spec, fmt.Errorf("ccm: unknown distribution %q", d)
}

func scyllaCCMArg(s string) (string, error) {
	if kind, rest, ok := strings.Cut(s, ":"); ok && (kind == "unstable" || kind == "hotfix") {
		// "unstable:master/latest" -> "unstable/master:latest".
		branch, build, ok := strings.Cut(rest, "/")
		if !ok {
			return "", fmt.Errorf("want %s/<branch>:<build>", kind)
		}
		s = kind + "/" + branch + ":" + build
	}
	reference, debug := strings.CutSuffix(s, ":debug")
	suffix := ""
	if debug {
		suffix = ":debug"
	}
	switch {
	case strings.HasPrefix(reference, "unstable/"), strings.HasPrefix(reference, "hotfix/"):
		typeAndBranch, build, ok := strings.Cut(reference, ":")
		if !ok {
			return "", fmt.Errorf("missing :<build>")
		}
		_, branch, _ := strings.Cut(typeAndBranch, "/")
		if branch == "" {
			return "", fmt.Errorf("missing branch")
		}
		if !mutableBuildRe.MatchString(build) {
			return "", fmt.Errorf("build %q is not latest, a build number or a timestamp", build)
		}
		return reference + suffix, nil
	case strings.HasPrefix(reference, "release:"):
		release := strings.TrimPrefix(reference, "release:")
		if !releaseRe.MatchString(release) {
			return "", fmt.Errorf("release %q is not a version number", release)
		}
		return reference + suffix, nil
	case releaseRe.MatchString(reference):
		return "release:" + reference + suffix, nil
	}
	return "", fmt.Errorf("not a release number or ccm reference")
}

// ServerVersion is the version a running node reports.
type ServerVersion struct {
	Distribution Distribution
	Pre          string
	Raw          string
	Major        int
	Minor        int
	Patch        int
}

var serverVersionRe = regexp.MustCompile(`^([0-9]+)\.([0-9]+)(?:\.([0-9]+))?(.*)$`)

// ParseServerVersion parses a version string a server reports.
func ParseServerVersion(d Distribution, s string) (ServerVersion, error) {
	v := ServerVersion{Distribution: d, Raw: s}
	m := serverVersionRe.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return v, fmt.Errorf("ccm: unparsable %s server version %q", d, s)
	}
	v.Major, _ = strconv.Atoi(m[1])
	v.Minor, _ = strconv.Atoi(m[2])
	if m[3] != "" {
		v.Patch, _ = strconv.Atoi(m[3])
	}
	v.Pre = m[4]
	return v, nil
}

// AtLeast reports whether v is major.minor.patch or newer, ignoring Pre.
func (v ServerVersion) AtLeast(major, minor, patch int) bool {
	if v.Major != major {
		return v.Major > major
	}
	if v.Minor != minor {
		return v.Minor > minor
	}
	return v.Patch >= patch
}

func (v ServerVersion) String() string {
	return fmt.Sprintf("%d.%d.%d%s", v.Major, v.Minor, v.Patch, v.Pre)
}

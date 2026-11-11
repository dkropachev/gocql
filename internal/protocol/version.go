// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import "fmt"

// Version describes what one native protocol version puts on the wire. Each
// supported version is defined in a file of its own (version_v4.go,
// version_v5.go). Code that writes or reads the wire asks a connection's
// Version what to do; it never compares version numbers.
//
// A connection fixes its Version when it is created, from the dialect the
// session was opened with: the driver negotiates no version. The fields are
// plain values, so a check costs what the version comparison it replaces did.
// ScyllaDB extensions that add a version's behaviour to an older one (the
// result metadata ID, for one) are negotiated per connection and combined
// with these fields where they are used.
type Version struct {
	// Number is the version's number in every frame header.
	Number byte
	// WideFlags reports that QUERY, EXECUTE and BATCH flags are an [int];
	// otherwise they are a [byte].
	WideFlags bool
	// Segments reports that frames travel inside checksummed transport
	// segments once the connection is ready, and that compression applies to
	// segments; otherwise each frame is compressed on its own, flagged in its
	// header.
	Segments bool
	// ResultMetadataID reports that PREPARED and ROWS results carry a result
	// metadata ID and that EXECUTE sends it back.
	ResultMetadataID bool
	// QueryKeyspace reports that QUERY, EXECUTE, BATCH and PREPARE may name
	// their keyspace.
	QueryKeyspace bool
	// NowInSeconds reports that a request may set the server's notion of now.
	NowInSeconds bool
	// FailureReasonMap reports that READ_FAILURE and WRITE_FAILURE errors
	// carry a map of failure reasons by replica; otherwise they carry a count.
	FailureReasonMap bool
}

// VersionOf returns the Version numbered n.
func VersionOf(n byte) (Version, error) {
	switch n {
	case V4.Number:
		return V4, nil
	case V5.Number:
		return V5, nil
	}
	return Version{}, fmt.Errorf("gocql: unsupported native protocol version %d: the driver speaks versions 4 and 5", n)
}

// FlagsWidth returns the size in bytes of the QUERY, EXECUTE and BATCH flags.
func (v *Version) FlagsWidth() int {
	if v.WideFlags {
		return 4
	}
	return 1
}

// VersionFor returns the Version of a connection whose frames are numbered
// n: V5 from version 5 on, V4 below it. Sessions open connections of
// versions 4 and 5 only; the lookup is a single compare, so hot paths call it
// instead of storing a Version beside the number.
func VersionFor(n byte) *Version {
	if n&ProtoVersionMask >= ProtoVersion5 {
		return &V5
	}
	return &V4
}

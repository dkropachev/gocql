// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"bytes"
)

type countedMetadataSource struct {
	*bytes.Reader
	consumed int
}

func (s *countedMetadataSource) Read(p []byte) (int, error) {
	n, e := s.Reader.Read(p)
	s.consumed += n
	return n, e
}

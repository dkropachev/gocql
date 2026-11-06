// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package wirecompress

type aliasingDecoder struct{}

func (aliasingDecoder) Name() string                        { return "lz4" }
func (aliasingDecoder) Decode(input []byte) ([]byte, error) { return input, nil }

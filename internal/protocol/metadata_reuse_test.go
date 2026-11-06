// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"encoding/binary"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

func reuseMetadataBody(name string, kind uint16) []byte {
	b := binary.BigEndian.AppendUint32(nil, frm.ResultKindRows)
	b = binary.BigEndian.AppendUint32(b, uint32(frm.FlagGlobalTableSpec))
	b = binary.BigEndian.AppendUint32(b, 1)
	for _, s := range []string{"ks", "table", name} {
		b = binary.BigEndian.AppendUint16(b, uint16(len(s)))
		b = append(b, s...)
	}
	b = binary.BigEndian.AppendUint16(b, kind)
	return binary.BigEndian.AppendUint32(b, 0)
}

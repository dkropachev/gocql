// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"github.com/scylladb/gocql/v2/internal/transport"
)

var _ transport.InternalCqlProtocolExtension = &transport.InternalTabletsRoutingV1Ext{}

var _ transport.InternalCqlProtocolExtension = &transport.InternalRateLimitExt{}

var _ transport.InternalCqlProtocolExtension = &transport.InternalLwtAddMetadataMarkExt{}

var _ transport.InternalCqlProtocolExtension = &transport.InternalScyllaUseMetadataIDExt{}

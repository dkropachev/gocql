// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"github.com/scylladb/gocql/v2/internal/endpoint"
)

var _ endpoint.InternalCqlProtocolExtension = &endpoint.InternalTabletsRoutingV1Ext{}

var _ endpoint.InternalCqlProtocolExtension = &endpoint.InternalRateLimitExt{}

var _ endpoint.InternalCqlProtocolExtension = &endpoint.InternalLwtAddMetadataMarkExt{}

var _ endpoint.InternalCqlProtocolExtension = &endpoint.InternalScyllaUseMetadataIDExt{}

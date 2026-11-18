// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"log/slog"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/protocol"
)

// V2's version block follows the pinned extension, independent of protocol v4/5.
type tabletsRoutingV2Ext struct{}

func (tabletsRoutingV2Ext) InternalName() string { return InternalTabletsRoutingV2 }

func (tabletsRoutingV2Ext) InternalSerialize() map[string]string {
	return map[string]string{InternalTabletsRoutingV2: ""}
}

func InternalTypedTabletExtensions(exts []InternalCqlProtocolExtension, supported map[string][]string) []InternalCqlProtocolExtension {
	if _, ok := supported[InternalTabletsRoutingV2]; !ok {
		return exts
	}
	filtered := exts[:0]
	for _, ext := range exts {
		if ext.InternalName() != InternalTabletsRoutingV1 {
			filtered = append(filtered, ext)
		}
	}
	return append(filtered, tabletsRoutingV2Ext{})
}

// CQL Protocol extension interface for Scylla.
// Each extension is identified by a name and defines a way to serialize itself
// in STARTUP message payload.
type InternalCqlProtocolExtension interface {
	InternalName() string
	InternalSerialize() map[string]string
}

func InternalFindCQLProtoExtByName(exts []InternalCqlProtocolExtension, name string) InternalCqlProtocolExtension {
	for i := range exts {
		if exts[i].InternalName() == name {
			return exts[i]
		}
	}
	return nil
}

// Top-level keys used for serialization/deserialization of CQL protocol
// extensions in SUPPORTED/STARTUP messages.
// Each key identifies a single extension.
const (
	InternalLwtAddMetadataMarkKey = "SCYLLA_LWT_ADD_METADATA_MARK"
	InternalRateLimitError        = "SCYLLA_RATE_LIMIT_ERROR"
	InternalTabletsRoutingV1      = "TABLETS_ROUTING_V1"
	InternalTabletsRoutingV2      = "TABLETS_ROUTING_V2_EXPERIMENTAL"
	InternalScyllaUseMetadataID   = "SCYLLA_USE_METADATA_ID"
)

// "tabletsRoutingV1" CQL Protocol Extension.
// This extension, if enabled (properly negotiated), allows Scylla server
// to send a tablet information in `custom_payload`.
//
// Implements cqlProtocolExtension interface.
type InternalTabletsRoutingV1Ext struct {
}

// Factory function to deserialize and create an `tabletsRoutingV1Ext` instance
// from SUPPORTED message payload.
func newTabletsRoutingV1Ext(supported map[string][]string) *InternalTabletsRoutingV1Ext {
	if _, found := supported[InternalTabletsRoutingV1]; found {
		return &InternalTabletsRoutingV1Ext{}
	}
	return nil
}

func (ext *InternalTabletsRoutingV1Ext) InternalSerialize() map[string]string {
	return map[string]string{
		InternalTabletsRoutingV1: "",
	}
}

func (ext *InternalTabletsRoutingV1Ext) InternalName() string {
	return InternalTabletsRoutingV1
}

// "Rate limit" CQL Protocol Extension.
// This extension, if enabled (properly negotiated), allows Scylla server
// to send a special kind of error.
//
// Implements cqlProtocolExtension interface.
type InternalRateLimitExt struct {
	InternalRateLimitErrorCode int
}

// Factory function to deserialize and create an `rateLimitExt` instance
// from SUPPORTED message payload.
func newRateLimitExt(supported map[string][]string, logger *slog.Logger) *InternalRateLimitExt {
	logger = logging.Or(logger)
	const rateLimitErrorCode = "ERROR_CODE"

	if v, found := supported[InternalRateLimitError]; found {
		for i := range v {
			splitVal := strings.SplitN(v[i], "=", 2)
			if len(splitVal) != 2 {
				continue
			}
			if splitVal[0] == rateLimitErrorCode {
				var (
					err       error
					errorCode int
				)
				if errorCode, err = strconv.Atoi(splitVal[1]); err != nil {
					logger.Warn("unparsable SUPPORTED option", "option", rateLimitErrorCode, "value", splitVal[1], logging.Err(err))
					return nil
				}
				return &InternalRateLimitExt{
					InternalRateLimitErrorCode: errorCode,
				}
			}
		}
	}
	return nil
}

func (ext *InternalRateLimitExt) InternalSerialize() map[string]string {
	return map[string]string{
		InternalRateLimitError: "",
	}
}

func (ext *InternalRateLimitExt) InternalName() string {
	return InternalRateLimitError
}

// "LWT prepared statements metadata mark" CQL Protocol Extension.
// This extension, if enabled (properly negotiated), allows Scylla server
// to set a special bit in prepared statements metadata, which would indicate
// whether the statement at hand is LWT statement or not.
//
// This is further used to consistently choose primary replicas in a predefined
// order for these queries, which can reduce contention over hot keys and thus
// increase LWT performance.
//
// Implements cqlProtocolExtension interface.
type InternalLwtAddMetadataMarkExt struct {
	InternalLwtOptMetaBitMask int
}

// Factory function to deserialize and create an `lwtAddMetadataMarkExt` instance
// from SUPPORTED message payload.
func InternalNewLwtAddMetaMarkExt(supported map[string][]string, logger *slog.Logger) *InternalLwtAddMetadataMarkExt {
	logger = logging.Or(logger)
	const lwtOptMetaBitMaskKey = "LWT_OPTIMIZATION_META_BIT_MASK"

	if v, found := supported[InternalLwtAddMetadataMarkKey]; found {
		for i := range v {
			splitVal := strings.SplitN(v[i], "=", 2)
			if len(splitVal) != 2 {
				continue
			}
			if splitVal[0] == lwtOptMetaBitMaskKey {
				var (
					err     error
					bitMask int
				)
				value, parseErr := strconv.ParseUint(splitVal[1], 10, 32)
				bitMask, err = int(value), parseErr
				if err != nil {
					logger.Warn("unparsable SUPPORTED option", "option", lwtOptMetaBitMaskKey, "value", splitVal[1], logging.Err(err))
					return nil
				}
				return &InternalLwtAddMetadataMarkExt{
					InternalLwtOptMetaBitMask: bitMask,
				}
			}
		}
	}
	return nil
}

func (ext *InternalLwtAddMetadataMarkExt) InternalSerialize() map[string]string {
	return map[string]string{
		InternalLwtAddMetadataMarkKey: fmt.Sprintf("LWT_OPTIMIZATION_META_BIT_MASK=%d", uint32(ext.InternalLwtOptMetaBitMask)),
	}
}

func (ext *InternalLwtAddMetadataMarkExt) InternalName() string {
	return InternalLwtAddMetadataMarkKey
}

// "SCYLLA_USE_METADATA_ID" CQL Protocol Extension.
// Enables storing and updating metadata IDs for prepared statements, similar to CQL v5.
// When negotiated, the driver tracks metadata changes and updates cached metadata accordingly.
type InternalScyllaUseMetadataIDExt struct {
}

// newScyllaUseMetadataIDExt returns the extension only for a protocol v4 connection,
// which is the whole of what the extension is: a backport of the v5 result metadata
// id to v4. Version matters because opting in changes the wire format — EXECUTE
// carries a [short bytes] result metadata id and RESULT/Prepared answers with one
// (see writeExecuteFrame and parseResultPrepared). On v5 the field is already
// mandatory and Conn.tracksResultMetadataID is true without the extension, so
// asking for it again would be redundant.
func InternalNewScyllaUseMetadataIDExt(supported map[string][]string, version byte) *InternalScyllaUseMetadataIDExt {
	if protocol.VersionFor(version).ResultMetadataID {
		return nil
	}
	if _, found := supported[InternalScyllaUseMetadataID]; found {
		return &InternalScyllaUseMetadataIDExt{}
	}
	return nil
}

// name implements cqlProtocolExtension.
func (ext *InternalScyllaUseMetadataIDExt) InternalName() string {
	return InternalScyllaUseMetadataID
}

// serialize implements cqlProtocolExtension.
func (ext *InternalScyllaUseMetadataIDExt) InternalSerialize() map[string]string {
	return map[string]string{
		InternalScyllaUseMetadataID: "",
	}
}

func InternalParseSupported(supported map[string][]string, logger *slog.Logger) implEndpoint.ScyllaConnectionFeatures {
	logger = logging.Or(logger)
	const (
		scyllaShard             = "SCYLLA_SHARD"
		scyllaNrShards          = "SCYLLA_NR_SHARDS"
		scyllaPartitioner       = "SCYLLA_PARTITIONER"
		scyllaShardingAlgorithm = "SCYLLA_SHARDING_ALGORITHM"
		scyllaShardingIgnoreMSB = "SCYLLA_SHARDING_IGNORE_MSB"
		scyllaShardAwarePort    = "SCYLLA_SHARD_AWARE_PORT"
		scyllaShardAwarePortSSL = "SCYLLA_SHARD_AWARE_PORT_SSL"
	)

	var (
		si  implEndpoint.ScyllaConnectionFeatures
		err error
	)
	conn := implEndpoint.ScyllaConnectionFeaturesData(&si)
	host := implEndpoint.ScyllaHostFeaturesData(&si.ScyllaHostFeatures)

	if s, ok := supported[scyllaShard]; ok && len(s) > 0 {
		if conn.InternalShard, err = strconv.Atoi(s[0]); err != nil {
			logger.Warn("unparsable SUPPORTED option", "option", scyllaShard, "value", s, logging.Err(err))
		}
	}
	if s, ok := supported[scyllaNrShards]; ok && len(s) > 0 {
		if host.InternalNrShards, err = strconv.Atoi(s[0]); err != nil {
			logger.Warn("unparsable SUPPORTED option", "option", scyllaNrShards, "value", s, logging.Err(err))
		}
	}
	if s, ok := supported[scyllaShardingIgnoreMSB]; ok && len(s) > 0 {
		if host.InternalMsbIgnore, err = strconv.ParseUint(s[0], 10, 64); err != nil {
			logger.Warn("unparsable SUPPORTED option", "option", scyllaShardingIgnoreMSB, "value", s, logging.Err(err))
		}
	}

	if s, ok := supported[scyllaPartitioner]; ok && len(s) > 0 {
		host.InternalPartitioner = s[0]
	}
	if s, ok := supported[scyllaShardingAlgorithm]; ok && len(s) > 0 {
		host.InternalShardingAlgorithm = s[0]
	}
	if s, ok := supported[scyllaShardAwarePort]; ok && len(s) > 0 {
		if shardAwarePort, err := strconv.ParseUint(s[0], 10, 16); err != nil {
			logger.Warn("unparsable SUPPORTED option", "option", scyllaShardAwarePort, "value", s, logging.Err(err))
		} else {
			host.InternalShardAwarePort = uint16(shardAwarePort)
		}
	}
	if s, ok := supported[scyllaShardAwarePortSSL]; ok && len(s) > 0 {
		if shardAwarePortTLS, err := strconv.ParseUint(s[0], 10, 16); err != nil {
			logger.Warn("unparsable SUPPORTED option", "option", scyllaShardAwarePortSSL, "value", s, logging.Err(err))
		} else {
			host.InternalShardAwarePortTLS = uint16(shardAwarePortTLS)
		}
	}

	if lwtInfo := InternalNewLwtAddMetaMarkExt(supported, logger); lwtInfo != nil {
		host.InternalLwtFlagMask = lwtInfo.InternalLwtOptMetaBitMask
	}

	if rateLimitInfo := newRateLimitExt(supported, logger); rateLimitInfo != nil {
		host.InternalRateLimitErrorCode = rateLimitInfo.InternalRateLimitErrorCode
	}

	if _, ok := supported[InternalScyllaUseMetadataID]; ok {
		host.InternalIsMetadataIDSupported = true
	}

	_, hasLWT := supported[InternalLwtAddMetadataMarkKey]
	_, hasRateLimit := supported[InternalRateLimitError]
	host.InternalIsScylla = hasLWT || hasRateLimit || host.InternalIsMetadataIDSupported || host.InternalNrShards != 0

	complete := true
	for _, key := range []string{scyllaShard, scyllaNrShards, scyllaPartitioner, scyllaShardingAlgorithm, scyllaShardingIgnoreMSB} {
		v, ok := supported[key]
		if !ok || len(v) == 0 || v[0] == "" {
			complete = false
		}
	}
	if !complete || conn.InternalShard < 0 || conn.InternalShard >= host.InternalNrShards || host.InternalNrShards > 65535 || host.InternalMsbIgnore > 63 || host.InternalPartitioner != "org.apache.cassandra.dht.Murmur3Partitioner" || host.InternalShardingAlgorithm != "biased-token-round-robin" || host.InternalNrShards <= 0 {
		level := slog.LevelWarn
		if !complete {
			// Not a sharded Scylla node (e.g. Cassandra): nothing is wrong.
			level = slog.LevelDebug
		}
		features := host
		logger.Log(context.Background(), level, "shard awareness disabled: unsupported sharding configuration",
			"partitioner", features.InternalPartitioner, "algorithm", features.InternalShardingAlgorithm,
			"nr_shards", features.InternalNrShards, "msb_ignore", features.InternalMsbIgnore, logging.KeyShard, conn.InternalShard)
		conn.InternalShard = 0
		host.InternalNrShards = 0
		host.InternalMsbIgnore = 0
		host.InternalShardingAlgorithm = ""
	}

	return si
}

// parseCQLProtocolExtensions turns the server's SUPPORTED multimap into the
// extensions this connection will opt into. version is the connection's protocol
// version: an extension whose wire effect is version-specific is gated on it here,
// which is the single point that feeds both the STARTUP opt-in (startupCoordinator.startup)
// and the framer config (connFramers.initCache), so the two cannot disagree.
func InternalParseCQLProtocolExtensions(supported map[string][]string, version byte, logger *slog.Logger) []InternalCqlProtocolExtension {
	logger = logging.Or(logger)
	exts := []InternalCqlProtocolExtension{}

	lwtExt := InternalNewLwtAddMetaMarkExt(supported, logger)
	if lwtExt != nil {
		exts = append(exts, lwtExt)
	}

	rateLimitExt := newRateLimitExt(supported, logger)
	if rateLimitExt != nil {
		exts = append(exts, rateLimitExt)
	}

	tabletsExt := newTabletsRoutingV1Ext(supported)
	if tabletsExt != nil {
		exts = append(exts, tabletsExt)
	}

	metadataIDExt := InternalNewScyllaUseMetadataIDExt(supported, version)
	if metadataIDExt != nil {
		exts = append(exts, metadataIDExt)
	}

	return exts
}

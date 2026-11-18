// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package link

// The slots, grouped by the API they serve. Each comment names the function
// type the slot holds, and says who fills it when the engine does.
var (
	// ConfigResolve holds func(config.Config) (settings.Config, error).
	ConfigResolve = NewSlot("config.Config.resolve")
	// ProfileToEngine holds func(config.Profile) settings.Profile.
	ProfileToEngine = NewSlot("config.Profile.toEngine")
	// ProfileFromEngine holds func(settings.Profile) config.Profile.
	ProfileFromEngine = NewSlot("config.Profile.fromEngine")
)

// Package cql's codec runtime, which the engine drives. Each comment names the
// function type, with cql's types.
var (
	// NewParamWriter holds func(*wire.Builder, cql.Columns, bool) *cql.ParamWriter.
	NewParamWriter = NewSlot("cql.newParamWriter")
	// ResetParamWriter holds func(*cql.ParamWriter, *wire.Builder, cql.Columns, bool).
	ResetParamWriter = NewSlot("cql.resetParamWriter")
	// FinishParamWriter holds func(*cql.ParamWriter) error.
	FinishParamWriter = NewSlot("cql.finishParamWriter")
	// CaptureParamCells holds func(*cql.ParamWriter, unsafe.Pointer, int): the
	// pointer is the first element of n cells laid out as ParamCellType.
	CaptureParamCells = NewSlot("cql.captureParamCells")
	// ParamCellType holds func() reflect.Type, the type CaptureParamCells writes.
	ParamCellType = NewSlot("cql.paramCellType")
	// SetParamOwnership holds func(*cql.ParamWriter, bool).
	SetParamOwnership = NewSlot("cql.setParamOwnership")
	// ReadValue holds func(*wire.Cursor, cql.DataType, *wire.Lease, bool) (cql.ValueReader, error).
	ReadValue = NewSlot("cql.readValue")
	// NewValueReader holds func(cql.DataType, cql.CellState, wire.Chain, *wire.Lease) cql.ValueReader.
	NewValueReader = NewSlot("cql.newValueReader")
	// RowReaderFromCursor holds func(cql.Columns, *wire.Cursor, *wire.Lease) cql.RowReader.
	RowReaderFromCursor = NewSlot("cql.rowReaderFromCursor")
	// NewRowReader holds func(cql.Columns, wire.Chain, *wire.Lease) *cql.RowReader.
	NewRowReader = NewSlot("cql.newRowReader")
	// FinishRowReader holds func(*cql.RowReader) error.
	FinishRowReader = NewSlot("cql.finishRowReader")
	// BeginStreamRow holds func(*cql.StreamRowReader).
	BeginStreamRow = NewSlot("cql.beginStreamRow")
	// NewStreamRowReader holds func(cql.Columns, *wire.Stream, *cql.StreamRowReader) (*cql.StreamRowReader, error);
	// a nil prior validates the columns afresh.
	NewStreamRowReader = NewSlot("cql.newStreamRowReader")
	// StreamReaderStorageBytes holds func(*cql.StreamRowReader) int64.
	StreamReaderStorageBytes = NewSlot("cql.streamReaderStorageBytes")
	// DetachRow holds func(plan any, dst unsafe.Pointer) bool: it detaches the
	// row at dst when plan is a built-in plan, and reports whether it did.
	DetachRow = NewSlot("cql.detachRow")
	// ManagesRowDestination holds func(plan any) bool.
	ManagesRowDestination = NewSlot("cql.managesRowDestination")
	// PreservesRowDestination holds func(plan any) bool.
	PreservesRowDestination = NewSlot("cql.preservesRowDestination")
)

// Package cql's Result, which the engine fills in. The functions take and
// return Results by value, so no Result escapes through the indirect call.
var (
	// CloneResult holds func(cql.Result) cql.Result.
	CloneResult = NewSlot("cql.cloneResult")
	// MergeResult holds func(dst, src cql.Result) cql.Result.
	MergeResult = NewSlot("cql.mergeResult")
	// ResultAddWarnings holds func(cql.Result, []string) cql.Result.
	ResultAddWarnings = NewSlot("cql.Result.addWarnings")
	// ResultAddCQLTrace holds func(cql.Result, cql.CQLTraceRef) cql.Result.
	ResultAddCQLTrace = NewSlot("cql.Result.addCQLTrace")
	// ResultAddCustomPayload holds func(cql.Result, cql.PayloadEntry) cql.Result.
	ResultAddCustomPayload = NewSlot("cql.Result.addCustomPayload")
	// ResultSetDiagnostics holds func(cql.Result, []string, []cql.CQLTraceRef, []cql.PayloadEntry) cql.Result.
	ResultSetDiagnostics = NewSlot("cql.Result.setDiagnostics")
)

// BuiltinFieldTypes holds func(stream bool) []structmap.BuiltinFieldType: the
// Go field types package cql maps without configuration, for RowOf and
// ParamsOf, or for cql.StreamRowOf. cmd/cqlgen reads it.
var BuiltinFieldTypes = NewSlot("cql.builtinFieldTypes")

// Package cqlerr's server errors, which the engine decodes and builds.
var (
	// DecodeServerError holds func(body any, warnings []string,
	// failureReasonMap bool, rateLimitCode int) cqlerr.ServerError. body has
	// the methods I32, U16, U8, Str, Strings, ShortBytes and ReadErrorMap of a
	// response body positioned at an ERROR's code.
	DecodeServerError = NewSlot("cqlerr.decodeServerError")
	// ServerErrorMessage holds func(cqlerr.ServerError, string, []string): it
	// sets a server error's message and warnings, for engine tests.
	ServerErrorMessage = NewSlot("cqlerr.serverErrorMessage")
)

// Package metadata's Snapshot, a handle on the engine's snapshot.
var (
	// NewSnapshot holds func(src any) *metadata.Snapshot: it wraps the
	// engine's snapshot src.
	NewSnapshot = NewSlot("metadata.newSnapshot")
	// SnapshotSource holds func(*metadata.Snapshot) any: it returns the
	// engine's snapshot a Snapshot wraps.
	SnapshotSource = NewSlot("metadata.snapshotSource")
)

// Package routing's policies and package metadata's routes, which the engine
// implements. The engine fills these slots; the public packages call them
// once a session exists.
var (
	// BuildPolicy holds func(single bool, options routing.Options, target
	// metadata.Target) routing.Policy: the session's
	// implementation of a policy NewPolicy (or, with single, SingleTarget)
	// describes.
	BuildPolicy = NewSlot("routing.buildPolicy")
	// BuiltinPolicy holds func(routing.Policy) (routing.Policy, bool): the
	// session's implementation behind a policy NewPolicy or SingleTarget
	// returned. Package routing fills it.
	BuiltinPolicy = NewSlot("routing.builtinPolicy")
	// HostRoutes holds func(*metadata.Snapshot) []metadata.Route.
	HostRoutes = NewSlot("routing.hostRoutes")
	// ReplicaRoutes holds func(*metadata.Snapshot, routing.Info) []metadata.Route.
	ReplicaRoutes = NewSlot("routing.replicaRoutes")
	// RouteHost holds func(host unsafe.Pointer) (id cql.UUID, usable bool):
	// what a metadata.Route's host reports.
	RouteHost = NewSlot("metadata.routeHost")
	// RouteShardOf holds func(host unsafe.Pointer, token int64) int32.
	RouteShardOf = NewSlot("metadata.routeShardOf")
)

// NewSubscription holds func(events <-chan event.Event, close func())
// *event.Subscription: package event's handle on a session's subscription.
var NewSubscription = NewSlot("event.newSubscription")

// IsMurmur3Partitioner holds func(routing.TokenPartitioner) bool: whether a
// partitioner is routing's built-in Murmur3 one.
var IsMurmur3Partitioner = NewSlot("routing.isMurmur3Partitioner")

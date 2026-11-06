//go:build integration

/*
 * Copyright (C) 2026 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package integration

import (
	"fmt"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	frm "github.com/scylladb/gocql/v2/internal/frame"
)

// Independent native request fixtures produce byte oracles for the canonical
// span writer. These own already-encoded test bytes only; no production request
// can enter them. Oracle self-tests are distinct from typed writer regressions.
type fixtureQueryValue struct {
	name    string
	value   []byte
	isUnset bool
}

type fixtureQueryParams struct {
	nowInSeconds          *int
	keyspace              string
	values                []fixtureQueryValue
	pagingState           []byte
	pageSize              int
	defaultTimestampValue int64
	consistency           cql.Consistency
	serialConsistency     cql.Consistency
	skipMeta              bool
	defaultTimestamp      bool
}

func (q fixtureQueryParams) String() string {
	return fmt.Sprintf("[query_params consistency=%v skip_meta=%v page_size=%d paging_state=%q serial_consistency=%v default_timestamp=%v values=%v keyspace=%s now_in_seconds=%v]",
		q.consistency, q.skipMeta, q.pageSize, q.pagingState, q.serialConsistency, q.defaultTimestamp, q.values, q.keyspace, q.nowInSeconds)
}

func fixtureWriteQueryParams(f *framer, opts *fixtureQueryParams) error {
	// Validated again here, not only in the callers' BuildFrame: this function is
	// package-internal and nothing else would enforce the precondition.
	if err := f.ValidateV5Options(opts.keyspace, opts.nowInSeconds); err != nil {
		return err
	}

	f.WriteConsistency(opts.consistency)

	var flags uint32
	names := false

	if len(opts.values) > 0 {
		flags |= frm.FlagValues
	}
	if opts.skipMeta {
		flags |= frm.FlagSkipMetaData
	}
	if opts.pageSize > 0 {
		flags |= frm.FlagPageSize
	}
	if len(opts.pagingState) > 0 {
		flags |= frm.FlagWithPagingState
	}
	if opts.serialConsistency > 0 {
		flags |= frm.FlagWithSerialConsistency
	}

	// protoV3 specific things
	if opts.defaultTimestamp {
		flags |= frm.FlagDefaultTimestamp
	}

	if len(opts.values) > 0 && opts.values[0].name != "" {
		flags |= frm.FlagWithNameValues
		names = true
	}

	if opts.keyspace != "" {
		flags |= frm.FlagWithKeyspace
	}

	if opts.nowInSeconds != nil {
		flags |= frm.FlagWithNowInSeconds
	}

	if f.Proto > protoVersion4 {
		f.WriteUint(flags)
	} else {
		f.WriteByteValue(byte(flags))
	}

	if n := len(opts.values); n > 0 {
		f.WriteShort(uint16(n))

		for i := 0; i < n; i++ {
			if names {
				f.WriteString(opts.values[i].name)
			}
			if opts.values[i].isUnset {
				f.WriteUnset()
			} else {
				f.WriteBytes(opts.values[i].value)
			}
		}
	}

	if opts.pageSize > 0 {
		f.WriteInt(int32(opts.pageSize))
	}

	if len(opts.pagingState) > 0 {
		f.WriteBytes(opts.pagingState)
	}

	if opts.serialConsistency > 0 {
		f.WriteConsistency(opts.serialConsistency)
	}

	if opts.defaultTimestamp {
		// timestamp in microseconds
		var ts int64
		if opts.defaultTimestampValue != 0 {
			ts = opts.defaultTimestampValue
		} else {
			ts = time.Now().UnixNano() / 1000
		}
		f.WriteLong(ts)
	}

	if opts.keyspace != "" {
		f.WriteString(opts.keyspace)
	}

	if opts.nowInSeconds != nil {
		// Bounds already validated at the top of this function.
		f.WriteInt(int32(*opts.nowInSeconds))
	}
	return nil
}

type fixtureExecuteFrame struct {
	customPayload    map[string][]byte
	preparedID       []byte
	resultMetadataID []byte
	params           fixtureQueryParams
}

func (e *fixtureExecuteFrame) String() string {
	return fmt.Sprintf("[execute id=% X params=%v]", e.preparedID, &e.params)
}

func (e *fixtureExecuteFrame) BuildFrame(fr *framer, streamID int) error {
	return fixtureWriteExecute(fr, streamID, e.preparedID, e.resultMetadataID, &e.params, &e.customPayload)
}

func fixtureWriteExecute(f *framer, streamID int, preparedID, resultMetadataID []byte, params *fixtureQueryParams, customPayload *map[string][]byte) error {
	// Validate first, as in fixtureQueryFrame: the prepared id (and, on v5, the
	// result metadata id) are written before writeFixtureQueryParams runs.
	if err := f.ValidateV5Options(params.keyspace, params.nowInSeconds); err != nil {
		return err
	}
	writeResultMetadataID := f.Proto > protoVersion4 || f.ScyllaUseMetadataID
	if err := validateShortBytes("prepared id", preparedID); err != nil {
		return err
	}
	if writeResultMetadataID {
		if err := validateShortBytes("result metadata id", resultMetadataID); err != nil {
			return err
		}
	}

	if len(*customPayload) > 0 {
		f.Payload()
	}
	f.WriteHeader(f.Flags, frm.OpExecute, streamID)
	f.WriteCustomPayload(customPayload)
	if err := f.WriteShortBytes(preparedID); err != nil {
		return err
	}

	if writeResultMetadataID {
		if err := f.WriteShortBytes(resultMetadataID); err != nil {
			return err
		}
	}

	if err := fixtureWriteQueryParams(f, params); err != nil {
		return err
	}

	return f.Finish()
}

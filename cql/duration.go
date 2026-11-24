/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package cql

import (
	"fmt"
	"strconv"
	"strings"
)

// Duration is a CQL duration: months, days and nanoseconds, which the server
// keeps apart. The zero Duration has zero length.
type Duration struct {
	// Months is the number of months.
	Months int32
	// Days is the number of days.
	Days int32
	// Nanoseconds is the number of nanoseconds.
	Nanoseconds int64
}

// TimeUUID is a version 1 UUID with canonical string, JSON and text forms.
// String can format zero for diagnostics; JSON/text serialization, checked
// parsing and CQL binding reject zero and other non-v1 values.
type TimeUUID UUID

// String returns the duration in CQL's unit form, such as "1y2mo3d4h5m6s7ms",
// with a leading "-" for a negative duration and "0s" for zero. Components
// with mixed signs, which the server rejects, print as {months days ns}.
func (d Duration) String() string {
	if !durationSignsValid(d) {
		return fmt.Sprintf("{%d %d %d}", d.Months, d.Days, d.Nanoseconds)
	}
	if d == (Duration{}) {
		return "0s"
	}
	var b strings.Builder
	months, days, nanos := uint64(d.Months), uint64(d.Days), uint64(d.Nanoseconds)
	if d.Months < 0 || d.Days < 0 || d.Nanoseconds < 0 {
		b.WriteByte('-')
		months, days, nanos = uint64(-int64(d.Months)), uint64(-int64(d.Days)), -nanos
	}
	unit := func(n uint64, name string) {
		if n != 0 {
			b.WriteString(strconv.FormatUint(n, 10))
			b.WriteString(name)
		}
	}
	unit(months/12, "y")
	unit(months%12, "mo")
	unit(days, "d")
	for _, u := range [...]struct {
		name string
		size uint64
	}{{"h", 3600e9}, {"m", 60e9}, {"s", 1e9}, {"ms", 1e6}, {"us", 1e3}, {"ns", 1}} {
		unit(nanos/u.size, u.name)
		nanos %= u.size
	}
	return b.String()
}

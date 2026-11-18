/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cql

import (
	"errors"
)

type directTestRowDecoder struct {
	address       *int32
	decodes, into *int
}
type directTestCompiledRow struct{ directTestRowDecoder }

func (d directTestRowDecoder) Compile(Columns) (CompiledRow[int32], error) {
	return directTestCompiledRow{d}, nil
}
func (d directTestRowDecoder) Clone(v int32) int32 { return v }
func (p directTestCompiledRow) Decode(r *RowReader) (int32, error) {
	*p.decodes++
	return r.ReadColumn(Int32Codec)
}
func (p directTestCompiledRow) DecodeInto(r *RowReader, dst *int32) error {
	*p.into++
	if dst != p.address {
		return errors.New("destination address changed")
	}
	return readColumnInto(r, Int32Codec, dst)
}
func (p directTestCompiledRow) Clone(v int32) int32 { return v }

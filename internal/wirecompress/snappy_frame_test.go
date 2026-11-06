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
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package wirecompress

import (
	"os"
)

type frameExample struct {
	Name     string
	Frame    []byte
	FilePath string
}

var frameExamples = struct {
	Requests  []frameExample
	Responses []frameExample
}{
	Requests: []frameExample{
		{
			Name:     "Small query request",
			FilePath: "../../testdata/frames/small_query_request.bin",
		},
		{
			Name:     "Medium query request",
			FilePath: "../../testdata/frames/medium_query_request.bin",
		},
		{
			Name:     "Big query request",
			FilePath: "../../testdata/frames/big_query_request.bin",
		},
		{
			Name:     "Prepare statement request",
			FilePath: "../../testdata/frames/prepare_statement_request.bin",
		},
	},
	Responses: []frameExample{
		{
			Name:     "Small query response",
			FilePath: "../../testdata/frames/small_query_response.bin",
		},
		{
			Name:     "Medium query response",
			FilePath: "../../testdata/frames/medium_query_response.bin",
		},
		{
			Name:     "Big query response",
			FilePath: "../../testdata/frames/big_query_response.bin",
		},
		{
			Name:     "Prepare statement response",
			FilePath: "../../testdata/frames/prepare_statement_response.bin",
		},
	},
}

func init() {
	var err error
	for id, def := range frameExamples.Requests {
		frameExamples.Requests[id].Frame, err = os.ReadFile(def.FilePath)
		if err != nil {
			panic("can't read file " + def.FilePath)
		}
	}
	for id, def := range frameExamples.Responses {
		frameExamples.Responses[id].Frame, err = os.ReadFile(def.FilePath)
		if err != nil {
			panic("can't read file " + def.FilePath)
		}
	}
}

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

package logging

import (
	"bytes"
	"log/slog"
)

// Printfer is satisfied by *log.Logger and most printf-style loggers.
type Printfer interface {
	Printf(format string, v ...any)
}

// NewPrintfHandler formats each record at or above level as one logfmt line and passes it
// to p.Printf. The time attribute is dropped because printf loggers add their own.
func NewPrintfHandler(p Printfer, level slog.Leveler) slog.Handler {
	return slog.NewTextHandler(printfWriter{p: p}, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) == 0 && a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	})
}

type printfWriter struct{ p Printfer }

func (w printfWriter) Write(b []byte) (int, error) {
	w.p.Printf("%s", bytes.TrimSuffix(b, []byte("\n")))
	return len(b), nil
}

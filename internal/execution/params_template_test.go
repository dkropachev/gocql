// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import "testing"

// Queries without their own options share one session template per encoded
// combination; a query with its own options keeps a template of its own; an
// execution with its own options and no query encodes field by field.
func TestParamsTemplateCacheCases(t *testing.T) {
	rt := &InternalSessionRuntime{}
	sessionOptions := func() *attemptRequest {
		return &attemptRequest{PageSize: 5000, DefaultTimestamp: true, Timestamp: 1}
	}
	var none OptionValues

	// Case 1: no custom options. The second query and a later execution
	// with another timestamp value reuse the first query's template.
	var a, b queryState
	ta := rt.paramsTemplate(&a, &none, sessionOptions(), 4)
	if ta == nil {
		t.Fatal("no template for session options")
	}
	w := sessionOptions()
	w.Timestamp = 99
	if tb := rt.paramsTemplate(&b, &none, w, 4); tb != ta {
		t.Fatal("second query without options did not share the session template")
	}
	if got := rt.paramsTemplate(&a, &none, w, 4); got != ta {
		t.Fatal("the query's own slot missed for an equal request")
	}
	// Executions without a query share it too.
	if got := rt.paramsTemplate(nil, &none, sessionOptions(), 4); got != ta {
		t.Fatal("an execution without a query did not share the session template")
	}
	// Another protocol, or a conditional statement without a timestamp, is
	// another shared template.
	if got := rt.paramsTemplate(&a, &none, sessionOptions(), 5); got == ta || got == nil || got.Protocol != 5 {
		t.Fatal("protocol v5 reused the v4 template")
	}
	noTimestamp := sessionOptions()
	noTimestamp.DefaultTimestamp = false
	if got := rt.paramsTemplate(&b, &none, noTimestamp, 4); got == ta || got == nil || got.DefaultTimestamp {
		t.Fatal("a request without a timestamp reused the timestamped template")
	}
	shared := rt.paramsTemplates.find(sessionOptions(), 4)
	if shared != ta {
		t.Fatal("session template not in the shared slots")
	}

	// Case 2: custom options on the query. The template is the query's own
	// and stays out of the shared slots.
	custom := OptionValues{PageSize: Option[int32]{Set: true, Value: 10}}
	var c queryState
	wc := &attemptRequest{PageSize: 10, DefaultTimestamp: true}
	tc := rt.paramsTemplate(&c, &custom, wc, 4)
	if tc == nil || tc.PageSize != 10 {
		t.Fatal("no template for the query's own options")
	}
	if rt.paramsTemplates.find(wc, 4) != nil {
		t.Fatal("a query's own template entered the shared slots")
	}
	if got := rt.paramsTemplate(&c, &custom, &attemptRequest{PageSize: 10, DefaultTimestamp: true, Timestamp: 7}, 4); got != tc {
		t.Fatal("the query's own template was rebuilt")
	}
	// Executions with their own options and no query have nowhere to keep
	// a template and encode field by field.
	if got := rt.paramsTemplate(nil, &custom, wc, 4); got != nil {
		t.Fatal("an execution with its own options and no query got a template")
	}
	// Batches never use one.
	if got := rt.paramsTemplate(&a, &none, &attemptRequest{Batch: &typedBatchRequest{}}, 4); got != nil {
		t.Fatal("a batch got a template")
	}
}

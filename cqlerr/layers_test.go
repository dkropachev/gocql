// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
)

// layers wraps err the way the driver does, alone and next to other errors.
func layers(err error) map[string]error {
	attempt := newQueryError(queryErrorState{InternalErr: err, InternalPotentiallyExecuted: true})
	wrapped := fmt.Errorf("gocql: prepare: %w", attempt)
	other := NewOtherServerError(CodeOverloaded, "other")
	return map[string]error{
		"QueryError":          attempt,
		"fmt.Errorf":          wrapped,
		"ExecutionError.Err":  newExecutionError(wrapped, cql.Result{}, MaybeExecuted, []AttemptFailure{{Err: other}, {Err: wrapped}}),
		"ExecutionError.Prev": newExecutionError(context.Canceled, cql.Result{}, MaybeExecuted, []AttemptFailure{{Err: wrapped}, {Err: other}}),
		"ConnectError":        &ConnectError{Stage: "s", Hosts: []HostError{{Address: "a", Err: other}, {Address: "b", Err: wrapped}}},
		"NoHostsError":        &NoHostsError{Operation: "o", Causes: []error{other, wrapped}},
		"PartialRefreshError": &PartialRefreshError{Topology: other, Keyspaces: map[string]error{"ks": wrapped}},
		"errors.Join":         errors.Join(other, wrapped),
		"DDL":                 fmt.Errorf("gocql: DDL succeeded; automatic agreement/refresh failed: %w", newExecutionError(wrapped, cql.Result{}, PartiallyExecuted, nil)),
	}
}

// TestErrorsAsIsThroughLayers checks that errors.Is finds every error type
// and sentinel, errors.As every error type, and errors.Is a server error's
// code, through every layer the driver wraps an error in.
func TestErrorsAsIsThroughLayers(t *testing.T) {
	t.Parallel()
	typed := errorTypes()
	typed["cql.ValidationError"] = &cql.ValidationError{Field: "f", Err: errors.New("bad")}
	typed["cql.CodecError"] = &cql.CodecError{Stage: "decode", Column: "c", Err: errors.New("bad")}
	typed["cql.ProtocolError"] = &cql.ProtocolError{Stage: "rows", Err: errors.New("bad")}
	leaves := map[string]error{"context.Canceled": context.Canceled}
	for name, s := range sentinels {
		leaves[name] = s.err
	}
	for name, err := range typed {
		leaves[name] = err
	}
	// The layers are of these types themselves, so errors.As may find a
	// layer before a leaf of the same type.
	layerTypes := map[reflect.Type]bool{}
	for _, err := range []error{&QueryError{}, &ExecutionError{}, &ConnectError{}, &NoHostsError{}, &PartialRefreshError{}, &OtherServerError{}} {
		layerTypes[reflect.TypeOf(err)] = true
	}
	for name, leaf := range leaves {
		for layer, top := range layers(leaf) {
			if !errors.Is(top, leaf) {
				t.Errorf("%s in %s: errors.Is lost it", name, layer)
			}
			if server, ok := leaf.(ServerError); ok && !errors.Is(top, server.Code()) {
				t.Errorf("%s in %s: errors.Is lost code %v", name, layer, server.Code())
			}
			if _, ok := typed[name]; !ok {
				continue // a sentinel's type is errors' own
			}
			target := reflect.New(reflect.TypeOf(leaf))
			if !errors.As(top, target.Interface()) {
				t.Errorf("%s in %s: errors.As lost its type", name, layer)
			} else if target.Elem().Interface() != leaf && !layerTypes[reflect.TypeOf(leaf)] {
				t.Errorf("%s in %s: errors.As found another %T", name, layer, leaf)
			}
		}
	}
}

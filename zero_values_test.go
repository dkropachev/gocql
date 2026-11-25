// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql_test

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/endpoint"
	"github.com/scylladb/gocql/v2/event"
	"github.com/scylladb/gocql/v2/exec"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/retry"
)

// TestStructZeroValueDocs checks that every exported struct type of the
// public packages says what its zero value is: usable, and what it means, or
// rejected with a cql.ValidationError.
func TestStructZeroValueDocs(t *testing.T) {
	api, err := os.ReadFile(apiGoldenPath)
	if err != nil {
		t.Fatal(err)
	}
	structLine := regexp.MustCompile(`^\ttype (\w+)\.(\w+)(\[[^\]]*\])? struct$`)
	docs := map[string]string{}
	for _, line := range strings.Split(string(api), "\n") {
		m := structLine.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		pkg, name := m[1], m[2]
		dir := pkg
		if pkg == "gocql" {
			dir = "."
		}
		if _, ok := docs[dir]; !ok {
			docs[dir] = ""
			for typ, doc := range typeDocs(t, dir) {
				docs[dir+"."+typ] = doc
			}
		}
		doc, ok := docs[dir+"."+name]
		if !ok {
			t.Errorf("%s.%s: no declaration found in %s", pkg, name, dir)
			continue
		}
		if !regexp.MustCompile(`(?i)\bzero\b`).MatchString(doc) {
			t.Errorf("%s.%s does not document its zero value", pkg, name)
		}
	}
}

// typeDocs returns the doc comment of every exported type declared in the
// package in dir.
func typeDocs(t *testing.T, dir string) map[string]string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	docs := map[string]string{}
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		// go/parser does not accept generic methods yet; it still returns the
		// whole file, which is all this needs.
		file, _ := parser.ParseFile(fset, name, nil, parser.ParseComments)
		if file == nil {
			t.Fatalf("parse %s", name)
		}
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			for _, spec := range gen.Specs {
				ts := spec.(*ast.TypeSpec)
				doc := ts.Doc
				if doc == nil && len(gen.Specs) == 1 {
					doc = gen.Doc
				}
				if ts.Name.IsExported() && doc != nil {
					docs[ts.Name.Name] = doc.Text()
				}
			}
		}
	}
	return docs
}

// requireValidation fails unless err is a cql.ValidationError.
func requireValidation(t *testing.T, what string, err error) {
	t.Helper()
	if _, ok := errors.AsType[*cql.ValidationError](err); !ok {
		t.Errorf("%s: error = %v (%T), want a cql.ValidationError", what, err, err)
	}
}

// TestZeroValuesRejected checks the zero values documented as rejected.
func TestZeroValuesRejected(t *testing.T) {
	ctx := context.Background()
	_, err := gocql.Connect(ctx, config.Config{})
	requireValidation(t, "Connect(zero Config)", err)
	_, err = config.New(config.Dialect(0), "127.0.0.1").Build()
	requireValidation(t, "config.New(zero Dialect).Build", err)
	_, err = cql.ListType(cql.DataType{})
	requireValidation(t, "ListType(zero DataType)", err)

	_, err = new(config.ConfigBuilder).Build()
	requireValidation(t, "zero ConfigBuilder.Build", err)
	_, err = new(config.ConfigBuilder).
		WithDefaultProfile(config.NewProfile().WithPageSize(10).MustBuild()).
		WithPartitioner("p", nil).
		Build()
	requireValidation(t, "zero ConfigBuilder with settings", err)
	_, err = new(config.ProfileBuilder).WithPageSize(10).Build()
	requireValidation(t, "zero ProfileBuilder.Build", err)
	_, err = config.New(config.ScyllaV4, "127.0.0.1").WithDefaultProfile(config.Profile{}).Build()
	requireValidation(t, "WithDefaultProfile(zero Profile)", err)
	_, err = config.NewTLSFileSource(nil, config.TLSFiles{})
	requireValidation(t, "NewTLSFileSource(zero TLSFiles)", err)

	var source config.TLSFileSource
	if source.TLSConfig() != nil {
		t.Error("zero TLSFileSource returned a TLS configuration")
	}
	requireValidation(t, "zero TLSFileSource.Reload", source.Reload())
	_, err = config.New(config.ScyllaV4, "127.0.0.1").WithTLSSource(&source).Build()
	requireValidation(t, "WithTLSSource(zero TLSFileSource)", err)

	var session gocql.Session
	if !session.Closed() || session.Dialect() != 0 || session.Metadata() != nil || !session.ID().IsZero() {
		t.Errorf("zero Session: Closed %v, Dialect %v, Metadata %v, ID %v", session.Closed(), session.Dialect(), session.Metadata(), session.ID())
	}
	requireValidation(t, "zero Session.Close", session.Close())
	requireValidation(t, "zero Session.AwaitSchemaAgreement", session.AwaitSchemaAgreement(ctx))
	snapshot, err := session.RefreshMetadata(ctx)
	requireValidation(t, "zero Session.RefreshMetadata", err)
	if snapshot != nil {
		t.Errorf("zero Session.RefreshMetadata returned a snapshot")
	}
	_, err = session.Subscribe(event.Filter{})
	requireValidation(t, "zero Session.Subscribe", err)
	_, err = session.CompileOptions(exec.Options{})
	requireValidation(t, "zero Session.CompileOptions", err)
	stmt := cql.Statement[cql.EmptyParams, cql.EmptyRow]{CQL: "SELECT now() FROM system.local"}
	_, err = session.UnpreparedExec(ctx, stmt, cql.EmptyParams{})
	requireValidation(t, "zero Session.UnpreparedExec", err)
	_, err = session.Prepare(ctx, stmt)
	requireValidation(t, "zero Session.Prepare", err)
	requireValidation(t, "zero Session.UnpreparedAddToBatch", session.UnpreparedAddToBatch(new(gocql.Batch), stmt, cql.EmptyParams{}))
	_, _, err = session.UnpreparedOne(ctx, stmt, cql.EmptyParams{}, new(cql.EmptyRow))
	requireValidation(t, "zero Session.UnpreparedOne", err)
	_, err = session.UnpreparedEach(ctx, stmt, cql.EmptyParams{}, new(cql.EmptyRow), func(*cql.EmptyRow) error { return nil })
	requireValidation(t, "zero Session.UnpreparedEach", err)
	for _, err = range session.UnpreparedAll(ctx, stmt, cql.EmptyParams{}, new(cql.EmptyRow)) {
		break
	}
	requireValidation(t, "zero Session.UnpreparedAll", err)

	var prepared gocql.Prepared[cql.EmptyParams, cql.EmptyRow]
	_, err = prepared.Exec(ctx, cql.EmptyParams{})
	requireValidation(t, "zero Prepared.Exec", err)
	_, err = prepared.Worker(exec.Options{})
	requireValidation(t, "zero Prepared.Worker", err)
	requireValidation(t, "zero Prepared.AddToBatch", prepared.AddToBatch(new(gocql.Batch), cql.EmptyParams{}))
	_, err = new(gocql.PreparedStream[cql.EmptyParams, cql.EmptyRow]).Worker(exec.StreamOptions{})
	requireValidation(t, "zero PreparedStream.Worker", err)
	var query gocql.Worker[cql.EmptyParams, cql.EmptyRow]
	_, err = query.Exec(ctx, cql.EmptyParams{})
	requireValidation(t, "zero Worker.Exec", err)
	query.Close()
	var streamQuery gocql.StreamWorker[cql.EmptyParams, cql.EmptyRow]
	_, err = streamQuery.Run(ctx, cql.EmptyParams{})
	requireValidation(t, "zero StreamWorker.Run", err)
	streamQuery.Close()
	_, err = gocql.CASBatchRows[cql.EmptyRow]{}.Applied()
	requireValidation(t, "zero CASBatchRows.Applied", err)

	_, err = cql.Value{}.Signed()
	requireValidation(t, "zero Value.Signed", err)
	_, err = cql.DecodeValue(cql.Int32Codec, cql.EncodedValue{})
	requireValidation(t, "DecodeValue(zero EncodedValue)", err)
	requireValidation(t, "zero ParamWriter.WriteParam", new(cql.ParamWriter).WriteParam(cql.Int32Codec, 1))
	requireValidation(t, "zero RowReader.SkipColumn", new(cql.RowReader).SkipColumn())
	requireValidation(t, "zero ValueWriter.Null", new(cql.ValueWriter).Null())
	_, err = new(cql.ValueCursor).Uint8()
	requireValidation(t, "zero ValueCursor.Uint8", err)
	_, err = (cql.ValueReader{}).Cursor().Uint8()
	if _, ok := errors.AsType[*cql.ProtocolError](err); !ok {
		t.Errorf("zero ValueReader.Cursor().Uint8: error = %v, want a cql.ProtocolError", err)
	}
	requireValidation(t, "zero StreamValueReader.Skip", new(cql.StreamValueReader).Skip())
	requireValidation(t, "zero StreamRowReader.SkipColumn", new(cql.StreamRowReader).SkipColumn())
}

// TestZeroValuesUsable checks the zero values documented as usable.
func TestZeroValuesUsable(t *testing.T) {
	var batch gocql.Batch
	if batch.Kind != cql.LoggedBatch || batch.Len() != 0 || batch.Size() != 0 {
		t.Errorf("zero Batch: kind %v, %d statements, %d bytes", batch.Kind, batch.Len(), batch.Size())
	}
	batch.Reset()
	if zero := (cql.PageState{}); zero.Complete() || zero.More() || zero.Token() != nil {
		t.Error("the zero PageState does not start paging")
	}
	var row cql.EmptyRow
	var rows gocql.Rows[cql.EmptyRow]
	if rows.NextInto(&row) || rows.Err() != nil || rows.Close() != nil {
		t.Error("zero Rows is not empty")
	}
	var page gocql.Page[cql.EmptyRow]
	if page.NextInto(&row) || page.Err() != nil || page.Close() != nil || !page.State().Complete() {
		t.Error("zero Page is not empty and complete")
	}
	var stream gocql.RowStream[cql.EmptyRow]
	if stream.NextInto(&row) || stream.Err() != nil || stream.Close() != nil {
		t.Error("zero RowStream is not empty")
	}
	new(gocql.PageLease).Close()
	var sub event.Subscription
	if _, open := <-sub.Events(); open {
		t.Error("zero Subscription has an open queue")
	}
	sub.Close()
	if route := (metadata.Route{}); route.Usable() || !route.HostID().IsZero() {
		t.Error("the zero Route names a host")
	}
	if (retry.Decision{}).Action != retry.Stop {
		t.Error("the zero Decision does not stop")
	}
	unavailable := &cqlerr.UnavailableError{}
	if d := (retry.Safe{}).Decide(retry.Attempt{Attempt: 2, Err: unavailable}); d.Action != retry.Next {
		t.Errorf("zero Safe stopped after attempt 2: %v", d.Action)
	}
	if d := (retry.Safe{}).Decide(retry.Attempt{Attempt: 3, Err: unavailable}); d.Action != retry.Stop {
		t.Errorf("zero Safe retried after attempt 3: %v", d.Action)
	}
	if !new(endpoint.SimpleConvictionPolicy).AddFailure(endpoint.ConvictionEvent{}) {
		t.Error("zero SimpleConvictionPolicy did not convict")
	}
	if n := (cql.Varint{}).BigInt(); n.Sign() != 0 {
		t.Errorf("zero Varint = %v", n)
	}
	if n := (cql.Decimal{}).Unscaled.BigInt(); n.Sign() != 0 {
		t.Errorf("zero Decimal unscaled = %v", n)
	}
	// The driver's error types print without detail rather than panic.
	for _, err := range []error{
		new(cqlerr.ExecutionError), new(cqlerr.ConnectError),
		new(cqlerr.UnavailableError), new(cqlerr.OtherServerError), new(cqlerr.WriteTimeoutError),
		new(cqlerr.ReadTimeoutError), new(cqlerr.WriteFailureError), new(cqlerr.ReadFailureError),
		new(cqlerr.AlreadyExistsError), new(cqlerr.UnpreparedError),
		new(cqlerr.FunctionFailureError), new(cqlerr.CASWriteUnknownError), new(cqlerr.RateLimitReachedError),
		new(cqlerr.PartialRefreshError), new(cqlerr.NoHostsError), new(cqlerr.SchemaChangedError),
		new(cqlerr.UnsupportedSettingError), new(cqlerr.ServerMismatchError), new(cql.ValidationError),
		new(cql.CodecError), new(cql.ProtocolError), new(cql.StreamCapacityError), new(cql.StreamCapabilityError),
	} {
		if err.Error() == "" {
			t.Errorf("zero %T prints nothing", err)
		}
	}
}

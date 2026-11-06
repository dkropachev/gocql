//go:build integration

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

package integration

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"

	inf "gopkg.in/inf.v0"
)

type WikiPage struct {
	Title       string
	RevId       cql.UUID
	Body        string
	Views       int64
	Protected   bool
	Modified    time.Time
	Rating      *inf.Dec
	Tags        []string
	Attachments map[string]WikiAttachment
}

type WikiAttachment []byte

var wikiTestData = []*WikiPage{
	{
		Title:    "Frontpage",
		RevId:    cql.NewTimeUUID(),
		Body:     "Welcome to this wiki page!",
		Rating:   inf.NewDec(131, 3),
		Modified: time.Date(2013, time.August, 13, 9, 52, 3, 0, time.UTC),
		Tags:     []string{"start", "important", "test"},
		Attachments: map[string]WikiAttachment{
			"logo":    WikiAttachment("\x00company logo\x00"),
			"favicon": WikiAttachment("favicon.ico"),
		},
	},
	{
		Title:    "Foobar",
		RevId:    cql.NewTimeUUID(),
		Body:     "foo::Foo f = new foo::Foo(foo::Foo::INIT);",
		Modified: time.Date(2013, time.August, 13, 9, 52, 3, 0, time.UTC),
	},
}

type WikiTest struct {
	session *gocql.Session
	tb      testing.TB

	table string
}

func CreateSchema(session *gocql.Session, tb testing.TB, table string) *WikiTest {
	table = serializationTableName(tb, table)
	if err := v2CreateTable(context.Background(), session, fmt.Sprintf(("DROP TABLE IF EXISTS "+fixtureKeyspace("")+".%s"), table)); err != nil {
		tb.Fatal("CreateSchema:", err)
	}

	err := v2CreateTable(context.Background(), session, fmt.Sprintf((`CREATE TABLE `+fixtureKeyspace("")+`.%s (
			title       varchar,
			revid       timeuuid,
			body        varchar,
			views       bigint,
			protected   boolean,
			modified    timestamp,
			rating      decimal,
			tags        set<varchar>,
			attachments map<varchar, blob>,
			PRIMARY KEY (title, revid)
		)`), table))

	if err != nil {
		tb.Fatal("CreateSchema:", err)
	}

	return &WikiTest{
		session: session,
		tb:      tb,
		table:   table,
	}
}

func (w *WikiTest) CreatePages(n int) {
	var page WikiPage
	t0 := time.Now()
	for i := 0; i < n; i++ {
		page.Title = fmt.Sprintf("generated_%d", (i&16)+1)
		page.Modified = t0.Add(time.Duration(i-n) * time.Minute)
		page.RevId = cql.UUIDFromTime(page.Modified)
		page.Body = fmt.Sprintf("text %d", i)
		if err := w.InsertPage(&page); err != nil {
			w.tb.Error("CreatePages:", err)
		}
	}
}

var wikiRevisionCodec = cql.NamedScalar(cql.TimeUUIDCodec, func(v cql.UUID) cql.TimeUUID { return cql.TimeUUID(v) }, func(v cql.TimeUUID) cql.UUID { return cql.UUID(v) })
var wikiRatingCodec = cql.NamedScalar(cql.NullableCodec(serializationDecimal), func(v *inf.Dec) cql.Nullable[inf.Dec] {
	if v == nil {
		return cql.Nullable[inf.Dec]{}
	}
	return cql.Nullable[inf.Dec]{Value: *v, Valid: true}
}, func(v cql.Nullable[inf.Dec]) *inf.Dec {
	if !v.Valid {
		return nil
	}
	return &v.Value
})
var wikiTagsCodec = cql.NamedScalar(cql.NullableCodec(serializationSet(serializationString)), func(v []string) cql.Nullable[[]string] { return cql.Nullable[[]string]{Value: v, Valid: v != nil} }, func(v cql.Nullable[[]string]) []string { return v.Value })
var wikiAttachmentCodec = cql.NamedScalar(serializationBlob, func(v WikiAttachment) []byte { return []byte(v) }, func(v []byte) WikiAttachment { return WikiAttachment(v) })
var wikiAttachmentsCodec = cql.NamedScalar(cql.NullableCodec(serializationMap(serializationString, wikiAttachmentCodec)), func(v map[string]WikiAttachment) cql.Nullable[map[string]WikiAttachment] {
	return cql.Nullable[map[string]WikiAttachment]{Value: v, Valid: v != nil}
}, func(v cql.Nullable[map[string]WikiAttachment]) map[string]WikiAttachment { return v.Value })
var wikiParams = cql.Params(
	cql.Param("title", serializationString, func(v WikiPage) string { return v.Title }),
	cql.Param("revid", wikiRevisionCodec, func(v WikiPage) cql.UUID { return v.RevId }),
	cql.Param("body", serializationString, func(v WikiPage) string { return v.Body }),
	cql.Param("views", cql.Int64, func(v WikiPage) int64 { return v.Views }),
	cql.Param("protected", cql.Bool, func(v WikiPage) bool { return v.Protected }),
	cql.Param("modified", serializationTimestamp, func(v WikiPage) time.Time { return v.Modified }),
	cql.Param("rating", wikiRatingCodec, func(v WikiPage) *inf.Dec { return v.Rating }),
	cql.Param("tags", wikiTagsCodec, func(v WikiPage) []string { return v.Tags }),
	cql.Param("attachments", wikiAttachmentsCodec, func(v WikiPage) map[string]WikiAttachment { return v.Attachments }))
var wikiRows = cql.Row(
	cql.Column("title", serializationString, func(v *WikiPage) *string { return &v.Title }),
	cql.Column("revid", wikiRevisionCodec, func(v *WikiPage) *cql.UUID { return &v.RevId }),
	cql.Column("body", serializationString, func(v *WikiPage) *string { return &v.Body }),
	cql.Column("views", cql.Int64, func(v *WikiPage) *int64 { return &v.Views }),
	cql.Column("protected", cql.Bool, func(v *WikiPage) *bool { return &v.Protected }),
	cql.Column("modified", serializationTimestamp, func(v *WikiPage) *time.Time { return &v.Modified }),
	cql.Column("rating", wikiRatingCodec, func(v *WikiPage) **inf.Dec { return &v.Rating }),
	cql.Column("tags", wikiTagsCodec, func(v *WikiPage) *[]string { return &v.Tags }),
	cql.Column("attachments", wikiAttachmentsCodec, func(v *WikiPage) *map[string]WikiAttachment { return &v.Attachments }))

func (w *WikiTest) InsertPage(page *WikiPage) error {
	return serializationExec(w.session, fmt.Sprintf("INSERT INTO %s (title,revid,body,views,protected,modified,rating,tags,attachments) VALUES(?,?,?,?,?,?,?,?,?)", w.table), wikiParams, *page)
}
func (w *WikiTest) SelectPage(page *WikiPage, title string, revid cql.UUID) error {
	revision, err := cql.TimeUUIDValue(cql.TimeUUID(revid))
	if err != nil {
		return err
	}
	result, err := serializationOne(w.session, fmt.Sprintf("SELECT title,revid,body,views,protected,modified,tags,attachments,rating FROM %s WHERE title=? AND revid=? LIMIT 1", w.table), wikiRows, codecTestMust(cql.TextValue(title)), revision)
	if err == nil {
		*page = result
	}
	return err
}
func (w *WikiTest) GetPageCount() int {
	count, err := serializationOne(w.session, fmt.Sprintf("SELECT COUNT(*) FROM %s", w.table), cql.OneColumn(cql.Int64))
	if err != nil {
		w.tb.Error("GetPageCount", err)
	}
	return int(count)
}

func TestWikiCreateSchema(t *testing.T) {
	t.Parallel()

	session := createV2Session(t)
	defer session.Close()

	CreateSchema(session, t, "create")
}

func TestWikiCreatePages(t *testing.T) {
	t.Parallel()

	session := createV2Session(t)
	defer session.Close()

	w := CreateSchema(session, t, "create_pages")

	numPages := 5
	w.CreatePages(numPages)
	if count := w.GetPageCount(); count != numPages {
		t.Errorf("expected %d pages, got %d pages.", numPages, count)
	}
}

func TestWikiTypicalCRUD(t *testing.T) {
	t.Parallel()

	session := createV2Session(t)
	defer session.Close()

	w := CreateSchema(session, t, "crud")

	for _, page := range wikiTestData {
		if err := w.InsertPage(page); err != nil {
			t.Error("InsertPage:", err)
		}
	}
	if count := w.GetPageCount(); count != len(wikiTestData) {
		t.Errorf("count: expected %d, got %d\n", len(wikiTestData), count)
	}
	for _, original := range wikiTestData {
		page := new(WikiPage)
		if err := w.SelectPage(page, original.Title, original.RevId); err != nil {
			t.Error("SelectPage:", err)
			continue
		}
		sort.Sort(sort.StringSlice(page.Tags))
		sort.Sort(sort.StringSlice(original.Tags))
		if !reflect.DeepEqual(page, original) {
			t.Errorf("page: expected %#v, got %#v\n", original, page)
		}
	}
}

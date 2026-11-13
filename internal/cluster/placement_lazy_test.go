// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"sync/atomic"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/transport"
)

// TestTabletIdentityLazy checks that a request reads its table's schema
// source identity only when its response carries a tablet hint: capturing
// the feedback stamp and receiving a response without a hint read none.
func TestTabletIdentityLazy(t *testing.T) {
	var lookups atomic.Int64
	testHookSchemaIdentity = func() { lookups.Add(1) }
	t.Cleanup(func() { testHookSchemaIdentity = nil })
	s := testSession(t)
	info := model.RoutingInfo{Keyspace: "ks", Table: "table", Token: cqltype.Option[int64]{Value: 1, Set: true}}
	features := transport.InternalFramerConfig{InternalTabletsRoutingV1: true, InternalTabletsRoutingV2: true}

	stamp := s.CaptureTabletFeedback(info, s.Metadata())
	s.ApplyTypedFeedbackStamped(&transport.InternalResponsePage{}, info, stamp, features)
	if n := lookups.Load(); n != 0 {
		t.Fatalf("a response without a tablet hint read the schema identity %d times", n)
	}

	h := testHint(t, 0, 100, cqltype.Option[uint64]{Value: 1, Set: true}, hTarget(1, 0))
	page := &transport.InternalResponsePage{ResponseData: model.ResponseData{Extras: &model.ResponseExtras{Payload: []model.PayloadEntry{{Key: "tablets-routing-v2", Value: cqltype.Nullable[cqltype.Bytes]{Valid: true, Value: cqltype.BorrowBytes(testPayload(h))}}}}}}
	s.ApplyTypedFeedbackStamped(page, info, stamp, features)
	if lookups.Load() == 0 {
		t.Fatal("a tablet hint was checked without the schema identity")
	}
	p := s.Peripheral()
	p.InternalFeedbackMu.Lock()
	pending := len(p.InternalFeedback)
	p.InternalFeedbackMu.Unlock()
	if pending != 1 {
		t.Fatalf("the hint was not queued: %d pending", pending)
	}
}

// TestTabletIdentityCapturedSource checks that a hint is checked against
// the schema source as it was when the request was sent: a table dropped
// and recreated meanwhile has a new identity, and its hint is dropped.
func TestTabletIdentityCapturedSource(t *testing.T) {
	s := testSession(t)
	info := model.RoutingInfo{Keyspace: "ks", Table: "table", Token: cqltype.Option[int64]{Value: 1, Set: true}}
	features := transport.InternalFramerConfig{InternalTabletsRoutingV1: true, InternalTabletsRoutingV2: true}
	md := MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata)
	ks, _ := md.InternalKeyspaceMetadata.InternalGetKeyspace("ks")

	// The source changed before the request was sent: the stamp sees it.
	recreated := ks.Clone()
	table := *recreated.Tables["table"]
	table.ID = "identity-2"
	recreated.Tables["table"] = &table
	md.InternalKeyspaceMetadata.InternalSet("ks", recreated)
	stamp := s.CaptureTabletFeedback(info, s.Metadata())
	md.InternalKeyspaceMetadata.InternalSet("ks", ks)

	h := testHint(t, 0, 100, cqltype.Option[uint64]{Value: 1, Set: true}, hTarget(1, 0))
	page := &transport.InternalResponsePage{ResponseData: model.ResponseData{Extras: &model.ResponseExtras{Payload: []model.PayloadEntry{{Key: "tablets-routing-v2", Value: cqltype.Nullable[cqltype.Bytes]{Valid: true, Value: cqltype.BorrowBytes(testPayload(h))}}}}}}
	s.ApplyTypedFeedbackStamped(page, info, stamp, features)
	p := s.Peripheral()
	p.InternalFeedbackMu.Lock()
	pending := len(p.InternalFeedback)
	p.InternalFeedbackMu.Unlock()
	if pending != 0 {
		t.Fatalf("a hint for a source identity that differed from the snapshot's was queued: %d pending", pending)
	}
}

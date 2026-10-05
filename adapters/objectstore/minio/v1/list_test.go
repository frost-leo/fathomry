/**
 * fathomry
 * Copyright (C) 2026  Frost Leo
 * SPDX-License-Identifier: GPL-3.0-or-later
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package minio

import (
	"errors"
	"io"
	"net/http"
	"testing"
)

func TestListingProtocolFailuresRetainPublicEvidence(t *testing.T) {
	peer := newPeer(t)
	test := newFixture(t, peer.options)
	ctx := testContext(t)
	peer.mu.Lock()
	peer.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		_, _ = io.WriteString(writer, "<ListBucketResult><Name>other</Name><IsTruncated>false</IsTruncated><Contents><Key>owned/key</Key><Size>0</Size></Contents></ListBucketResult>")
		return true
	}
	peer.mu.Unlock()
	receipt, err := test.client.List(ctx, ListRequest{Prefix: "owned/"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil || !errors.Is(snapshot.Err(), ErrProtocol) {
		t.Fatal("public protocol error missing")
	}
	facts, present := snapshot.ValueCopy()
	if !present || facts.Complete() || len(facts.ObjectsCopy()) != 0 {
		t.Fatal("invalid object observations escaped")
	}
	delivery, err := test.inbox.NextReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Retry(); err != nil {
		t.Fatal(err)
	}
	before := peer.requests()
	delivery, err = test.inbox.NextReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	independent, _ := delivery.Receipt()
	evidence, err := independent.WaitReleased(ctx)
	if err != nil || !errors.Is(evidence.Err(), ErrProtocol) || evidence.Info().Sequence != snapshot.Info().Sequence || peer.requests() != before {
		t.Fatal("evidence was lost or replayed")
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
}

func TestCursorEvidenceDoesNotWaitForRoot(t *testing.T) {
	peer := newPeer(t)
	options := peer.options
	options.MaxEntries = 1
	test := newFixture(t, options)
	ctx := testContext(t)
	peer.mu.Lock()
	peer.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.URL.Query().Get("continuation-token") == "" {
			_, _ = io.WriteString(writer, "<ListBucketResult><Name>fixture</Name><IsTruncated>true</IsTruncated><NextContinuationToken>opaque+%</NextContinuationToken><Contents><Key>owned/one</Key><Size>1</Size></Contents></ListBucketResult>")
		} else {
			if request.URL.Query().Get("continuation-token") != "opaque+%" {
				t.Error("continuation changed")
			}
			_, _ = io.WriteString(writer, "<ListBucketResult><Name>fixture</Name><IsTruncated>false</IsTruncated></ListBucketResult>")
		}
		return true
	}
	peer.mu.Unlock()
	cursor, root, err := test.client.Enumerate(ctx, ctx, ListRequest{Prefix: "owned/"})
	if err != nil || cursor == nil {
		t.Fatal(err)
	}
	next, err := cursor.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := next.WaitReleased(ctx)
	if err != nil || snapshot.Err() != nil {
		t.Fatal("next", err)
	}
	value, _ := snapshot.ValueCopy()
	if value.Complete() || len(value.ObjectsCopy()) != 1 {
		t.Fatal("first page")
	}
	rootSnapshot, _ := root.Snapshot()
	if rootSnapshot.Info().Released {
		t.Fatal("root ended before EOF")
	}
	delivery, err := test.inbox.NextReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	observed, _ := delivery.Receipt()
	child, _ := observed.Snapshot()
	if child.Info().Sequence != snapshot.Info().Sequence || child.Info().Parent != rootSnapshot.Info().Sequence {
		t.Fatal("root blocked or relabeled child")
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	value = test.result(cursor.Next(ctx))
	if !value.Complete() {
		t.Fatal("native EOF absent")
	}
	if snapshot, err := root.WaitReleased(ctx); err != nil || snapshot.Err() != nil {
		t.Fatal("root not joined", err)
	}
}

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

package mysql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestSessionSaturatedFinalization(t *testing.T) {
	peer := newPeer(t, true, false)
	owner, inbox, _ := testOwner(t, peer.options(), 2)
	session, err := owner.Client().Retain(context.Background(), SessionOptions{})
	if err != nil || session == nil {
		t.Fatal("retaining session failed", err)
	}
	before := peer.queries.Load()
	if _, err := session.Query(context.Background(), "SELECT cells"); !errors.Is(err, adapters.ErrEvidence) {
		t.Fatal("full evidence admitted a child", err)
	}
	if peer.queries.Load() != before {
		t.Fatal("rejected child reached native SQL")
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatal("saturated close failed", err)
	}
	snapshot, resolved := session.Receipt().Snapshot()
	value, present := snapshot.ValueCopy()
	if !resolved || !snapshot.Info().Released || !present || value.HasData() || value.Complete() ||
		value.TransactionOutcome() != TransactionUnobserved {
		t.Fatal("closure fabricated statement or transaction facts")
	}
	ack(t, inbox)
	if err := session.Close(context.Background()); err != nil {
		t.Fatal("repeated close lost final evidence", err)
	}
	stats, err := owner.Client().Stats(context.Background())
	if err != nil || stats.OpenConnections != 0 {
		t.Fatal("retained connection returned to idle pool", err)
	}
}

func TestSessionChildFactsAndLifetime(t *testing.T) {
	peer := newPeer(t, false, false)
	owner, inbox, _ := testOwner(t, peer.options(), 0)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	cause := errors.New("session-owner-cancellation")
	session, err := owner.Client().Retain(ctx, SessionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	value, err := session.Query(context.Background(), "SELECT ?", "session-value")
	if err != nil {
		t.Fatal(err)
	}
	row, err := value.First()
	if err != nil || string(row.ValuesCopy()[0]) != "session-value" {
		t.Fatal("session result lost", err)
	}
	root, _ := session.Receipt().Snapshot()
	if value.Attribution().Parent != root.Info().Sequence || value.Attribution().Depth != root.Info().Depth+1 {
		t.Fatal("child escaped session attribution")
	}
	ack(t, inbox)
	for _, sql := range []string{"START TRANSACTION", "COMMIT", "CREATE TABLE fixture (id int)"} {
		value, err := session.Exec(context.Background(), sql)
		if err != nil || !value.Complete() || value.TransactionOutcome() != TransactionUnobserved {
			t.Fatal("session confused SQL with controlled commit", err)
		}
		ack(t, inbox)
	}
	cancel(cause)
	wait, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	snapshot, err := session.Receipt().WaitReleased(wait)
	if err != nil || !errors.Is(snapshot.Primary(), cause) || !errors.Is(snapshot.Primary(), context.Canceled) {
		t.Fatal("lifetime cause lost", err, snapshot.Primary())
	}
	ack(t, inbox)
	if err := session.Close(wait); !errors.Is(err, cause) {
		t.Fatal("repeated close erased original lifecycle failure", err)
	}
	before := peer.queries.Load()
	if _, err := session.Query(context.Background(), "SELECT cells"); err == nil {
		t.Fatal("closed session executed")
	}
	if peer.queries.Load() != before {
		t.Fatal("closed session reconnected")
	}
	if _, err := owner.Client().Ping(context.Background()); err != nil {
		t.Fatal("session closed its non-owning source", err)
	}
	ack(t, inbox)
}

func TestSessionCancellationWhileCalling(t *testing.T) {
	peer := newPeer(t, false, false)
	owner, inbox, _ := testOwner(t, peer.options(), 0)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	cause := errors.New("session-cancel-during-read")
	session, err := owner.Client().Retain(ctx, SessionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := session.Query(context.Background(), "SELECT wait"); done <- err }()
	select {
	case <-peer.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("session query did not enter")
	}
	if err := session.Close(context.Background()); !errors.Is(err, ErrState) {
		t.Fatal("concurrent close was not refused", err)
	}
	cancel(cause)
	select {
	case err := <-done:
		if !errors.Is(err, cause) || !errors.Is(err, context.Canceled) {
			t.Fatal("query lost original cancellation", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled query did not finish")
	}
	wait, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	snapshot, err := session.Receipt().WaitReleased(wait)
	if err != nil || !errors.Is(snapshot.Primary(), cause) {
		t.Fatal("parent session did not release", err)
	}
	ack(t, inbox)
	ack(t, inbox)
}

func TestSessionInvalidHandlesAndBounds(t *testing.T) {
	var nilClient *Client
	if handle, err := nilClient.Retain(context.Background(), SessionOptions{}); handle != nil || !errors.Is(err, ErrInput) {
		t.Fatal("nil client admitted")
	}
	var missing Session
	if missing.Receipt() != nil {
		t.Fatal("zero session exposed receipt")
	}
	if _, err := missing.Query(context.Background(), "SELECT 1"); !errors.Is(err, ErrState) {
		t.Fatal("zero query admitted")
	}
	if _, err := missing.Exec(context.Background(), "SELECT 1"); !errors.Is(err, ErrState) {
		t.Fatal("zero execution admitted")
	}
	if err := missing.Close(context.Background()); !errors.Is(err, ErrState) {
		t.Fatal("zero close admitted")
	}
	peer := newPeer(t, false, false)
	owner, inbox, _ := testOwner(t, peer.options(), 0)
	if handle, err := owner.Client().Retain(context.Background(), SessionOptions{Timeout: time.Minute + 1}); handle != nil || !errors.Is(err, ErrInput) {
		t.Fatal("invalid timeout admitted", err)
	}
	ack(t, inbox)
	if stats, err := owner.Client().Stats(context.Background()); err != nil || stats.OpenConnections != 0 {
		t.Fatal("invalid timeout performed acquisition", err)
	}
}

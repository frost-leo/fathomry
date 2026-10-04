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

package postgres

import (
	"context"
	"errors"
	"testing"
)

func TestSavepointSemantics(t *testing.T) {
	t.Run("recover_statement_error", func(t *testing.T) {
		peer := newProtocolPeer(t, false)
		owner, inbox, _ := testOwner(t, peer.options(), 4)
		ctx := context.Background()
		transaction, err := owner.Client().Begin(ctx, txOptions())
		if err != nil {
			t.Fatal(err)
		}
		point, err := transaction.Savepoint(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot, _ := point.Receipt().Snapshot(); snapshot.Info().Resolved {
			t.Fatal("live savepoint prematurely finalized")
		}
		if _, err = transaction.Query(ctx, "SELECT partial"); err == nil {
			t.Fatal("missing statement failure")
		}
		ack(t, inbox)
		value, err := point.Rollback(ctx)
		if err != nil || value.SavepointOutcome() != SavepointRolledBack || value.TransactionOutcome() != TransactionUnobserved {
			t.Fatal("savepoint recovery became outer commit evidence", err)
		}
		ack(t, inbox)
		if _, err = transaction.Query(ctx, "SELECT cells"); err != nil {
			t.Fatal("recovered transaction unusable", err)
		}
		ack(t, inbox)
		if value, err = transaction.Commit(ctx); err != nil || value.TransactionOutcome() != CommitAcknowledged {
			t.Fatal(err)
		}
		ack(t, inbox)
	})
	t.Run("lifo_parent_finalization_at_saturation", func(t *testing.T) {
		peer := newProtocolPeer(t, false)
		owner, inbox, _ := testOwner(t, peer.options(), 4)
		ctx := context.Background()
		transaction, err := owner.Client().Begin(ctx, txOptions())
		if err != nil {
			t.Fatal(err)
		}
		first, err := transaction.Savepoint(ctx)
		if err != nil {
			t.Fatal(err)
		}
		second, err := transaction.Savepoint(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = first.Release(ctx); !errors.Is(err, ErrState) {
			t.Fatal("out-of-order savepoint accepted", err)
		}
		if _, err = transaction.Rollback(ctx); err != nil {
			t.Fatal("saturated parent needed another reservation", err)
		}
		for _, point := range []*Savepoint{first, second} {
			value, err := point.Release(ctx)
			if err != nil || value.SavepointOutcome() != SavepointParentEnded || value.Complete() {
				t.Fatal("parent finalization invented individual acknowledgement", err)
			}
			if snapshot, _ := point.Receipt().Snapshot(); !snapshot.Info().Released {
				t.Fatal("parent did not join savepoint release")
			}
		}
		ack(t, inbox)
		ack(t, inbox)
		ack(t, inbox)
	})
	t.Run("depth_and_reused_slots", func(t *testing.T) {
		peer := newProtocolPeer(t, false)
		owner, inbox, _ := testOwner(t, peer.options(), 0)
		ctx := context.Background()
		transaction, err := owner.Client().Begin(ctx, txOptions())
		if err != nil {
			t.Fatal(err)
		}
		points := make([]*Savepoint, MaxSavepoints)
		for index := range points {
			points[index], err = transaction.Savepoint(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}
		if point, err := transaction.Savepoint(ctx); point != nil || !errors.Is(err, ErrLimit) {
			t.Fatal("savepoint depth exceeded", err)
		}
		ack(t, inbox)
		for index := len(points) - 1; index >= 0; index-- {
			if _, err = points[index].Release(ctx); err != nil {
				t.Fatal(err)
			}
			ack(t, inbox)
		}
		for range 80 {
			point, err := transaction.Savepoint(ctx)
			if err != nil {
				t.Fatal("released slots accumulated", err)
			}
			copy := *point
			if _, err = copy.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			ack(t, inbox)
		}
		if _, err = transaction.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		ack(t, inbox)
	})
}

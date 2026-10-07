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

package doris

import (
	"context"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
)

func testOwner(t testing.TB, settings Settings, capacity int) (*Owner, *adapters.Inbox[Result], *adapters.Runtime) {
	t.Helper()
	policy, err := Recommend(settings)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	if capacity > 0 {
		policy.Evidence.Capacity = capacity
	}
	inbox, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := Open(context.Background(), settings, Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := owner.Close(ctx); err != nil {
			t.Error("owner cleanup", err)
		}
		if !owner.ShutdownComplete() {
			t.Error("owner not released")
		}
		if err := runtime.Close(ctx); err != nil {
			t.Error("runtime cleanup", err)
		}
		for status, _ := inbox.Inspect(); status.Outstanding > 0; status, _ = inbox.Inspect() {
			delivery, err := inbox.NextReleased(ctx)
			if err != nil {
				t.Error(err)
				break
			}
			if err := delivery.Ack(); err != nil {
				t.Error(err)
				break
			}
		}
	})
	return owner, inbox, runtime
}

func receive(t testing.TB, inbox *adapters.Inbox[Result]) adapters.Delivery[Result] {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	delivery, err := inbox.NextReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return delivery
}

func ack(t testing.TB, inbox *adapters.Inbox[Result]) {
	t.Helper()
	if err := receive(t, inbox).Ack(); err != nil {
		t.Fatal(err)
	}
}

func TestDeclaredSourceChargesHaveIndependentLifetimes(t *testing.T) {
	settings := newSQLPeer(t, false).options()
	policy, err := Recommend(settings)
	if err != nil {
		t.Fatal(err)
	}
	owner, inbox, runtime := testOwner(t, settings, 0)
	work, err := runtime.Inspect()
	if err != nil || work.Active != 1 || work.WorkBytes != policy.SourceWorkBytes {
		t.Fatal("idle source work differs from its declaration", err)
	}
	evidence, err := inbox.Inspect()
	if err != nil || evidence.Outstanding != 1 || evidence.Bytes != policy.SourceEvidenceBytes {
		t.Fatal("idle source evidence differs from its declaration", err)
	}
	if err := owner.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	work, err = runtime.Inspect()
	if err != nil || work.Active != 0 || work.WorkBytes != 0 {
		t.Fatal("released source still holds work", err)
	}
	retained, err := inbox.Inspect()
	if err != nil || retained.Outstanding != 1 || retained.Bytes != evidence.Bytes {
		t.Fatal("native release acknowledged source evidence", err)
	}
	delivery := receive(t, inbox)
	receipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, ready := receipt.Snapshot()
	if !ready || !snapshot.Info().Released || snapshot.Info().Operation != "database.doris.open" {
		t.Fatal("wrong or incomplete source evidence")
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	retained, err = inbox.Inspect()
	if err != nil || retained.Outstanding != 0 || retained.Bytes != 0 {
		t.Fatal("source acknowledgement retained its evidence charge", err)
	}
}

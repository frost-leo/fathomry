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
	"sync"
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

func TestSourceReleasePublishesComplete(t *testing.T) {
	peer := newProtocolPeer(t, false)
	owner, _, _ := testOwner(t, peer.options(), 0)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- owner.Close(ctx) }()
	if _, err := owner.state.owner.call.Receipt().WaitReleased(ctx); err != nil {
		t.Fatal(err)
	}
	if !owner.ShutdownComplete() {
		t.Fatal("released evidence preceded confirmed source completion")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestSourceCloseTimeoutKeepsOwner(t *testing.T) {
	peer := newProtocolPeer(t, false)
	settings := peer.options()
	settings.MaxConnections = 2
	owner, inbox, runtime := testOwner(t, settings, 0)
	transaction, err := owner.Client().Begin(context.Background(), txOptions())
	if err != nil {
		t.Fatal(err)
	}
	sourceDelivery, err := inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sourceReceipt, err := sourceDelivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	claimed := true
	t.Cleanup(func() {
		if !claimed {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := owner.Close(cleanup); err != nil {
			t.Error("claimed source cleanup failed", err)
			return
		}
		if _, err := sourceReceipt.WaitReleased(cleanup); err != nil {
			t.Error("claimed source release unconfirmed", err)
			return
		}
		if err := sourceDelivery.Ack(); err != nil {
			t.Error("claimed source evidence cleanup failed", err)
		}
	})
	before, err := runtime.Inspect()
	if err != nil || before.Active != 2 {
		t.Fatal("source and transaction ownership not established", err)
	}
	transaction.session.family.gate.Lock()
	unlock := sync.OnceFunc(transaction.session.family.gate.Unlock)
	defer unlock()
	deadline, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := owner.Close(deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("shutdown wait lost its deadline", err)
	}
	if owner.ShutdownComplete() {
		t.Fatal("timed-out shutdown discarded its retained family")
	}
	if released := owner.Release(deadline); released.Complete || !errors.Is(released.Err, context.DeadlineExceeded) {
		t.Fatal("resource release falsely confirmed completion")
	}
	after, err := runtime.Inspect()
	if err != nil || after.Active != before.Active || after.WorkBytes != before.WorkBytes {
		t.Fatal("timeout released work reservations", err)
	}
	for _, receipt := range []*adapters.Receipt[Result]{sourceReceipt, transaction.Receipt()} {
		snapshot, _ := receipt.Snapshot()
		if snapshot.Info().Released {
			t.Fatal("timed-out shutdown reported public release")
		}
	}
	if err := sourceDelivery.Ack(); !errors.Is(err, adapters.ErrPending) {
		t.Fatal("live source evidence was acknowledged", err)
	}

	queries := peer.queries.Load()
	if _, err := owner.Client().Query(context.Background(), "SELECT cells"); err == nil {
		t.Fatal("closing source admitted new SQL")
	}
	if peer.queries.Load() != queries {
		t.Fatal("closing source dispatched new SQL")
	}
	if status, err := inbox.Inspect(); err != nil || status.Outstanding != 2 {
		t.Fatal("rejected work changed evidence custody", err)
	}
	unlock()
	cleanup, stopCleanup := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCleanup()
	if err := owner.Close(cleanup); err != nil || !owner.ShutdownComplete() {
		t.Fatal("same owner could not continue cleanup", err)
	}
	for _, receipt := range []*adapters.Receipt[Result]{sourceReceipt, transaction.Receipt()} {
		if _, err := receipt.WaitReleased(cleanup); err != nil {
			t.Fatal("original receipt not released by continuation", err)
		}
	}
	snapshot, _ := transaction.Receipt().Snapshot()
	observed, present := snapshot.ValueCopy()
	if !present || observed.TransactionOutcome() != RollbackAcknowledged {
		t.Fatal("continuation lost actual rollback evidence")
	}
	if released := owner.Release(cleanup); !released.Complete || released.Err != nil {
		t.Fatal("completed owner did not retain its final state")
	}
	after, err = runtime.Inspect()
	if err != nil || after.Active != 0 || after.WorkBytes != 0 {
		t.Fatal("completed shutdown retained work reservations", err)
	}
	if err := sourceDelivery.Ack(); err != nil {
		t.Fatal(err)
	}
	claimed = false
	ack(t, inbox)
	if status, err := inbox.Inspect(); err != nil || status.Outstanding != 0 {
		t.Fatal("continuation lost evidence custody", err)
	}
}

func TestOpenFailureRetainsOwner(t *testing.T) {
	peer := newProtocolPeer(t, false)
	settings := peer.options()
	settings.ParserHome += "/unapproved"
	if err := Validate(settings); err != nil {
		t.Fatal("control failed before native construction", err)
	}
	policy, err := Recommend(settings)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	var owner *Owner
	var sourceDelivery adapters.Delivery[Result]
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if owner != nil {
			if err := owner.Close(cleanup); err != nil {
				t.Error(err)
			}
		}
		_ = sourceDelivery.Ack()
		if err := runtime.Close(cleanup); err != nil {
			t.Error(err)
		}
	})
	owner, openErr := Open(context.Background(), settings, Dependencies{Runtime: runtime, Evidence: inbox})
	if owner == nil || !errors.Is(openErr, ErrEnvironment) {
		t.Fatal("failed native construction did not return its owner", openErr)
	}
	if owner.ShutdownComplete() {
		t.Fatal("Open failure ended public ownership without cleanup")
	}
	before, err := runtime.Inspect()
	if err != nil || before.Active != 1 || before.WorkBytes != sourceWorkBytes {
		t.Fatal("failed source lost its ownership reservation", err)
	}
	sourceDelivery, err = inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sourceReceipt, err := sourceDelivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	if err := sourceDelivery.Ack(); !errors.Is(err, adapters.ErrPending) {
		t.Fatal("failed but still-owned source was acknowledged", err)
	}
	if _, err := owner.Client().Ping(context.Background()); !errors.Is(err, ErrState) {
		t.Fatal("failed source exposed operation authority", err)
	}
	ack(t, inbox)
	if peer.connects.Load() != 0 {
		t.Fatal("environment-refused source contacted a service")
	}
	cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if err := owner.Close(cleanup); err != nil || !owner.ShutdownComplete() {
		t.Fatal("failed source owner could not release", err)
	}
	snapshot, err := sourceReceipt.WaitReleased(cleanup)
	if err != nil || snapshot.Primary() != openErr || snapshot.Cleanup() != nil {
		t.Fatal("source cleanup changed the original Open failure", err)
	}
	after, err := runtime.Inspect()
	if err != nil || after.Active != 0 || after.WorkBytes != 0 {
		t.Fatal("failed source cleanup retained reservations", err)
	}
	if err := sourceDelivery.Ack(); err != nil {
		t.Fatal(err)
	}
	if status, err := inbox.Inspect(); err != nil || status.Outstanding != 0 {
		t.Fatal("failed source evidence lost custody", err)
	}
}

func TestMaxRetainedFamilySourceClose(t *testing.T) {
	peer := newProtocolPeer(t, false)
	capacity := 2 + MaxPreparedStatements + MaxSavepoints
	owner, inbox, runtime := testOwner(t, peer.options(), capacity)
	transaction, err := owner.Client().Begin(context.Background(), txOptions())
	if err != nil {
		t.Fatal(err)
	}
	receipts := []*adapters.Receipt[Result]{transaction.Receipt()}
	for range MaxPreparedStatements {
		statement, err := transaction.Prepare(context.Background(), "SELECT cells")
		if err != nil {
			t.Fatal(err)
		}
		receipts = append(receipts, statement.Receipt())
	}
	for range MaxSavepoints {
		point, err := transaction.Savepoint(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		receipts = append(receipts, point.Receipt())
	}
	evidence, err := inbox.Inspect()
	if err != nil || evidence.Outstanding != capacity {
		t.Fatal("maximal family did not fill required evidence")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := owner.Close(ctx); err != nil {
		t.Fatal("source shutdown required fresh admission", err)
	}
	for _, receipt := range receipts {
		if _, err := receipt.WaitReleased(ctx); err != nil {
			t.Fatal("source did not join retained child", err)
		}
	}
	if !owner.ShutdownComplete() {
		t.Fatal("source release not confirmed")
	}
	if err := runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}
	usage, err := runtime.Inspect()
	if err != nil || usage.Active != 0 || usage.WorkBytes != 0 {
		t.Fatal("source shutdown retained work reservations")
	}
	for range len(receipts) + 1 {
		ack(t, inbox)
	}
	evidence, err = inbox.Inspect()
	if err != nil || evidence.Outstanding != 0 {
		t.Fatal("released family evidence not receivable")
	}
}

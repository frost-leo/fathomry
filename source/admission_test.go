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

package source_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/frost-leo/fathomry/source"
)

func limited(t *testing.T, limits source.Limits, release source.ReleaseFunc) (*source.Assembly, source.Selection[reader], *source.Access) {
	t.Helper()
	selected := source.WithLimits(selection(t, "controlled", release), limits)
	assembly := assemble(t, "controls", selected)
	access, err := source.AccessFor(assembly, selected)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = assembly.Close(context.Background()) })
	return assembly, selected, access
}

func limits() source.Limits {
	return source.Limits{Active: 1, Queued: 1, Bytes: 8, QueuedBytes: 8, MaxLeases: 4}
}

func TestAdmissionRejectsInvalidPolicyBeforeConstruction(t *testing.T) {
	for _, policy := range []source.Limits{
		{}, {Active: 1}, {Active: 1, Bytes: 1, MaxLeases: 1, Queued: -1},
		{Active: 1, Bytes: 1, MaxLeases: 1, Queued: 1},
		{Active: 1, Bytes: 1, MaxLeases: 1, QueuedBytes: 1},
	} {
		var calls atomic.Int32
		spec := source.WithLimits(source.Select(prepared(t, "invalid", "example", ""), func(context.Context, settings) (source.Resource[reader], error) {
			calls.Add(1)
			return source.Resource[reader]{}, nil
		}), policy)
		if assembly, err := source.Assemble(context.Background(), context.Background(), "invalid", spec); assembly != nil || !errors.Is(err, source.ErrSelection) || calls.Load() != 0 {
			t.Fatal("invalid policy reached construction")
		}
	}
	owner, selected, _ := limited(t, limits(), complete)
	override := source.WithLimits(source.Borrow("alias", owner, selected), limits())
	if assembly, err := source.Assemble(context.Background(), context.Background(), "override", override); assembly != nil || !errors.Is(err, source.ErrSelection) {
		t.Fatal("alias acquired another quota")
	}
	unlimited := selection(t, "uncontrolled", complete)
	assembly := assemble(t, "legacy", unlimited)
	defer assembly.Close(context.Background())
	if _, err := source.AccessFor(assembly, unlimited); !errors.Is(err, source.ErrSelection) {
		t.Fatal("missing policy treated as unlimited admission")
	}
}

func TestAdmissionFIFOBytesAndCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		policy := limits()
		policy.Queued, policy.QueuedBytes = 2, 8
		assembly, _, access := limited(t, policy, complete)
		first, err := access.Acquire(context.Background(), 8)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancelCause(context.Background())
		queued := make(chan error, 1)
		go func() {
			lease, err := access.Acquire(ctx, 5)
			if lease != nil {
				lease.Release()
			}
			queued <- err
		}()
		synctest.Wait()
		if usage := assembly.Snapshot().Sources[0].Usage; usage != (source.Usage{Active: 1, ActiveBytes: 8, Queued: 1, QueuedBytes: 5}) {
			t.Fatalf("wrong dimensions: %+v", usage)
		}
		if lease, err := access.Acquire(context.Background(), 4); lease != nil || !errors.Is(err, source.ErrCapacity) {
			t.Fatal("queued bytes ceiling bypassed")
		}
		cause := errors.New("cancel reason")
		cancel(cause)
		if err := <-queued; !errors.Is(err, cause) || !errors.Is(err, context.Canceled) {
			t.Fatal("queue cause lost")
		}
		order := make(chan int, 2)
		release := make(chan struct{})
		go func() {
			lease, err := access.Acquire(context.Background(), 7)
			if err != nil {
				t.Error(err)
				return
			}
			order <- 1
			<-release
			lease.Release()
		}()
		synctest.Wait()
		go func() {
			lease, err := access.Acquire(context.Background(), 1)
			if err != nil {
				t.Error(err)
				return
			}
			order <- 2
			lease.Release()
		}()
		synctest.Wait()
		first.Release()
		if got := <-order; got != 1 {
			t.Fatal("new small use bypassed FIFO")
		}
		close(release)
		if got := <-order; got != 2 {
			t.Fatal("queued work did not proceed")
		}
		synctest.Wait()
		if usage := assembly.Snapshot().Sources[0].Usage; usage != (source.Usage{}) {
			t.Fatalf("accounting leaked: %+v", usage)
		}
	})
}

func TestExpiredQueueNeverAcquiresAndDoesNotBlockShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		assembly, _, access := limited(t, limits(), complete)
		first, err := access.Acquire(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		result := make(chan error, 1)
		go func() {
			lease, err := access.Acquire(ctx, 1)
			if lease != nil {
				t.Error("expired waiter acquired")
				lease.Release()
			}
			result <- err
		}()
		synctest.Wait()
		time.Sleep(2 * time.Second)
		first.Release()
		if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("deadline not preserved")
		}
		if err := assembly.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestCallBorrowingPreservesScopeShutdownAndDependencies(t *testing.T) {
	var closed atomic.Int32
	owner, selected, access := limited(t, limits(), func(ctx context.Context) source.ReleaseResult {
		closed.Add(1)
		return complete(ctx)
	})
	borrowed := source.Borrow("alias", owner, selected)
	borrower := assemble(t, "borrower", borrowed)
	other, err := source.AccessFor(borrower, borrowed)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(context.Background()); !errors.Is(err, source.ErrIncomplete) {
		t.Fatal("owner ignored scope borrower")
	}
	if lease, err := access.Acquire(context.Background(), 0); lease != nil || !errors.Is(err, source.ErrSelection) {
		t.Fatal("closed owner admitted")
	}
	lease, err := other.Acquire(context.Background(), 5)
	if err != nil {
		t.Fatal("existing borrower stopped with owner", err)
	}
	if info := other.Info(); info.Scope != "controls" || info.Configuration.Identity.Name != "controlled" {
		t.Fatal("alias relabeled resource")
	}
	if err := borrower.Close(context.Background()); !errors.Is(err, source.ErrIncomplete) {
		t.Fatal("borrowed scope dropped its active lease")
	}
	if borrower.Snapshot().Sources[0].Returned || closed.Load() != 0 {
		t.Fatal("borrowed resource prematurely returned")
	}
	child, err := lease.Retain()
	if err != nil {
		t.Fatal("cleanup handoff blocked during shutdown", err)
	}
	lease.Release()
	if err := owner.Close(context.Background()); !errors.Is(err, source.ErrIncomplete) {
		t.Fatal("retained work lost owner")
	}
	child.Release()
	if err := borrower.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(context.Background()); err != nil || closed.Load() != 1 {
		t.Fatal("owner cleanup failed")
	}

	var dependencyClosed atomic.Int32
	dependency := selection(t, "dependency", func(ctx context.Context) source.ReleaseResult {
		dependencyClosed.Add(1)
		return complete(ctx)
	})
	consumer := source.WithLimits(selection(t, "consumer", complete), limits())
	assembly := assemble(t, "ordered", dependency, consumer)
	use, err := source.AccessFor(assembly, consumer)
	if err != nil {
		t.Fatal(err)
	}
	held, err := use.Acquire(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := assembly.Close(context.Background()); !errors.Is(err, source.ErrIncomplete) || dependencyClosed.Load() != 0 {
		t.Fatal("active consumer lost earlier dependency")
	}
	held.Release()
	if err := assembly.Close(context.Background()); err != nil || dependencyClosed.Load() != 1 {
		t.Fatal("dependency not released")
	}
}

func TestBorrowTreeChargesOneUseAndBoundsRetainedAncestors(t *testing.T) {
	policy := limits()
	policy.MaxLeases = 3
	assembly, selected, access := limited(t, policy, complete)
	root, err := access.Acquire(context.Background(), 7)
	if err != nil {
		t.Fatal(err)
	}
	child, err := root.Retain()
	if err != nil {
		t.Fatal(err)
	}
	grandchild, err := child.Retain()
	if err != nil {
		t.Fatal(err)
	}
	root.Release()
	child.Release()
	if _, err := grandchild.Retain(); !errors.Is(err, source.ErrCapacity) {
		t.Fatal("released ancestors escaped memory bound")
	}
	if _, err := child.Retain(); !errors.Is(err, source.ErrAdmission) {
		t.Fatal("released authority reused")
	}
	if usage := assembly.Snapshot().Sources[0].Usage; usage.Active != 1 || usage.ActiveBytes != 7 {
		t.Fatal("nested use charged another resource")
	}
	if receiver, err := source.Assemble(context.Background(), context.Background(), "receiver", source.Delegate("take", assembly, selected)); receiver != nil || !errors.Is(err, source.ErrSelection) {
		t.Fatal("active operation delegated")
	}
	grandchild.Release()
	grandchild.Release()
	for _, lease := range []*source.Lease{root, child, grandchild} {
		select {
		case <-lease.Done():
		default:
			t.Fatal("completed subtree still pending")
		}
	}
	if usage := assembly.Snapshot().Sources[0].Usage; usage != (source.Usage{}) {
		t.Fatal("tree leaked reservation")
	}
	if assembly.Snapshot().Sources[0].Borrowers != 0 {
		t.Fatal("calls counted as borrowed scopes")
	}
}

func TestConcurrentLeaseReleaseAndCloseAreIdempotent(t *testing.T) {
	policy := limits()
	policy.MaxLeases = 64
	var releases atomic.Int32
	assembly, _, access := limited(t, policy, func(ctx context.Context) source.ReleaseResult {
		releases.Add(1)
		return complete(ctx)
	})
	root, err := access.Acquire(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for range 32 {
		child, err := root.Retain()
		if err != nil {
			t.Fatal(err)
		}
		group.Go(func() {
			for range 10 {
				child.Release()
				_ = assembly.Close(context.Background())
				_ = assembly.Snapshot()
			}
		})
	}
	root.Release()
	group.Wait()
	if err := assembly.Close(context.Background()); err != nil || releases.Load() != 1 {
		t.Fatal("release race lost accounting")
	}
}

func TestRuntimeAccessAndLeaseCannotBeSerialized(t *testing.T) {
	_, _, access := limited(t, limits(), complete)
	lease, err := access.Acquire(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	for _, value := range []any{access, *access, lease, *lease} {
		if _, err := json.Marshal(value); err == nil {
			t.Fatal("runtime capability serialized")
		}
	}
	for _, value := range []any{new(source.Access), new(source.Lease)} {
		if err := json.Unmarshal([]byte("{}"), value); err == nil {
			t.Fatal("runtime ownership reconstructed")
		}
	}
}

func TestCanceledCloseWithActiveCallsKeepsCancellationCause(t *testing.T) {
	assembly, _, access := limited(t, limits(), complete)
	lease, err := access.Acquire(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("shutdown budget exhausted")
	cancel(cause)
	for range 100 {
		err := assembly.Close(ctx)
		if !errors.Is(err, source.ErrIncomplete) || !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
			t.Fatal("active-use shutdown lost cancellation evidence")
		}
	}
	lease.Release()
	if err := assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAliasesCannotMultiplyAdmission(t *testing.T) {
	policy := limits()
	policy.Queued, policy.QueuedBytes = 0, 0
	owner, selected, first := limited(t, policy, complete)
	borrowed := source.Borrow("second", owner, selected)
	borrower := assemble(t, "borrower", borrowed)
	defer borrower.Close(context.Background())
	second, err := source.AccessFor(borrower, borrowed)
	if err != nil {
		t.Fatal(err)
	}
	held, err := first.Acquire(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	if lease, err := second.Acquire(context.Background(), 0); lease != nil || !errors.Is(err, source.ErrCapacity) {
		t.Fatal("alias multiplied the active ceiling")
	}
	if second.Limits() != first.Limits() || borrower.Snapshot().Sources[0].Limits != policy {
		t.Fatal("applied policy is not inspectable")
	}
	held.Release()
	held, err = second.Acquire(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	held.Release()
}

func TestActiveBytesAreIndependentOfAvailableCallSlots(t *testing.T) {
	policy := limits()
	policy.Active, policy.Queued, policy.QueuedBytes = 3, 0, 0
	assembly, _, access := limited(t, policy, complete)
	first, err := access.Acquire(context.Background(), 7)
	if err != nil {
		t.Fatal(err)
	}
	if lease, err := access.Acquire(context.Background(), 2); lease != nil || !errors.Is(err, source.ErrCapacity) {
		t.Fatal("spare call slot bypassed active byte ceiling")
	}
	second, err := access.Acquire(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if usage := assembly.Snapshot().Sources[0].Usage; usage.Active != 2 || usage.ActiveBytes != 8 {
		t.Fatal("active byte accounting incorrect")
	}
	first.Release()
	second.Release()
}

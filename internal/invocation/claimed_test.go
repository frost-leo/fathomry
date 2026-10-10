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

package invocation_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestClaimedEvidenceSharesInboxLimitsWithoutEnteringQueue(t *testing.T) {
	for _, test := range []struct {
		name     string
		capacity int
		bytes    int64
	}{
		{name: "count", capacity: 2, bytes: 512},
		{name: "bytes", capacity: 4, bytes: 256},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				limits := policy()
				limits.Active = 4
				_, access, _ := fixture(t, limits)
				evidence, err := invocation.NewInbox[writeEvidence](test.capacity, test.bytes)
				if err != nil {
					t.Fatal(err)
				}
				queued := begin(t, access, request("queued", invocation.Finite), evidence, nil)
				claimed, delivery, err := invocation.BeginClaimed(context.Background(), access, request("claimed", invocation.Finite), evidence, nil)
				if err != nil || claimed == nil || delivery == nil {
					t.Fatal("claimed admission failed", err)
				}
				if err := delivery.Release(); !errors.Is(err, invocation.ErrPending) {
					t.Fatal("live claimed record relinquished custody", err)
				}
				claimed.Complete(invocation.Outcome[writeEvidence]{Value: writeEvidence{Owner: "claimed"}, Present: true})
				if call, record, err := invocation.BeginClaimed(context.Background(), access, request("claimed-full", invocation.Finite), evidence, nil); call != nil || record != nil || !errors.Is(err, invocation.ErrEvidence) {
					t.Fatal("claimed evidence did not share the full Inbox", err)
				}
				if call, err := invocation.Begin(context.Background(), access, request("queued-full", invocation.Finite), evidence, nil); call != nil || !errors.Is(err, invocation.ErrEvidence) {
					t.Fatal("queued evidence ignored claimed custody", err)
				}
				queued.Complete(invocation.Outcome[writeEvidence]{Value: writeEvidence{Owner: "queued"}, Present: true})
				if result := releaseDelivery(t, evidence); result.Context.Correlation.Call != "queued" || result.Outcome.Value.Owner != "queued" {
					t.Fatal("Next stole a directly claimed record")
				}
				if usage := evidence.Usage(); usage != (invocation.InboxUsage{Outstanding: 1, ReservedBytes: 128}) {
					t.Fatal("handling another record discarded claimed custody", usage)
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if stolen, err := evidence.Next(ctx); stolen != nil || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("claimed record was also queued", err)
				}
				result, err := delivery.Receipt().WaitReleased(context.Background())
				if err != nil || result.Context.Correlation.Call != "claimed" || result.Outcome.Value.Owner != "claimed" {
					t.Fatal("claimed record lost exact call association", err)
				}
				if err := delivery.Release(); err != nil {
					t.Fatal(err)
				}
				if err := delivery.Release(); err != nil || evidence.Usage() != (invocation.InboxUsage{}) {
					t.Fatal("claimed release was not idempotent", err)
				}
			})
		})
	}
}

func TestNestedClaimedEvidenceUsesExistingSaturatedRootAndActualDescendants(t *testing.T) {
	limits := policy()
	limits.Queued, limits.QueuedBytes, limits.MaxLeases = 0, 0, 3
	assembly, access, releases := fixture(t, limits)
	evidence := inbox(t, 2)
	parent, parentDelivery, err := invocation.BeginClaimed(context.Background(), access, request("worker", invocation.Session), evidence, nil)
	if err != nil {
		t.Fatal(err)
	}
	input := request("task", invocation.Finite)
	input.Bytes, input.Correlation.Parent = 0, "worker"
	child, childDelivery, err := invocation.BeginNestedClaimed(context.Background(), parent.Scope(), input, evidence, nil)
	if err != nil {
		t.Fatal("child attempted another saturated root admission", err)
	}
	if call, delivery, err := invocation.BeginNestedClaimed(context.Background(), parent.Scope(), input, evidence, nil); call != nil || delivery != nil || !errors.Is(err, invocation.ErrEvidence) {
		t.Fatal("nested claimed evidence exceeded shared capacity", err)
	}
	guard, err := child.Scope().Hold()
	if err != nil {
		t.Fatal(err)
	}
	if next, err := guard.Scope().Hold(); next != nil || !errors.Is(err, resource.ErrCapacity) {
		t.Fatal("retained descendant bypassed root node bound", err)
	}
	parent.Complete(invocation.Outcome[writeEvidence]{Present: true})
	child.Complete(invocation.Outcome[writeEvidence]{Present: true})
	for _, delivery := range []*invocation.DeliveryRecord[writeEvidence]{childDelivery, parentDelivery} {
		if err := delivery.Release(); !errors.Is(err, invocation.ErrPending) {
			t.Fatal("completion discarded a live descendant", err)
		}
	}
	if err := assembly.Close(context.Background()); !errors.Is(err, resource.ErrIncomplete) || releases.Load() != 0 {
		t.Fatal("source released before actual descendant completion", err)
	}
	guard.End()
	for _, delivery := range []*invocation.DeliveryRecord[writeEvidence]{childDelivery, parentDelivery} {
		result, err := delivery.Receipt().WaitReleased(context.Background())
		if err != nil || !result.Released || !result.Final {
			t.Fatal("actual descendant completion did not release the record", err)
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if err := assembly.Close(context.Background()); err != nil || releases.Load() != 1 || evidence.Usage() != (invocation.InboxUsage{}) {
		t.Fatal("claimed cleanup lost custody or source ownership", err)
	}
}

func TestClaimedAdmissionRejectionReturnsNoCustody(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limits := policy()
		_, access, _ := fixture(t, limits)
		evidence := inbox(t, 3)
		parent, parentDelivery, err := invocation.BeginClaimed(context.Background(), access, request("active", invocation.Session), evidence, nil)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			call, delivery, err := invocation.BeginClaimed(ctx, access, request("queued", invocation.Finite), evidence, nil)
			if call != nil || delivery != nil {
				t.Error("canceled claimed admission returned custody")
			}
			done <- err
		}()
		synctest.Wait()
		if usage := evidence.Usage(); usage != (invocation.InboxUsage{Outstanding: 2, ReservedBytes: 256}) {
			t.Fatal("queued claimed admission did not reserve evidence first", usage)
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal("claimed admission lost cancellation", err)
		}
		invalid := request("wrong-parent", invocation.Finite)
		invalid.Bytes, invalid.Correlation.Parent = 0, "other"
		if call, delivery, err := invocation.BeginNestedClaimed(context.Background(), parent.Scope(), invalid, evidence, nil); call != nil || delivery != nil || !errors.Is(err, invocation.ErrInvalid) {
			t.Fatal("nested claimed admission accepted another parent", err)
		}
		if usage := evidence.Usage(); usage != (invocation.InboxUsage{Outstanding: 1, ReservedBytes: 128}) {
			t.Fatal("rejected claimed admission leaked capacity", usage)
		}
		parent.Complete(invocation.Outcome[writeEvidence]{})
		if err := parentDelivery.Release(); err != nil || evidence.Usage() != (invocation.InboxUsage{}) {
			t.Fatal("cleanup leaked original claimed evidence", err)
		}
	})
}

func TestConcurrentClaimedAndQueuedEvidenceKeepsExactAssociation(t *testing.T) {
	const count = 32
	limits := policy()
	limits.Active, limits.Queued = 4, count
	_, access, _ := fixture(t, limits)
	evidence := inbox(t, count)
	var group sync.WaitGroup
	for index := range count {
		group.Go(func() {
			id := fmt.Sprintf("claimed-%d", index)
			if index%2 == 0 {
				call, err := invocation.Begin(context.Background(), access, request(id, invocation.Finite), evidence, nil)
				if err != nil {
					t.Error(err)
					return
				}
				call.Complete(invocation.Outcome[writeEvidence]{Value: writeEvidence{Owner: id, Rows: index}, Present: true})
				return
			}
			call, delivery, err := invocation.BeginClaimed(context.Background(), access, request(id, invocation.Finite), evidence, nil)
			if err != nil {
				t.Error(err)
				return
			}
			runtime.Gosched()
			call.Complete(invocation.Outcome[writeEvidence]{Value: writeEvidence{Owner: id, Rows: index}, Present: true})
			result, err := delivery.Receipt().WaitReleased(context.Background())
			if err != nil || result.Context.Correlation.Call != id || result.Outcome.Value.Owner != id || result.Outcome.Value.Rows != index {
				t.Error("direct custody crossed concurrent call identity", err)
			}
			if err := delivery.Release(); err != nil {
				t.Error(err)
			}
		})
	}
	group.Wait()
	if usage := evidence.Usage(); usage != (invocation.InboxUsage{Outstanding: count / 2, ReservedBytes: count / 2 * 128}) {
		t.Fatal("claimed transfer discarded or retained unrelated records", usage)
	}
	seen := make(map[string]bool)
	for range count / 2 {
		result := releaseDelivery(t, evidence)
		id := result.Context.Correlation.Call
		if seen[id] || result.Outcome.Value.Owner != id || result.Outcome.Value.Rows%2 != 0 {
			t.Fatal("queue received a duplicate, claimed or misattributed record")
		}
		seen[id] = true
	}
	if usage := evidence.Usage(); usage != (invocation.InboxUsage{}) {
		t.Fatal("concurrent transfer leaked evidence", usage)
	}
}

func TestClaimedAbruptExecutionKeepsUnresolvedResponsibility(t *testing.T) {
	for _, abrupt := range []string{"panic", "goexit"} {
		t.Run(abrupt, func(t *testing.T) {
			_, access, _ := fixture(t, policy())
			evidence := inbox(t, 1)
			call, delivery, err := invocation.BeginClaimed(context.Background(), access, request(abrupt, invocation.Finite), evidence, nil)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { _ = recover() }()
				_ = call.Execute(context.Background(), invocation.Budget{Limit: time.Second}, func(context.Context, invocation.Scope) invocation.Outcome[writeEvidence] {
					if abrupt == "goexit" {
						runtime.Goexit()
					}
					panic("private native panic")
				})
			}()
			<-done
			if _, present := delivery.Receipt().Result(); present {
				t.Fatal("abrupt execution invented completion")
			}
			if err := delivery.Release(); !errors.Is(err, invocation.ErrPending) {
				t.Fatal("abrupt execution discarded custody", err)
			}
			cause := errors.New("provider confirmed native exit")
			if !call.Complete(invocation.Outcome[writeEvidence]{Primary: cause}) {
				t.Fatal("provider could not confirm completion after abrupt exit")
			}
			result, err := delivery.Receipt().WaitReleased(context.Background())
			if err != nil || !errors.Is(result.Err(), cause) {
				t.Fatal("provider completion evidence was lost", err)
			}
			if err := delivery.Release(); err != nil || evidence.Usage() != (invocation.InboxUsage{}) {
				t.Fatal("confirmed abrupt exit leaked evidence", err)
			}
		})
	}
}

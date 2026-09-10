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

package conformance_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

// These fixture-owned values belong to the boundary, not to the generic
// invocation model. The envelope preserves attribution even when data is absent.
type boundaryAttribution struct {
	Call, Run, Item, Owner string
	Attempt                uint32
}

type boundaryEnvelope struct {
	Attribution boundaryAttribution
	Data        transfer
	DataPresent bool
}

type boundaryNativeError struct{ request string }

func (*boundaryNativeError) Error() string { panic("secret-canary-native") }

func TestTechnicalFaultsSupportGoErrorWrappingWithoutLosingIndependentEvidence(t *testing.T) {
	f := newFixture(t, 2, 64, 2)
	inbox, err := invocation.NewInbox[boundaryEnvelope](2, 1024)
	if err != nil {
		t.Fatal(err)
	}
	const technicalKind fault.Kind = "fixture.delivery.unconfirmed"
	native := &boundaryNativeError{request: "request-shared"}
	cleanup := errors.New("secret-canary-cleanup")
	cancelCause := errors.New("secret-canary-cancel")
	canceled, cancel := context.WithCancelCause(context.Background())
	cancel(cancelCause)
	meanings := []error{
		errors.New("fixture orders write unconfirmed"),
		errors.New("fixture progress write unconfirmed"),
	}
	var guards []*invocation.Guard
	var endNative []func()
	var expected []boundaryAttribution
	for index := range 2 {
		callID := fmt.Sprintf("call-%d", index)
		caller := boundaryAttribution{
			Call: callID, Run: fmt.Sprintf("run-%d", index), Item: fmt.Sprintf("item-%d", index),
			Owner: fmt.Sprintf("account-%d", index), Attempt: uint32(index + 3),
		}
		frozen := caller
		expected = append(expected, frozen)
		call, err := invocation.Begin(context.Background(), f.access, request(callID, invocation.Async, 32), inbox, nil)
		if err != nil {
			t.Fatal(err)
		}
		guard, err := call.Scope().Hold()
		if err != nil {
			t.Fatal(err)
		}
		guards = append(guards, guard)
		endNative = append(endNative, f.counts.enter(32))
		if _, err := call.Attempt(); err != nil {
			t.Fatal(err)
		}
		pending, _ := call.Receipt().Result()
		technical := technicalKind.New(pending.Context, native, canceled.Err(), context.Cause(canceled))
		caller.Item, caller.Attempt = "caller-mutated", 99
		outcome := invocation.Outcome[boundaryEnvelope]{Present: true,
			Value:   boundaryEnvelope{Attribution: frozen, Data: transfer{Bytes: index + 1, Unknown: 1}, DataPresent: true},
			Primary: technical, Cleanup: cleanup}
		if !call.Complete(outcome) {
			t.Fatal("technical completion rejected")
		}
		result, err := call.Receipt().Wait(context.Background())
		if err != nil || result.Released || !result.Final || result.Outcome.Primary != technical {
			t.Fatal("callback erased its technical identity or released the submit stack")
		}
		boundaryErr := errors.Join(meanings[index], result.Err())
		if !errors.Is(boundaryErr, meanings[index]) || errors.Is(boundaryErr, meanings[1-index]) || result.Outcome.Value.Attribution != frozen {
			t.Fatal("boundary meaning or frozen attribution was lost")
		}
		conformance.Runtime(t, technical, new(fault.Error), "secret-canary")
		conformance.Private(t, boundaryErr, "secret-canary")
	}
	if inbox.Usage().Outstanding != 2 || !errors.Is(f.owner.Close(context.Background()), resource.ErrIncomplete) {
		t.Fatal("handled boundary errors released independent evidence or live resources")
	}
	for index := range 2 {
		delivery, err := inbox.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !errors.Is(delivery.Release(), invocation.ErrPending) {
			t.Fatal("evidence released before native submit use ended")
		}
		result, err := delivery.Receipt().WaitFinal(context.Background())
		if err != nil || !result.Final || !result.Outcome.Present || result.Outcome.Value.Attribution != expected[index] ||
			result.Context.Correlation.Call != expected[index].Call ||
			!result.Outcome.Value.DataPresent || result.Outcome.Value.Data.Bytes != index+1 || result.Outcome.Value.Data.Unknown != 1 {
			t.Fatal("independent reception lost context or partial/unknown facts")
		}
		boundaryErr := fmt.Errorf("%w: %w", meanings[index], result.Err())
		for _, cause := range []error{meanings[index], technicalKind, native, context.Canceled, cancelCause, cleanup} {
			if !errors.Is(boundaryErr, cause) {
				t.Fatal("Go error wrapping discarded a boundary, technical, native, cancellation or cleanup cause")
			}
		}
		if errors.Is(boundaryErr, meanings[1-index]) {
			t.Fatal("independent reception confused boundary meanings")
		}
		conformance.Cause(t, boundaryErr, func(original *boundaryNativeError) bool {
			return original == native && original.request == "request-shared"
		})
		endNative[index]()
		guards[index].End()
		final, err := delivery.Receipt().WaitReleased(context.Background())
		if err != nil || final.Outcome.Value.Attribution != expected[index] || !errors.Is(final.Err(), cleanup) {
			t.Fatal("local release changed the retained boundary evidence")
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if inbox.Usage() != (invocation.InboxUsage{}) {
		t.Fatal("boundary evidence leaked its reservation")
	}
}

func TestBoundaryRetainsFrozenAttributionAfterCallerWaitEnds(t *testing.T) {
	f := newFixture(t, 1, 32, 1)
	inbox, err := invocation.NewInbox[boundaryEnvelope](1, 512)
	if err != nil {
		t.Fatal(err)
	}
	caller := boundaryAttribution{
		Call: "late-call", Run: "original-run", Item: "original-item", Owner: "account-A", Attempt: 4,
	}
	frozen := caller
	call, err := invocation.Begin(context.Background(), f.access, request("late-call", invocation.Async, 32), inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	native := &boundaryNativeError{request: "request-late"}
	cleanup := errors.New("secret-canary-cleanup")
	allowCallback, callbackDone := make(chan struct{}), make(chan struct{})
	var release sync.Once
	unblock := func() { release.Do(func() { close(allowCallback) }) }
	defer func() { unblock(); <-callbackDone }()
	endNative := f.counts.enter(32)
	go func() {
		defer close(callbackDone)
		<-allowCallback
		endNative()
		call.Complete(invocation.Outcome[boundaryEnvelope]{Present: true,
			Value:   boundaryEnvelope{Attribution: frozen, Data: transfer{Unknown: 1}, DataPresent: false},
			Primary: native, Cleanup: cleanup})
	}()
	caller.Run, caller.Item, caller.Attempt = "changed", "changed", 99
	wait, cancel := context.WithCancelCause(context.Background())
	waitCause := errors.New("caller-stopped-waiting")
	cancel(waitCause)
	_, waitErr := call.Receipt().Wait(wait)
	waitMeaning := errors.New("fixture caller stopped waiting")
	boundaryWait := errors.Join(waitMeaning, waitErr)
	if !errors.Is(boundaryWait, waitMeaning) || !errors.Is(boundaryWait, context.Canceled) || !errors.Is(boundaryWait, waitCause) {
		t.Fatal("Go error wrapping lost a waiting error or cancellation cause")
	}
	conformance.Cause(t, boundaryWait, func(original *fault.Error) bool {
		return original.Diagnostic().Context.Correlation.Call == frozen.Call
	})
	if inbox.Usage().Outstanding != 1 || !errors.Is(delivery.Release(), invocation.ErrPending) ||
		!errors.Is(f.owner.Close(context.Background()), resource.ErrIncomplete) {
		t.Fatal("ending the caller wait discarded unfinished responsibility")
	}
	unblock()
	result, err := delivery.Receipt().WaitReleased(context.Background())
	if err != nil || !result.Final || !result.Released || !result.Outcome.Present ||
		result.Context.Correlation.Call != frozen.Call ||
		result.Outcome.Value.Attribution != frozen || result.Outcome.Value.DataPresent ||
		result.Outcome.Value.Data.Unknown != 1 || errors.Is(result.Err(), waitCause) || errors.Is(result.Err(), context.Canceled) ||
		!errors.Is(result.Outcome.Primary, native) || !errors.Is(result.Outcome.Cleanup, cleanup) {
		t.Fatal("late completion lost frozen context or replaced facts with waiting policy")
	}
	resultMeaning := errors.New("fixture write unconfirmed")
	boundaryErr := errors.Join(resultMeaning, result.Err())
	if !errors.Is(boundaryErr, resultMeaning) || errors.Is(boundaryErr, waitMeaning) {
		t.Fatal("late result acquired the caller's waiting meaning")
	}
	conformance.Cause(t, boundaryErr, func(original *boundaryNativeError) bool {
		return original == native && original.request == "request-late"
	})
	conformance.Private(t, boundaryErr, "secret-canary")
	if err := delivery.Release(); err != nil || inbox.Usage() != (invocation.InboxUsage{}) {
		t.Fatal("late evidence could not release its own slot")
	}
}

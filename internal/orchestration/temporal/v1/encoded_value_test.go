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

package temporal

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/frost-leo/fathomry/internal/invocation"
	commonpb "go.temporal.io/api/common/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

type encodedMappingState struct {
	mapper   func(converter.EncodedValue) converter.EncodedValue
	retained converter.EncodedValue
	exit     string
}

type encodedMappingValue struct {
	child converter.EncodedValue
	state *encodedMappingState
}

func (value *encodedMappingValue) HasValue() bool { return value.child.HasValue() }
func (value *encodedMappingValue) Get(output any) error {
	return value.child.Get(output)
}
func (value *encodedMappingValue) Payloads() *commonpb.Payloads {
	return value.child.(converter.ValuesPayloads).Payloads()
}
func (value *encodedMappingValue) FathomryMapEncodedValueV1(mapper func(converter.EncodedValue) converter.EncodedValue) converter.EncodedValue {
	copy := *value
	copy.child = mapper(value.child)
	if value.state != nil {
		value.state.mapper = mapper
		value.state.retained = copy.child
		switch value.state.exit {
		case "panic":
			panic("mapper panic")
		case "goexit":
			runtime.Goexit()
		}
	}
	return &copy
}

type encodedOpaqueWrapper struct{ converter.EncodedValue }

func encodedTransferFixture(t testing.TB, native converter.EncodedValue) (*connection, *nativeCall, context.Context, converter.EncodedValue) {
	t.Helper()
	owner := &connection{}
	origin := &nativeCall{owner: owner, scopeOwner: sdk.NewFathomryScopeOwnerV1()}
	current := &nativeCall{owner: owner, scopeOwner: sdk.NewFathomryScopeOwnerV1()}
	work := context.WithValue(context.Background(), nativeCallKey{}, current)
	borrowed := encodedInterceptorResult(&interceptorValue{native: native, authority: origin, factory: &clientFactory{owner: owner}, present: true,
		guard: func(func() error) error { return failure(ErrAuthority, "expired-original-view") }})
	return owner, origin, work, borrowed
}

func TestEncodedValueTransferPreservesOptionalPayloadsWithoutRevivingAliases(t *testing.T) {
	payloads, err := converter.GetDefaultDataConverter().ToPayloads("retained")
	if err != nil {
		t.Fatal(err)
	}
	native := &queryNativeValue{payloads: payloads}
	owner, origin, work, borrowed := encodedTransferFixture(t, native)
	state := &encodedMappingState{}
	original := &encodedMappingValue{child: borrowed, state: state}
	mapped, release, err := transferEncodedValue(work, owner, origin, original)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var output string
	if mapped == original || original.child != borrowed || mapped.Get(&output) != nil || output != "retained" {
		t.Fatal("cooperative value copy lost native response or mutated the original")
	}
	if payloads := mapped.(converter.ValuesPayloads).Payloads(); !proto.Equal(payloads, native.payloads) {
		t.Fatal("optional native payload accessor was lost")
	}
	if err := original.Get(&output); !errors.Is(err, ErrAuthority) {
		t.Fatal("transfer revived original wrapper/borrowed alias", err)
	}
	if err := state.mapper(borrowed).Get(&output); !errors.Is(err, ErrAuthority) {
		t.Fatal("retained mapper extended its synchronous mapping authority", err)
	}
	release()
	if err := mapped.Get(&output); !errors.Is(err, ErrAuthority) {
		t.Fatal("mapped wrapper outlived consumption", err)
	}
	if !mapped.HasValue() {
		t.Fatal("local captured presence was revoked")
	}
	func() {
		defer func() {
			cause, _ := recover().(error)
			if !errors.Is(cause, ErrAuthority) {
				t.Error("expired no-error payload accessor did not refuse", cause)
			}
		}()
		_ = state.retained.(converter.ValuesPayloads).Payloads()
	}()
}

func TestEncodedValueTransferKeepsForeignAndOpaqueRestrictions(t *testing.T) {
	native := &queryNativeValue{}
	owner, origin, work, borrowed := encodedTransferFixture(t, native)
	foreign := &nativeCall{owner: owner, scopeOwner: sdk.NewFathomryScopeOwnerV1()}
	if _, release, err := transferEncodedValue(work, owner, foreign, &encodedMappingValue{child: borrowed}); !errors.Is(err, ErrAuthority) {
		release()
		t.Fatal("different native operation promoted a foreign view", err)
	} else {
		release()
	}
	opaque := &encodedOpaqueWrapper{borrowed}
	mapped, release, err := transferEncodedValue(work, owner, origin, opaque)
	defer release()
	if err != nil || mapped != opaque || !errors.Is(mapped.Get(nil), ErrAuthority) || native.reads.Load() != 0 {
		t.Fatal("unknown wrapper was automatically traversed or promoted", err)
	}
	cycle := &encodedMappingValue{}
	cycle.child = cycle
	_, release, err = transferEncodedValue(work, owner, origin, cycle)
	release()
	if !errors.Is(err, ErrLimit) {
		t.Fatal("cooperative graph cycle exceeded its depth bound", err)
	}
}

func TestEncodedValueTransferNestedViewsDoNotSelfWait(t *testing.T) {
	payloads, _ := converter.GetDefaultDataConverter().ToPayloads("nested")
	owner, origin, work, inner := encodedTransferFixture(t, &queryNativeValue{payloads: payloads})
	outer := encodedInterceptorResult(&interceptorValue{native: &encodedMappingValue{child: inner}, authority: origin, factory: &clientFactory{owner: owner}, present: true,
		guard: func(func() error) error { return failure(ErrAuthority, "expired-original-view") }})
	mapped, release, err := transferEncodedValue(work, owner, origin, &encodedMappingValue{child: outer})
	defer release()
	var output string
	if err != nil || mapped.Get(&output) != nil || output != "nested" {
		t.Fatal("nested borrowed values reacquired one exclusive continuation", err)
	}
}

func TestEncodedValueTransferReleaseSealsAllViewsAndJoinsActualDecode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, unblock := make(chan struct{}), make(chan struct{})
		var decodes atomic.Int32
		native := &queryNativeValue{get: func(any) error {
			decodes.Add(1)
			close(entered)
			<-unblock
			return nil
		}}
		owner, origin, work, borrowed := encodedTransferFixture(t, native)
		mapped, release, err := transferEncodedValue(work, owner, origin, &encodedMappingValue{child: borrowed})
		if err != nil {
			t.Fatal(err)
		}
		decoded, joined := make(chan struct{}), make(chan struct{})
		go func() { defer close(decoded); _ = mapped.Get(nil) }()
		<-entered
		go func() { defer close(joined); release() }()
		synctest.Wait()
		select {
		case <-joined:
			t.Fatal("transfer release discarded an entered decoder")
		default:
		}
		if err := mapped.Get(nil); !errors.Is(err, ErrAuthority) || decodes.Load() != 1 {
			t.Fatal("closing consumption window entered another decoder", err)
		}
		close(unblock)
		<-decoded
		<-joined
	})
}

func TestEncodedValueTransferMapperPanicAndGoexitCloseCopiedViews(t *testing.T) {
	for _, mode := range []string{"panic", "goexit"} {
		t.Run(mode, func(t *testing.T) {
			native := &queryNativeValue{}
			owner, origin, work, borrowed := encodedTransferFixture(t, native)
			state := &encodedMappingState{exit: mode}
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() {
					cause := recover()
					if mode == "panic" && cause != "mapper panic" || mode == "goexit" && cause != nil {
						t.Error("custom mapper abnormal exit changed", cause)
					}
				}()
				_, release, _ := transferEncodedValue(work, owner, origin, &encodedMappingValue{child: borrowed, state: state})
				release()
				t.Error("custom mapper abnormal exit returned")
			}()
			<-done
			if state.retained == nil || !errors.Is(state.retained.Get(nil), ErrAuthority) || native.reads.Load() != 0 {
				t.Fatal("mapper abnormal exit retained copied decoder authority")
			}
		})
	}
}

func TestCallbackEncodedValueRetainsOnlyLiveTaskAndCachedPresence(t *testing.T) {
	worker, client := finalizationWorkerFixture(t, 1)
	client.inbox, _ = invocation.NewInbox[Execution](8, 8*ExecutionEvidenceBytes)
	task, binding, err := worker.beginTask(context.Background(), TaskResult{Kind: "activity"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		binding.closed.Store(true)
		task.Complete(invocation.Outcome[TaskResult]{Present: true})
		binding.finish()
		<-worker.handlers
	}()
	borrower := &callbackClient{nativeClient: client.owner.native, binding: binding}
	payloads, _ := converter.GetDefaultDataConverter().ToPayloads("callback")
	native := &queryNativeValue{payloads: payloads}
	value, err := callbackNative(context.Background(), borrower, "", Execution{Operation: "callback.workflow.query"}, func(work context.Context, _ *Execution) (converter.EncodedValue, error) {
		origin := work.Value(nativeCallKey{}).(*nativeCall)
		view := encodedInterceptorResult(&interceptorValue{native: native, authority: origin, factory: &clientFactory{owner: client.owner}, present: true,
			guard: func(func() error) error { return failure(ErrAuthority, "expired-original-view") }})
		return newCallbackQueryValue(work, borrower, context.Background(), "workflow", "run", &encodedMappingValue{child: view})
	})
	if err != nil {
		t.Fatal(err)
	}
	var output string
	if !value.HasValue() || value.Get(&output) != nil || output != "callback" {
		t.Fatal("live callback lost cooperative encoded response")
	}
	copy := value.(converter.ValuesPayloads).Payloads()
	copy.Payloads[0].Data = []byte("mutation")
	if err := value.Get(&output); err != nil || output != "callback" {
		t.Fatal("callback raw payload alias mutated retained response", err)
	}
	binding.closed.Store(true)
	if !value.HasValue() || !errors.Is(value.Get(&output), ErrAuthority) {
		t.Fatal("closed callback retained decode or lost local presence")
	}
}

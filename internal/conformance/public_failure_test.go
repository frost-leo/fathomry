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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/failure"
	viper "github.com/frost-leo/fathomry/internal/configsource/viper/v1"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	native "github.com/spf13/viper"
)

const (
	publicMalformed failure.Code = "example.config.malformed"
	publicUnmapped  failure.Code = "example.config.acquisition_failed"
	publicWrite     failure.Code = "example.orders.write_unconfirmed"
	publicCleanup   failure.Code = "example.orders.cleanup_failed"
	publicWait      failure.Code = "example.wait.ended"
)

func mapConfiguration(technical error) error {
	if technical == nil {
		return nil
	}
	code := publicUnmapped
	if errors.Is(technical, viper.ErrDecode) {
		code = publicMalformed
	}
	return failure.New(code, nil)
}

func TestPublicFailureMapsActualViperWithoutExposingNative(t *testing.T) {
	const secret = "synthetic-private-config"
	path := filepath.Join(t.TempDir(), "synthetic-private-source.json")
	raw := []byte(`{"token":"` + secret + `", "enabled": }`)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	documents, technical := viper.Load(context.Background(), []viper.LoadInput{{
		File: path, Options: viper.OptionsV1{Encoding: "json"},
	}})
	if documents != nil || !errors.Is(technical, viper.ErrDecode) {
		t.Fatal("actual malformed Viper input did not fail decoding")
	}
	var nativeParse native.ConfigParseError
	var syntax *json.SyntaxError
	var fact *fault.Error
	if !errors.As(technical, &nativeParse) || !errors.As(technical, &syntax) ||
		!errors.As(technical, &fact) || fact.Diagnostic().Context.Provider != viper.ProviderID {
		t.Fatal("native parse cause or original technical provenance absent")
	}
	frozen := struct {
		Native      error
		Attribution boundaryAttribution
	}{
		technical, boundaryAttribution{Call: "config-call", Run: "config-run", Item: "config-item", Owner: "account-a", Attempt: 2},
	}
	public := mapConfiguration(technical)
	if !errors.Is(public, publicMalformed) || errors.Is(public, viper.ErrDecode) || errors.As(public, &syntax) {
		t.Fatal("public mapping lost meaning or promised internal native types")
	}
	conformance.Runtime(t, public, new(failure.Error), secret, path, string(raw), nativeParse.Error())
	if frozen.Native != technical || frozen.Attribution.Owner != "account-a" || !errors.As(frozen.Native, &syntax) {
		t.Fatal("handling public error erased native evidence")
	}
	if mapConfiguration(nil) != nil {
		t.Fatal("successful absence became an error")
	}
	unmapped := mapConfiguration(errors.New("synthetic-unmapped-native"))
	if !errors.Is(unmapped, publicUnmapped) || errors.Is(unmapped, failure.Invalid) {
		t.Fatal("unmapped technical error was confused with malformed public identity")
	}
	unfamiliar := failure.New("extension.unrecognized_meaning", nil)
	if unfamiliar.Code() == publicUnmapped || unfamiliar.Code() == failure.Invalid {
		t.Fatal("unfamiliar extension code acquired fallback meaning")
	}
}

func TestPublicFailurePreservesPrimaryCleanupAndIndependentPartialEvidence(t *testing.T) {
	fixture := newFixture(t, 2, 64, 2)
	inbox, err := invocation.NewInbox[boundaryEnvelope](2, 1024)
	if err != nil {
		t.Fatal(err)
	}
	expected := make([]boundaryAttribution, 2)
	for index := range 2 {
		caller := boundaryAttribution{Call: fmt.Sprintf("public-%d", index),
			Run: fmt.Sprintf("run-%d", index), Item: fmt.Sprintf("item-%d", index),
			Owner: fmt.Sprintf("account-%d", index), Attempt: uint32(index + 1)}
		frozen := caller
		expected[index] = frozen
		call, err := invocation.Begin(context.Background(), fixture.access, request(caller.Call, invocation.Async, 32), inbox, nil)
		if err != nil {
			t.Fatal(err)
		}
		nativeCause := &boundaryNativeError{request: caller.Call}
		cleanupCause := errors.New("secret-canary-cleanup")
		technical := fault.Kind("fixture.write.unconfirmed").New(fault.Context{Operation: "write"}, nativeCause)
		caller.Item, caller.Owner = "changed", "changed"
		if !call.Complete(invocation.Outcome[boundaryEnvelope]{Present: true,
			Value:   boundaryEnvelope{Attribution: frozen, DataPresent: true, Data: transfer{Bytes: index + 1, Unknown: 1}},
			Primary: technical, Cleanup: cleanupCause}) {
			t.Fatal("completion rejected")
		}
		result, err := call.Receipt().WaitReleased(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		primary := failure.New(publicWrite, nil)
		cleanup := failure.New(publicCleanup, nil)
		for _, aggregate := range []error{errors.Join(primary, cleanup), errors.Join(cleanup, errors.Join(primary))} {
			if !errors.Is(aggregate, publicWrite) || !errors.Is(aggregate, publicCleanup) {
				t.Fatal("public aggregate lost supported matching")
			}
			if _, ok := failure.Inspect(aggregate); ok {
				t.Fatal("aggregate guessed primary")
			}
		}
		if primary.Code() != publicWrite || cleanup.Code() != publicCleanup || !errors.Is(result.Outcome.Primary, technical) ||
			!errors.Is(result.Outcome.Cleanup, cleanupCause) || result.Outcome.Value.Data.Unknown != 1 {
			t.Fatal("mapping collapsed main, cleanup or unknown effect evidence")
		}
		// Presentation failure is auxiliary; the independent result is unchanged.
		before := result.Outcome.Value
		present := func(failure.Error) (string, error) { return "", errors.New("fixture renderer refusal") }
		if _, renderErr := present(primary); renderErr == nil || result.Outcome.Value != before {
			t.Fatal("presentation failure changed evidence")
		}
	}
	if inbox.Usage().Outstanding != 2 {
		t.Fatal("handling public errors acknowledged evidence")
	}
	for index := range 2 {
		delivery, err := inbox.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		result, err := delivery.Receipt().WaitReleased(context.Background())
		if err != nil || result.Outcome.Value.Attribution != expected[index] ||
			!result.Outcome.Present || !result.Outcome.Value.DataPresent ||
			result.Outcome.Value.Data.Bytes != index+1 || result.Outcome.Value.Data.Unknown != 1 {
			t.Fatal("independent evidence lost partial output or frozen ownership")
		}
		var cause *boundaryNativeError
		if !errors.As(result.Outcome.Primary, &cause) || cause.request != expected[index].Call ||
			result.Outcome.Cleanup == nil {
			t.Fatal("internal native evidence lost")
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if inbox.Usage() != (invocation.InboxUsage{}) {
		t.Fatal("evidence reservation leaked")
	}
}

func TestPublicFailureEndedWaitDoesNotReplaceLateResult(t *testing.T) {
	fixture := newFixture(t, 1, 32, 1)
	inbox, err := invocation.NewInbox[boundaryEnvelope](1, 512)
	if err != nil {
		t.Fatal(err)
	}
	caller := boundaryAttribution{Call: "public-late", Run: "run-a", Item: "item-a", Owner: "account-a", Attempt: 4}
	frozen := caller
	call, err := invocation.Begin(context.Background(), fixture.access, request(caller.Call, invocation.Async, 32), inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	nativeCause := &boundaryNativeError{request: "late-request"}
	allow, done := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(allow) }) }
	defer func() { unblock(); <-done }()
	go func() {
		defer close(done)
		<-allow
		call.Complete(invocation.Outcome[boundaryEnvelope]{Present: true,
			Value:   boundaryEnvelope{Attribution: frozen, DataPresent: false, Data: transfer{Unknown: 1}},
			Primary: nativeCause})
	}()
	caller.Item, caller.Attempt = "new-attempt", 99
	wait, cancel := context.WithCancelCause(context.Background())
	reason := errors.New("caller stopped waiting")
	cancel(reason)
	_, internalWait := call.Receipt().Wait(wait)
	if !errors.Is(internalWait, context.Canceled) || !errors.Is(internalWait, reason) {
		t.Fatal("internal wait lost intentional cancellation observations")
	}
	public := failure.New(publicWait, errors.Join(wait.Err(), context.Cause(wait)))
	if !errors.Is(public, context.Canceled) || !errors.Is(public, reason) {
		t.Fatal("public reasons lost")
	}
	var internal *fault.Error
	if errors.As(public, &internal) {
		t.Fatal("native wait implementation accidentally promised")
	}
	if inbox.Usage().Outstanding != 1 || !errors.Is(delivery.Release(), invocation.ErrPending) ||
		!errors.Is(fixture.owner.Close(context.Background()), resource.ErrIncomplete) {
		t.Fatal("ending a public wait erased unresolved responsibility")
	}
	unblock()
	result, err := delivery.Receipt().WaitReleased(context.Background())
	if err != nil || !result.Final || !result.Released || result.Outcome.Value.Attribution != frozen ||
		result.Outcome.Value.DataPresent || result.Outcome.Value.Data.Unknown != 1 ||
		!errors.Is(result.Outcome.Primary, nativeCause) || errors.Is(result.Err(), reason) || errors.Is(result.Err(), context.Canceled) {
		t.Fatal("late operation facts replaced by waiting error or mutated attribution")
	}
	if failure.New(publicWrite, nil).Code() == public.Code() {
		t.Fatal("late outcome inherited wait identity")
	}
	if err := delivery.Release(); err != nil || inbox.Usage() != (invocation.InboxUsage{}) {
		t.Fatal("late evidence not released")
	}
}

func TestPublicFailureDoesNotManufactureErrorsForKnownEmptyOrFiltering(t *testing.T) {
	fixture := newFixture(t, 1, 16, 1)
	type output struct {
		Count    int
		Filtered bool
	}
	inbox, err := invocation.NewInbox[output](1, 128)
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range []output{{Count: 0}, {Count: 0, Filtered: true}} {
		call, err := invocation.Begin(context.Background(), fixture.access, request("successful-empty", invocation.Finite, 16), inbox, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !call.Complete(invocation.Outcome[output]{Present: true, Value: data}) {
			t.Fatal("empty success rejected")
		}
		delivery, err := inbox.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		result, err := delivery.Receipt().WaitReleased(context.Background())
		if err != nil || !result.Outcome.Present || !reflect.DeepEqual(result.Outcome.Value, data) || result.Err() != nil {
			t.Fatal("explicit empty output or normal filtering became a failure")
		}
		if mapConfiguration(result.Err()) != nil {
			t.Fatal("boundary manufactured an error")
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
}

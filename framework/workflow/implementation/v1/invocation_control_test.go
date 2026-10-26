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

package implementation_test

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"github.com/frost-leo/fathomry/framework/workflow/declaration/v1/testdata/health"
	implementation "github.com/frost-leo/fathomry/framework/workflow/implementation/v1"
)

type invocationMarker struct{}
type opaqueHandlerError struct{}

func (*opaqueHandlerError) Error() string { panic("native error text evaluated") }

func TestInvokeNativeErrorAndContextIdentity(t *testing.T) {
	ctx := context.WithValue(context.Background(), invocationMarker{}, "observed")
	var marker *opaqueHandlerError
	registry := invokeRegistry(t, stringSchema(1, 8), integerSchema(0, 100), func(received context.Context, _ invocationInput[string]) (invocationOutput[int64], error) {
		if received != ctx || received.Value(invocationMarker{}) != "observed" {
			t.Fatal("context was substituted")
		}
		return invocationOutput[int64]{42}, marker
	})
	result, err := registry.Invoke(ctx, "read_cpu", []byte(`{"target":"host"}`))
	if err == nil || !result.Invoked || result.Output != nil || !errors.Is(err, marker) {
		t.Fatal("typed nil handler cause not retained")
	}
	_ = err.Error()
	if invocationPhase(t, err) != "handler" {
		t.Fatal("wrong handler phase")
	}
	bundle, err := declaration.Compile(context.Background(), health.Declarations())
	if err != nil {
		t.Fatal(err)
	}
	_, staticError := implementation.Prepare(context.Background(), bundle, declaration.Identity{Namespace: "example", Code: "server-health", Version: "1"}, nil)
	if _, ok := implementation.Diagnose(staticError); !ok {
		t.Fatal("static failure control")
	}
	registry = invokeRegistry(t, stringSchema(1, 8), integerSchema(0, 100), func(context.Context, invocationInput[string]) (invocationOutput[int64], error) {
		return invocationOutput[int64]{42}, staticError
	})
	result, err = registry.Invoke(ctx, "read_cpu", []byte(`{"target":"host"}`))
	if !result.Invoked || !errors.Is(err, staticError) || invocationPhase(t, err) != "handler" {
		t.Fatal("handler error became validation")
	}
}

func TestInvokePanicAndGoexit(t *testing.T) {
	token := &struct{ Value string }{"private_panic"}
	registry := invokeRegistry(t, stringSchema(1, 8), integerSchema(0, 100), func(context.Context, invocationInput[string]) (invocationOutput[int64], error) { panic(token) })
	func() {
		defer func() {
			if recovered := recover(); recovered != token {
				t.Fatal("panic converted or swallowed")
			}
		}()
		_, _ = registry.Invoke(context.Background(), "read_cpu", []byte(`{"target":"host"}`))
		t.Error("panic unexpectedly returned")
	}()
	registry = invokeRegistry(t, stringSchema(1, 8), integerSchema(0, 100), func(context.Context, invocationInput[string]) (invocationOutput[int64], error) {
		runtime.Goexit()
		return invocationOutput[int64]{}, nil
	})
	done := make(chan struct{})
	returned := make(chan struct{}, 1)
	go func() {
		defer close(done)
		_, _ = registry.Invoke(context.Background(), "read_cpu", []byte(`{"target":"host"}`))
		returned <- struct{}{}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Goexit goroutine did not terminate")
	}
	select {
	case <-returned:
		t.Fatal("Goexit became a returned result")
	default:
	}
}

func TestInvokeUncooperativeHandler(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := invokeRegistry(t, stringSchema(1, 8), integerSchema(0, 100), func(context.Context, invocationInput[string]) (invocationOutput[int64], error) {
		close(entered)
		<-release
		return invocationOutput[int64]{42}, nil
	})
	var result implementation.Invocation
	var failure error
	go func() {
		defer close(done)
		result, failure = registry.Invoke(ctx, "read_cpu", []byte(`{"target":"host"}`))
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		close(release)
		<-done
		t.Fatal("handler never entered")
	}
	cancel()
	select {
	case <-done:
		close(release)
		t.Fatal("boundary returned while handler still owns work")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("released handler did not finish")
	}
	if !result.Invoked || result.Output != nil || !errors.Is(failure, context.Canceled) {
		t.Fatal("post-call cancellation lost")
	}
}

type returnCancellationContext struct {
	context.Context
	checks int
	cancel context.CancelCauseFunc
	cause  error
}

func (ctx *returnCancellationContext) Err() error {
	var callers [2]uintptr
	if count := runtime.Callers(2, callers[:]); count == 2 {
		frames := runtime.CallersFrames(callers[:count])
		first, _ := frames.Next()
		second, _ := frames.Next()
		if strings.HasSuffix(first.Function, "/implementation/v1.invocationCanceled") && strings.HasSuffix(second.Function, "/implementation/v1.(*Registry).Invoke") {
			ctx.checks++
			if ctx.checks == 4 {
				ctx.cancel(ctx.cause)
			}
		}
	}
	return ctx.Context.Err()
}
func TestInvokeFinalCancellationObservation(t *testing.T) {
	base, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	cause := errors.New("final checkpoint")
	ctx := &returnCancellationContext{Context: base, cancel: cancel, cause: cause}
	calls := 0
	registry := invokeRegistry(t, stringSchema(1, 8), integerSchema(0, 100), func(context.Context, invocationInput[string]) (invocationOutput[int64], error) {
		calls++
		return invocationOutput[int64]{42}, nil
	})
	result, err := registry.Invoke(ctx, "read_cpu", []byte(`{"target":"host"}`))
	if ctx.checks < 4 || calls != 1 || !result.Invoked || result.Output != nil || !errors.Is(err, cause) || invocationPhase(t, err) != "output" {
		t.Fatal("final checkpoint did not suppress publication", err, ctx.checks)
	}
}

func TestInvokeBooleanAndExactIntegers(t *testing.T) {
	type boolean bool
	booleans := invokeRegistry(t, declaration.DataSchema{Type: "boolean"}, declaration.DataSchema{Type: "boolean"}, func(_ context.Context, input invocationInput[boolean]) (invocationOutput[boolean], error) {
		return invocationOutput[boolean]{input.Target}, nil
	})
	for _, raw := range []string{`{"target":false}`, `{"target":true}`} {
		if _, err := booleans.Invoke(context.Background(), "read_cpu", []byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	const maximum = int64(1<<53 - 1)
	integers := invokeRegistry(t, integerSchema(-maximum, maximum), integerSchema(-maximum, maximum), func(_ context.Context, input invocationInput[int64]) (invocationOutput[int64], error) {
		return invocationOutput[int64]{input.Target}, nil
	})
	for _, raw := range []string{`{"target":9007199254740991}`, `{"target":-9007199254740991}`} {
		if _, err := integers.Invoke(context.Background(), "read_cpu", []byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestInvokeEmptyPortObjects(t *testing.T) {
	source := health.Declarations()
	cpu := source.Workflows[0].Spec.Bindings["read_cpu"]
	cpu.Inputs = nil
	source.Workflows[0].Spec.Bindings["read_cpu"] = cpu
	sample := source.Workflows[0].Spec.Nodes["sample"]
	call := sample.Activities["cpu"]
	call.Inputs = nil
	sample.Activities["cpu"] = call
	source.Workflows[0].Spec.Nodes["sample"] = sample
	bundle, err := declaration.Compile(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	metric := implementation.Activity(func(context.Context, invocationInput[string]) (invocationOutput[int64], error) {
		return invocationOutput[int64]{42}, nil
	})
	registry, err := implementation.Prepare(context.Background(), bundle, declaration.Identity{Namespace: "example", Code: "server-health", Version: "1"}, []implementation.Binding{
		{Key: "read_cpu", Callable: implementation.Activity(func(context.Context, struct{}) (invocationOutput[int64], error) {
			return invocationOutput[int64]{42}, nil
		})},
		{Key: "read_memory", Callable: metric}, {Key: "summarize", Callable: implementation.Activity(func(context.Context, invocationSummary[int64]) (invocationText, error) {
			return invocationText{"ok"}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Invoke(context.Background(), "read_cpu", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"", "null", "[]", `{"extra":true}`} {
		if result, err := registry.Invoke(context.Background(), "read_cpu", []byte(raw)); err == nil || result.Invoked {
			t.Fatal("non-object empty input admitted")
		}
	}
}

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
	"bytes"
	"context"
	"encoding/base64"
	jsonv1 "encoding/json"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/failure/v1"
	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"github.com/frost-leo/fathomry/framework/workflow/declaration/v1/testdata/health"
	implementation "github.com/frost-leo/fathomry/framework/workflow/implementation/v1"
)

type invocationInput[T any] struct {
	Target T `json:"target"`
}
type invocationOutput[T any] struct {
	Value T `json:"value"`
}
type invocationSummary[T any] struct {
	CPU    T `json:"cpu"`
	Memory T `json:"memory"`
}
type invocationText struct {
	Text string `json:"text"`
}

func integerSchema(minimum, maximum int64) declaration.DataSchema {
	return declaration.DataSchema{Type: "integer", Minimum: &minimum, Maximum: &maximum}
}
func stringSchema(minimum, maximum int64) declaration.DataSchema {
	return declaration.DataSchema{Type: "string", MinLength: &minimum, MaxLength: &maximum}
}
func arraySchema(item declaration.DataSchema, minimum, maximum int64) declaration.DataSchema {
	return declaration.DataSchema{Type: "array", Items: &item, MinItems: &minimum, MaxItems: &maximum}
}
func objectSchema(properties map[string]declaration.DataSchema) declaration.DataSchema {
	closed := false
	names := make([]string, 0, len(properties))
	for name := range properties {
		names = append(names, name)
	}
	return declaration.DataSchema{Type: "object", Properties: properties, Required: names, AdditionalProperties: &closed}
}
func invokeRegistry[Target, Metric any](t testing.TB, target, metric declaration.DataSchema, function func(context.Context, invocationInput[Target]) (invocationOutput[Metric], error)) *implementation.Registry {
	t.Helper()
	source := health.Declarations()
	source.Workflows[0].Spec.Contracts["target"] = target
	source.Workflows[0].Spec.Contracts["metric"] = metric
	bundle, err := declaration.Compile(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	callable := implementation.Activity(function)
	registry, err := implementation.Prepare(context.Background(), bundle, declaration.Identity{Namespace: "example", Code: "server-health", Version: "1"}, []implementation.Binding{
		{Key: "read_cpu", Callable: callable}, {Key: "read_memory", Callable: callable},
		{Key: "summarize", Callable: implementation.Activity(func(context.Context, invocationSummary[Metric]) (invocationText, error) {
			return invocationText{"ok"}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}
func invocationPhase(t testing.TB, err error) string {
	t.Helper()
	if !errors.Is(err, implementation.ErrInvocation) {
		t.Fatal("not an invocation failure", err)
	}
	if _, ok := implementation.Diagnose(err); ok {
		t.Fatal("runtime failure became static validation")
	}
	var detail *failure.Detailed[implementation.InvocationDiagnostic]
	if !errors.As(err, &detail) {
		t.Fatal("missing runtime diagnostic")
	}
	value, ok := detail.Details()
	if !ok {
		t.Fatal("missing detail")
	}
	return value.Phase
}

func TestInvokeValueBoundary(t *testing.T) {
	var calls atomic.Int64
	registry := invokeRegistry(t, stringSchema(1, 8), integerSchema(0, 100), func(ctx context.Context, input invocationInput[string]) (invocationOutput[int64], error) {
		calls.Add(1)
		if input.Target != "host" {
			t.Error("decoded input changed")
		}
		return invocationOutput[int64]{42}, nil
	})
	if calls.Load() != 0 {
		t.Fatal("preparation invoked code")
	}
	input := []byte(`{"target":"host"}`)
	result, err := registry.Invoke(context.Background(), "read_cpu", input)
	if err != nil || !result.Invoked || string(result.Output) != `{"value":42}` || calls.Load() != 1 {
		t.Fatal(result, err, calls.Load())
	}
	if bytes.Contains([]byte(fmt.Sprintf("%+v", result)), []byte("42")) {
		t.Fatal("format leaked output")
	}
	result.Output[0] = '!'
	second, err := registry.Invoke(context.Background(), "read_cpu", input)
	if err != nil || string(second.Output) != `{"value":42}` {
		t.Fatal("output alias", err)
	}
	for _, raw := range []string{
		`{}`, `null`, `[]`, `{"target":null}`, `{"target":""}`, `{"target":"123456789"}`, `{"Target":"host"}`, `{"target":"host","private_marker":true}`, `{"target":"host","target":"host"}`, `{"target":"host","\u0074arget":"host"}`, `{"target":"\ud800"}`, `{"target":"host"} {}`,
	} {
		t.Run(raw, func(t *testing.T) {
			before := calls.Load()
			result, err := registry.Invoke(context.Background(), "read_cpu", []byte(raw))
			if err == nil || result.Invoked || result.Output != nil || calls.Load() != before || invocationPhase(t, err) != "input" {
				t.Fatal("invalid input dispatched", err)
			}
			if bytes.Contains([]byte(err.Error()), []byte("private_marker")) {
				t.Fatal("error leaked input")
			}
		})
	}
}

func TestInvokeIntegerAndDuration(t *testing.T) {
	registry := invokeRegistry(t, integerSchema(0, 100), integerSchema(0, 100), func(_ context.Context, input invocationInput[time.Duration]) (invocationOutput[time.Duration], error) {
		return invocationOutput[time.Duration]{input.Target}, nil
	})
	for _, raw := range []string{`{"target":0}`, `{"target":100}`} {
		result, err := registry.Invoke(context.Background(), "read_cpu", []byte(raw))
		if err != nil || !result.Invoked {
			t.Fatal("duration-compatible integer refused", err)
		}
	}
	for _, raw := range []string{`{"target":-1}`, `{"target":101}`, `{"target":1e0}`, `{"target":1.0}`, `{"target":-0}`, `{"target":9007199254740992}`, `{"target":9223372036854775808}`} {
		result, err := registry.Invoke(context.Background(), "read_cpu", []byte(raw))
		if err == nil || result.Invoked || result.Output != nil {
			t.Fatal("invalid integer admitted", raw, err)
		}
	}
}

func TestInvokeNestedNullableAndAliasing(t *testing.T) {
	type record struct {
		Name   string  `json:"name"`
		Values []int64 `json:"values"`
	}
	schema := objectSchema(map[string]declaration.DataSchema{"name": stringSchema(1, 1), "values": arraySchema(integerSchema(0, 10), 0, 2)})
	nullable := declaration.DataSchema{AnyOf: []declaration.DataSchema{schema, {Type: "null"}}}
	var returned *record
	registry := invokeRegistry(t, nullable, nullable, func(_ context.Context, input invocationInput[*record]) (invocationOutput[*record], error) {
		returned = input.Target
		if returned != nil && len(returned.Values) > 0 {
			returned.Values[0] = 9
		}
		return invocationOutput[*record]{returned}, nil
	})
	input := []byte(`{"target":{"name":"é","values":[1]}}`)
	original := bytes.Clone(input)
	result, err := registry.Invoke(context.Background(), "read_cpu", input)
	if err != nil || !result.Invoked || !bytes.Equal(input, original) {
		t.Fatal(result, err)
	}
	stable := bytes.Clone(result.Output)
	returned.Values[0] = 3
	if !bytes.Equal(result.Output, stable) {
		t.Fatal("published output aliases handler storage")
	}
	for _, raw := range []string{`{"target":null}`, `{"target":{"name":"é","values":[]}}`} {
		if _, err := registry.Invoke(context.Background(), "read_cpu", []byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	for _, raw := range []string{`{"target":{"name":"é"}}`, `{"target":{"name":"é","values":null}}`, `{"target":{"name":"ee","values":[]}}`, `{"target":{"name":"é","values":[1,2,3]}}`} {
		result, err := registry.Invoke(context.Background(), "read_cpu", []byte(raw))
		if err == nil || result.Invoked {
			t.Fatal("bad nested input dispatched", err)
		}
	}
}

func TestInvokeOutputAndHandlerFailures(t *testing.T) {
	cause := errors.New("private_handler_cause")
	for _, handlerError := range []error{nil, cause} {
		registry := invokeRegistry(t, stringSchema(1, 8), integerSchema(0, 100), func(context.Context, invocationInput[string]) (invocationOutput[int64], error) {
			return invocationOutput[int64]{101}, handlerError
		})
		result, err := registry.Invoke(context.Background(), "read_cpu", []byte(`{"target":"host"}`))
		if err == nil || !result.Invoked || result.Output != nil {
			t.Fatal("failed output published", err)
		}
		want := "output"
		if handlerError != nil {
			want = "handler"
			if !errors.Is(err, cause) {
				t.Fatal("cause lost")
			}
		}
		if invocationPhase(t, err) != want || bytes.Contains([]byte(err.Error()), []byte(cause.Error())) {
			t.Fatal("classification or privacy", err)
		}
	}
	metric := arraySchema(integerSchema(0, 10), 0, 2)
	for _, values := range [][]int64{nil, {}, {1, 2, 3}, {11}} {
		registry := invokeRegistry(t, stringSchema(1, 8), metric, func(context.Context, invocationInput[string]) (invocationOutput[[]int64], error) {
			return invocationOutput[[]int64]{values}, nil
		})
		result, err := registry.Invoke(context.Background(), "read_cpu", []byte(`{"target":"host"}`))
		wantSuccess := values != nil && len(values) == 0
		if (err == nil) != wantSuccess || !result.Invoked || !wantSuccess && result.Output != nil {
			t.Fatal("nil/empty/invalid output conflated", err)
		}
	}
}

func TestInvokeCancellationAndConcurrentCalls(t *testing.T) {
	var count atomic.Int64
	registry := invokeRegistry(t, stringSchema(1, 8), integerSchema(0, 100), func(context.Context, invocationInput[string]) (invocationOutput[int64], error) {
		count.Add(1)
		return invocationOutput[int64]{42}, nil
	})
	input := []byte(`{"target":"host"}`)
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("private_cancel")
	cancel(cause)
	result, err := registry.Invoke(ctx, "read_cpu", input)
	if result.Invoked || result.Output != nil || !errors.Is(err, context.Canceled) || !errors.Is(err, cause) || count.Load() != 0 {
		t.Fatal("pre-cancel", err)
	}
	var wait sync.WaitGroup
	for range 8 {
		wait.Go(func() {
			for range 10 {
				value, err := registry.Invoke(context.Background(), "read_cpu", input)
				if err != nil || !value.Invoked {
					t.Error(err)
				}
			}
		})
	}
	wait.Wait()
	if count.Load() != 80 {
		t.Fatal("wrong attempt count", count.Load())
	}
	native := errors.New("native handler")
	ctx, cancel = context.WithCancelCause(context.Background())
	defer cancel(nil)
	registry = invokeRegistry(t, stringSchema(1, 8), integerSchema(0, 100), func(context.Context, invocationInput[string]) (invocationOutput[int64], error) {
		cancel(cause)
		return invocationOutput[int64]{42}, native
	})
	result, err = registry.Invoke(ctx, "read_cpu", input)
	if !result.Invoked || result.Output != nil || !errors.Is(err, cause) || !errors.Is(err, native) || !errors.Is(err, implementation.ErrCanceled) {
		t.Fatal("post-dispatch evidence lost", err)
	}
	if invocationPhase(t, err) != "handler" {
		t.Fatal("wrong phase")
	}
}

func TestInvokeHandlesAndEnums(t *testing.T) {
	enum := stringSchema(1, 2)
	enum.Enum = []string{"é"}
	registry := invokeRegistry(t, enum, enum, func(_ context.Context, input invocationInput[string]) (invocationOutput[string], error) {
		return invocationOutput[string]{input.Target}, nil
	})
	if _, err := registry.Invoke(context.Background(), "read_cpu", []byte(`{"target":"é"}`)); err != nil {
		t.Fatal(err)
	}
	if result, err := registry.Invoke(context.Background(), "read_cpu", []byte(`{"target":"e"}`)); err == nil || result.Invoked {
		t.Fatal("enum not enforced")
	}
	for _, value := range []*implementation.Registry{nil, new(implementation.Registry)} {
		if result, err := value.Invoke(context.Background(), "read_cpu", nil); !errors.Is(err, implementation.ErrHandle) || result.Invoked {
			t.Fatal("handle", err)
		}
	}
	if result, err := registry.Invoke(nil, "read_cpu", nil); !errors.Is(err, implementation.ErrHandle) || result.Invoked {
		t.Fatal("context", err)
	}
	if result, err := registry.Invoke(context.Background(), "private_key", nil); !errors.Is(err, implementation.ErrInvalid) || result.Invoked || bytes.Contains([]byte(err.Error()), []byte("private_key")) {
		t.Fatal("key", err)
	}
	invalid := []byte("{\"target\":\"")
	invalid = append(invalid, 0xff)
	invalid = append(invalid, []byte("\"}")...)
	if result, err := registry.Invoke(context.Background(), "read_cpu", invalid); err == nil || result.Invoked {
		t.Fatal("invalid UTF8 accepted")
	}
}

func TestInvokeRetainsOwnedSchema(t *testing.T) {
	inputSchema := stringSchema(1, 8)
	outputSchema := integerSchema(0, 100)
	registry := invokeRegistry(t, inputSchema, outputSchema, func(context.Context, invocationInput[string]) (invocationOutput[int64], error) {
		return invocationOutput[int64]{42}, nil
	})
	*inputSchema.MinLength = 8
	*outputSchema.Maximum = 0
	if _, err := registry.Invoke(context.Background(), "read_cpu", []byte(`{"target":"host"}`)); err != nil {
		t.Fatal("authored schema mutation changed prepared invocation", err)
	}
}

func TestInvocationNestedLoggingAndSerialization(t *testing.T) {
	const marker = "synthetic_private_payload"
	registry := invokeRegistry(t, stringSchema(1, 8), stringSchema(1, 64), func(context.Context, invocationInput[string]) (invocationOutput[string], error) {
		return invocationOutput[string]{marker}, nil
	})
	result, err := registry.Invoke(context.Background(), "read_cpu", []byte(`{"target":"host"}`))
	if err != nil {
		t.Fatal(err)
	}
	encoded := base64.StdEncoding.EncodeToString(result.Output)
	for _, value := range []any{result, &result, struct{ Result implementation.Invocation }{result}, []implementation.Invocation{result}} {
		for _, jsonHandler := range []bool{false, true} {
			var buffer bytes.Buffer
			var handler slog.Handler = slog.NewTextHandler(&buffer, nil)
			if jsonHandler {
				handler = slog.NewJSONHandler(&buffer, nil)
			}
			slog.New(handler).Info("control", "result", value)
			if strings.Contains(buffer.String(), marker) || strings.Contains(buffer.String(), encoded) {
				t.Fatalf("nested default logging exposed output for %T", value)
			}
		}
		if _, err := jsonv1.Marshal(value); !errors.Is(err, implementation.ErrHandle) {
			t.Fatal("legacy JSON restored implicit result serialization", err)
		}
		if _, err := json.Marshal(value); !errors.Is(err, implementation.ErrHandle) {
			t.Fatal("JSON v2 restored implicit result serialization", err)
		}
	}
	for _, decode := range []func([]byte, any) error{jsonv1.Unmarshal, func(raw []byte, value any) error { return json.Unmarshal(raw, value) }} {
		var reconstructed implementation.Invocation
		if err := decode([]byte(`{"Invoked":true,"Output":"cHJpdmF0ZQ=="}`), &reconstructed); !errors.Is(err, implementation.ErrHandle) || reconstructed.Invoked || reconstructed.Output != nil {
			t.Fatal("serialized data reconstructed invocation observation", err)
		}
	}
}

func FuzzInvokeInput(f *testing.F) {
	registry := invokeRegistry(f, stringSchema(1, 8), integerSchema(0, 100), func(context.Context, invocationInput[string]) (invocationOutput[int64], error) {
		return invocationOutput[int64]{42}, nil
	})
	for _, raw := range []string{`{"target":"host"}`, `{}`, `null`, `{"target":null}`} {
		f.Add([]byte(raw))
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 1<<20 {
			return
		}
		result, err := registry.Invoke(context.Background(), "read_cpu", input)
		if err != nil {
			if result.Output != nil {
				t.Fatal("partial output")
			}
			return
		}
		var output invocationOutput[int64]
		if !result.Invoked || json.Unmarshal(result.Output, &output) != nil || !reflect.DeepEqual(output, invocationOutput[int64]{42}) {
			t.Fatal("false success")
		}
	})
}

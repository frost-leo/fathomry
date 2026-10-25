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

package implementation

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"slices"

	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
)

// Callable retains a deliberately borrowed Go function. It owns no captured
// resources and does not make receiver/captured state immutable or thread-safe.
type Callable struct {
	function any
	input    reflect.Type
	output   reflect.Type
}

// Activity declares the supported two-argument/two-result ABI without validation.
// A typed-nil function remains representable so selection precedes validation.
func Activity[Input, Output any](function func(context.Context, Input) (Output, error)) Callable {
	return Callable{function: function, input: reflect.TypeFor[Input](), output: reflect.TypeFor[Output]()}
}

// Binding associates one declaration-local key with its explicit callable.
// A sequence preserves duplicate attempts; do not mutate inputs during Prepare.
type Binding struct {
	Key      string
	Callable Callable
}

// VersionBindings opts one exact workflow version into complete binding checking.
// A present value with an empty Bindings slice is not an absent implementation set.
type VersionBindings struct {
	Version  string
	Bindings []Binding
}

// Registry owns immutable metadata and copied registration slots. Callable values
// retain their original captures; no deep copying or lifecycle authority is implied.
type Registry struct {
	manifest  Manifest
	functions map[string]any
	valid     bool
}

// Prepare validates bindings against one exact workflow version in a verified
// bundle. It never invokes functions or gives partial executable authority on error.
func Prepare(ctx context.Context, bundle *declaration.Bundle, identity declaration.Identity, bindings []Binding) (*Registry, error) {
	projection, err := project(ctx, bundle, identity)
	if err != nil {
		return nil, err
	}
	if len(bindings) > 4096 {
		return nil, problem(ErrLimit, "bindings", "/bindings")
	}
	functions := make(map[string]any, len(bindings))
	supplied := make(map[string]int, len(bindings))
	for index, binding := range bindings {
		if err := canceled(ctx); err != nil {
			return nil, err
		}
		path := indexed("/bindings", index)
		if len(binding.Key) > 64 {
			return nil, problem(ErrInvalid, "binding_key", path)
		}
		if _, ok := projection.spec.Bindings[binding.Key]; !ok {
			return nil, problem(ErrInvalid, "binding_key", path)
		}
		if _, ok := supplied[binding.Key]; ok {
			return nil, problem(ErrInvalid, "duplicate_binding", path)
		}
		supplied[binding.Key] = index
	}
	if len(supplied) != len(projection.spec.Bindings) {
		return nil, problem(ErrInvalid, "missing_binding", "/bindings")
	}
	matcher := typeMatcher{ctx: ctx, active: make(map[reflect.Type]bool)}
	for _, entry := range projection.manifest.Bindings {
		if err := canceled(ctx); err != nil {
			return nil, err
		}
		index := supplied[entry.Key]
		callable := bindings[index].Callable
		path := indexed("/bindings", index)
		if callable.function == nil || callable.input == nil || callable.output == nil || reflect.ValueOf(callable.function).IsNil() {
			return nil, problem(ErrInvalid, "nil_callable", path)
		}
		declared := projection.spec.Bindings[entry.Key]
		if err := matcher.ports(callable.input, declared.Inputs, projection.spec.Contracts, path+"/input"); err != nil {
			return nil, err
		}
		if err := matcher.ports(callable.output, declared.Outputs, projection.spec.Contracts, path+"/output"); err != nil {
			return nil, err
		}
		functions[entry.Key] = callable.function
	}
	if err := manifestBound(projection.manifest); err != nil {
		return nil, err
	}
	if err := canceled(ctx); err != nil {
		return nil, err
	}
	return &Registry{manifest: projection.manifest, functions: functions, valid: true}, nil
}

// ManifestCopy returns independent metadata, not a function serialization.
func (registry *Registry) ManifestCopy() (Manifest, error) {
	if registry == nil || !registry.valid {
		return Manifest{}, problem(ErrHandle, "registry", "")
	}
	value := registry.manifest
	value.Bindings = slices.Clone(value.Bindings)
	value.Calls = slices.Clone(value.Calls)
	return value, nil
}

// Lookup returns the original borrowed function for a declared binding key.
// It does not invoke it, authorize execution or transfer captured-resource ownership.
func (registry *Registry) Lookup(key string) (any, error) {
	if registry == nil || !registry.valid {
		return nil, problem(ErrHandle, "registry", "")
	}
	function, ok := registry.functions[key]
	if !ok {
		return nil, problem(ErrInvalid, "binding_key", "")
	}
	return function, nil
}

func (Callable) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("implementation.Callable"))
}
func (Callable) LogValue() slog.Value { return slog.StringValue("implementation.Callable") }
func (Callable) MarshalJSON() ([]byte, error) {
	return nil, problem(ErrHandle, "runtime_serialization", "")
}
func (*Callable) UnmarshalJSON([]byte) error   { return problem(ErrHandle, "runtime_serialization", "") }
func (Binding) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("implementation.Binding")) }
func (Binding) LogValue() slog.Value           { return slog.StringValue("implementation.Binding") }
func (Binding) MarshalJSON() ([]byte, error) {
	return nil, problem(ErrHandle, "runtime_serialization", "")
}
func (*Binding) UnmarshalJSON([]byte) error { return problem(ErrHandle, "runtime_serialization", "") }
func (VersionBindings) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("implementation.VersionBindings"))
}
func (VersionBindings) LogValue() slog.Value {
	return slog.StringValue("implementation.VersionBindings")
}
func (VersionBindings) MarshalJSON() ([]byte, error) {
	return nil, problem(ErrHandle, "runtime_serialization", "")
}
func (*VersionBindings) UnmarshalJSON([]byte) error {
	return problem(ErrHandle, "runtime_serialization", "")
}
func (Registry) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("implementation.Registry"))
}
func (Registry) LogValue() slog.Value { return slog.StringValue("implementation.Registry") }
func (Registry) MarshalJSON() ([]byte, error) {
	return nil, problem(ErrHandle, "runtime_serialization", "")
}
func (*Registry) UnmarshalJSON([]byte) error { return problem(ErrHandle, "runtime_serialization", "") }

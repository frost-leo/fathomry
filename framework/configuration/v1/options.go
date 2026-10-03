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

package configuration

import (
	"context"
	"reflect"
	"slices"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/resource/v1"
)

// Encoding names the original document syntax, independently of its provider.
type Encoding string

const (
	JSON Encoding = "json"
	YAML Encoding = "yaml"
	TOML Encoding = "toml"
)

// LayerKind assigns precedence, not provider identity.
type LayerKind uint8

const (
	Base        LayerKind = 1
	Environment LayerKind = 2
	Override    LayerKind = 3
	// Variables appears only in preparation provenance, not external source slots.
	Variables LayerKind = 4
)

// Schema declares project data and bounded, non-panicking validation.
// Defaults is frozen at admission; validation receives a separate candidate.
type Schema[T any] struct {
	Version  uint32
	Defaults T
	Validate func(context.Context, T) error
}

type layer struct {
	Kind     LayerKind `json:"kind"`
	Encoding Encoding  `json:"encoding"`
	Optional bool      `json:"optional"`
}

// Variable is an explicitly captured value. It grants no environment-read
// authority. JSON=false means literal text; Present distinguishes absent/empty.
type Variable struct {
	Path    string
	Value   string
	Present bool
	JSON    bool
}

// Declaration contains configuration policy, not process inputs or SDK handles.
type Declaration[T any] struct {
	Schema    Schema[T]
	Variables []Variable
}

// Dependencies explicitly selects a provider. A zero Provider is invalid, never
// a default. Resources optionally borrows an existing adoption scope; the loader
// does not close it. No Adapter runtime, inbox or receiver is caller-owned here.
type Dependencies struct {
	Provider  Provider
	Resources *resource.Scope
}

// WatchOptions bounds decision notifications, not native polling. Zero uses 16;
// valid capacities are 1..64. Required evidence is separately retained until release.
type WatchOptions struct {
	QueueCapacity int `json:"queue_capacity"`
}

type plan[T any] struct {
	schema    configsource.Schema[T]
	source    configsource.Source
	layers    []layer
	variables *configsource.Layer
}

func freeze[T any](ctx context.Context, declaration Declaration[T], provider Provider, watch bool) (plan[T], error) {
	if provider.state == nil || provider.state.open == nil || watch && len(provider.state.layers) == 0 {
		return plan[T]{}, fail(ErrDeclaration, "provider")
	}
	if len(declaration.Variables) > configsource.MaxVariables {
		return plan[T]{}, fail(ErrLimit, "declaration")
	}
	if err := validateLayers(provider.state.layers); err != nil {
		return plan[T]{}, err
	}
	schema := configsource.Schema[T]{Version: declaration.Schema.Version, Defaults: declaration.Schema.Defaults, Validate: declaration.Schema.Validate}
	defaults, err := configsource.Prepare(ctx, configsource.Schema[T]{Version: schema.Version, Defaults: schema.Defaults}, nil)
	if err != nil {
		return plan[T]{}, err
	}
	schema.Defaults, err = defaults.ValueCopy()
	if err != nil {
		return plan[T]{}, err
	}
	result := plan[T]{schema: schema, layers: slices.Clone(provider.state.layers)}
	if len(declaration.Variables) > 0 {
		variables := make([]configsource.Variable, len(declaration.Variables))
		for index, value := range declaration.Variables {
			variables[index] = configsource.Variable{Path: value.Path, Value: value.Value, Present: value.Present, JSON: value.JSON}
		}
		layer, err := configsource.BindVariables[T](variables)
		if err != nil {
			return plan[T]{}, err
		}
		result.variables = &layer
	}
	return result, nil
}

func validateLayers(layers []layer) error {
	if len(layers) > 3 {
		return fail(ErrLimit, "layers")
	}
	seen := map[LayerKind]bool{}
	for _, value := range layers {
		if value.Kind < Base || value.Kind > Override || seen[value.Kind] || value.Encoding != JSON && value.Encoding != YAML && value.Encoding != TOML {
			return fail(ErrDeclaration, "layers")
		}
		seen[value.Kind] = true
	}
	return nil
}
func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface:
		return reflected.IsNil()
	}
	return false
}

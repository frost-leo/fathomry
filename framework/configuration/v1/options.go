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
	"os"
	"reflect"
	"slices"
	"strings"
	"unicode/utf8"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/resource/v1"
)

// Layer assigns application policy to one source slot, in source order.
// Only Base/Environment/Local are external slots. Present-empty is not optional.
type Layer struct {
	Kind     configsource.LayerKind `json:"kind"`
	Encoding configsource.Encoding  `json:"encoding"`
	Optional bool                   `json:"optional"`
}

// Environment binds one explicit process name to a declared struct-field path.
// JSON=false is literal string text; JSON=true uses strict JSON values. Values
// are captured once per Load/Watch admission; Watch never rereads os.Environ.
type Environment struct {
	Name string `json:"name"`
	Path string `json:"path"`
	JSON bool   `json:"json"`
}

// Declaration keeps project data/schema separate from already-resolved bootstrap.
// Source is optional only for defaults/environment-only Load. Watch needs a source.
// Defaults and slices are borrowed only during admission, then independently frozen.
// Validate and the selected Source are retained; they must obey their own bounded,
// concurrent-use contracts and must not reenter shutdown waiting for themselves.
type Declaration[T any] struct {
	Schema      configsource.Schema[T]
	Source      configsource.Source
	Layers      []Layer
	Environment []Environment
}

// Dependencies explicitly borrows public operations/evidence and optional instance
// adoption. Accepting settings does not imply all requested instances adopted them.
type Dependencies struct {
	Runtime   *adapters.Runtime
	Evidence  *adapters.Inbox[Evidence]
	Observer  *adapters.Observer
	Resources *resource.Scope
}

// WatchOptions bounds status notifications, not native polling. Zero selects 16,
// valid 1..64. Pending source input is separately coalesced to one latest value.
type WatchOptions struct {
	QueueCapacity int `json:"queue_capacity"`
}

type plan[T any] struct {
	schema    configsource.Schema[T]
	source    configsource.Source
	layers    []Layer
	variables *configsource.Layer
}

func freeze[T any](ctx context.Context, declaration Declaration[T], watch bool) (plan[T], error) {
	if len(declaration.Layers) > 3 || len(declaration.Environment) > configsource.MaxVariables {
		return plan[T]{}, fail(ErrLimit, "declaration")
	}
	missing := nilInterface(declaration.Source)
	if missing != (len(declaration.Layers) == 0) || watch && missing {
		return plan[T]{}, fail(ErrDeclaration, "source")
	}
	seen := map[configsource.LayerKind]bool{}
	for _, layer := range declaration.Layers {
		if layer.Kind < configsource.Base || layer.Kind > configsource.Local || seen[layer.Kind] || layer.Encoding != configsource.JSON && layer.Encoding != configsource.YAML {
			return plan[T]{}, fail(ErrDeclaration, "layers")
		}
		seen[layer.Kind] = true
	}
	defaults, err := configsource.Prepare(ctx, configsource.Schema[T]{Version: declaration.Schema.Version, Defaults: declaration.Schema.Defaults}, nil)
	if err != nil {
		return plan[T]{}, err
	}
	schema := declaration.Schema
	schema.Defaults, err = defaults.ValueCopy()
	if err != nil {
		return plan[T]{}, err
	}
	result := plan[T]{schema: schema, source: declaration.Source, layers: slices.Clone(declaration.Layers)}
	if missing {
		result.source = nil
	}
	variables := make([]configsource.Variable, len(declaration.Environment))
	for index, binding := range declaration.Environment {
		if binding.Name == "" || len(binding.Name) > 256 || !utf8.ValidString(binding.Name) || strings.ContainsAny(binding.Name, "=\x00") {
			return plan[T]{}, fail(ErrDeclaration, "environment")
		}
		variables[index] = configsource.Variable{Path: binding.Path, JSON: binding.JSON}
	}
	if len(variables) > 0 {
		if _, err := configsource.BindVariables[T](variables); err != nil {
			return plan[T]{}, err
		}
		type captured struct {
			value   string
			present bool
		}
		values := map[string]captured{}
		for index, binding := range declaration.Environment {
			value, exists := values[binding.Name]
			if !exists {
				value.value, value.present = os.LookupEnv(binding.Name)
				values[binding.Name] = value
			}
			variables[index].Value, variables[index].Present = value.value, value.present
		}
		layer, err := configsource.BindVariables[T](variables)
		if err != nil {
			return plan[T]{}, err
		}
		result.variables = &layer
	}
	return result, nil
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
func bind(dependencies Dependencies) (adapters.Endpoint[Evidence], error) {
	return adapters.Bind(dependencies.Runtime, adapters.Declaration[Evidence]{Evidence: dependencies.Evidence, Observer: dependencies.Observer, Copy: func(value Evidence) Evidence { return value }})
}
func request(operation string) adapters.Request {
	return adapters.Request{Operation: "configuration." + operation, WorkBytes: 16 << 20, EvidenceBytes: 64 << 10}
}

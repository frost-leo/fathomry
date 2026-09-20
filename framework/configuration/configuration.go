/*
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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"strings"

	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/internal/resource"
)

// Configuration owns immutable prepared data. Its zero value is invalid. Copies
// share private immutable storage; Value and Description return independent data.
// Concurrent readers are supported. No source, file, client or watcher is retained.
type Configuration[T any] struct {
	runtimeValue
	prepared    resource.Prepared[T]
	description *Description
}

// Value returns a fresh copy, including nested maps, slices and pointers. It
// deliberately exposes potentially sensitive settings; do not treat it as a log
// projection. Mutation never changes this Configuration or another Value result.
func (configuration Configuration[T]) Value() (T, error) {
	if configuration.description == nil {
		var zero T
		return zero, problem(InvalidInput)
	}
	value, err := configuration.prepared.Copy()
	if err != nil {
		var zero T
		return zero, problem(Invalid)
	}
	return value, nil
}

// Description returns independent metadata. Zero configurations return zero.
func (configuration Configuration[T]) Description() Description {
	if configuration.description == nil {
		return Description{}
	}
	result := *configuration.description
	result.Sources = slices.Clone(result.Sources)
	result.Variables = slices.Clone(result.Variables)
	result.Contributions = slices.Clone(result.Contributions)
	for index := range result.Contributions {
		result.Contributions[index].Fields = slices.Clone(result.Contributions[index].Fields)
	}
	return result
}

// Load acquires one explicitly selected Provider and prepares the
// full configuration, or returns an entirely zero result. Source order does not
// affect precedence. Bindings are validated and their selected environment values
// captured before Provider acquisition; each process variable is read once.
// There is no atomic multi-file/environment snapshot or automatic reload.
//
// ctx is caller-owned; cancellation is checked at phase boundaries but cannot
// forcibly interrupt a Provider, validator or parser. Providers receive that same
// context. No goroutine, service, logger or process signal handler is installed.
func Load[T any](ctx context.Context, schema Schema[T], request Request) (Configuration[T], error) {
	fail := func(err error) (Configuration[T], error) { return Configuration[T]{}, err }
	if ctx == nil || schema.SchemaVersion == 0 || nilProvider(request.Provider) {
		return fail(problem(InvalidInput))
	}
	if err := checkContext(ctx); err != nil {
		return fail(err)
	}
	variables, err := prepareVariables[T](request.Variables, request.LookupVariable)
	if err != nil {
		return fail(err)
	}
	if err := checkContext(ctx); err != nil {
		return fail(err)
	}
	input, err := request.Provider.ReadConfiguration(ctx)
	if err != nil {
		if _, public := failure.Inspect(err); public {
			return fail(err)
		}
		if cancelled := checkContext(ctx); cancelled != nil {
			return fail(cancelled)
		}
		return fail(problem(Unavailable))
	}
	if err := checkContext(ctx); err != nil {
		return fail(err)
	}
	if !validLabel(input.Provider) || len(input.Documents) > MaxSources {
		return fail(problem(InvalidInput))
	}
	if input.SchemaVersion != schema.SchemaVersion {
		return fail(problem(UnsupportedSchema))
	}
	seenNames := make(map[string]bool)
	seenLayers := make(map[Layer]bool)
	description := Description{Provider: strings.Clone(input.Provider), Variables: variables.info}
	var layers []resource.Layer
	for _, document := range input.Documents {
		if !validLabel(document.Name) || seenNames[document.Name] ||
			document.Layer < Base || document.Layer > Local ||
			seenLayers[document.Layer] || document.Absent && len(document.Data) != 0 {
			return fail(problem(InvalidInput))
		}
		if len(document.Data) > MaxDocumentBytes {
			return fail(problem(LimitExceeded))
		}
		seenNames[document.Name], seenLayers[document.Layer] = true, true
		description.Sources = append(description.Sources, SourceInfo{
			Name: strings.Clone(document.Name), Layer: document.Layer, Present: !document.Absent,
		})
		if !document.Absent {
			var kind resource.LayerKind
			switch document.Layer {
			case Base:
				kind = resource.Base
			case Environment:
				kind = resource.Environment
			case Local:
				kind = resource.Local
			}
			layers = append(layers, resource.Layer{Kind: kind, Content: slices.Clone(document.Data)})
		}
	}
	slices.SortFunc(description.Sources, func(left, right SourceInfo) int { return int(left.Layer) - int(right.Layer) })
	if variables.data != nil {
		layers = append(layers, resource.Layer{Kind: resource.Variables, Content: variables.data})
	}
	if err := checkContext(ctx); err != nil {
		return fail(err)
	}
	var validationError error
	validate := func(value T) error {
		if schema.Validate != nil {
			validationError = schema.Validate(value)
		}
		return validationError
	}
	prepared, err := resource.PrepareData(resource.Schema[T]{Format: schema.SchemaVersion, Defaults: schema.Defaults, Validate: validate}, layers)
	if err != nil {
		if validationError != nil {
			return fail(failure.New(ValidationFailed, validationError))
		}
		return fail(problem(Invalid))
	}
	if err := checkContext(ctx); err != nil {
		return fail(err)
	}
	metadata := prepared.Description()
	description.SchemaVersion, description.Revision = metadata.Format, metadata.Revision
	for _, supplied := range metadata.Provenance {
		var kind Layer
		switch supplied.Kind {
		case resource.Defaults:
			kind = Defaults
		case resource.Base:
			kind = Base
		case resource.Environment:
			kind = Environment
		case resource.Local:
			kind = Local
		case resource.Variables:
			kind = Variables
		default:
			return fail(problem(Invalid))
		}
		description.Contributions = append(description.Contributions, Contribution{
			Layer: kind, Fields: supplied.Fields,
		})
	}
	return Configuration[T]{prepared: prepared, description: &description}, nil
}

func nilProvider(provider Provider) bool {
	if provider == nil {
		return true
	}
	value := reflect.ValueOf(provider)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	}
	return false
}

func checkContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return failure.New(Cancelled, errors.Join(err, context.Cause(ctx)))
	}
	return nil
}

func problem(code failure.Code) error { return failure.New(code, nil) }

func validLabel(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '.' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}

type runtimeValue struct{}

func (runtimeValue) Format(state fmt.State, verb rune) {
	_, _ = io.WriteString(state, "framework.Configuration[restricted]")
}
func (runtimeValue) LogValue() slog.Value {
	return slog.StringValue("framework.Configuration[restricted]")
}
func (runtimeValue) MarshalJSON() ([]byte, error) {
	return nil, problem(InvalidInput)
}
func (*runtimeValue) UnmarshalJSON([]byte) error {
	return problem(InvalidInput)
}

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

package configsource

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"slices"

	"github.com/frost-leo/fathomry/settings/v1"
)

// LayerInfo describes declared struct fields, not values or dynamic map keys.
type LayerInfo struct {
	Kind   LayerKind
	Fields []string
}

// Description is detached, non-secret provenance. Revision is random identity,
// not a sortable source cursor, content hash or proof of instance readiness.
type Description struct {
	Version  uint32
	Revision string
	Layers   []LayerInfo
}

// Prepared holds accepted bytes, not caller/validator aliases. The zero handle
// is unusable. Copies share immutable storage; ValueCopy isolates mutable values.
type Prepared[T any] struct {
	state *prepared
	_     typeIdentity[T]
}
type typeIdentity[T any] [0]func() T
type prepared struct {
	raw         []byte
	description Description
}

// Prepare rejects every malformed layer before overlay, even when a later layer
// would hide it. It never publishes a partial or usable-looking zero value.
func Prepare[T any](ctx context.Context, schema Schema[T], layers []Layer) (Prepared[T], error) {
	if ctx == nil || schema.Version == 0 || len(layers) > 4 {
		return Prepared[T]{}, fail(ErrInput, "prepare")
	}
	if err := interrupted(ctx); err != nil {
		return Prepared[T]{}, err
	}
	kind := reflect.TypeFor[T]()
	if kind.Kind() != reflect.Struct {
		return Prepared[T]{}, fail(ErrInput, "schema")
	}
	shape, err := compile(kind)
	if err != nil {
		return Prepared[T]{}, err
	}
	budget := newBudget()
	defaults, err := fromGo(shape, reflect.ValueOf(&schema.Defaults).Elem(), &budget, 0)
	if err != nil {
		return Prepared[T]{}, err
	}
	effective := defaults.(map[string]any)
	ordered := slices.Clone(layers)
	slices.SortFunc(ordered, func(left, right Layer) int { return int(left.Kind) - int(right.Kind) })
	description := Description{Version: schema.Version}
	previous := LayerKind(0)
	for _, layer := range ordered {
		if layer.Kind < Base || layer.Kind > Variables || layer.Kind == previous {
			return Prepared[T]{}, fail(ErrInput, "layers")
		}
		previous = layer.Kind
		if err := interrupted(ctx); err != nil {
			return Prepared[T]{}, err
		}
		object, err := parse(layer.Content, layer.Encoding)
		if err != nil {
			return Prepared[T]{}, err
		}
		budget := newBudget()
		if _, err := toGo(shape, object, &budget, 0); err != nil {
			return Prepared[T]{}, err
		}
		description.Layers = append(description.Layers, LayerInfo{Kind: layer.Kind, Fields: fields(shape, object, "")})
		merge(effective, object)
	}
	raw, err := json.Marshal(effective)
	if err != nil {
		return Prepared[T]{}, fail(ErrDecode, "encode", err)
	}
	if len(raw) > MaxDocumentBytes {
		return Prepared[T]{}, fail(ErrLimit, "resolved")
	}
	budget = newBudget()
	value, err := toGo(shape, effective, &budget, 0)
	if err != nil {
		return Prepared[T]{}, err
	}
	if schema.Validate != nil {
		if err := schema.Validate(ctx, value.Interface().(T)); err != nil {
			return Prepared[T]{}, fail(ErrDecode, "validate", err)
		}
	}
	if err := interrupted(ctx); err != nil {
		return Prepared[T]{}, err
	}
	var revision [16]byte
	if _, err := rand.Read(revision[:]); err != nil {
		return Prepared[T]{}, fail(ErrRead, "revision", err)
	}
	description.Revision = hex.EncodeToString(revision[:])
	return Prepared[T]{state: &prepared{raw: raw, description: description}}, nil
}

// ValueCopy deliberately exposes a detached, potentially sensitive accepted value.
func (value Prepared[T]) ValueCopy() (T, error) {
	var result T
	if value.state == nil {
		return result, fail(ErrInput, "value_copy")
	}
	if err := json.Unmarshal(value.state.raw, &result); err != nil {
		return result, fail(ErrDecode, "value_copy", err)
	}
	return result, nil
}

// Snapshot composes with the public accepted-data store without publishing it.
func (value Prepared[T]) Snapshot() (settings.Snapshot[T], error) {
	result, err := value.ValueCopy()
	if err != nil {
		return settings.Snapshot[T]{}, err
	}
	return settings.New(result, func(T) T {
		// Only this sealed value can reach the private copy function.
		copied, _ := value.ValueCopy()
		return copied
	})
}

// Description excludes raw input, file paths, environment names and map contents.
func (value Prepared[T]) Description() Description {
	if value.state == nil {
		return Description{}
	}
	result := value.state.description
	result.Layers = slices.Clone(result.Layers)
	for index := range result.Layers {
		result.Layers[index].Fields = slices.Clone(result.Layers[index].Fields)
	}
	return result
}
func interrupted(ctx context.Context) error {
	if ctx.Err() != nil {
		return fail(ErrRead, "prepare", ctx.Err(), context.Cause(ctx))
	}
	return nil
}
func merge(target, patch map[string]any) {
	for key, value := range patch {
		object, objectOK := value.(map[string]any)
		inherited, inheritedOK := target[key].(map[string]any)
		if objectOK && inheritedOK {
			merge(inherited, object)
		} else {
			target[key] = value
		}
	}
}
func fields(shape *shape, object map[string]any, prefix string) []string {
	var result []string
	for _, field := range shape.fields {
		value, present := object[field.name]
		if !present {
			continue
		}
		path := prefix + "/" + field.name
		result = append(result, path)
		child := field.shape
		for child.kind.Kind() == reflect.Pointer {
			child = child.element
		}
		if child.kind.Kind() == reflect.Struct {
			if nested, ok := value.(map[string]any); ok {
				result = append(result, fields(child, nested, path)...)
			}
		}
	}
	return result
}

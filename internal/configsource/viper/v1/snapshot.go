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

package viper

import (
	"context"
	"reflect"
	"sort"
	"strings"

	sdk "github.com/spf13/viper"
)

// Snapshot owns one bounded set of native resolved values. It captures live
// environment queries once, not an atomic transaction across process state/files.
// No environment binding or mutable native handle escapes. RawCopy on Document
// remains the original file and is never relabeled as this effective view.
type Snapshot struct {
	private
	native *sdk.Viper
}

// Keys returns the bounded native registered-key inventory. AutomaticEnv alone
// does not enumerate environment-only keys; Capture/Decode accept explicit keys.
func (document *Document) Keys() ([]string, error) {
	if document == nil || document.native == nil {
		return nil, fail(ErrInput, "keys")
	}
	keys := document.native.AllKeys()
	if len(keys) > MaxNodes {
		return nil, fail(ErrLimit, "keys")
	}
	for _, key := range keys {
		if !validQuery(key) {
			return nil, fail(ErrInput, "keys")
		}
	}
	sort.Strings(keys)
	return keys, nil
}

// Capture combines registered and explicitly selected native keys, querying each
// once and freezing copied values in an independent native instance. Callers must
// keep sources/environment stable when they require a coherent preparation epoch.
// No arbitrary environment scan or process mutation occurs.
func (document *Document) Capture(ctx context.Context, extraKeys ...string) (*Snapshot, error) {
	if ctx == nil {
		return nil, fail(ErrInput, "capture")
	}
	if len(extraKeys) > MaxNodes {
		return nil, fail(ErrLimit, "capture")
	}
	keys, err := document.Keys()
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(keys)+len(extraKeys))
	for _, key := range append(keys, extraKeys...) {
		if !validQuery(key) {
			return nil, fail(ErrInput, "capture")
		}
		seen[strings.ToLower(key)] = true
	}
	if len(seen) > MaxNodes {
		return nil, fail(ErrLimit, "capture")
	}
	keys = keys[:0]
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	native := sdk.New()
	budget := valueBudget{bytes: MaxTotalBytes, nodes: MaxNodes}
	for _, key := range keys {
		if ctx.Err() != nil {
			return nil, fail(ErrRead, "capture", ctx.Err(), context.Cause(ctx))
		}
		value := document.native.Get(key)
		budget.bytes -= len(key)
		copied, err := budget.copy(value, 0)
		if err != nil {
			return nil, err
		}
		if value != nil {
			native.Set(strings.Clone(key), copied)
		}
	}
	if ctx.Err() != nil {
		return nil, fail(ErrRead, "capture", ctx.Err(), context.Cause(ctx))
	}
	return &Snapshot{native: native}, nil
}

// ValueCopy queries only captured values; later environment changes cannot affect it.
func (snapshot *Snapshot) ValueCopy(key string) (any, error) {
	if snapshot == nil || snapshot.native == nil || !validQuery(key) {
		return nil, fail(ErrInput, "snapshot-query")
	}
	return copyValue(snapshot.native.Get(key)), nil
}

// ValuesCopy exposes owned native AllSettings values, not exact original syntax.
func (snapshot *Snapshot) ValuesCopy() (map[string]any, error) {
	if snapshot == nil || snapshot.native == nil {
		return nil, fail(ErrInput, "snapshot-values")
	}
	return copyValue(snapshot.native.AllSettings()).(map[string]any), nil
}

// Decode freezes a schema-complete native key selection before Viper Unmarshal.
// Exported mapstructure-tagged struct fields are discovered within type/path
// bounds, including environment-only fields. Map/interface keys require extraKeys.
// Native weak conversions/default hooks remain native semantics, not strict JSON
// precision or business validation. Validate the returned T before publication.
// Recursive struct schemas are refused; arbitrary settings storage is separate.
func Decode[T any](ctx context.Context, document *Document, extraKeys ...string) (T, error) {
	var zero T
	keys, err := typeKeys(reflect.TypeFor[T]())
	if err != nil {
		return zero, err
	}
	if len(extraKeys) > MaxNodes-len(keys) {
		return zero, fail(ErrLimit, "decode")
	}
	snapshot, err := document.Capture(ctx, append(keys, extraKeys...)...)
	if err != nil {
		return zero, err
	}
	var result T
	if err := snapshot.native.Unmarshal(&result); err != nil {
		return zero, fail(ErrDecode, "decode-settings", err)
	}
	if ctx.Err() != nil {
		return zero, fail(ErrRead, "decode-settings", ctx.Err(), context.Cause(ctx))
	}
	return result, nil
}

func typeKeys(root reflect.Type) ([]string, error) {
	var result []string
	active := map[reflect.Type]bool{}
	nodes := 0
	var visit func(reflect.Type, string, int) error
	visit = func(kind reflect.Type, prefix string, depth int) error {
		nodes++
		if nodes > MaxNodes || depth > MaxDepth {
			return fail(ErrLimit, "schema-keys")
		}
		if kind.Kind() == reflect.Pointer {
			return visit(kind.Elem(), prefix, depth+1)
		}
		if kind.Kind() != reflect.Struct || kind.PkgPath() == "time" {
			if prefix != "" {
				if !validQuery(prefix) {
					return fail(ErrInput, "schema-keys")
				}
				result = append(result, prefix)
			}
			return nil
		}
		if active[kind] {
			return fail(ErrInput, "recursive-schema")
		}
		active[kind] = true
		defer delete(active, kind)
		for index := 0; index < kind.NumField(); index++ {
			field := kind.Field(index)
			if !field.IsExported() {
				continue
			}
			tag := strings.Split(field.Tag.Get("mapstructure"), ",")
			name := tag[0]
			if name == "-" {
				continue
			}
			if name == "" {
				name = strings.ToLower(field.Name)
			}
			path := name
			if prefix != "" {
				path = prefix + "." + name
			}
			for _, option := range tag[1:] {
				if option == "squash" {
					path = prefix
				}
			}
			if err := visit(field.Type, path, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(root, "", 0); err != nil {
		return nil, err
	}
	return result, nil
}

type valueBudget struct{ bytes, nodes int }

func (budget *valueBudget) copy(value any, depth int) (any, error) {
	budget.nodes--
	budget.bytes -= 8
	if depth > MaxDepth || budget.nodes < 0 || budget.bytes < 0 {
		return nil, fail(ErrLimit, "capture")
	}
	switch value := value.(type) {
	case string:
		if len(value) > MaxDocumentBytes {
			return nil, fail(ErrLimit, "capture")
		}
		budget.bytes -= len(value)
		if budget.bytes < 0 {
			return nil, fail(ErrLimit, "capture")
		}
		return strings.Clone(value), nil
	case map[string]any:
		if len(value) > budget.nodes {
			return nil, fail(ErrLimit, "capture")
		}
		result := make(map[string]any, len(value))
		for key, child := range value {
			budget.bytes -= len(key)
			copied, err := budget.copy(child, depth+1)
			if err != nil {
				return nil, err
			}
			result[strings.Clone(key)] = copied
		}
		return result, nil
	case []any:
		if len(value) > budget.nodes {
			return nil, fail(ErrLimit, "capture")
		}
		result := make([]any, len(value))
		for index, child := range value {
			copied, err := budget.copy(child, depth+1)
			if err != nil {
				return nil, err
			}
			result[index] = copied
		}
		return result, nil
	default:
		return value, nil
	}
}

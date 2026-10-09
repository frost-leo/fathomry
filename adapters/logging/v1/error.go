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

package logging

import (
	"reflect"

	"github.com/frost-leo/fathomry/adapters/internal/errorbridge"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

func fail(code failure.Code, operation string, causes ...error) error {
	value, err := failure.New(definition(code), failure.Location{Operation: operation}, causes...)
	if err != nil {
		return err
	}
	return value
}

// SafeError projects trusted public diagnostics only. Nil, empty/unknown error
// graphs and exhausted graph bounds reject without Error/Stringer/LogValue calls.
// Public Failure and neutral Unwrap accessors retain their existing bounded,
// cooperative contracts. Native cause subtrees under public occurrences remain
// opaque; they are never formatted or recursively projected.
func SafeError(original error) (Value, error) {
	if original == nil {
		return Value{}, fail(ErrInput, "safe-error")
	}
	remaining := 128
	var diagnostics []Value
	seen := make(map[*failure.Error]bool)
	var visit func(error) error
	visit = func(err error) error {
		if err == nil {
			return fail(ErrUnsupported, "safe-error")
		}
		if remaining == 0 {
			return fail(ErrLimit, "safe-error")
		}
		remaining--
		reflected := reflect.ValueOf(err)
		switch reflected.Kind() {
		case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
			if reflected.IsNil() {
				return fail(ErrUnsupported, "safe-error")
			}
		}
		if core, ok := failure.Inspect(err); ok {
			if seen[core] {
				return nil
			}
			seen[core] = true
			value := core.Diagnostic()
			message := value.Definition.Message
			fields := []Field{
				{Key: "code", Value: String(value.Definition.Code.String())},
				{Key: "identifier", Value: String(string(value.Definition.Identifier))},
				{Key: "module", Value: String(value.Definition.Module)},
				{Key: "component", Value: String(value.Definition.Component)},
				{Key: "operation", Value: String(value.Location.Operation)},
				{Key: "instance", Value: String(value.Location.Instance)},
				{Key: "cause_count", Value: Int64(int64(value.CauseCount))},
			}
			if presented, ok := err.(*i18n.Presented); ok {
				info := presented.Info()
				message = info.Message.Text
				issueCode := ""
				if issue, ok := failure.Inspect(presented.Issue()); ok {
					issueCode = issue.Diagnostic().Definition.Code.String()
				}
				fields = append(fields, Field{Key: "locale", Value: String(info.Message.Locale)},
					Field{Key: "requested_locale", Value: String(info.Selection.Requested)},
					Field{Key: "fallback", Value: String(string(info.Selection.Fallback))},
					Field{Key: "form_fallback", Value: Bool(info.Message.FormFallback)},
					Field{Key: "presentation_issue", Value: String(issueCode)})
			}
			fields = append(fields, Field{Key: "message", Value: String(message)})
			diagnostics = append(diagnostics, Group(fields...))
			return nil
		}
		if projected, ok := errorbridge.PublicProjection(err); ok {
			return visit(projected)
		}
		switch value := err.(type) {
		case interface{ Unwrap() []error }:
			children := value.Unwrap()
			if len(children) == 0 {
				return fail(ErrUnsupported, "safe-error")
			}
			if len(children) > remaining {
				return fail(ErrLimit, "safe-error")
			}
			for _, child := range children {
				if err := visit(child); err != nil {
					return err
				}
			}
			return nil
		case interface{ Unwrap() error }:
			return visit(value.Unwrap())
		default:
			return fail(ErrUnsupported, "safe-error")
		}
	}
	if err := visit(original); err != nil {
		return Value{}, err
	}
	if len(diagnostics) == 0 {
		return Value{}, fail(ErrUnsupported, "safe-error")
	}
	value := diagnostics[0]
	if len(diagnostics) > 1 {
		value = Group(Field{Key: "errors", Value: Array(diagnostics...)})
	}
	return Freeze(value, Limits{MaxFields: 1024, MaxNodes: 4096, MaxDepth: 8, MaxBytes: 64 << 10})
}

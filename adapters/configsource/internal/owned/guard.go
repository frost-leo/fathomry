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

package owned

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"reflect"

	source "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/failure/v1"
)

// Guard prevents supported generic diagnostics/JSON from exposing runtime data.
type Guard struct{}

func (Guard) Format(state fmt.State, _ rune) {
	_, _ = io.WriteString(state, "configuration[restricted]")
}
func (Guard) LogValue() slog.Value         { return slog.StringValue("configuration[restricted]") }
func (Guard) MarshalJSON() ([]byte, error) { return nil, Fail(source.ErrValue) }
func (*Guard) UnmarshalJSON([]byte) error  { return Fail(source.ErrValue) }

func Fail(condition failure.Condition, causes ...error) error {
	value, err := failure.New(condition, causes...)
	if err != nil {
		panic(err)
	}
	return value
}
func ContextError(condition failure.Condition, ctx context.Context) error {
	return Fail(condition, ctx.Err(), context.Cause(ctx))
}

// NativeContext preserves cancellation, deadlines and ordinary values, but hides
// borrowed cancellation causes from native error classification. The public
// boundary retains the original cause only after interpreting native evidence.
func NativeContext(ctx context.Context) context.Context {
	return nativeContext{Context: ctx, values: context.WithoutCancel(ctx)}
}

type nativeContext struct {
	context.Context
	values context.Context
}

func (ctx nativeContext) Value(key any) any { return ctx.values.Value(key) }

func Nil(value any) bool {
	if value == nil {
		return true
	}
	kind := reflect.ValueOf(value)
	switch kind.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan:
		return kind.IsNil()
	}
	return false
}
func Label(value string) bool {
	if len(value) == 0 || len(value) > 64 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '.' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}

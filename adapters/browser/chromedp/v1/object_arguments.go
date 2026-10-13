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

package chromedp

import (
	"context"

	"github.com/chromedp/cdproto/runtime"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// Argument distinguishes JSON values/null, undefined and managed references.
// The zero value is invalid. JSON values are borrowed and immutable until return.
type Argument struct {
	private
	native native.Argument
}

func ValueArgument(value any) Argument { return Argument{native: native.ValueArgument(value)} }
func ObjectArgument(object *Object) Argument {
	if object == nil {
		return Argument{native: native.ObjectArgument(nil)}
	}
	return Argument{native: native.ObjectArgument(object.native)}
}
func UndefinedArgument() Argument { return Argument{native: native.UndefinedArgument()} }
func UnserializableArgument(value runtime.UnserializableValue) Argument {
	return Argument{native: native.UnserializableArgument(value)}
}

// CallArguments invokes the native receiver with values and opaque references.
// References must belong to the same page, document and captured source generation.
// Other groups on that page are allowed. Concurrent release can make a native
// call fail; this is not a transaction with another controller or a concurrent Close.
func (object *Object) CallArguments(ctx context.Context, function string, arguments ...Argument) (*Object, *adapters.Receipt[Result], error) {
	if object == nil || object.native == nil || object.group == nil {
		return nil, nil, fail(ErrInput, "object")
	}
	return object.group.run(ctx, "object_call_arguments", func(ctx context.Context) (*native.Object, error) {
		return native.CallObjectArguments(ctx, object.native, function, len(arguments), func(index int) native.Argument {
			return arguments[index].native
		})
	})
}

// CallWithOptions retains native promise, preview, user-gesture, side-effect and
// serialization choices. Identity comes from this receiver and its ObjectGroup.
// Leave native Arguments absent and pass values or managed references below.
// Parameters and arguments are borrowed, immutable until the operation returns.
func (object *Object) CallWithOptions(ctx context.Context, params runtime.CallFunctionOnParams, arguments ...Argument) (*Object, *adapters.Receipt[Result], error) {
	if object == nil || object.native == nil || object.group == nil {
		return nil, nil, fail(ErrInput, "object")
	}
	return object.group.run(ctx, "object_call", func(ctx context.Context) (*native.Object, error) {
		return native.CallObjectParameters(ctx, object.native, params, len(arguments), func(index int) native.Argument {
			return arguments[index].native
		})
	})
}

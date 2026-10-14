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

	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/runtime"
	sdk "github.com/chromedp/chromedp"
)

// Argument is an explicit JSON value, undefined, native special value or managed
// reference. Its zero value is invalid. JSON values are borrowed until return.
type Argument struct {
	private
	kind    uint8
	value   any
	object  *Object
	special runtime.UnserializableValue
}

func ValueArgument(value any) Argument       { return Argument{kind: 1, value: value} }
func ObjectArgument(object *Object) Argument { return Argument{kind: 2, object: object} }
func UndefinedArgument() Argument            { return Argument{kind: 3} }
func UnserializableArgument(value runtime.UnserializableValue) Argument {
	return Argument{kind: 4, special: value}
}

// CallObjectArguments converts adapter-owned argument containers only after
// resource and command admission. The accessor receives no native owner.
func CallObjectArguments(ctx context.Context, object *Object, function string, count int, argument func(int) Argument) (result *Object, err error) {
	return CallObjectParameters(ctx, object, runtime.CallFunctionOnParams{FunctionDeclaration: function, Silent: new(true)}, count, argument)
}

// CallObjectParameters preserves native call options while deriving receiver and
// result-group identity from owned handles. Native Arguments must be absent;
// the admitted accessor provides explicit values and managed references instead.
func CallObjectParameters(ctx context.Context, object *Object, params runtime.CallFunctionOnParams, count int, argument func(int) Argument) (result *Object, err error) {
	if object == nil || object.state == nil || argument == nil || count < 0 {
		return nil, failure(ErrInput, "object-arguments")
	}
	entry, group := object.state, object.state.group
	defer func() { group.session.remember(err) }()
	if err := object.valid(); err != nil {
		return nil, err
	}
	if entry.id == "" {
		return nil, failure(ErrInput, "object-not-reference")
	}
	if params.ObjectID != "" && params.ObjectID != entry.id || params.ObjectGroup != "" || params.ExecutionContextID != 0 || params.UniqueContextID != "" || params.Arguments != nil {
		return nil, failure(ErrInput, "object-call-identity")
	}
	limit := group.session.owner.settings.MaxCommandBytes
	if int64(len(params.FunctionDeclaration)) > limit || int64(count) > limit/2 {
		return nil, failure(ErrLimit, "object-arguments")
	}
	work, leave, err := group.session.enter(ctx, false)
	if err != nil {
		return nil, err
	}
	defer leave()
	return (&ObjectGroup{state: group}).execute(work, func(name string) sdk.Action[*runtime.RemoteObject] {
		return func(ctx context.Context, target *sdk.Target) (*runtime.RemoteObject, error) {
			buffer := boundedBuffer{limit: limit}
			if err := marshalBounded(&buffer, params); err != nil {
				return nil, err
			}
			var copied runtime.CallFunctionOnParams
			if err := jsonv2.Unmarshal(buffer.data, &copied, sdk.DefaultUnmarshalOptions); err != nil {
				return nil, err
			}
			arguments, err := objectArguments(group.session, count, argument)
			if err != nil {
				return nil, err
			}
			if err := object.valid(); err != nil {
				return nil, err
			}
			copied.ObjectID, copied.ObjectGroup, copied.Arguments = entry.id, name, arguments
			reply, err := cdp.Call(ctx, target, runtime.CallFunctionOn, copied)
			if err != nil {
				return nil, err
			}
			if reply.ExceptionDetails != nil {
				return nil, &sdk.ExceptionError{ExceptionDetails: reply.ExceptionDetails}
			}
			return reply.Result, nil
		}
	}, true, entry)
}

func objectArguments(session *Session, count int, get func(int) Argument) ([]*runtime.CallArgument, error) {
	arguments := make([]Argument, count)
	for index := range arguments {
		argument := get(index)
		switch argument.kind {
		case 1, 3, 4:
		case 2:
			if err := argument.object.valid(); err != nil {
				return nil, err
			}
			entry := argument.object.state
			if entry.group.session.sessionState != session.sessionState || entry.id == "" {
				return nil, failure(ErrInput, "object-argument-origin")
			}
		default:
			return nil, failure(ErrInput, "object-argument")
		}
		arguments[index] = argument
	}
	values := make([]*runtime.CallArgument, count)
	var used int64
	limit := session.owner.settings.MaxCommandBytes
	for index, argument := range arguments {
		value := &runtime.CallArgument{}
		switch argument.kind {
		case 1:
			buffer := boundedBuffer{limit: limit - used}
			if err := marshalBounded(&buffer, argument.value); err != nil {
				return nil, err
			}
			used += int64(len(buffer.data))
			value.Value = buffer.data
		case 2:
			if err := argument.object.valid(); err != nil {
				return nil, err
			}
			value.ObjectID = argument.object.state.id
		case 4:
			value.UnserializableValue = argument.special
		}
		values[index] = value
	}
	for _, argument := range arguments {
		if argument.kind == 2 {
			if err := argument.object.valid(); err != nil {
				return nil, err
			}
		}
	}
	return values, nil
}

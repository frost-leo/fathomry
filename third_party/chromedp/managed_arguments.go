/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package chromedp

import (
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/debugger"
	"github.com/chromedp/cdproto/runtime"
)

type callArgumentValue struct {
	Value               jsonv2.Value                `json:"value,omitzero"`
	UnserializableValue runtime.UnserializableValue `json:"unserializableValue,omitempty,omitzero"`
	ObjectID            runtime.RemoteObjectID      `json:"objectId,omitempty,omitzero"`
}

type callFunctionValues struct {
	*runtime.CallFunctionOnParams
	Arguments []*callArgumentValue `json:"arguments,omitempty,omitzero"`
}

type variableArgumentValue struct {
	*debugger.SetVariableValueParams
	NewValue *callArgumentValue `json:"newValue"`
}

type returnArgumentValue struct {
	*debugger.SetReturnValueParams
	NewValue *callArgumentValue `json:"newValue"`
}

// Empty JSON values are arguments, not an omitted JavaScript undefined value.
func preserveCallArguments(method string, params any, limit int64) (any, error) {
	if method == debugger.CommandSetVariableValue {
		var native *debugger.SetVariableValueParams
		switch value := params.(type) {
		case debugger.SetVariableValueParams:
			native = &value
		case *debugger.SetVariableValueParams:
			native = value
		}
		if native != nil {
			value, err := preserveArgumentValue(native.NewValue, limit)
			return variableArgumentValue{SetVariableValueParams: native, NewValue: value}, err
		}
	}
	if method == debugger.CommandSetReturnValue {
		var native *debugger.SetReturnValueParams
		switch value := params.(type) {
		case debugger.SetReturnValueParams:
			native = &value
		case *debugger.SetReturnValueParams:
			native = value
		}
		if native != nil {
			value, err := preserveArgumentValue(native.NewValue, limit)
			return returnArgumentValue{SetReturnValueParams: native, NewValue: value}, err
		}
	}
	if method != runtime.CommandCallFunctionOn {
		return params, nil
	}
	var native *runtime.CallFunctionOnParams
	switch value := params.(type) {
	case runtime.CallFunctionOnParams:
		native = &value
	case *runtime.CallFunctionOnParams:
		native = value
	default:
		return params, nil
	}
	if native == nil || len(native.Arguments) == 0 {
		return params, nil
	}
	size := int64(len(native.Arguments)) * 2
	for _, argument := range native.Arguments {
		if argument != nil {
			size += int64(len(argument.Value) + len(argument.UnserializableValue) + len(argument.ObjectID))
		}
		if limit > 0 && size > limit {
			return nil, ErrCommandLimit
		}
	}
	values := make([]*callArgumentValue, len(native.Arguments))
	for index, argument := range native.Arguments {
		if argument != nil {
			values[index] = &callArgumentValue{Value: argument.Value, UnserializableValue: argument.UnserializableValue, ObjectID: argument.ObjectID}
		}
	}
	return callFunctionValues{CallFunctionOnParams: native, Arguments: values}, nil
}

func preserveArgumentValue(argument *runtime.CallArgument, limit int64) (*callArgumentValue, error) {
	if argument == nil {
		return nil, nil
	}
	size := int64(len(argument.Value)) + int64(len(argument.UnserializableValue)) + int64(len(argument.ObjectID))
	if limit > 0 && size > limit {
		return nil, ErrCommandLimit
	}
	return &callArgumentValue{Value: argument.Value, UnserializableValue: argument.UnserializableValue, ObjectID: argument.ObjectID}, nil
}

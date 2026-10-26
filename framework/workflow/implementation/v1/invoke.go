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

package implementation

import (
	"bytes"
	"context"
	jsonv1 "encoding/json"
	json "encoding/json/v2"
	"fmt"
	"log/slog"
	"reflect"

	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
)

// InvocationProfile names the narrow exact-integer JSON value representation,
// not a Temporal DataConverter, durable outcome or deployment qualification.
const InvocationProfile = "fathomry.activity-json-values/v1"

// Invocation describes one normally returned call. Invoked reports dispatch,
// not external effect acceptance/commit/visibility. Output is independently owned
// by the caller and is nil on every returned error. Panics/Goexit return no result.
// Default logging hides payloads, including nested JSON fallback. JSON encoding
// and decoding refuse; callers must explicitly access data for their own envelope.
type Invocation struct {
	Invoked bool
	Output  []byte
}

func (Invocation) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("implementation.Invocation"))
}
func (Invocation) LogValue() slog.Value { return slog.StringValue("implementation.Invocation") }
func (Invocation) MarshalJSON() ([]byte, error) {
	return nil, problem(ErrHandle, "invocation_serialization", "")
}
func (*Invocation) UnmarshalJSON([]byte) error {
	return problem(ErrHandle, "invocation_serialization", "")
}

type invocationSignature struct {
	inputType reflect.Type
	input     declaration.DataSchema
	output    declaration.DataSchema
}

func portSchema(ports map[string]declaration.Port, contracts map[string]declaration.DataSchema) declaration.DataSchema {
	properties := make(map[string]declaration.DataSchema, len(ports))
	for name, port := range ports {
		properties[name] = contracts[port.Contract]
	}
	closed := false
	return declaration.DataSchema{Type: "object", Properties: properties, Required: sortedKeys(properties), AdditionalProperties: &closed}
}

// Invoke validates one bounded JSON object, synchronously calls the checked
// binding once and validates/encodes its output. It borrows input until return;
// do not mutate it concurrently. Decoded argument storage is fresh, but captures
// and returned reachable data retain their caller/handler-owned concurrency rules.
//
// Cancellation is observed cooperatively before dispatch and after output;
// it is not an atomic fence against racing dispatch or return. A handler ignoring
// ctx can block the caller. Panics and Goexit retain ordinary Go semantics.
func (registry *Registry) Invoke(ctx context.Context, key string, input []byte) (result Invocation, err error) {
	if err := invocationCanceled(ctx, "admission"); err != nil {
		return result, err
	}
	if registry == nil || !registry.valid {
		return result, invocationFailure("admission", "registry", "", ErrHandle)
	}
	function, exists := registry.functions[key]
	signature, prepared := registry.signatures[key]
	if !exists || !prepared || len(key) > 64 {
		return result, invocationFailure("admission", "binding_key", "", ErrInvalid)
	}
	if len(input) > MaxInvocationBytes {
		return result, invocationFailure("input", "bytes", "/input", ErrLimit)
	}
	if err := validateInput(ctx, input, signature.input); err != nil {
		return result, preferInvocationCancellation(ctx, "input", err)
	}
	argument := reflect.New(signature.inputType)
	if err := json.Unmarshal(input, argument.Interface(), json.RejectUnknownMembers(true), jsonv1.FormatDurationAsNano(true)); err != nil {
		return result, preferInvocationCancellation(ctx, "input", invocationFailure("input", "decode", "/input", ErrInvalid, err))
	}
	if err := invocationCanceled(ctx, "input"); err != nil {
		return result, err
	}
	values := reflect.ValueOf(function).Call([]reflect.Value{reflect.ValueOf(ctx), argument.Elem()})
	result.Invoked = true
	if !values[1].IsNil() {
		handlerError := values[1].Interface().(error)
		return result, preferInvocationCancellation(ctx, "handler", invocationFailure("handler", "handler", "", nil, handlerError))
	}
	if err := invocationCanceled(ctx, "handler"); err != nil {
		return result, err
	}
	budget := valueBudget{ctx: ctx, phase: "output"}
	if err := validateOutput(&budget, values[0], signature.output, 1, "/output"); err != nil {
		return result, preferInvocationCancellation(ctx, "output", err)
	}
	buffer := invocationBuffer{ctx: ctx}
	if err := json.MarshalWrite(&buffer, values[0].Interface(), jsonv1.FormatDurationAsNano(true), json.FormatNilSliceAsNull(true)); err != nil {
		category := error(ErrInvalid)
		if buffer.err != nil {
			category = buffer.err
		}
		return result, preferInvocationCancellation(ctx, "output", invocationFailure("output", "encode", "/output", category, err))
	}
	if err := invocationCanceled(ctx, "output"); err != nil {
		return result, err
	}
	result.Output = buffer.data.Bytes()
	return result, nil
}

type invocationBuffer struct {
	data bytes.Buffer
	ctx  context.Context
	err  error
}

func (buffer *invocationBuffer) Write(data []byte) (int, error) {
	if err := invocationCanceled(buffer.ctx, "output"); err != nil {
		buffer.err = err
		return 0, err
	}
	if len(data) > MaxInvocationBytes-buffer.data.Len() {
		buffer.err = invocationFailure("output", "bytes", "/output", ErrLimit)
		return 0, buffer.err
	}
	return buffer.data.Write(data)
}

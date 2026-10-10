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

package temporal

import (
	"context"
	"reflect"
	"sync"

	commonpb "go.temporal.io/api/common/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

type encodedValueMapper interface {
	FathomryMapEncodedValueV1(func(converter.EncodedValue) converter.EncodedValue) converter.EncodedValue
}

func withEncodedValueDecoder(ctx context.Context, origin *nativeCall) context.Context {
	return sdk.FathomryWithEncodedValueDecoderV1(ctx, func(work context.Context, value converter.EncodedValue, output any) error {
		if work == nil || origin == nil || origin.access == nil {
			return failure(ErrAuthority, "encoded-result-origin")
		}
		current, _ := work.Value(nativeCallKey{}).(*nativeCall)
		if current == nil || current.closed.Load() || current.owner != origin.owner || !origin.access.SameScope(current.access) || current.callback != origin.callback {
			return failure(ErrAuthority, "encoded-result-origin")
		}
		if origin.callback && (origin.borrower == nil || current.borrower == nil || origin.borrower.binding != current.borrower.binding) {
			return failure(ErrAuthority, "encoded-result-task")
		}
		mapped, release, err := transferEncodedValue(work, origin.owner, origin, value)
		defer release()
		if err != nil {
			return err
		}
		return mapped.Get(output)
	})
}

type interceptorPayloadValue struct{ *interceptorValue }

type callbackPayloadValue struct{ *callbackValue }

func newCallbackQueryValue(work context.Context, client *callbackClient, ctx context.Context, workflowID, runID string, value converter.EncodedValue) (converter.EncodedValue, error) {
	if nilRuntime(value) {
		return nil, failure(ErrExecution, "query-value")
	}
	origin := work.Value(nativeCallKey{}).(*nativeCall)
	presence, release, err := transferEncodedValue(work, client.binding.worker.client.owner, origin, value)
	defer release()
	if err != nil {
		return nil, err
	}
	view := &callbackValue{native: value, borrower: client, ctx: context.WithoutCancel(ctx), origin: origin,
		present: presence.HasValue(), workflowID: workflowID, runID: runID}
	if _, supported := value.(converter.ValuesPayloads); supported {
		return &callbackPayloadValue{callbackValue: view}, nil
	}
	return view, nil
}

func (value *callbackPayloadValue) Payloads() *commonpb.Payloads {
	payloads, err := callbackNative(value.ctx, value.borrower, "", Execution{Operation: "callback.workflow.query-payloads", WorkflowID: value.workflowID, RunID: value.runID, IdentityOmitted: value.identityOmitted}, func(work context.Context, evidence *Execution) (*commonpb.Payloads, error) {
		owner := value.borrower.binding.worker.client.owner
		encoded, release, err := transferEncodedValue(work, owner, value.origin, value.native)
		defer release()
		if err != nil {
			return nil, err
		}
		payloads := encoded.(converter.ValuesPayloads).Payloads()
		evidence.ResultObtained = true
		if payloads == nil {
			return nil, nil
		}
		if _, err := messageSize(work, payloads, owner.settings.MaxResponseBytes); err != nil {
			return nil, err
		}
		return proto.Clone(payloads).(*commonpb.Payloads), nil
	})
	if err != nil {
		panic(err)
	}
	return payloads
}

func (value *interceptorPayloadValue) Payloads() *commonpb.Payloads {
	var payloads *commonpb.Payloads
	if err := value.guard(func() error {
		payloads = value.native.(converter.ValuesPayloads).Payloads()
		return nil
	}); err != nil {
		panic(err)
	}
	return payloads
}

func encodedInterceptorView(value converter.EncodedValue) *interceptorValue {
	switch value := value.(type) {
	case *interceptorValue:
		return value
	case *interceptorPayloadValue:
		if value != nil {
			return value.interceptorValue
		}
	}
	return nil
}

func encodedInterceptorResult(value *interceptorValue) converter.EncodedValue {
	if _, supported := value.native.(converter.ValuesPayloads); supported {
		return &interceptorPayloadValue{interceptorValue: value}
	}
	return value
}

// transferEncodedValue maps only explicit custom children and exact-origin
// borrowed views. release seals every copied view before joining entered work;
// callers defer it around consumption within an existing admitted native call.
func transferEncodedValue(work context.Context, owner *connection, origin *nativeCall, value converter.EncodedValue) (converter.EncodedValue, func(), error) {
	noop := func() {}
	_, cooperative := value.(encodedValueMapper)
	if encodedInterceptorView(value) == nil && !cooperative {
		return value, noop, nil
	}
	if work == nil || owner == nil || origin == nil || origin.owner != owner {
		return nil, noop, failure(ErrAuthority, "encoded-value-origin")
	}
	current, _ := work.Value(nativeCallKey{}).(*nativeCall)
	if current == nil || current.owner != owner || current.closed.Load() {
		return nil, noop, failure(ErrAuthority, "encoded-value-consumption")
	}
	var frames []*continuationFrame
	release := sync.OnceFunc(func() {
		var active []<-chan struct{}
		for _, frame := range frames {
			if entered := frame.seal(); entered != nil {
				active = append(active, entered)
			}
		}
		for _, entered := range active {
			<-entered
		}
	})
	returned := false
	defer func() {
		if !returned {
			release()
		}
	}()
	seen := make(map[*interceptorValue]converter.EncodedValue)
	var mappingError error
	nodes := 0
	var transfer func(converter.EncodedValue, int) converter.EncodedValue
	transfer = func(value converter.EncodedValue, depth int) converter.EncodedValue {
		if value == nil {
			return nil
		}
		if nilRuntime(value) {
			mappingError = failure(ErrExecution, "encoded-value-nil")
			return value
		}
		nodes++
		if depth > 64 || nodes > 4096 {
			mappingError = failure(ErrLimit, "encoded-value-graph")
			return value
		}
		if original := encodedInterceptorView(value); original != nil {
			if original.authority != origin || original.factory == nil || original.factory.owner != owner {
				mappingError = failure(ErrAuthority, "encoded-value-origin")
				return value
			}
			if mapped := seen[original]; mapped != nil {
				return mapped
			}
			frame := &continuationFrame{}
			frames = append(frames, frame)
			copy := *original
			copy.authority = current
			copy.guard = func(decode func() error) error {
				leave, err := frame.enter()
				if err != nil {
					return err
				}
				defer leave()
				return nativeBorrowGuard(work, owner, decode)
			}
			copy.scopeError = func(err error) error { return guardNativeError(work, owner, err) }
			mapped := encodedInterceptorResult(&copy)
			seen[original] = mapped
			copy.native = transfer(original.native, depth+1)
			return mapped
		}
		if mapper, ok := value.(encodedValueMapper); ok {
			frame := &continuationFrame{}
			mapped := func() converter.EncodedValue {
				defer frame.close()
				return mapper.FathomryMapEncodedValueV1(func(child converter.EncodedValue) converter.EncodedValue {
					leave, err := frame.enter()
					if err != nil {
						return &interceptorValue{guard: func(func() error) error { return err }}
					}
					defer leave()
					return transfer(child, depth+1)
				})
			}()
			if nilRuntime(mapped) || reflect.TypeOf(mapped) != reflect.TypeOf(value) {
				mappingError = failure(ErrExecution, "encoded-value-mapping")
			}
			return mapped
		}
		return value
	}
	mapped := transfer(value, 0)
	if mappingError != nil {
		return nil, noop, mappingError
	}
	returned = true
	return mapped, release, nil
}

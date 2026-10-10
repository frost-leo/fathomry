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

package internal

import (
	"errors"
	"reflect"
	"sync"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/converter"
)

// FathomryDecodeGuardV1 owns an entire synchronous lazy decode, including payload
// retrieval. It is an opt-in local compatibility contract, not an upstream API.
type FathomryDecodeGuardV1 func(func() error) error
type fathomryDecodeMutex = sync.Mutex

// FathomryScopeOwnerV1 is an opaque, nonzero-sized local owner identity. Never
// expose it through borrowed SDK options, contexts or callbacks.
type FathomryScopeOwnerV1 struct {
	marker byte
	parent *FathomryScopeOwnerV1
}

func NewFathomryScopeOwnerV1() *FathomryScopeOwnerV1 { return &FathomryScopeOwnerV1{marker: 1} }

func NewFathomryChildScopeOwnerV1(parent *FathomryScopeOwnerV1) *FathomryScopeOwnerV1 {
	return &FathomryScopeOwnerV1{marker: 1, parent: parent}
}

type fathomryScopeEntry struct {
	owner        *FathomryScopeOwnerV1
	guard        FathomryDecodeGuardV1
	finalization bool
}
type fathomryDecoderScope struct {
	entries []fathomryScopeEntry
	denied  bool
}

func fathomryScope(existing *fathomryDecoderScope, owner *FathomryScopeOwnerV1, guard FathomryDecodeGuardV1) *fathomryDecoderScope {
	if owner == nil || guard == nil {
		return existing
	}
	if existing == nil {
		return &fathomryDecoderScope{entries: []fathomryScopeEntry{{owner: owner, guard: guard}}}
	}
	if existing.denied {
		return existing
	}
	for index, entry := range existing.entries {
		if entry.owner == owner {
			copy := append([]fathomryScopeEntry(nil), existing.entries...)
			copy[index].guard = guard
			return &fathomryDecoderScope{entries: copy}
		}
	}
	if len(existing.entries) >= 64 {
		return &fathomryDecoderScope{denied: true}
	}
	return &fathomryDecoderScope{entries: append([]fathomryScopeEntry{{owner: owner, guard: guard}}, existing.entries...)}
}

func (scope *fathomryDecoderScope) run(decode func() error) error {
	if scope == nil {
		return decode()
	}
	if scope.denied {
		return errors.New("temporal: decoder scope chain exceeds its bound")
	}
	var invoke func(int) error
	invoke = func(index int) error {
		if index == len(scope.entries) {
			return decode()
		}
		return scope.entries[index].guard(func() error { return invoke(index + 1) })
	}
	return invoke(0)
}

type fathomryErrorOrigin struct {
	original error
	previous *fathomryErrorOrigin
}

func (origin fathomryErrorOrigin) Is(target error) bool {
	if target == nil || !reflect.TypeOf(target).Comparable() {
		return false
	}
	// Only SDK-owned copy ancestry is inspected, never user Is/Unwrap callbacks
	// or semantic causes. This is identity preservation, not cause selection.
	for remaining := 4096; remaining > 0; remaining-- {
		if origin.original == target {
			return true
		}
		if origin.previous == nil {
			return false
		}
		origin = *origin.previous
	}
	return false
}

// FathomryScopeErrorV1 clones known native errors without changing their failure
// round-trip or exposing unguarded native causes. Already-decoded details and
// opaque custom errors are caller-owned. Custom converters retaining lazy runtime
// dependencies must implement FathomryScopeDecodersV1 on their custom error.
func FathomryScopeErrorV1(cause error, owner *FathomryScopeOwnerV1, guard FathomryDecodeGuardV1) error {
	if cause == nil || owner == nil || guard == nil {
		return cause
	}
	result, _ := fathomryMapErrorScopes(cause, func(existing *fathomryDecoderScope) *fathomryDecoderScope {
		return fathomryScope(existing, owner, guard)
	}, guard)
	return result
}

// FathomryDecoderScopeV1 is opaque decoder authority for cooperating custom
// errors. Mapping must return an isolated same-shape error, never mutate aliases.
type FathomryDecoderScopeV1 = fathomryDecoderScope

func (scope *fathomryDecoderScope) Decode(decode func() error) error {
	if decode == nil {
		return errors.New("temporal: nil decoder")
	}
	return scope.run(decode)
}

func (*fathomryDecoderScope) MarshalJSON() ([]byte, error) {
	return nil, errors.New("temporal: decoder scope serialization unsupported")
}

func (*fathomryDecoderScope) UnmarshalJSON([]byte) error {
	return errors.New("temporal: decoder scope reconstruction unsupported")
}

func fathomryMapErrorScopes(cause error, transform func(*fathomryDecoderScope) *fathomryDecoderScope, legacy FathomryDecodeGuardV1) (error, bool) {
	seen := make(map[error]error)
	complete := true
	var scope func(error, int) error
	details := func(values converter.EncodedValues) converter.EncodedValues {
		if encoded, ok := values.(*EncodedValues); ok && encoded != nil {
			copy := *encoded
			copy.fathomryScope = transform(encoded.fathomryScope)
			return &copy
		}
		return values
	}
	scope = func(value error, depth int) error {
		if value == nil {
			return nil
		}
		if depth > 64 || len(seen) > 4096 {
			complete = false
			return errors.New("temporal: error scope graph exceeds its bound")
		}
		reflected := reflect.ValueOf(value)
		if reflected.Kind() == reflect.Pointer && reflected.IsNil() {
			return value
		}
		if reflected.Type().Comparable() {
			if existing, ok := seen[value]; ok {
				return existing
			}
		}
		switch original := value.(type) {
		case *ApplicationError:
			copy := *original
			seen[value] = &copy
			copy.fathomryErrorOrigin = fathomryErrorOrigin{original: original, previous: &original.fathomryErrorOrigin}
			copy.details = details(original.details)
			copy.cause = scope(original.cause, depth+1)
			return &copy
		case *CanceledError:
			copy := *original
			seen[value] = &copy
			copy.fathomryErrorOrigin = fathomryErrorOrigin{original: original, previous: &original.fathomryErrorOrigin}
			copy.details = details(original.details)
			copy.cause = scope(original.cause, depth+1)
			return &copy
		case *TimeoutError:
			copy := *original
			seen[value] = &copy
			copy.fathomryErrorOrigin = fathomryErrorOrigin{original: original, previous: &original.fathomryErrorOrigin}
			copy.lastHeartbeatDetails = details(original.lastHeartbeatDetails)
			copy.cause = scope(original.cause, depth+1)
			return &copy
		case *ActivityError:
			copy := *original
			seen[value] = &copy
			copy.fathomryErrorOrigin = fathomryErrorOrigin{original: original, previous: &original.fathomryErrorOrigin}
			copy.cause = scope(original.cause, depth+1)
			return &copy
		case *ChildWorkflowExecutionError:
			copy := *original
			seen[value] = &copy
			copy.fathomryErrorOrigin = fathomryErrorOrigin{original: original, previous: &original.fathomryErrorOrigin}
			copy.cause = scope(original.cause, depth+1)
			return &copy
		case *ServerError:
			copy := *original
			seen[value] = &copy
			copy.fathomryErrorOrigin = fathomryErrorOrigin{original: original, previous: &original.fathomryErrorOrigin}
			copy.cause = scope(original.cause, depth+1)
			return &copy
		case *WorkflowExecutionError:
			copy := *original
			seen[value] = &copy
			copy.fathomryErrorOrigin = fathomryErrorOrigin{original: original, previous: &original.fathomryErrorOrigin}
			copy.cause = scope(original.cause, depth+1)
			return &copy
		case *NexusOperationError:
			copy := *original
			seen[value] = &copy
			copy.Cause = scope(original.Cause, depth+1)
			return &copy
		case *nexus.HandlerError:
			copy := *original
			seen[value] = &copy
			copy.Cause = scope(original.Cause, depth+1)
			return &copy
		case *nexus.OperationError:
			copy := *original
			seen[value] = &copy
			copy.Cause = scope(original.Cause, depth+1)
			return &copy
		case interface {
			FathomryMapDecoderScopeV1(func(*FathomryDecoderScopeV1) *FathomryDecoderScopeV1) error
		}:
			return original.FathomryMapDecoderScopeV1(transform)
		case interface {
			FathomryScopeDecodersV1(func(func() error) error) error
		}:
			if legacy != nil {
				return original.FathomryScopeDecodersV1(legacy)
			}
			return value
		default:
			return value
		}
	}
	result := scope(cause, 0)
	return result, complete
}

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

package failure

import (
	"fmt"
	"log/slog"
)

// Detailed composes the common error core with any component-owned type T.
// The core is immutable. Detail ownership, copying, bounds and concurrent use
// follow the component's explicit clone contract, not a reflected type whitelist.
// Copies of this handle share state; do not overwrite a shared handle.
type Detailed[T any] struct {
	state *detailState[T]
	_     typeIdentity[T]
}

type typeIdentity[T any] [0]func() T

type detailState[T any] struct {
	core    *Error
	details T
	clone   func(T) T
}

// NewDetailed composes declared details with the shared error contract. The
// definition must name a detail contract and clone must be non-nil. All Go types,
// including private fields, interfaces and recursive models, are permitted.
//
// The component supplies clone once as a typed function, invoked on input and by
// each Details read. It must not mutate its argument, must isolate mutable data
// according to the declared contract, and must be synchronous, concurrency-safe,
// bounded and non-panicking. Immutable data may be shared; intentionally borrowed
// references need an owner-defined lifetime. The function and its captures remain
// retained with the occurrence. No resource ownership or cleanup is inferred.
//
// Metadata and cause admission happen before clone is called. No methods on T are
// invoked implicitly and there is no fallback shallow copy. The component owns
// correspondence between T, its contract identifier and its semantic revision.
// Failure cannot prove a supplied copy function satisfies those promises.
func NewDetailed[T any](definition Definition, location Location, details T, clone func(T) T, causes ...error) (*Detailed[T], error) {
	if definition.Details.ID == "" || clone == nil {
		return nil, reject(ErrDetails)
	}
	core, err := New(definition, location, causes...)
	if err != nil {
		return nil, err
	}
	return &Detailed[T]{state: &detailState[T]{core: core, details: clone(details), clone: clone}}, nil
}

// Details returns the component's copied view of deliberately sensitive data.
// Its isolation and borrowed-reference semantics are the clone contract's.
// It never traverses causes or selects another occurrence's data. Nil/zero
// receivers return (zero, false). The component's clone runs on the caller's stack.
func (err *Detailed[T]) Details() (T, bool) {
	if err == nil || err.state == nil {
		var zero T
		return zero, false
	}
	return err.state.clone(err.state.details), true
}

// Failure exposes only this occurrence's shared core, not its detail payload.
func (err *Detailed[T]) Failure() *Error {
	if err == nil || err.state == nil {
		return nil
	}
	return err.state.core
}

func (err *Detailed[T]) Error() string { return err.Failure().Error() }
func (err *Detailed[T]) Unwrap() error {
	if core := err.Failure(); core != nil {
		return core
	}
	return nil
}
func (err Detailed[T]) Format(state fmt.State, verb rune) {
	formatError(state, verb, (&err).Error())
}

// LogValue, formatting and JSON guards never invoke the component's clone or
// inspect details. This is separate from deliberately calling Details.
func (err *Detailed[T]) LogValue() slog.Value    { return err.Failure().LogValue() }
func (Detailed[T]) MarshalJSON() ([]byte, error) { return nil, reject(ErrSerialization) }
func (*Detailed[T]) UnmarshalJSON([]byte) error  { return reject(ErrSerialization) }

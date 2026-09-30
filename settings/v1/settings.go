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

package settings

import (
	"fmt"
	"log/slog"
	"reflect"
)

// Snapshot holds a prepared value and its copy function. Copies of the handle
// share data. ValueCopy returns an owner-defined copy, not a mutable current value.
// A zero Snapshot is unprepared. Do not overwrite a shared handle concurrently.
type Snapshot[T any] struct {
	data *data
	_    typeIdentity[T]
}

type typeIdentity[T any] [0]func() T

type data struct {
	value   reflect.Value
	payload any
}

type frozen[T any] struct {
	value T
	clone func(T) T
}

// View captures one snapshot without exposing its root Go type. It remains valid
// after further publications; Capture again to observe newer data. A zero View
// is unprepared. Holding a View keeps its snapshot and copy function alive.
type View struct{ data *data }

// New prepares a snapshot using the project's non-nil clone function. There is no
// data-type whitelist, serialization or implicit business validation. Nil values
// are valid when permitted by the project's own schema.
//
// Clone runs synchronously on input and each ValueCopy. It must not mutate its
// argument; it must be bounded, concurrency-safe, non-panicking and isolate mutable
// data according to the project's contract. Immutable data may be shared; borrowed
// references require an explicit owner/lifetime. Its captures remain retained.
// Settings cannot prove the supplied function meets these obligations and does not
// recover programmer panics or assume cleanup responsibility for stored values.
func New[T any](value T, clone func(T) T) (Snapshot[T], error) {
	if clone == nil {
		return Snapshot[T]{}, reject(ErrCopy, "new")
	}
	owned := &frozen[T]{value: clone(value), clone: clone}
	return Snapshot[T]{data: &data{value: reflect.ValueOf(&owned.value).Elem(), payload: owned}}, nil
}

// ValueCopy copies the prepared root using its owner's copy function. The result
// may contain secrets. An unprepared snapshot returns ErrSnapshot without calling
// any owner code. Concurrent reads require the documented clone contract.
func (snapshot Snapshot[T]) ValueCopy() (T, error) {
	if snapshot.data != nil {
		if owned, ok := snapshot.data.payload.(*frozen[T]); ok {
			return owned.clone(owned.value), nil
		}
	}
	var zero T
	return zero, reject(ErrSnapshot, "value_copy")
}

// View erases only the root type, without copying or modifying the snapshot.
// An unprepared Snapshot returns an unprepared View.
func (snapshot Snapshot[T]) View() View { return View{data: snapshot.data} }

// As recovers the exact original root type, including struct tags and named types.
// It does not copy data or coerce a different type. ErrSnapshot denotes an
// unprepared View; ErrType denotes a different prepared root type.
func As[T any](view View) (Snapshot[T], error) {
	if view.data == nil {
		return Snapshot[T]{}, reject(ErrSnapshot, "as")
	}
	if _, ok := view.data.payload.(*frozen[T]); !ok {
		return Snapshot[T]{}, reject(ErrType, "as")
	}
	return Snapshot[T]{data: view.data}, nil
}

func (Snapshot[T]) Format(state fmt.State, verb rune) { formatHandle(state, verb, "settings.Snapshot") }
func (Snapshot[T]) LogValue() slog.Value              { return slog.StringValue("settings.Snapshot") }
func (Snapshot[T]) MarshalJSON() ([]byte, error)      { return nil, reject(ErrSerialization, "marshal") }
func (*Snapshot[T]) UnmarshalJSON([]byte) error       { return reject(ErrSerialization, "unmarshal") }

func (View) Format(state fmt.State, verb rune) { formatHandle(state, verb, "settings.View") }
func (View) LogValue() slog.Value              { return slog.StringValue("settings.View") }
func (View) MarshalJSON() ([]byte, error)      { return nil, reject(ErrSerialization, "marshal") }
func (*View) UnmarshalJSON([]byte) error       { return reject(ErrSerialization, "unmarshal") }

func formatHandle(state fmt.State, verb rune, label string) {
	if verb == 'q' {
		_, _ = fmt.Fprintf(state, "%q", label)
		return
	}
	_, _ = state.Write([]byte(label))
}

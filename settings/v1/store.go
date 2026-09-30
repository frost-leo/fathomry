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
	"sync/atomic"
)

// Store is the write authority for one typed data domain. Create it with NewStore;
// a zero Store is invalid. Copies share the same publication cell and authority.
// Publication/capture are concurrent-safe; do not overwrite a shared handle.
// The store owns no goroutines or resources and requires no Close.
type Store[T any] struct {
	cell *cell
	_    typeIdentity[T]
}

// Reader borrows read-only access to a store without publication authority. Copies
// share the same cell. A zero Reader or a store without a publication cannot
// Capture. Holding a Reader keeps its cell/current snapshot alive.
type Reader struct{ cell *cell }

type cell struct{ current atomic.Pointer[data] }

// NewStore creates an initially empty data domain. No default is selected and no
// validation, source acquisition, listener registration or I/O occurs.
func NewStore[T any]() Store[T] { return Store[T]{cell: &cell{}} }

// Publish atomically replaces the complete current snapshot. Admission failure
// leaves the previous value untouched. Existing Views retain their own snapshot.
// No clone function runs during publication or capture.
//
// The producer owns semantic validation, source ordering and rejection of stale
// asynchronous results BEFORE publishing. Concurrent Publish calls have atomic
// replacement order only; source revision/freshness cannot be inferred from it.
// There is no per-field update, reset, generation counter or callback invocation.
func (store Store[T]) Publish(snapshot Snapshot[T]) error {
	if store.cell == nil {
		return reject(ErrStore, "publish")
	}
	if snapshot.data == nil {
		return reject(ErrSnapshot, "publish")
	}
	if _, ok := snapshot.data.payload.(*frozen[T]); !ok {
		return reject(ErrSnapshot, "publish")
	}
	store.cell.current.Store(snapshot.data)
	return nil
}

// Reader returns the same domain's read-only handle, without publishing data.
func (store Store[T]) Reader() Reader { return Reader{cell: store.cell} }

// Capture returns one coherent snapshot or ErrUnconfigured. No owner copy
// function runs; use As/Read for explicit access. Separate Capture calls may
// observe different publications, even within one operation.
func (reader Reader) Capture() (View, error) {
	if reader.cell != nil {
		if current := reader.cell.current.Load(); current != nil {
			return View{data: current}, nil
		}
	}
	return View{}, reject(ErrUnconfigured, "capture")
}

func (Store[T]) Format(state fmt.State, verb rune) { formatHandle(state, verb, "settings.Store") }
func (Store[T]) LogValue() slog.Value              { return slog.StringValue("settings.Store") }
func (Store[T]) MarshalJSON() ([]byte, error)      { return nil, reject(ErrSerialization, "marshal") }
func (*Store[T]) UnmarshalJSON([]byte) error       { return reject(ErrSerialization, "unmarshal") }

func (Reader) Format(state fmt.State, verb rune) { formatHandle(state, verb, "settings.Reader") }
func (Reader) LogValue() slog.Value              { return slog.StringValue("settings.Reader") }
func (Reader) MarshalJSON() ([]byte, error)      { return nil, reject(ErrSerialization, "marshal") }
func (*Reader) UnmarshalJSON([]byte) error       { return reject(ErrSerialization, "unmarshal") }

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
	"sync"

	failurepb "go.temporal.io/api/failure/v1"
)

// FathomryPrepareErrorFinalizationV1 marks only scopes owned by this task group
// in an isolated native-shaped copy. Unknown graphs and unscoped user errors are
// unchanged. The marked copy itself does not grant decoding after task return.
func FathomryPrepareErrorFinalizationV1(cause error, group *FathomryScopeOwnerV1) error {
	if cause == nil || group == nil {
		return cause
	}
	changed := false
	result, complete := fathomryMapErrorScopes(cause, func(scope *fathomryDecoderScope) *fathomryDecoderScope {
		if scope == nil || scope.denied {
			return scope
		}
		var entries []fathomryScopeEntry
		for index, entry := range scope.entries {
			if entry.owner != group && (entry.owner == nil || entry.owner.parent != group) {
				continue
			}
			if entries == nil {
				entries = append([]fathomryScopeEntry(nil), scope.entries...)
			}
			entries[index].finalization = true
		}
		if entries == nil {
			return scope
		}
		changed = true
		return &fathomryDecoderScope{entries: entries}
	}, nil)
	if !complete || !changed {
		return cause
	}
	return result
}

// FathomryConvertErrorV1 grants the marked copy's decoder access only during the
// synchronous native conversion window. It retains already-entered decodes on
// return, panic and Goexit. Foreign restrictions and original aliases are intact.
func FathomryConvertErrorV1(cause error, convert func(error) *failurepb.Failure) *failurepb.Failure {
	window := &fathomryFinalizationWindow{}
	defer window.close()
	changed := false
	result, complete := fathomryMapErrorScopes(cause, func(scope *fathomryDecoderScope) *fathomryDecoderScope {
		if scope == nil || scope.denied {
			return scope
		}
		var entries []fathomryScopeEntry
		for index, entry := range scope.entries {
			if !entry.finalization {
				continue
			}
			if entries == nil {
				entries = append([]fathomryScopeEntry(nil), scope.entries...)
			}
			entries[index].guard = window.decode
			entries[index].finalization = false
		}
		if entries == nil {
			return scope
		}
		changed = true
		return &fathomryDecoderScope{entries: entries}
	}, nil)
	if !complete || !changed {
		return convert(cause)
	}
	return convert(result)
}

type fathomryFinalizationWindow struct {
	mu      sync.Mutex
	changed *sync.Cond
	closed  bool
	active  int
}

func (window *fathomryFinalizationWindow) decode(decode func() error) error {
	window.mu.Lock()
	if window.closed {
		window.mu.Unlock()
		return errors.New("temporal: failure conversion decoder expired")
	}
	window.active++
	window.mu.Unlock()
	defer func() {
		window.mu.Lock()
		window.active--
		if window.changed != nil {
			window.changed.Broadcast()
		}
		window.mu.Unlock()
	}()
	return decode()
}
func (window *fathomryFinalizationWindow) close() {
	window.mu.Lock()
	defer window.mu.Unlock()
	window.closed = true
	for window.active != 0 {
		if window.changed == nil {
			window.changed = sync.NewCond(&window.mu)
		}
		window.changed.Wait()
	}
}

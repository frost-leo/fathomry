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

package viper

import (
	"context"
	native "github.com/frost-leo/fathomry/internal/configsource/viper/v1"
)

// Document retains immutable native state and original bytes. Local queries are
// concurrent-safe; they perform no remote I/O or additional operation admission.
type Document struct {
	private
	native *native.Document
}

// Snapshot freezes native values, not original syntax or a common-time transaction.
type Snapshot struct {
	private
	native *native.Snapshot
}

// RawCopy deliberately exposes detached original bytes, not resolved environment.
func (document *Document) RawCopy() []byte {
	if document == nil {
		return nil
	}
	return document.native.RawCopy()
}

// Encoding reports the selected syntax; empty means no initialized document.
func (document *Document) Encoding() string {
	if document == nil {
		return ""
	}
	return document.native.Encoding()
}

// Keys returns registered native names, never an enumeration of process environment.
func (document *Document) Keys() ([]string, error) {
	if document == nil {
		return nil, fail(ErrInput, "keys")
	}
	value, err := document.native.Keys()
	return value, translate(err, "keys")
}

// ValueCopy deliberately exposes sensitive native results. Case folding, "."
// paths, null/default fallback and live environment remain native semantics.
func (document *Document) ValueCopy(key string) (any, error) {
	if document == nil {
		return nil, fail(ErrInput, "query")
	}
	value, err := document.native.ValueCopy(key)
	return value, translate(err, "query")
}

// Capture queries registered and explicit keys once. Keep sources/environment
// stable for a coherent epoch; this is not an atomic process snapshot.
func (document *Document) Capture(ctx context.Context, extraKeys ...string) (*Snapshot, error) {
	if document == nil {
		return nil, fail(ErrInput, "capture")
	}
	value, err := document.native.Capture(ctx, extraKeys...)
	if err != nil {
		return nil, translate(err, "capture")
	}
	return &Snapshot{native: value}, nil
}

// ValueCopy reads only captured state; later environment updates cannot affect it.
func (snapshot *Snapshot) ValueCopy(key string) (any, error) {
	if snapshot == nil {
		return nil, fail(ErrInput, "snapshot")
	}
	value, err := snapshot.native.ValueCopy(key)
	return value, translate(err, "snapshot")
}

// ValuesCopy returns detached native AllSettings, never original-syntax evidence.
func (snapshot *Snapshot) ValuesCopy() (map[string]any, error) {
	if snapshot == nil {
		return nil, fail(ErrInput, "snapshot")
	}
	value, err := snapshot.native.ValuesCopy()
	return value, translate(err, "snapshot")
}

// Decode freezes schema-discovered and explicit dynamic keys, then performs
// native weak decoding with mapstructure tags/hooks. Validate T before publication.
// It does not promise strict original-number or unknown-field semantics.
func Decode[T any](ctx context.Context, document *Document, extraKeys ...string) (T, error) {
	if document == nil {
		var zero T
		return zero, fail(ErrInput, "decode")
	}
	value, err := native.Decode[T](ctx, document.native, extraKeys...)
	return value, translate(err, "decode")
}

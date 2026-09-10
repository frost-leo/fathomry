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
	"strconv"
	"strings"

	sdk "github.com/spf13/viper"
)

// Document owns raw bytes and private native state. It has no mutating operations
// and supports concurrent queries/copies. This does not promise concurrent access
// to an externally mutable Viper: no SDK object or borrowed mutable result escapes.
// Environment bindings remain live, including native empty-value fallback.
type Document struct {
	private
	raw      []byte
	encoding string
	native   *sdk.Viper
}

// RawCopy returns caller-owned original bytes, not AllSettings, an environment
// snapshot, or a prepared configuration. The zero/nil document returns nil.
func (document *Document) RawCopy() []byte {
	if document == nil {
		return nil
	}
	return append([]byte(nil), document.raw...)
}

// Encoding reports the explicitly selected document encoding, not a schema,
// SDK version or prepared configuration revision. Zero/nil means unknown.
func (document *Document) Encoding() string {
	if document == nil {
		return ""
	}
	return document.encoding
}

// ValueCopy performs a native case-insensitive Get with "." path separation,
// then copies maps/slices. Native types, null/default fallback and live binding
// precedence are preserved; absent/native nil is (nil, nil). JSON numbers retain
// native float64 semantics, NOT exact integer preparation semantics. Raw values
// are deliberate sensitive inspection, never safe diagnostics. A live environment
// string over MaxDocumentBytes is rejected, not truncated or silently fallen back.
// After the root component, negative native-integer path components are refused
// even when a literal map key or binding would match; Viper can panic on negative
// array indices. Copied parent maps and original raw bytes retain those keys.
func (document *Document) ValueCopy(key string) (any, error) {
	if document == nil || document.native == nil || !validQuery(key) {
		return nil, fail(ErrInput, "query")
	}
	value := document.native.Get(key)
	if text, ok := value.(string); ok && len(text) > MaxDocumentBytes {
		return nil, fail(ErrLimit, "query")
	}
	return copyValue(value), nil
}

func validQuery(key string) bool {
	if !validKey(key) {
		return false
	}
	for _, component := range strings.Split(key, ".")[1:] {
		if index, err := strconv.Atoi(component); err == nil && index < 0 {
			return false
		}
	}
	return true
}

func copyValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(value))
		for key, child := range value {
			result[key] = copyValue(child)
		}
		return result
	case []any:
		result := make([]any, len(value))
		for index, child := range value {
			result[index] = copyValue(child)
		}
		return result
	default:
		return value
	}
}

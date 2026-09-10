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
	"math"
	"strings"
	"unicode/utf8"
)

// OptionsV1 is revision 1 of this integration's process-local option contract,
// independent of the SDK major in its import path. Encoding is required ("yaml"
// or "json"); the zero value is invalid. Defaults and Environment are ordered,
// bounded lists; empty lists disable those native sources. AllowEmptyEnv defaults
// to false. No file/reader, callback, schema or business-layer policy belongs here.
//
// Values are borrowed during Load only. Mutating them afterward cannot reconfigure
// a Document. This Go contract has no serialized representation or migration
// protocol. A future incompatible option contract needs a distinct type and
// explicit conversion, not reinterpretation of OptionsV1 or an SDK version bump.
type OptionsV1 struct {
	private
	Encoding      string
	Defaults      []Default
	Environment   []Binding
	AllowEmptyEnv bool
}

// Default supplies a native SetDefault scalar: nil, string, bool, a built-in
// integer type, or a finite float. Maps, slices, named types and callbacks are
// deliberately unsupported; this is not the application's typed defaults layer.
type Default struct {
	private
	Key   string
	Value any
}

// Binding supplies both a Viper key and an exact case-sensitive environment name.
// Repeated keys append names in caller order, as native BindEnv does. Values are
// live, not captured by Load. No prefix, replacer or AutomaticEnv is enabled.
type Binding struct {
	private
	Key  string
	Name string
}

func validKey(key string) bool {
	return key != "" && len(key) <= MaxKeyBytes && utf8.ValidString(key) &&
		!strings.ContainsRune(key, 0) && strings.Count(key, ".") < MaxDepth
}

func scalarSize(value any) (int, bool) {
	switch value := value.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return 8, true
	case float32:
		return 4, !math.IsNaN(float64(value)) && !math.IsInf(float64(value), 0)
	case float64:
		return 8, !math.IsNaN(value) && !math.IsInf(value, 0)
	case string:
		return len(value), utf8.ValidString(value)
	}
	return 0, false
}

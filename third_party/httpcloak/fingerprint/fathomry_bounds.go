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

package fingerprint

import (
	"errors"
	"reflect"
)

// FathomryErrPresetBound identifies oversized or non-data native containers.
var FathomryErrPresetBound = errors.New("httpcloak: native containers exceed bound")

// FathomryGetStrictBounded snapshots one registry entry without a second lookup.
func FathomryGetStrictBounded(name string, maximum int64) (*Preset, error) {
	var preset *Preset
	if registered, ok := customPresets.Load(name); ok {
		preset, _ = registered.(*Preset)
	} else if factory, ok := presets[name]; ok {
		preset = factory()
	}
	if preset == nil {
		return nil, nil
	}
	if _, err := FathomryDataBytes(preset, maximum); err != nil {
		return nil, err
	}
	return clonePreset(preset), nil
}

// FathomryDataBytes inspects pure preset containers, never borrowed live
// authority or user marshalers. Depth and visits bound repeated pointer structures.
func FathomryDataBytes(input any, maximum int64) (int64, error) {
	if maximum < 1 {
		return 0, FathomryErrPresetBound
	}
	var total int64
	visits := 0
	var walk func(reflect.Value, int) bool
	charge := func(count uint64) bool {
		if count > uint64(maximum-total) {
			return false
		}
		total += int64(count)
		return true
	}
	walk = func(value reflect.Value, depth int) bool {
		visits++
		if visits > 65536 || depth > 32 {
			return false
		}
		if !value.IsValid() {
			return true
		}
		switch value.Kind() {
		case reflect.Pointer, reflect.Interface:
			if value.IsNil() {
				return true
			}
			return charge(32) && walk(value.Elem(), depth+1)
		case reflect.String:
			return charge(uint64(value.Len()) + 16)
		case reflect.Struct:
			if !charge(uint64(value.Type().Size())) {
				return false
			}
			for index := 0; index < value.NumField(); index++ {
				if !walk(value.Field(index), depth+1) {
					return false
				}
			}
			return true
		case reflect.Array, reflect.Slice:
			if uint64(value.Len()) > uint64(maximum-total)/max(uint64(value.Type().Elem().Size()), 1) {
				return false
			}
			switch value.Type().Elem().Kind() {
			case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
				return charge(uint64(value.Len())*uint64(value.Type().Elem().Size()) + 32)
			}
			for index := 0; index < value.Len(); index++ {
				if !walk(value.Index(index), depth+1) {
					return false
				}
			}
			return charge(32)
		case reflect.Map:
			if uint64(value.Len()) > uint64(maximum-total)/64 || !charge(uint64(value.Len())*64) {
				return false
			}
			iterator := value.MapRange()
			for iterator.Next() {
				if !walk(iterator.Key(), depth+1) || !walk(iterator.Value(), depth+1) {
					return false
				}
			}
			return true
		case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
			return charge(uint64(value.Type().Size()))
		default:
			return false
		}
	}
	if !walk(reflect.ValueOf(input), 0) {
		return 0, FathomryErrPresetBound
	}
	return total, nil
}

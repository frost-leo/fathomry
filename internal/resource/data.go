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

package resource

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"unicode/utf8"
)

// PreparedData is identity-neutral prepared data, not a resource selection.
// It shares the strict preparation engine but cannot be passed to Select.
type PreparedData[T any] struct {
	prepared Prepared[T]
	_        typeIdentity[T]
}

func (data PreparedData[T]) Format(state fmt.State, verb rune) { data.prepared.Format(state, verb) }
func (*PreparedData[T]) LogValue() slog.Value {
	return slog.StringValue("resource.PreparedData[redacted]")
}
func (data PreparedData[T]) MarshalJSON() ([]byte, error) { return data.prepared.MarshalJSON() }
func (*PreparedData[T]) UnmarshalJSON([]byte) error {
	return errors.New("source: data must pass through PrepareData")
}

// PrepareData applies the existing strict layer contract without inventing a
// resource identity or creating an Assembly.
func PrepareData[T any](schema Schema[T], layers []Layer) (PreparedData[T], error) {
	value, err := prepare(schema, Input{Format: schema.Format, Layers: layers}, false)
	if err != nil {
		return PreparedData[T]{}, err
	}
	return PreparedData[T]{prepared: value}, nil
}

// ValueCopy returns independent, deliberately sensitive plain settings.
func (data PreparedData[T]) ValueCopy() (T, error) { return data.prepared.settings() }

// Description returns owned preparation metadata with an empty resource identity.
func (data PreparedData[T]) Description() Description { return data.prepared.Description() }

// CheckDocumentFormat preflights the original mapping with the strict parser.
// It does not normalize/re-emit input or supply defaults for absent headers.
func CheckDocumentFormat(data []byte, expected uint32) error {
	if expected == 0 || !utf8.Valid(data) {
		return errors.New("source: invalid document format")
	}
	values, err := parseMapping(data)
	if err != nil {
		return err
	}
	number, ok := values["format"].(json.Number)
	if !ok {
		return errors.New("source: missing integer format")
	}
	parsed, err := strconv.ParseUint(string(number), 10, 32)
	if err != nil || uint32(parsed) != expected {
		return errors.New("source: unsupported document format")
	}
	return nil
}

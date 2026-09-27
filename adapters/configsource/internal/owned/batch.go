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

package owned

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"unicode/utf8"

	source "github.com/frost-leo/fathomry/adapters/configsource/v1"
)

// Entry is borrowed during NewBatch only.
type Entry struct {
	Name     string
	Presence source.Presence
	Raw      []byte
}
type batch struct {
	Guard
	entries []Entry
}

func (*batch) Format(state fmt.State, verb rune) { Guard{}.Format(state, verb) }
func (*batch) LogValue() slog.Value              { return Guard{}.LogValue() }

// NewBatch checks the complete aggregate before retaining any raw content.
func NewBatch(entries []Entry) (source.Batch, error) {
	if len(entries) < 1 || len(entries) > source.MaxDocuments {
		return nil, Fail(source.ErrLimit)
	}
	total := 0
	seen := make(map[string]bool, len(entries))
	for _, entry := range entries {
		if !Label(entry.Name) || seen[entry.Name] || entry.Presence != source.Present && entry.Presence != source.Missing ||
			entry.Presence == source.Missing && len(entry.Raw) != 0 || !utf8.Valid(entry.Raw) {
			return nil, Fail(source.ErrValue)
		}
		seen[entry.Name] = true
		if len(entry.Raw) > source.MaxDocumentBytes || len(entry.Raw) > source.MaxBatchBytes-total {
			return nil, Fail(source.ErrLimit)
		}
		total += len(entry.Raw)
	}
	result := &batch{entries: make([]Entry, len(entries))}
	for index, entry := range entries {
		result.entries[index] = Entry{Name: strings.Clone(entry.Name), Presence: entry.Presence, Raw: bytes.Clone(entry.Raw)}
	}
	return result, nil
}
func (value *batch) Documents() []source.Document {
	if value == nil {
		return nil
	}
	result := make([]source.Document, len(value.entries))
	for index, entry := range value.entries {
		result[index] = source.Document{Name: entry.Name, Presence: entry.Presence, Bytes: len(entry.Raw)}
	}
	return result
}
func (value *batch) RawCopy(name string) ([]byte, source.Presence, error) {
	if value != nil {
		for _, entry := range value.entries {
			if entry.Name == name {
				return bytes.Clone(entry.Raw), entry.Presence, nil
			}
		}
	}
	return nil, 0, Fail(source.ErrValue)
}
func Equal(left, right source.Batch) bool {
	if Nil(left) || Nil(right) {
		return Nil(left) && Nil(right)
	}
	a, b := left.Documents(), right.Documents()
	if len(a) != len(b) {
		return false
	}
	for index, entry := range a {
		if entry != b[index] {
			return false
		}
		raw, _, err := left.RawCopy(entry.Name)
		other, _, otherErr := right.RawCopy(entry.Name)
		if err != nil || otherErr != nil || !bytes.Equal(raw, other) {
			return false
		}
	}
	return true
}

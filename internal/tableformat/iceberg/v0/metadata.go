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

package iceberg

import (
	"bytes"
	"encoding/json"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go/table"
)

// decodeObject prevents different SDK/map decoders from interpreting duplicate
// keys differently. UseNumber preserves 64-bit snapshot/field identifiers.
func decodeObject(raw []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var read func(int) (any, error)
	read = func(depth int) (any, error) {
		if depth > 64 {
			return nil, failure(ErrLimit, "json-depth")
		}
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		delimiter, compound := token.(json.Delim)
		if !compound {
			return token, nil
		}
		switch delimiter {
		case '{':
			object := map[string]any{}
			seen := map[string]bool{}
			for decoder.More() {
				token, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				key, ok := token.(string)
				if !ok {
					return nil, failure(ErrProtocol, "json-key")
				}
				folded := strings.ToUpper(key)
				if seen[folded] || len(seen) >= 2048 {
					return nil, failure(ErrProtocol, "duplicate-json-key")
				}
				seen[folded] = true
				value, err := read(depth + 1)
				if err != nil {
					return nil, err
				}
				object[key] = value
			}
			if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
				return nil, failure(ErrProtocol, "json-object")
			}
			return object, nil
		case '[':
			values := []any{}
			for decoder.More() {
				if len(values) >= 2048 {
					return nil, failure(ErrLimit, "json-array")
				}
				value, err := read(depth + 1)
				if err != nil {
					return nil, err
				}
				values = append(values, value)
			}
			if token, err = decoder.Token(); err != nil || token != json.Delim(']') {
				return nil, failure(ErrProtocol, "json-array")
			}
			return values, nil
		default:
			return nil, failure(ErrProtocol, "json-delimiter")
		}
	}
	value, err := read(0)
	if err != nil {
		return nil, failure(ErrProtocol, "json", err)
	}
	if _, err = decoder.Token(); err != io.EOF {
		return nil, failure(ErrProtocol, "json-trailing")
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, failure(ErrProtocol, "json-object")
	}
	return object, nil
}
func exactControlNames(object map[string]any, names ...string) error {
	for key := range object {
		for _, name := range names {
			if key != name && strings.EqualFold(key, name) {
				return failure(ErrProtocol, "json-field-case")
			}
		}
	}
	return nil
}
func metadataNames(value any, freeKeys bool) error {
	switch value := value.(type) {
	case map[string]any:
		for key, child := range value {
			if !freeKeys && key != strings.ToLower(key) {
				return failure(ErrProtocol, "metadata-field-case")
			}
			if !freeKeys {
				for _, char := range key {
					if char > 127 {
						return failure(ErrProtocol, "metadata-field-case")
					}
				}
			}
			if err := metadataNames(child, key == "properties" || key == "summary" || key == "refs"); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range value {
			if err := metadataNames(child, false); err != nil {
				return err
			}
		}
	}
	return nil
}
func parseMetadata(s settings, raw []byte) (metadata table.Metadata, err error) {
	// Metadata decoding is pure, owns no I/O, and starts no native work. Contain
	// upstream decoder panics at this input boundary rather than losing the process.
	defer func() {
		if recovered := recover(); recovered != nil {
			cause, _ := recovered.(error)
			metadata = nil
			err = failure(ErrProtocol, "metadata-decoder", cause)
		}
	}()
	object, err := decodeObject(raw)
	if err != nil {
		return nil, err
	}
	if err = metadataNames(object, false); err != nil {
		return nil, err
	}
	schemas, ok := object["schemas"].([]any)
	if !ok || len(schemas) == 0 || len(schemas) > 128 {
		return nil, failure(ErrProtocol, "metadata-schemas")
	}
	for _, schema := range schemas {
		object, ok := schema.(map[string]any)
		if !ok {
			return nil, failure(ErrProtocol, "metadata-schema")
		}
		fields, ok := object["fields"].([]any)
		if !ok || len(fields) == 0 || len(fields) > 128 {
			return nil, failure(ErrProtocol, "metadata-fields")
		}
		for _, field := range fields {
			if _, ok := field.(map[string]any); !ok {
				return nil, failure(ErrProtocol, "metadata-field")
			}
		}
	}
	metadata, err = table.ParseMetadataBytes(raw)
	if err != nil {
		return nil, failure(ErrProtocol, "metadata", err)
	}
	if metadata == nil {
		return nil, failure(ErrProtocol, "metadata")
	}
	if err = validateMetadata(s, metadata); err != nil {
		return nil, err
	}
	return metadata, nil
}
func equalMetadata(left, right table.Metadata) (bool, error) {
	encode := func(value table.Metadata) (map[string]any, error) {
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		object, err := decodeObject(raw)
		if err != nil {
			return nil, err
		}
		// These are ID-addressed collections, unlike the ordered snapshot and
		// metadata logs or the ordered fields within a schema/partition spec.
		for _, field := range []struct{ name, id string }{{"snapshots", "snapshot-id"}, {"schemas", "schema-id"}, {"partition-specs", "spec-id"}, {"sort-orders", "order-id"}} {
			values, _ := object[field.name].([]any)
			identifier := func(value any) int64 {
				entry, _ := value.(map[string]any)
				number, _ := entry[field.id].(json.Number)
				id, _ := strconv.ParseInt(string(number), 10, 64)
				return id
			}
			slices.SortFunc(values, func(left, right any) int {
				a, b := identifier(left), identifier(right)
				if a < b {
					return -1
				}
				if a > b {
					return 1
				}
				return 0
			})
		}
		return object, nil
	}
	a, err := encode(left)
	if err != nil {
		return false, err
	}
	b, err := encode(right)
	if err != nil {
		return false, err
	}
	leftJSON, err := json.Marshal(a)
	if err != nil {
		return false, err
	}
	rightJSON, err := json.Marshal(b)
	if err != nil {
		return false, err
	}
	return bytes.Equal(leftJSON, rightJSON), nil
}

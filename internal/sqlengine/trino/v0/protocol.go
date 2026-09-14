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

package trino

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"

	native "github.com/trinodb/trino-go-client/trino"
)

type pageEnvelope struct {
	ID          string           `json:"id"`
	NextURI     string           `json:"nextUri"`
	Columns     []columnEnvelope `json:"columns"`
	Data        json.RawMessage  `json:"data"`
	Error       json.RawMessage  `json:"error"`
	UpdateCount *int64           `json:"updateCount"`
}
type columnEnvelope struct {
	Name      string          `json:"name"`
	Type      string          `json:"type"`
	Signature json.RawMessage `json:"typeSignature"`
}
type signature struct {
	RawType   string              `json:"rawType"`
	Arguments []signatureArgument `json:"arguments"`
}
type signatureArgument struct {
	Kind  string          `json:"kind"`
	Value json.RawMessage `json:"value"`
}

func jsonDocument(body []byte) error {
	if !utf8.Valid(body) {
		return failure(ErrProtocol, "json-encoding")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	nodes := 0
	var value func(int, bool) error
	value = func(depth int, structural bool) error {
		nodes++
		if depth > 32 || nodes > 1<<18 {
			return failure(ErrLimit, "json-shape")
		}
		token, err := decoder.Token()
		if err != nil {
			return failure(ErrProtocol, "json", err)
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			keys := make(map[string]bool)
			for decoder.More() {
				token, err := decoder.Token()
				key, ok := token.(string)
				if err != nil || !ok {
					return failure(ErrProtocol, "json-keys")
				}
				identity := key
				if structural {
					if !ascii(key, 1024, true) {
						return failure(ErrProtocol, "json-keys")
					}
					identity = strings.ToLower(key)
				}
				if keys[identity] {
					return failure(ErrProtocol, "json-keys")
				}
				keys[identity] = true
				childStructural := structural && !(depth == 0 && identity == "data")
				if err := value(depth+1, childStructural); err != nil {
					return err
				}
			}
			token, err = decoder.Token()
			if err != nil || token != json.Delim('}') {
				return failure(ErrProtocol, "json-object")
			}
		case '[':
			for decoder.More() {
				if err := value(depth+1, structural); err != nil {
					return err
				}
			}
			token, err = decoder.Token()
			if err != nil || token != json.Delim(']') {
				return failure(ErrProtocol, "json-array")
			}
		default:
			return failure(ErrProtocol, "json")
		}
		return nil
	}
	if err := value(0, true); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return failure(ErrProtocol, "json-trailing")
	}
	return nil
}
func queryID(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' ||
			char >= '0' && char <= '9' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}
func (e *exchange) page(body []byte) error {
	if err := jsonDocument(body); err != nil {
		return err
	}
	var page pageEnvelope
	if err := json.Unmarshal(body, &page); err != nil {
		return failure(ErrProtocol, "page", err)
	}
	if !queryID(page.ID) || e.data.queryID != "" && page.ID != e.data.queryID || e.data.terminal {
		return failure(ErrProtocol, "query-id")
	}
	e.data.queryID = page.ID
	if page.NextURI != "" && (!e.allowedURI(page.NextURI) || page.NextURI == e.next) {
		return failure(ErrAuthority, "next-uri")
	}
	e.next = page.NextURI
	hasError := len(page.Error) != 0 && string(page.Error) != "null"
	if hasError {
		var nativeError native.ErrTrino
		if json.Unmarshal(page.Error, &nativeError) != nil || !queryID(nativeError.ErrorName) ||
			!queryID(nativeError.ErrorType) {
			return failure(ErrProtocol, "server-error")
		}
	}
	if page.UpdateCount != nil && *page.UpdateCount < 0 {
		return failure(ErrProtocol, "update-count")
	}
	if page.NextURI == "" {
		e.data.terminal = true
		e.data.success = !hasError
		if !hasError {
			e.data.updateCount = page.UpdateCount
		}
	}
	if hasError {
		return nil
	}
	if len(page.Columns) > e.settings.MaxColumns {
		return failure(ErrLimit, "columns")
	}
	for i, column := range page.Columns {
		if len(column.Name) > 1024 || len(column.Type) == 0 || len(column.Type) > 4096 {
			return failure(ErrLimit, "column-metadata")
		}
		if len(e.data.columns) != 0 {
			if len(e.data.columns) != len(page.Columns) {
				return failure(ErrProtocol, "columns-changed")
			}
			old := e.data.columns[i]
			var left, right any
			_ = json.Unmarshal(old.Signature, &left)
			_ = json.Unmarshal(column.Signature, &right)
			if old.Name != column.Name || old.Type != column.Type || !reflect.DeepEqual(left, right) {
				return failure(ErrProtocol, "columns-changed")
			}
		}
	}
	if len(e.data.columns) == 0 && len(page.Columns) != 0 {
		for _, column := range page.Columns {
			var sig signature
			if json.Unmarshal(column.Signature, &sig) != nil {
				return failure(ErrProtocol, "type-signature")
			}
			if err := validateSignature(sig, 0); err != nil {
				return err
			}
			if !matchingType(column.Type, sig) {
				return failure(ErrProtocol, "column-type")
			}
			e.resultBytes += len(column.Name) + len(column.Type) + len(column.Signature) + 64
			if e.resultBytes > e.settings.MaxResultBytes {
				return failure(ErrLimit, "result-metadata")
			}
			e.data.columns = append(e.data.columns, Column{Name: column.Name, Type: column.Type, Signature: column.Signature})
			e.signatures = append(e.signatures, sig)
		}
	}
	data := bytes.TrimSpace(page.Data)
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		return nil
	}
	if data[0] == '{' {
		return failure(ErrUnsupported, "spooling")
	}
	if data[0] != '[' {
		return failure(ErrProtocol, "data")
	}
	var rows []json.RawMessage
	if err := json.Unmarshal(data, &rows); err != nil {
		return failure(ErrProtocol, "rows", err)
	}
	if len(rows) > e.settings.MaxRows-e.seenRows {
		return failure(ErrLimit, "rows")
	}
	for _, row := range rows {
		var cells []json.RawMessage
		if json.Unmarshal(row, &cells) != nil || len(e.signatures) == 0 || len(cells) != len(e.signatures) {
			return failure(ErrProtocol, "row-arity")
		}
		for i, cell := range cells {
			if err := validateCell(cell, e.signatures[i]); err != nil {
				return err
			}
		}
		if e.collect {
			added := len(row)
			if e.data.rows != 0 {
				added++
			}
			if added > e.settings.MaxResultBytes-e.resultBytes {
				return failure(ErrLimit, "result")
			}
			e.resultBytes += added
			if e.data.rows != 0 {
				e.data.json = append(e.data.json, ',')
			}
			e.data.json = append(e.data.json, row...)
			e.data.rows++
		}
		e.seenRows++
	}
	return nil
}
func child(argument signatureArgument) (signature, error) {
	var sig signature
	raw := argument.Value
	if argument.Kind == "NAMED_TYPE" {
		var named struct {
			TypeSignature json.RawMessage `json:"typeSignature"`
		}
		if json.Unmarshal(raw, &named) != nil {
			return sig, failure(ErrProtocol, "named-type")
		}
		raw = named.TypeSignature
	} else if argument.Kind != "TYPE" {
		return sig, failure(ErrProtocol, "nested-type")
	}
	if json.Unmarshal(raw, &sig) != nil {
		return sig, failure(ErrProtocol, "nested-type")
	}
	return sig, nil
}
func validateSignature(sig signature, depth int) error {
	if depth > 8 || len(sig.Arguments) > 256 {
		return failure(ErrLimit, "type-depth")
	}
	switch sig.RawType {
	case "array", "map", "row":
		required := 1
		if sig.RawType == "map" {
			required = 2
		}
		if sig.RawType == "row" {
			required = len(sig.Arguments)
		}
		if required == 0 || len(sig.Arguments) != required {
			return failure(ErrProtocol, "type-arity")
		}
		for _, arg := range sig.Arguments {
			if sig.RawType == "row" && arg.Kind != "NAMED_TYPE" || sig.RawType != "row" && arg.Kind != "TYPE" {
				return failure(ErrProtocol, "type-kind")
			}
			nested, err := child(arg)
			if err != nil {
				return err
			}
			if err := validateSignature(nested, depth+1); err != nil {
				return err
			}
		}
	case "decimal", "char", "varchar", "time", "time with time zone", "timestamp", "timestamp with time zone":
		count := 1
		if sig.RawType == "decimal" {
			count = 2
		}
		if len(sig.Arguments) != count {
			return failure(ErrProtocol, "type-arity")
		}
		numbers := make([]int64, count)
		for i, arg := range sig.Arguments {
			if arg.Kind != "LONG" || bytes.Equal(bytes.TrimSpace(arg.Value), []byte("null")) || json.Unmarshal(arg.Value, &numbers[i]) != nil || numbers[i] < 0 {
				return failure(ErrProtocol, "type-parameter")
			}
		}
		switch sig.RawType {
		case "decimal":
			if numbers[0] < 1 || numbers[0] > 38 || numbers[1] > numbers[0] {
				return failure(ErrUnsupported, "decimal-type")
			}
		case "char", "varchar":
		default:
			if numbers[0] > 12 {
				return failure(ErrUnsupported, "temporal-precision")
			}
		}
	case "boolean", "tinyint", "smallint", "integer", "bigint", "real", "double", "date", "varbinary",
		"json", "uuid", "ipaddress", "interval year to month", "interval day to second", "unknown":
		if len(sig.Arguments) != 0 {
			return failure(ErrProtocol, "type-arity")
		}
	default:
		return failure(ErrUnsupported, "result-type")
	}
	return nil
}
func validateCell(raw json.RawMessage, sig signature) error {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return failure(ErrProtocol, "empty-cell")
	}
	if bytes.Equal(raw, []byte("null")) {
		return nil
	}
	switch sig.RawType {
	case "array", "row":
		if len(raw) == 0 || raw[0] != '[' {
			return failure(ErrProtocol, "nested-array")
		}
		var cells []json.RawMessage
		if json.Unmarshal(raw, &cells) != nil || sig.RawType == "row" && len(cells) != len(sig.Arguments) {
			return failure(ErrProtocol, "nested-arity")
		}
		for i, cell := range cells {
			index := 0
			if sig.RawType == "row" {
				index = i
			}
			nested, _ := child(sig.Arguments[index])
			if err := validateCell(cell, nested); err != nil {
				return err
			}
		}
	case "map":
		if len(raw) == 0 || raw[0] != '{' {
			return failure(ErrProtocol, "nested-map")
		}
		var cells map[string]json.RawMessage
		if json.Unmarshal(raw, &cells) != nil {
			return failure(ErrProtocol, "nested-map")
		}
		nested, _ := child(sig.Arguments[1])
		keyType, _ := child(sig.Arguments[0])
		for key, cell := range cells {
			keyJSON, _ := json.Marshal(key)
			switch keyType.RawType {
			case "boolean", "tinyint", "smallint", "integer", "bigint":
				keyJSON = []byte(key)
			case "real", "double":
				if key != "NaN" && key != "Infinity" && key != "-Infinity" {
					keyJSON = []byte(key)
				}
			}
			if bytes.Equal(keyJSON, []byte("null")) {
				return failure(ErrProtocol, "map-key")
			}
			if err := validateCell(keyJSON, keyType); err != nil {
				return err
			}
			if err := validateCell(cell, nested); err != nil {
				return err
			}
		}
	case "boolean":
		if !bytes.Equal(raw, []byte("true")) && !bytes.Equal(raw, []byte("false")) {
			return failure(ErrProtocol, "boolean")
		}
	case "tinyint", "smallint", "integer", "bigint":
		bits := map[string]int{"tinyint": 8, "smallint": 16, "integer": 32, "bigint": 64}[sig.RawType]
		if _, err := strconv.ParseInt(string(raw), 10, bits); err != nil {
			return failure(ErrProtocol, "integer", err)
		}
	case "real", "double":
		bits := 64
		if sig.RawType == "real" {
			bits = 32
		}
		if raw[0] == '"' {
			var text string
			_ = json.Unmarshal(raw, &text)
			if text != "NaN" && text != "Infinity" && text != "-Infinity" {
				return failure(ErrProtocol, "float")
			}
		} else if _, err := strconv.ParseFloat(string(raw), bits); err != nil {
			return failure(ErrProtocol, "float", err)
		}
	case "unknown":
		return failure(ErrUnsupported, "unknown-value")
	default:
		var text string
		if len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, &text) != nil {
			return failure(ErrProtocol, "string-value")
		}
		if sig.RawType == "varbinary" {
			if _, err := base64.StdEncoding.DecodeString(text); err != nil {
				return failure(ErrProtocol, "binary", err)
			}
		}
		if sig.RawType == "decimal" && !decimalCell(text, sig) {
			return failure(ErrProtocol, "decimal")
		}
	}
	return nil
}

func decimalCell(text string, sig signature) bool {
	if !decimalNumber.MatchString(text) || strings.ContainsAny(text, "eE") {
		return false
	}
	var precision, scale int64
	_ = json.Unmarshal(sig.Arguments[0].Value, &precision)
	_ = json.Unmarshal(sig.Arguments[1].Value, &scale)
	whole, fraction, _ := strings.Cut(strings.TrimPrefix(text, "-"), ".")
	return int64(len(fraction)) == scale && int64(len(strings.TrimLeft(whole, "0"))) <= precision-scale
}

func matchingType(text string, sig signature) bool {
	text = strings.ToLower(text)
	if opening := strings.IndexByte(text, '('); opening >= 0 {
		base := text[:opening]
		if base == "time" || base == "timestamp" {
			closing := strings.IndexByte(text[opening:], ')')
			if closing < 0 {
				return false
			}
			text = base + text[opening+closing+1:]
		} else {
			text = base
		}
	}
	return text == sig.RawType
}

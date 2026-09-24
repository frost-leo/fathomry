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
	"bytes"
	"crypto/rand"
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/internal/fault"
	"go.yaml.in/yaml/v3"
)

// Layer supplies one explicitly authorized YAML mapping (JSON syntax is accepted
// as YAML). Bytes are borrowed during Prepare only, with at most one layer per kind.
// Environment/Variables are declarations, NOT instructions to read process state.
type Layer struct {
	Kind    LayerKind
	Content []byte
}

// Input selects one Provider configuration format. Format is not a source revision.
type Input struct {
	Identity Identity
	Format   uint32
	Layers   []Layer
}

// Schema owns field meanings, units, defaults, validation, and format compatibility.
// T must be a nonrecursive plain-data struct with explicit json field names and no
// tag options, embedded/private fields, custom serialization, interfaces, or handles.
// Validate receives an isolated copy and must be pure and safe for concurrent calls.
// Callers must not concurrently mutate defaults or input bytes during Prepare.
type Schema[T any] struct {
	Format   uint32
	Defaults T
	Validate func(T) error
}

// Prepared owns frozen resolved configuration. Only Description is diagnostic.
// It is safe for concurrent reuse; construction gets a fresh settings copy.
// Ordinary conversions cannot change its settings type, including struct tags.
type Prepared[T any] struct {
	state *preparedState
	_     typeIdentity[T]
}

// A defined generic type preserves argument identity even in conversions that
// ignore tags within anonymous structs. A bare function/array marker does not.
type typeIdentity[T any] [0]func() T

type preparedState struct {
	data        []byte
	description Description
}

func (prepared Prepared[T]) Description() Description {
	if prepared.state == nil {
		return Description{}
	}
	return prepared.state.description.Clone()
}

// Format prevents ordinary fmt formatting from exposing resolved secrets.
func (prepared Prepared[T]) Format(state fmt.State, verb rune) {
	_, _ = io.WriteString(state, "source.Prepared[redacted]")
}

// MarshalJSON refuses accidental serialization of runtime configuration.
func (prepared Prepared[T]) MarshalJSON() ([]byte, error) {
	return nil, errors.New("source: prepared configuration is not a diagnostic or durable payload")
}

// UnmarshalJSON rejects reconstruction outside the version-checked preparation path.
func (prepared *Prepared[T]) UnmarshalJSON([]byte) error {
	return errors.New("source: configuration must pass through Prepare")
}

func (prepared Prepared[T]) settings() (T, error) {
	var value T
	if prepared.state == nil {
		return value, errors.New("source: unprepared configuration")
	}
	err := json.Unmarshal(prepared.state.data, &value)
	return value, err
}

// Prepare structurally validates every layer before merging; unknown or mistyped
// overridden fields are not ignored. Semantic validation runs on resolved settings.
// Precedence is Defaults < Base < Environment < Local < Variables, independent of
// input order. Objects merge recursively (an empty object preserves children);
// arrays replace; empty strings/arrays override; null clears only pointers/maps/slices. Absent fields retain
// inherited settings (or Go zero values when an object is newly introduced).
// Documents and resolved JSON are limited to 1 MiB, nesting to 64 levels.
// Anchors, aliases, explicit/custom tags (including bare !), merge keys,
// duplicate/unknown fields, deletion, multiple documents, SDK objects, and
// implicit reload are unsupported. Numbers
// must use JSON numeric syntax; integer fields reject fractions and exponents.
func Prepare[T any](schema Schema[T], input Input) (Prepared[T], error) {
	location := fault.Context{Operation: "prepare"}
	if validID(input.Identity.Provider) {
		location.Provider = input.Identity.Provider
	}
	if validID(input.Identity.Name) {
		location.Source = input.Identity.Name
	}
	fail := func(cause error) (Prepared[T], error) {
		return Prepared[T]{}, ErrConfiguration.New(location, cause)
	}
	if !validID(input.Identity.Provider) || !validID(input.Identity.Name) {
		return fail(errors.New("source: invalid identity"))
	}
	if schema.Format == 0 || input.Format != schema.Format {
		return fail(errors.New("source: unsupported configuration format"))
	}
	kind := reflect.TypeFor[T]()
	if kind.Kind() != reflect.Struct || !plainType(kind, make(map[reflect.Type]bool)) {
		return fail(errors.New("source: unsupported configuration type"))
	}
	if !validStrings(reflect.ValueOf(schema.Defaults)) {
		return fail(errors.New("source: invalid UTF-8 in configuration defaults"))
	}
	data, err := json.Marshal(schema.Defaults)
	if err != nil {
		return fail(err)
	}
	if len(data) > 1<<20 {
		return fail(errors.New("source: defaults exceed size limit"))
	}
	var effective map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&effective); err != nil {
		return fail(err)
	}
	provenance := []LayerInfo{{Kind: Defaults, Fields: suppliedFields(kind, effective, "")}}
	layers := append([]Layer(nil), input.Layers...)
	sort.Slice(layers, func(left, right int) bool { return layers[left].Kind < layers[right].Kind })
	previous := Defaults
	for _, layer := range layers {
		if layer.Kind <= previous || layer.Kind > Variables {
			return fail(errors.New("source: invalid or duplicate layer kind"))
		}
		previous = layer.Kind
		values, err := parseMapping(layer.Content)
		if err != nil {
			return fail(err)
		}
		if !matchesType(kind, values) {
			return fail(errors.New("source: unknown field or incompatible field value"))
		}
		provenance = append(provenance, LayerInfo{Kind: layer.Kind, Fields: suppliedFields(kind, values, "")})
		overlay(effective, values)
	}
	data, err = json.Marshal(effective)
	if err != nil {
		return fail(err)
	}
	if len(data) > 1<<20 {
		return fail(errors.New("source: resolved configuration exceeds size limit"))
	}
	prepared := Prepared[T]{state: &preparedState{data: data}}
	value, err := prepared.settings()
	if err != nil {
		return fail(err)
	}
	if schema.Validate != nil {
		if err := schema.Validate(value); err != nil {
			return fail(err)
		}
	}
	var revision [16]byte
	if _, err := rand.Read(revision[:]); err != nil {
		return fail(err)
	}
	prepared.state.description = Description{Identity: input.Identity, Format: input.Format,
		Revision: hex.EncodeToString(revision[:]), Provenance: provenance}
	return prepared, nil
}

func validID(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '.' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}

func fieldName(field reflect.StructField) string { return field.Tag.Get("json") }

func validStrings(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.String:
		return utf8.ValidString(value.String())
	case reflect.Pointer:
		return value.IsNil() || validStrings(value.Elem())
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			if !validStrings(value.Field(index)) {
				return false
			}
		}
	case reflect.Slice:
		for index := 0; index < value.Len(); index++ {
			if !validStrings(value.Index(index)) {
				return false
			}
		}
	case reflect.Map:
		iterator := value.MapRange()
		for iterator.Next() {
			if !validStrings(iterator.Key()) || !validStrings(iterator.Value()) {
				return false
			}
		}
	}
	return true
}

func plainType(kind reflect.Type, visiting map[reflect.Type]bool) bool {
	if visiting[kind] || len(visiting) > 64 {
		return false
	}
	for _, contract := range []reflect.Type{reflect.TypeFor[json.Marshaler](), reflect.TypeFor[json.Unmarshaler](),
		reflect.TypeFor[encoding.TextMarshaler](), reflect.TypeFor[encoding.TextUnmarshaler]()} {
		if kind.Implements(contract) || reflect.PointerTo(kind).Implements(contract) {
			return false
		}
	}
	visiting[kind] = true
	defer delete(visiting, kind)
	switch kind.Kind() {
	case reflect.Bool, reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		return true
	case reflect.Pointer, reflect.Slice:
		return !(kind.Kind() == reflect.Slice && kind.Elem().Kind() == reflect.Uint8) && plainType(kind.Elem(), visiting)
	case reflect.Map:
		return kind.Key() == reflect.TypeFor[string]() && plainType(kind.Elem(), visiting)
	case reflect.Struct:
		names := make(map[string]bool)
		for index := 0; index < kind.NumField(); index++ {
			field := kind.Field(index)
			name := fieldName(field)
			if !field.IsExported() || field.Anonymous || !validID(name) || name == "-" || names[name] || !plainType(field.Type, visiting) {
				return false
			}
			names[name] = true
		}
		return true
	}
	return false
}

func matchesType(kind reflect.Type, value any) bool {
	if value == nil {
		return kind.Kind() == reflect.Pointer || kind.Kind() == reflect.Map || kind.Kind() == reflect.Slice
	}
	if kind.Kind() == reflect.Pointer {
		return matchesType(kind.Elem(), value)
	}
	switch kind.Kind() {
	case reflect.Struct:
		object, ok := value.(map[string]any)
		if !ok {
			return false
		}
		fields := make(map[string]reflect.Type)
		for index := 0; index < kind.NumField(); index++ {
			field := kind.Field(index)
			fields[fieldName(field)] = field.Type
		}
		for name, item := range object {
			field, found := fields[name]
			if !found || !matchesType(field, item) {
				return false
			}
		}
		return true
	case reflect.Map:
		object, ok := value.(map[string]any)
		if !ok {
			return false
		}
		for _, item := range object {
			if !matchesType(kind.Elem(), item) {
				return false
			}
		}
		return true
	case reflect.Slice:
		items, ok := value.([]any)
		if !ok {
			return false
		}
		for _, item := range items {
			if !matchesType(kind.Elem(), item) {
				return false
			}
		}
		return true
	default:
		data, err := json.Marshal(value)
		return err == nil && json.Unmarshal(data, reflect.New(kind).Interface()) == nil
	}
}

func overlay(target, patch map[string]any) {
	for name, value := range patch {
		incoming, incomingObject := value.(map[string]any)
		current, currentObject := target[name].(map[string]any)
		if incomingObject && currentObject {
			overlay(current, incoming)
		} else {
			target[name] = value
		}
	}
}

func suppliedFields(kind reflect.Type, value any, path string) []string {
	for kind.Kind() == reflect.Pointer {
		kind = kind.Elem()
	}
	var fields []string
	if object, ok := value.(map[string]any); kind.Kind() == reflect.Struct && ok {
		for index := 0; index < kind.NumField(); index++ {
			field := kind.Field(index)
			if item, found := object[fieldName(field)]; found {
				fields = append(fields, suppliedFields(field.Type, item, path+"/"+fieldName(field))...)
			}
		}
	} else {
		fields = append(fields, path)
	}
	sort.Strings(fields)
	return fields
}

func parseMapping(data []byte) (map[string]any, error) {
	if len(data) == 0 || len(data) > 1<<20 {
		return nil, errors.New("source: document size outside supported bounds")
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var document, extra yaml.Node
	if err := decoder.Decode(&document); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, errors.New("source: exactly one document required")
	}
	if len(document.Content) != 1 {
		return nil, errors.New("source: mapping required")
	}
	value, err := nodeValue(document.Content[0], 0, newYAMLSource(data))
	if err != nil {
		return nil, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("source: mapping required")
	}
	return object, nil
}

func nodeValue(node *yaml.Node, depth int, source yamlSource) (any, error) {
	invalid := errors.New("source: unsupported or ambiguous configuration syntax")
	if depth > 64 || node.Anchor != "" || node.Alias != nil || source.tagged(node) {
		return nil, invalid
	}
	switch node.Kind {
	case yaml.MappingNode:
		object := make(map[string]any, len(node.Content)/2)
		for index := 0; index < len(node.Content); index += 2 {
			key := node.Content[index]
			if key.Kind != yaml.ScalarNode || key.Tag != "!!str" || key.Anchor != "" || source.tagged(key) {
				return nil, invalid
			}
			if _, duplicate := object[key.Value]; duplicate {
				return nil, invalid
			}
			value, err := nodeValue(node.Content[index+1], depth+1, source)
			if err != nil {
				return nil, err
			}
			object[key.Value] = value
		}
		return object, nil
	case yaml.SequenceNode:
		items := make([]any, len(node.Content))
		for index, child := range node.Content {
			value, err := nodeValue(child, depth+1, source)
			if err != nil {
				return nil, err
			}
			items[index] = value
		}
		return items, nil
	case yaml.ScalarNode:
		switch node.Tag {
		case "!!null":
			return nil, nil
		case "!!str":
			return node.Value, nil
		case "!!bool":
			var value any
			if err := node.Decode(&value); err != nil {
				return nil, err
			}
			return value, nil
		case "!!int", "!!float":
			if !json.Valid([]byte(node.Value)) {
				return nil, invalid
			}
			return json.Number(node.Value), nil
		}
	}
	return nil, invalid
}

type yamlSource struct {
	text  []rune
	lines []int
}

// YAML v3.0.5 drops TaggedStyle for non-specific ! but keeps the tag's start
// position. Index the already parsed source in its native character coordinates,
// not bytes, without interpreting scalar contents or implementing another lexer.
func newYAMLSource(data []byte) yamlSource {
	if bytes.IndexByte(data, '!') < 0 {
		return yamlSource{}
	}
	var text []rune
	if bytes.HasPrefix(data, []byte{0xff, 0xfe}) || bytes.HasPrefix(data, []byte{0xfe, 0xff}) {
		var order binary.ByteOrder = binary.LittleEndian
		if data[0] == 0xfe {
			order = binary.BigEndian
		}
		units := make([]uint16, 0, (len(data)-2)/2)
		for index := 2; index+1 < len(data); index += 2 {
			units = append(units, order.Uint16(data[index:]))
		}
		text = utf16.Decode(units)
	} else {
		text = []rune(string(bytes.TrimPrefix(data, []byte{0xef, 0xbb, 0xbf})))
	}
	lines := []int{0}
	for index := 0; index < len(text); index++ {
		switch text[index] {
		case '\r':
			if index+1 < len(text) && text[index+1] == '\n' {
				index++
			}
		case '\n', '\u0085', '\u2028', '\u2029':
		default:
			continue
		}
		lines = append(lines, index+1)
	}
	return yamlSource{text: text, lines: lines}
}

func (source yamlSource) tagged(node *yaml.Node) bool {
	if node.Style&yaml.TaggedStyle != 0 {
		return true
	}
	if node.Line < 1 || node.Line > len(source.lines) || node.Column < 1 {
		return false
	}
	index := source.lines[node.Line-1] + node.Column - 1
	return index < len(source.text) && source.text[index] == '!'
}

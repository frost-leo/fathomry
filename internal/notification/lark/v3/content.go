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

package lark

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"unicode/utf8"
)

const maxContentBytes = 1 << 20

// JSON is an immutable, bounded native JSON value. Constructors reject malformed
// UTF-8, duplicate keys, trailing values and excessive nesting/complexity. Unknown
// native fields and exact numbers are preserved, not converted through float64.
// It is a runtime payload; Bytes returns a new caller-owned copy.
type JSON struct {
	private
	value string
}

func (value JSON) Bytes() []byte { return []byte(value.value) }

// NewJSON freezes an object or array, at most 1 MiB, 32 levels and 32768 tokens.
func NewJSON(data []byte) (JSON, error) {
	if err := checkJSON(data, maxContentBytes); err != nil {
		return JSON{}, err
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || trimmed[0] != '{' && trimmed[0] != '[' {
		return JSON{}, failure(ErrInput, "json")
	}
	return JSON{value: string(trimmed)}, nil
}
func checkJSON(data []byte, limit int) error {
	if len(data) == 0 || len(data) > limit {
		return failure(ErrLimit, "json")
	}
	if !utf8.Valid(data) || !json.Valid(data) {
		return failure(ErrInput, "json")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	budget := 32768
	if err := jsonToken(decoder, 0, &budget); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return failure(ErrInput, "json")
	}
	return nil
}

func exactFields(data []byte, names ...string) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil || fields == nil {
		return nil, failure(ErrInput, "json-object", err)
	}
	for key := range fields {
		for _, name := range names {
			if key != name && strings.EqualFold(key, name) {
				return nil, failure(ErrInput, "json-field-case")
			}
		}
	}
	return fields, nil
}
func jsonToken(decoder *json.Decoder, depth int, budget *int) error {
	*budget--
	if depth > 32 || *budget < 0 {
		return failure(ErrLimit, "json")
	}
	token, err := decoder.Token()
	if err != nil {
		return failure(ErrInput, "json", err)
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return failure(ErrInput, "json", err)
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return failure(ErrInput, "json")
			}
			seen[name] = true
			if err := jsonToken(decoder, depth+1, budget); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := jsonToken(decoder, depth+1, budget); err != nil {
				return err
			}
		}
	default:
		return failure(ErrInput, "json")
	}
	_, err = decoder.Token()
	return err
}
func object(value JSON) bool { return len(value.value) > 0 && value.value[0] == '{' }
func identifier(value string) bool {
	if value == "" || len(value) > 256 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-' || char == '.') {
			return false
		}
	}
	return value != "." && value != ".."
}
func shortText(value string, limit int) bool {
	return len(value) <= limit && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

// Content freezes one native message content object. Type is a Feishu message
// type, not an inferred MIME type. Local schema checks do not certify service or
// recipient-client support. Mentions, links and native rich-post nodes are kept.
type Content struct {
	private
	kind string
	json JSON
}

func (value Content) Type() string { return value.kind }
func (value Content) JSON() JSON   { return value.json }

// NewContent accepts notification message types, not arbitrary SDK operations.
// Interactive content must be a JSON 2.0 card, template or card-instance reference.
func NewContent(kind string, data []byte) (Content, error) {
	value, err := NewJSON(data)
	if err != nil {
		return Content{}, err
	}
	if !object(value) {
		return Content{}, failure(ErrInput, "content")
	}
	switch kind {
	case "text", "post", "image", "file", "audio", "media", "sticker", "share_chat", "share_user":
	case "interactive":
		if err := cardContent(value); err != nil {
			return Content{}, err
		}
	default:
		return Content{}, failure(ErrUnsupported, "message-type")
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(value.Bytes(), &fields)
	required := ""
	switch kind {
	case "text":
		required = "text"
	case "image":
		required = "image_key"
	case "file", "audio", "sticker":
		required = "file_key"
	case "media":
		required = "file_key"
	case "share_chat":
		required = "chat_id"
	case "share_user":
		required = "user_id"
	}
	if required != "" {
		var item string
		if json.Unmarshal(fields[required], &item) != nil || item == "" || !shortText(item, maxContentBytes) {
			return Content{}, failure(ErrInput, "content")
		}
	}
	if kind == "post" && len(fields) == 0 {
		return Content{}, failure(ErrInput, "post")
	}
	return Content{kind: kind, json: value}, nil
}

// Text constructs a plain-text message, safely escaping JSON but not interpreting
// the native mention markup. Empty text is rejected.
func Text(text string) (Content, error) {
	if text == "" || !shortText(text, maxContentBytes/6) {
		return Content{}, failure(ErrInput, "text")
	}
	data, err := json.Marshal(struct {
		Text string `json:"text"`
	}{text})
	if err != nil {
		return Content{}, failure(ErrInput, "text", err)
	}
	return NewContent("text", data)
}
func cardContent(value JSON) error {
	fields, err := exactFields(value.Bytes(), "schema", "type", "data", "body")
	if err != nil {
		return failure(ErrInput, "card", err)
	}
	if body, present := fields["body"]; present && string(body) != "null" {
		if _, err := exactFields(body, "elements"); err != nil {
			return failure(ErrInput, "card", err)
		}
	}
	var shape struct {
		Schema string          `json:"schema"`
		Type   string          `json:"type"`
		Data   json.RawMessage `json:"data"`
		Body   *struct {
			Elements []json.RawMessage `json:"elements"`
		} `json:"body"`
	}
	if json.Unmarshal(value.Bytes(), &shape) != nil {
		return failure(ErrInput, "card")
	}
	if shape.Type == "template" || shape.Type == "card" {
		key := "template_id"
		if shape.Type == "card" {
			key = "card_id"
		}
		fields, err := exactFields(shape.Data, key)
		if err != nil {
			return failure(ErrInput, "card-reference", err)
		}
		var id string
		if json.Unmarshal(fields[key], &id) != nil || !identifier(id) {
			return failure(ErrInput, "card-reference")
		}
		return nil
	}
	if shape.Schema != "2.0" || shape.Body == nil || shape.Body.Elements == nil {
		return failure(ErrUnsupported, "card-schema")
	}
	return nil
}

// Card constructs native interactive content without replacing charts with images.
func Card(data []byte) (Content, error) { return NewContent("interactive", data) }

// Template preserves the published template version and arbitrary bounded native
// variables. Template permission and rendering remain platform/client contracts.
func Template(id, version string, variables JSON) (Content, error) {
	if !identifier(id) || !shortText(version, 64) || !object(variables) {
		return Content{}, failure(ErrInput, "template")
	}
	data, _ := json.Marshal(struct {
		Type string `json:"type"`
		Data struct {
			ID        string          `json:"template_id"`
			Version   string          `json:"template_version_name,omitempty"`
			Variables json.RawMessage `json:"template_variable"`
		} `json:"data"`
	}{Type: "template", Data: struct {
		ID        string          `json:"template_id"`
		Version   string          `json:"template_version_name,omitempty"`
		Variables json.RawMessage `json:"template_variable"`
	}{id, version, variables.Bytes()}})
	return Card(data)
}

// CardInstance refers to a separately created, same-application CardKit entity.
func CardInstance(id string) (Content, error) {
	if !identifier(id) {
		return Content{}, failure(ErrInput, "card-id")
	}
	data, _ := json.Marshal(map[string]any{"type": "card", "data": map[string]string{"card_id": id}})
	return Card(data)
}

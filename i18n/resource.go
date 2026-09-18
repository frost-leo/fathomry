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

package i18n

import (
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/language"
)

type parameter struct {
	Type        string `json:"type"`
	Description string `json:"description"`
}

type sourceContract struct {
	ID          string               `json:"id"`
	Description string               `json:"description"`
	Parameters  map[string]parameter `json:"parameters"`
	Count       string               `json:"count"`
	Forms       map[string]string    `json:"forms"`
}

type resourceMessage struct {
	sourceContract
	Source string
}

type resourceFile struct {
	Locale   string
	Messages []resourceMessage
}

func decodeResource(data []byte) (resourceFile, error) {
	if !utf8.Valid(data) || !validEscapes(data) {
		return resourceFile{}, problem(InvalidResource)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := inspectJSON(decoder, 0); err != nil {
		return resourceFile{}, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return resourceFile{}, problem(InvalidResource)
	}
	var raw struct {
		Profile  string            `json:"profile"`
		Locale   string            `json:"locale"`
		License  []string          `json:"license"`
		Messages []json.RawMessage `json:"messages"`
	}
	if err := decodeObject(data, &raw, "profile", "locale", "license", "messages"); err != nil {
		return resourceFile{}, err
	}
	if raw.Profile != Profile {
		return resourceFile{}, problem(UnsupportedProfile)
	}
	tag, err := parseLocale(raw.Locale)
	if err != nil {
		return resourceFile{}, err
	}
	if len(raw.Messages) == 0 {
		return resourceFile{}, problem(InvalidResource)
	}
	if len(raw.Messages) > MaxMessages {
		return resourceFile{}, problem(LimitExceeded)
	}
	file := resourceFile{Locale: tag.String()}
	for _, encoded := range raw.Messages {
		var item struct {
			ID          string                     `json:"id"`
			Description string                     `json:"description"`
			Source      string                     `json:"source"`
			Parameters  map[string]json.RawMessage `json:"parameters"`
			Count       string                     `json:"count"`
			Forms       map[string]string          `json:"forms"`
		}
		if err := decodeObject(encoded, &item, "id", "description", "source", "parameters", "count", "forms"); err != nil {
			return resourceFile{}, err
		}
		if !validID(item.ID) || len(item.Forms) == 0 || item.Forms["other"] == "" {
			return resourceFile{}, problem(InvalidResource)
		}
		if len(item.Parameters) > MaxParameters {
			return resourceFile{}, problem(LimitExceeded)
		}
		parameters := make(map[string]parameter, len(item.Parameters))
		for name, encoded := range item.Parameters {
			var value parameter
			if !validName(name) {
				return resourceFile{}, problem(InvalidResource)
			}
			if err := decodeObject(encoded, &value, "type", "description"); err != nil {
				return resourceFile{}, err
			}
			if value.Type != "string" && value.Type != "integer" && value.Type != "boolean" && value.Type != "number" {
				return resourceFile{}, problem(InvalidResource)
			}
			if strings.TrimSpace(value.Description) == "" || len(value.Description) > 1024 {
				return resourceFile{}, problem(InvalidResource)
			}
			parameters[name] = value
		}
		for form, content := range item.Forms {
			switch form {
			case "zero", "one", "two", "few", "many", "other":
			default:
				return resourceFile{}, problem(InvalidResource)
			}
			if len(content) > MaxTemplateBytes {
				return resourceFile{}, problem(LimitExceeded)
			}
			if strings.TrimSpace(content) == "" {
				return resourceFile{}, problem(InvalidTemplate)
			}
		}
		file.Messages = append(file.Messages, resourceMessage{
			sourceContract: sourceContract{item.ID, item.Description, parameters, item.Count, item.Forms},
			Source:         item.Source,
		})
	}
	return file, nil
}

func validateSource(message *resourceMessage) error {
	if message.Source != "" || strings.TrimSpace(message.Description) == "" || len(message.Description) > 1024 {
		return problem(InvalidResource)
	}
	if message.Count != "" {
		if message.Parameters[message.Count].Type != "number" || message.Forms["one"] == "" {
			return problem(InvalidResource)
		}
	}
	return nil
}

func decodeObject(data []byte, destination any, allowed ...string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil || fields == nil {
		return problem(InvalidResource)
	}
	for name := range fields {
		found := false
		for _, field := range allowed {
			found = found || name == field
		}
		if !found {
			return problem(InvalidResource)
		}
	}
	if err := json.Unmarshal(data, destination); err != nil {
		return problem(InvalidResource)
	}
	return nil
}

func inspectJSON(decoder *json.Decoder, depth int) error {
	if depth > MaxJSONDepth {
		return problem(LimitExceeded)
	}
	token, err := decoder.Token()
	if err != nil || token == nil {
		return problem(InvalidResource)
	}
	switch token {
	case json.Delim('{'):
		keys := make(map[string]bool)
		for decoder.More() {
			token, err := decoder.Token()
			key, ok := token.(string)
			if err != nil || !ok {
				return problem(InvalidResource)
			}
			if keys[key] {
				return problem(Duplicate)
			}
			keys[key] = true
			if err := inspectJSON(decoder, depth+1); err != nil {
				return err
			}
		}
		if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
			return problem(InvalidResource)
		}
	case json.Delim('['):
		for decoder.More() {
			if err := inspectJSON(decoder, depth+1); err != nil {
				return err
			}
		}
		if token, err := decoder.Token(); err != nil || token != json.Delim(']') {
			return problem(InvalidResource)
		}
	default:
		if _, delimiter := token.(json.Delim); delimiter {
			return problem(InvalidResource)
		}
	}
	return nil
}

// encoding/json replaces unpaired escaped UTF-16 surrogates; resource validation
// refuses them instead of silently changing authored content.
func validEscapes(data []byte) bool {
	for index := 0; index < len(data); index++ {
		if data[index] != '\\' {
			continue
		}
		index++
		if index >= len(data) {
			return false
		}
		if data[index] != 'u' {
			continue
		}
		if index+4 >= len(data) {
			return false
		}
		value, err := strconv.ParseUint(string(data[index+1:index+5]), 16, 16)
		if err != nil {
			return false
		}
		index += 4
		if value >= 0xdc00 && value <= 0xdfff {
			return false
		}
		if value >= 0xd800 && value <= 0xdbff {
			if index+6 >= len(data) || data[index+1] != '\\' || data[index+2] != 'u' {
				return false
			}
			low, err := strconv.ParseUint(string(data[index+3:index+7]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return false
			}
			index += 6
		}
	}
	return true
}

func validID(value string) bool {
	if len(value) == 0 || len(value) > MaxIDBytes {
		return false
	}
	start, dots := true, 0
	for _, char := range value {
		if char == '.' {
			if start {
				return false
			}
			start = true
			dots++
			continue
		}
		if char < 'a' || char > 'z' {
			if start || !(char >= '0' && char <= '9' || char == '-' || char == '_') {
				return false
			}
		}
		start = false
	}
	return !start && dots > 0
}

func validName(value string) bool {
	if len(value) == 0 || len(value) > MaxNameBytes {
		return false
	}
	for index, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' ||
			index > 0 && (char >= '0' && char <= '9' || char == '_')) {
			return false
		}
	}
	return true
}

func parseLocale(value string) (language.Tag, error) {
	if len(value) == 0 || len(value) > MaxLocaleBytes {
		return language.Und, problem(InvalidLocale)
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '-') {
			return language.Und, problem(InvalidLocale)
		}
	}
	tag, err := language.Parse(value)
	base, _, _ := tag.Raw()
	if err != nil || base.String() == "und" || len(tag.Extensions()) != 0 || len(tag.Variants()) != 0 {
		return language.Und, problem(InvalidLocale)
	}
	return tag, nil
}

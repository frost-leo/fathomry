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

package resource_test

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"strings"
	"testing"
	"unicode/utf16"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/resource"
	"go.yaml.in/yaml/v3"
)

func TestPinnedYAMLOptionsDoNotReplaceThePreparationContract(t *testing.T) {
	type config struct {
		Limit int `json:"limit" yaml:"limit"`
	}
	input := "limit: 2\nunexpected: secret-canary"
	for _, knownFields := range []bool{false, true} {
		decoder := yaml.NewDecoder(strings.NewReader(input))
		decoder.KnownFields(knownFields)
		var value config
		err := decoder.Decode(&value)
		if knownFields {
			var native *yaml.TypeError
			if !errors.As(err, &native) {
				t.Fatal("fixed YAML known-field option did not reject unknown input")
			}
		} else if err != nil || value.Limit != 2 {
			t.Fatal("fixed YAML default behavior changed")
		}
	}
	schema := resource.Schema[config]{Format: 1, Defaults: config{Limit: 4}}
	identity := resource.Identity{Provider: "fixture.yaml", Name: "configuration"}
	for _, content := range []string{input, "limit: 2\nlimit: 3", strings.Repeat(" ", 1<<20+1)} {
		prepared, err := resource.Prepare(schema, resource.Input{Format: 1, Identity: identity,
			Layers: []resource.Layer{{Kind: resource.Base, Content: []byte(content)}}})
		if !errors.Is(err, resource.ErrConfiguration) || prepared.Description().Revision != "" {
			t.Fatal("native option behavior replaced the stronger framework preparation contract")
		}
		conformance.Private(t, err, "secret-canary")
	}
	prepared, err := resource.Prepare(schema, resource.Input{Format: 1, Identity: identity})
	if err != nil {
		t.Fatal(err)
	}
	selected := resource.Select(prepared, func(_ context.Context, effective config) (resource.Resource[int], error) {
		if effective.Limit != 4 {
			t.Error("effective default changed")
		}
		return resource.Resource[int]{Acquired: true, Capability: effective.Limit, Release: func(context.Context) resource.ReleaseResult {
			return resource.ReleaseResult{Quiescent: true, Released: true}
		}}, nil
	})
	assembly, err := resource.Assemble(context.Background(), context.Background(), "configuration", selected)
	if err != nil {
		t.Fatal(err)
	}
	if err := assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func yamlTestEncoding(text, encoding string) []byte {
	if encoding == "utf8" {
		return []byte(text)
	}
	if encoding == "utf8-bom" {
		return append([]byte{0xef, 0xbb, 0xbf}, []byte(text)...)
	}
	var order binary.AppendByteOrder = binary.LittleEndian
	if encoding == "utf16be" {
		order = binary.BigEndian
	}
	var data []byte
	for _, value := range utf16.Encode([]rune("\ufeff" + text)) {
		data = order.AppendUint16(data, value)
	}
	return data
}

func TestYAMLExplicitTagsAndLiteralText(t *testing.T) {
	type config struct {
		Enabled bool              `json:"enabled"`
		Items   []string          `json:"items"`
		Text    string            `json:"text"`
		Headers map[string]string `json:"headers"`
	}
	for _, fixture := range []struct {
		name, input string
		reject      bool
		want        config
	}{
		{"value", "enabled: ! true", true, config{}},
		{"root", "! {enabled: true}", true, config{}},
		{"key", "! enabled: true", true, config{}},
		{"sequence", "items: ! [value]", true, config{}},
		{"sequence-item", "items: [! value]", true, config{}},
		{"mapping", "headers: ! {key: value}", true, config{}},
		{"nested-key", "headers: {! key: value}", true, config{}},
		{"quoted-value", "text: ! 'literal !'", true, config{}},
		{"block-value", "text: ! |\n  literal !\n", true, config{}},
		{"null", "items: ! null", true, config{}},
		{"empty-tagged-value", "text: !", true, config{}},
		{"multiline-tag", "items: !\n- value", true, config{}},
		{"unicode-before-tag", "{text: 'é😀', enabled: ! true}", true, config{}},
		{"comment-before-tag", "# literal !\n\nenabled: ! true", true, config{}},
		{"bom-in-tagged-key", "headers: {! '\ufeff!': value}", true, config{}},
		{"explicit-core-tag", "text: !!str value", true, config{}},
		{"custom-tag", "text: !custom value", true, config{}},
		{"verbatim-tag", "text: !<tag:yaml.org,2002:str> value", true, config{}},
		{"verbatim-nonspecific", "text: !<!> value", true, config{}},
		{"plain", "text: hello!world", false, config{Text: "hello!world"}},
		{"single-quote", "text: '! true'", false, config{Text: "! true"}},
		{"double-quote", "text: \"! true\"", false, config{Text: "! true"}},
		{"quoted-key", "headers: {'! key': '! value'}", false, config{Headers: map[string]string{"! key": "! value"}}},
		{"literal", "text: |\n  ! true\n", false, config{Text: "! true\n"}},
		{"folded", "text: >-\n  ! first\n  ! second\n", false, config{Text: "! first ! second"}},
		{"comment", "enabled: true # ! explicit-looking text\n# ! more\n", false, config{Enabled: true}},
		{"unicode-before-quote", "{text: 'é😀', items: ['! value']}", false, config{Text: "é😀", Items: []string{"! value"}}},
		{"blank-lines", "\n# !\n\ntext: '!'", false, config{Text: "!"}},
		{"bom-in-quoted-key", "headers: {'\ufeff!': '!'}", false, config{Headers: map[string]string{"\ufeff!": "!"}}},
		{"bom-inside-quote", "text: 'first\n\ufeff! last'", false, config{Text: "first \ufeff! last"}},
		{"empty-value", "items:", false, config{}},
	} {
		for _, encoding := range []string{"utf8", "utf8-bom", "utf16le", "utf16be"} {
			t.Run(fixture.name+"/"+encoding, func(t *testing.T) {
				data := yamlTestEncoding(fixture.input, encoding)
				var native yaml.Node
				if err := yaml.Unmarshal(data, &native); err != nil {
					t.Fatal("fixture is not valid native YAML")
				}
				validations := 0
				prepared, err := resource.Prepare(resource.Schema[config]{Format: 1, Validate: func(value config) error {
					validations++
					if !fixture.reject && !reflect.DeepEqual(value, fixture.want) {
						t.Error("literal text or effective settings changed")
					}
					return nil
				}}, resource.Input{Identity: resource.Identity{Provider: "fixture.yaml", Name: "tags"}, Format: 1,
					Layers: []resource.Layer{{Kind: resource.Base, Content: data}}})
				if fixture.reject {
					if !errors.Is(err, resource.ErrConfiguration) || prepared.Description().Revision != "" || validations != 0 {
						t.Fatal("explicit tag crossed the syntax/validation boundary")
					}
					return
				}
				if err != nil || validations != 1 {
					t.Fatal("legal literal text did not reach validation")
				}
				selected := resource.Select(prepared, func(_ context.Context, value config) (resource.Resource[config], error) {
					if !reflect.DeepEqual(value, fixture.want) {
						t.Error("factory received different literal text")
					}
					return resource.Resource[config]{Acquired: true, Capability: value, Release: complete}, nil
				})
				assembly := assemble(t, "tags", selected)
				if err := assembly.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestYAMLTagPositionsAcrossLineBreaks(t *testing.T) {
	for _, newline := range []string{"\n", "\r\n", "\r", "\u0085", "\u2028", "\u2029"} {
		for _, tagged := range []bool{false, true} {
			text := "# ! text" + newline + "text: 'é😀'" + newline + "enabled: "
			if tagged {
				text += "! "
			}
			text += "true"
			prepared, err := resource.Prepare(resource.Schema[struct {
				Text    string `json:"text"`
				Enabled bool   `json:"enabled"`
			}]{Format: 1}, resource.Input{Identity: resource.Identity{Provider: "fixture.yaml", Name: "lines"}, Format: 1,
				Layers: []resource.Layer{{Kind: resource.Base, Content: []byte(text)}}})
			if tagged {
				if !errors.Is(err, resource.ErrConfiguration) || prepared.Description().Revision != "" {
					t.Error("line break hid an explicit tag")
				}
			} else if err != nil {
				t.Error("line break caused literal text rejection")
			}
		}
	}
}

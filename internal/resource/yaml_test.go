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
	"errors"
	"strings"
	"testing"

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

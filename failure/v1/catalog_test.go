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

package failure_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/failure/v1"
)

func TestCatalog(t *testing.T) {
	definitions := append(failure.Definitions(), detailedDefinition())
	other := definition()
	other.Code = 0xA4410001
	other.Component = "other"
	other.Identifier = "example.other.read_failed"
	definitions = append(definitions, other)
	catalog, err := failure.Prepare(definitions...)
	if err != nil {
		t.Fatal(err)
	}
	definitions[len(definitions)-1].Message = "mutated"

	t.Run("numeric symbolic and reverse component queries", func(t *testing.T) {
		got, found, err := catalog.Lookup(sampleCode)
		if err != nil || !found || got != detailedDefinition() {
			t.Fatal("numeric lookup differs", err)
		}
		byName, found, err := catalog.LookupIdentifier(got.Identifier)
		if err != nil || !found || byName != got {
			t.Fatal("symbolic lookup does not select the same definition", err)
		}
		components, err := catalog.Components()
		if err != nil || len(components) != 3 {
			t.Fatal(components, err)
		}
		if components[0].Module != "example" || components[0].Name != "other" || components[1].Name != "source" {
			t.Fatal("component query order changed")
		}
		if components[0].Facility != 0x441 || components[1].Facility != 0x440 || components[0].Domain != failure.DomainConfiguration || components[1].Domain != failure.DomainConfiguration {
			t.Fatal("component query lost allocated facility")
		}
		items, found, err := catalog.InComponent("example", "other")
		if err != nil || !found || len(items) != 1 || items[0] != other {
			t.Fatal("component membership or copying differs", err)
		}
		components[0].Codes[0] = 42
		items[0].Message = "changed"
		again, _, _ := catalog.Lookup(other.Code)
		if again != other {
			t.Fatal("query output mutated catalog")
		}
		all, err := catalog.Inspect()
		if err != nil || len(all) != 9 {
			t.Fatal(err)
		}
		for index := 1; index < len(all); index++ {
			if all[index-1].Code >= all[index].Code {
				t.Fatal("inspection is not numerically ordered")
			}
		}
		if raw, err := json.Marshal(all); err != nil || !strings.Contains(string(raw), sampleCode.String()) {
			t.Fatal("static atlas is not inspectable as data", err)
		}
	})
	t.Run("absence and invalid handles", func(t *testing.T) {
		if _, found, err := catalog.Lookup(0xA7FFFFFF); found || err != nil {
			t.Fatal("unknown valid code is not absence", err)
		}
		if _, found, err := catalog.LookupIdentifier("example.source.unknown"); found || err != nil {
			t.Fatal("unknown identifier is not absence", err)
		}
		if _, found, err := catalog.InComponent("example", "unknown"); found || err != nil {
			t.Fatal(err)
		}
		if _, _, err := catalog.Lookup(0); !errors.Is(err, failure.ErrCode) {
			t.Fatal(err)
		}
		if _, _, err := catalog.LookupIdentifier("private/token"); !errors.Is(err, failure.ErrDefinition) {
			t.Fatal(err)
		}
		if _, _, err := catalog.InComponent("", ""); !errors.Is(err, failure.ErrDefinition) {
			t.Fatal("empty owner selected the whole catalog", err)
		}
		for _, invalid := range []*failure.Catalog{nil, {}} {
			if _, err := invalid.Inspect(); !errors.Is(err, failure.ErrCatalog) {
				t.Fatal(err)
			}
			if _, err := invalid.Components(); !errors.Is(err, failure.ErrCatalog) {
				t.Fatal(err)
			}
			if _, _, err := invalid.Lookup(sampleCode); !errors.Is(err, failure.ErrCatalog) {
				t.Fatal(err)
			}
			if _, _, err := invalid.LookupIdentifier("example.source.read_failed"); !errors.Is(err, failure.ErrCatalog) {
				t.Fatal(err)
			}
			if _, _, err := invalid.InComponent("example", "source"); !errors.Is(err, failure.ErrCatalog) {
				t.Fatal(err)
			}
		}
		empty, err := failure.Prepare()
		if err != nil {
			t.Fatal(err)
		}
		all, err := empty.Inspect()
		if err != nil || len(all) != 0 {
			t.Fatal("prepared empty catalog became invalid", err)
		}
	})
	t.Run("concurrent detached snapshots", func(t *testing.T) {
		var workers sync.WaitGroup
		for range 12 {
			workers.Go(func() {
				for range 50 {
					all, err := catalog.Inspect()
					if err != nil {
						t.Error(err)
						return
					}
					all[0].Message = "changed"
					owners, _ := catalog.Components()
					owners[0].Codes[0] = 42
					got, _, _ := catalog.Lookup(sampleCode)
					if got != detailedDefinition() {
						t.Error("concurrent query changed definition")
					}
				}
			})
		}
		workers.Wait()
	})
}

func TestDefinitionAdmission(t *testing.T) {
	for name, modify := range map[string]func(*failure.Definition){
		"zero code":                func(d *failure.Definition) { d.Code = 0 },
		"wrong owner":              func(d *failure.Definition) { d.Identifier = "other.source.failed" },
		"empty component":          func(d *failure.Definition) { d.Component = "" },
		"zero revision":            func(d *failure.Definition) { d.Revision = 0 },
		"missing explanation":      func(d *failure.Definition) { d.Message = "" },
		"control in message":       func(d *failure.Definition) { d.Message = "secret\ntext" },
		"oversize message":         func(d *failure.Definition) { d.Message = strings.Repeat("a", failure.MaxMessageBytes+1) },
		"bad UTF-8":                func(d *failure.Definition) { d.Description = string([]byte{255}) },
		"private detail contract":  func(d *failure.Definition) { d.Details = failure.Contract{ID: "other.source.details", Version: 1} },
		"missing detail version":   func(d *failure.Definition) { d.Details = failure.Contract{ID: "example.source.details"} },
		"version without contract": func(d *failure.Definition) { d.Details.Version = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := definition()
			modify(&invalid)
			if invalid.Valid() {
				t.Fatal("invalid declaration reported valid")
			}
			if result, err := failure.Prepare(definition(), invalid); result != nil || err == nil {
				t.Fatal("partial or invalid catalog published")
			}
		})
	}
	t.Run("both identity directions are unique", func(t *testing.T) {
		for _, modify := range []func(*failure.Definition){
			func(*failure.Definition) {},
			func(d *failure.Definition) { d.Identifier = "example.source.other" },
			func(d *failure.Definition) { d.Code++ },
		} {
			other := definition()
			modify(&other)
			if catalog, err := failure.Prepare(definition(), other); catalog != nil || !errors.Is(err, failure.ErrDefinition) {
				t.Fatal("duplicate identity admitted", err)
			}
		}
	})
	t.Run("count and aggregate byte bounds", func(t *testing.T) {
		if catalog, err := failure.Prepare(make([]failure.Definition, failure.MaxDefinitions+1)...); catalog != nil || !errors.Is(err, failure.ErrLimit) {
			t.Fatal("count bound ignored", err)
		}
		definitions := make([]failure.Definition, 1100)
		for index := range definitions {
			value := definition()
			value.Code += failure.Code(index)
			value.Identifier = failure.Identifier("example.source.e" + value.Code.String()[2:])
			value.Identifier = failure.Identifier(strings.ToLower(string(value.Identifier)))
			value.Description = strings.Repeat("x", failure.MaxDescriptionBytes)
			definitions[index] = value
		}
		if catalog, err := failure.Prepare(definitions...); catalog != nil || !errors.Is(err, failure.ErrLimit) {
			t.Fatal("aggregate bytes unbounded", err)
		}
	})
	t.Run("builtin construction failures have definitions", func(t *testing.T) {
		definitions := failure.Definitions()
		catalog, err := failure.Prepare(definitions...)
		if err != nil {
			t.Fatal(err)
		}
		_, bad := failure.ParseCode("not a numeric code")
		current, ok := failure.Inspect(bad)
		if !ok {
			t.Fatal("admission error lacks occurrence")
		}
		got, found, err := catalog.Lookup(failure.ErrCode)
		if err != nil || !found || !reflect.DeepEqual(got, current.Diagnostic().Definition) {
			t.Fatal("admission diagnostic and atlas disagree", err)
		}
	})
}

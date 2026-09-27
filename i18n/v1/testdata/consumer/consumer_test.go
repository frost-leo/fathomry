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

package consumer

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

//go:embed resources/*.json
var resources embed.FS

func load(t *testing.T) *i18n.Catalog {
	t.Helper()
	var sources []i18n.Source
	for _, locale := range []string{"en", "zh-CN", "ru", "ar", "custom"} {
		data, err := resources.ReadFile("resources/" + locale + ".json")
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, i18n.Source{Name: "consumer." + locale, Data: data})
	}
	catalog, err := i18n.Prepare(sources...)
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func TestIndependentCatalog(t *testing.T) {
	catalog := load(t)
	definitions, err := catalog.Inspect()
	if err != nil || len(definitions) != 10 {
		t.Fatal(len(definitions), err)
	}
	found, err := catalog.Lookup("example.inventory:identity", "en")
	if err != nil || !found.MessageExists || !found.TranslationExists {
		t.Fatal(found, err)
	}
	found, err = catalog.Lookup("example.inventory:identity", "ru")
	if err != nil || !found.MessageExists || found.TranslationExists {
		t.Fatal(found, err)
	}
	found, err = catalog.Lookup("unknown", "ru")
	if err != nil || found.MessageExists || found.TranslationExists {
		t.Fatal(found, err)
	}
	var group sync.WaitGroup
	for _, locale := range []string{"en", "zh-CN", "ru", "ar"} {
		group.Go(func() {
			selection, err := catalog.Resolve("example.inventory:items", locale)
			if err != nil {
				t.Error(err)
				return
			}
			count := uint64(math.MaxUint64)
			result, err := selection.Render([]i18n.Argument{{Name: "place", Value: "shelf"}}, &count)
			info, _ := selection.Metadata()
			if err != nil || !strings.Contains(result.Text, "18446744073709551615") || info.Resource.Locale != locale {
				t.Error(result, err)
			}
			info.Resource.Arguments[0].Name = "caller mutation"
		})
	}
	group.Wait()
	// Translation authors obtain the exact source digest via the public English
	// catalog. No private hash helper or framework bootstrap is necessary.
	english, _ := resources.ReadFile("resources/en.json")
	sourceCatalog, err := i18n.Prepare(i18n.Source{Name: "source", Data: english})
	if err != nil {
		t.Fatal(err)
	}
	source, _ := sourceCatalog.Lookup("example.inventory:identity", "en")
	translation, err := json.Marshal(map[string]any{
		"schema": i18n.Schema, "profile": i18n.Profile, "owner": "example.inventory", "locale": "he",
		"messages": []any{map[string]any{"id": source.Definition.ID, "source": source.Definition.SourceDigest, "forms": map[string]string{"other": source.Definition.ID}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	composed, err := i18n.Prepare(i18n.Source{Name: "source", Data: english}, i18n.Source{Name: "translation", Data: translation})
	if err != nil {
		t.Fatal(err)
	}
	alias, _ := composed.Lookup(source.Definition.ID, "iw")
	if !alias.TranslationExists || alias.Definition.Locale != "he" {
		t.Fatal(alias)
	}
}

const invalidField failure.Condition = "example.inventory.invalid_field"

func compiledCatalog(t testing.TB, catalog *i18n.Catalog) *i18n.Bindings {
	t.Helper()
	definitions, err := failure.PrepareDefinitions(failure.ModuleDefinition{
		ID: "example.inventory", Source: "consumer",
		Conditions: []failure.ConditionDefinition{{Condition: invalidField, Contract: "v1", Facts: "example.inventory:field"}},
		Contracts: []failure.FactContract{
			{ID: "example.inventory:field", Revision: "v1", Use: failure.ErrorFacts, Access: failure.PublicFacts, Fields: []failure.FactField{{Name: "field", Kind: failure.StringFact, Required: true}}},
			{ID: "example.inventory:stock", Revision: "v1", Use: failure.PresentationInput, Access: failure.OwnerFacts, Fields: []failure.FactField{{Name: "quantity", Kind: failure.Uint64Fact, Required: true}, {Name: "place", Kind: failure.StringFact, Required: true}}},
		},
		Children: []failure.ModuleDefinition{{ID: "example.inventory.sync", Source: "consumer.sync"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	grouped, err := catalog.WithModules(
		i18n.Module{ID: "example.inventory", Owners: []string{"example.inventory"}, Children: []i18n.Module{{ID: "example.inventory.sync"}}},
		i18n.Module{ID: "partner.presentation", Owners: []string{"partner.presentation"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	bindings, err := i18n.PrepareBindings(definitions, grouped,
		i18n.Binding{ID: "example.inventory:identity", Surface: "cli", Role: "generic", Message: "example.inventory:identity", MessageContract: "v1"},
		i18n.Binding{ID: "example.inventory:field", Condition: invalidField, ConditionContract: "v1", Input: "example.inventory:field", InputRevision: "v1", Surface: "cli", Role: "explanation", Message: "example.inventory:field", MessageContract: "v1", Arguments: []i18n.ArgumentBinding{{Argument: "field", Fact: "field"}}},
		i18n.Binding{ID: "example.inventory:custom", Condition: invalidField, ConditionContract: "v1", Input: "example.inventory:field", InputRevision: "v1", Surface: "report", Role: "explanation", Message: "partner.presentation:field", MessageContract: "v1", Arguments: []i18n.ArgumentBinding{{Argument: "field", Fact: "field"}}},
		i18n.Binding{ID: "example.inventory:stock", Input: "example.inventory:stock", InputRevision: "v1", Surface: "report", Role: "status", Message: "example.inventory:items", MessageContract: "v1", Arguments: []i18n.ArgumentBinding{{Argument: "place", Fact: "place"}}, CountFact: "quantity"},
	)
	if err != nil {
		t.Fatal(err)
	}
	return bindings
}

func TestIndependentCompiledBindings(t *testing.T) {
	catalog := load(t)
	bindings := compiledCatalog(t, catalog)
	all, err := bindings.Inspect()
	if err != nil || len(all) != 4 {
		t.Fatal(all, err)
	}
	all[0].Arguments[0].Fact = "changed"
	selected, err := bindings.Resolve("example.inventory:stock", "ru")
	if err != nil {
		t.Fatal(err)
	}
	result, err := selected.Render([]i18n.Argument{{Name: "quantity", Value: uint64(math.MaxUint64)}, {Name: "place", Value: "shelf"}})
	if err != nil || !strings.Contains(result.Text, "18446744073709551615") {
		t.Fatal(result, err)
	}
	metadata, _ := selected.Metadata()
	if metadata.Resource.Locale != "ru" || metadata.Resource.Owner != "example.inventory" {
		t.Fatal(metadata)
	}
	if _, err := selected.Render([]i18n.Argument{{Name: "quantity", Value: nil}, {Name: "place", Value: "shelf"}}); !errors.Is(err, i18n.ErrArguments) {
		t.Fatal("unknown became zero")
	}
	if _, err := bindings.Resolve("missing", "en"); !errors.Is(err, i18n.ErrBindingMissing) {
		t.Fatal(err)
	}
}

// This capability owns its required public field, not an optional diagnostic bag.
// Selected field names in this fixture are code-owned public identifiers.
type fieldFailure struct {
	core  *failure.Error
	field string
}

func newField(t *testing.T, field string, causes ...error) *fieldFailure {
	t.Helper()
	core, err := failure.New(invalidField, causes...)
	if err != nil {
		t.Fatal(err)
	}
	return &fieldFailure{core: core, field: strings.Clone(field)}
}
func (err *fieldFailure) Failure() *failure.Error {
	if err == nil {
		return nil
	}
	return err.core
}
func (err *fieldFailure) Error() string                     { return err.Failure().Error() }
func (err *fieldFailure) Unwrap() error                     { return err.Failure() }
func (err *fieldFailure) Format(state fmt.State, verb rune) { err.Failure().Format(state, verb) }
func (err *fieldFailure) LogValue() slog.Value              { return err.Failure().LogValue() }
func (*fieldFailure) MarshalJSON() ([]byte, error)          { return nil, failure.ErrSerialization }
func (*fieldFailure) UnmarshalJSON([]byte) error            { return failure.ErrSerialization }

// present deliberately inspects only the explicitly supplied error. Both identity
// and facts must belong to that object. A raw aggregate gets a host-owned generic
// resource, never a recursively guessed primary or foreign Error text.
func present(bindings *i18n.Bindings, selected error, locale, binding string) (i18n.Rendered, error) {
	id := "example.inventory:identity"
	var args []i18n.Argument
	if core, ok := failure.Inspect(selected); ok {
		if owned, ok := selected.(*fieldFailure); ok && core.Diagnostic().Condition == invalidField {
			id = binding
			args = []i18n.Argument{{Name: "field", Value: owned.field}}
		}
	}
	resource, err := bindings.Resolve(id, locale)
	if err != nil {
		return i18n.Rendered{}, err
	}
	return resource.Render(args)
}

func TestSameSelectedOccurrenceAndCustomization(t *testing.T) {
	catalog := compiledCatalog(t, load(t))
	inner := newField(t, "inner")
	outer := newField(t, "outer", inner)
	sibling := newField(t, "sibling")
	for _, locale := range []string{"en", "zh-CN", "ru", "ja"} {
		for _, test := range []struct {
			selected error
			field    string
		}{
			{outer, "outer"}, {inner, "inner"}, {sibling, "sibling"},
		} {
			before, _ := failure.Inspect(test.selected)
			result, err := present(catalog, test.selected, locale, "example.inventory:field")
			after, _ := failure.Inspect(test.selected)
			if err != nil || !strings.Contains(result.Text, test.field) || before != after || !errors.Is(test.selected, invalidField) {
				t.Fatal(result, err)
			}
			custom, err := present(catalog, test.selected, locale, "example.inventory:custom")
			if err != nil || !strings.Contains(custom.Text, test.field) || strings.Contains(custom.Text, "Invalid field:") {
				t.Fatal("customization", custom, err)
			}
		}
	}
	for _, raw := range []error{errors.Join(inner, sibling), errors.Join(sibling, inner), fmt.Errorf("wrapper: %w", outer), errors.New("PRIVATE-CANARY"), nil, (*fieldFailure)(nil)} {
		result, err := present(catalog, raw, "zh-CN", "example.inventory:field")
		if err != nil || result.Text != "example.inventory:identity" {
			t.Fatal("inferred primary", result, err)
		}
	}
	_, renderErr := present(catalog, outer, "en", "missing")
	retained := errors.Join(outer, renderErr)
	if !errors.Is(retained, inner) || !errors.Is(retained, invalidField) || !errors.Is(retained, i18n.ErrBindingMissing) || outer.field != "outer" {
		t.Fatal("presentation replaced occurrence")
	}
}

func TestIndependentRejections(t *testing.T) {
	catalog := load(t)
	selection, err := catalog.Resolve("example.inventory:items", "ru")
	if err != nil {
		t.Fatal(err)
	}
	count := uint64(21)
	if got, err := selection.Render([]i18n.Argument{{Name: "place", Value: int(1)}}, &count); !errors.Is(err, i18n.ErrArguments) || got.Text != "" {
		t.Fatal(got, err)
	}
	if _, err := selection.Render([]i18n.Argument{{Name: "place", Value: "shelf"}}, nil); !errors.Is(err, i18n.ErrArguments) {
		t.Fatal(err)
	}
	if got, err := catalog.Resolve("missing", "en"); err == nil {
		_, _ = got.Metadata()
		t.Fatal("absent accepted")
	}
	if _, err := catalog.Resolve("example.inventory:items", "en_US"); !errors.Is(err, i18n.ErrLocale) {
		t.Fatal(err)
	}
}

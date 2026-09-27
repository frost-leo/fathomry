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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/failure/v1"
)

func bindingFixture(t testing.TB) (failure.ModuleDefinition, Binding) {
	t.Helper()
	module := failure.ModuleDefinition{ID: "example.inventory", Source: "test",
		Conditions: []failure.ConditionDefinition{{Condition: "example.inventory.failed", Contract: "v1", Facts: "example.inventory:items"}},
		Contracts: []failure.FactContract{{ID: "example.inventory:items", Revision: "v1", Use: failure.ErrorFacts, Access: failure.PublicFacts,
			Fields: []failure.FactField{{Name: "place", Kind: failure.StringFact, Required: true}, {Name: "quantity", Kind: failure.Uint64Fact, Required: true, Unit: "items"}},
		}},
	}
	binding := Binding{ID: "example.inventory:failure", Condition: "example.inventory.failed", ConditionContract: "v1", Input: "example.inventory:items", InputRevision: "v1", Surface: "cli", Role: "explanation", Message: "example.inventory:items", MessageContract: "v1", Arguments: []ArgumentBinding{{Argument: "place", Fact: "place"}}, CountFact: "quantity"}
	return module, binding
}

func TestResourceModulesAreExplicitAndIndependent(t *testing.T) {
	plain := preparedFixture(t)
	module := Module{ID: "example.inventory", Children: []Module{{ID: "example.inventory.text", Owners: []string{"example.inventory"}}}}
	grouped, err := plain.WithModules(module)
	if err != nil {
		t.Fatal(err)
	}
	if modules, _ := plain.Modules(); len(modules) != 0 {
		t.Fatal("grouping mutated original")
	}
	modules, _ := grouped.Modules()
	if len(modules) != 2 || modules[0].Parent != "" || modules[1].Parent != "example.inventory" {
		t.Fatal(modules)
	}
	module.Children[0].Owners[0] = "mutated"
	modules[0].Children[0] = "mutated"
	modules[1].Owners[0] = "mutated"
	again, _ := grouped.Modules()
	if again[0].Children[0] != "example.inventory.text" || again[1].Owners[0] != "example.inventory" {
		t.Fatal("group alias")
	}
	if before, _ := plain.Inspect(); true {
		after, _ := grouped.Inspect()
		a, _ := json.Marshal(before)
		b, _ := json.Marshal(after)
		if string(a) != string(b) {
			t.Fatal("grouping changed resources")
		}
	}
	source := fixtures(t)[0]
	source.Data = []byte(strings.ReplaceAll(string(source.Data), "example.inventory", "EXAMPLE.Inventory"))
	upper, err := Prepare(source)
	if err != nil {
		t.Fatal(err)
	}
	upper, err = upper.WithModules(Module{ID: "example.inventory", Owners: []string{"EXAMPLE.Inventory"}})
	if err != nil {
		t.Fatal("grouped uppercase owner rejected", err)
	}
	if got, err := upper.Lookup("EXAMPLE.Inventory:identity", "en"); err != nil || !got.TranslationExists {
		t.Fatal(got, err)
	}
	for _, roots := range [][]Module{
		nil,
		{{ID: "example.inventory", Owners: []string{"example.inventory", "example.inventory"}}},
		{{ID: "example.inventory", Owners: []string{"EXAMPLE.inventory"}}},
		{{ID: "example.inventory", Owners: []string{"example.inventory"}}, {ID: "example.inventory.child"}},
		{{ID: "example.inventory", Owners: []string{"example.inventory"}, Children: []Module{{ID: "example.inventory.skip.child"}}}},
	} {
		if result, err := plain.WithModules(roots...); err == nil || result != nil {
			t.Fatal("invalid groups published", roots)
		}
	}
	cycle := []Module{{ID: "example.inventory", Owners: []string{"example.inventory"}}}
	cycle[0].Children = cycle
	if _, err := plain.WithModules(cycle...); err == nil {
		t.Fatal("cycle accepted")
	}
	if _, err := plain.WithModules(make([]Module, failure.MaxModules+1)...); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if _, _, err := grouped.Module(strings.Repeat("x", failure.MaxDefinitionIDBytes+1)); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	for _, catalog := range []*Catalog{nil, {}} {
		if _, err := catalog.WithModules(); !errors.Is(err, ErrCatalog) {
			t.Fatal(err)
		}
	}
}

func TestCompiledBindingsRenderAndIsolate(t *testing.T) {
	module, declaration := bindingFixture(t)
	definitions, err := failure.PrepareDefinitions(module)
	if err != nil {
		t.Fatal(err)
	}
	catalog := preparedFixture(t)
	bindings, err := PrepareBindings(definitions, catalog, declaration)
	if err != nil {
		t.Fatal(err)
	}
	expected, _ := json.Marshal(declaration)
	declaration.Arguments[0].Fact = "changed"
	inspect, _ := bindings.Inspect()
	inspect[0].Arguments[0].Argument = "changed"
	found, exists, err := bindings.Lookup("example.inventory:failure")
	actual, _ := json.Marshal(found)
	if err != nil || !exists || string(actual) != string(expected) {
		t.Fatal("binding alias", found, err)
	}
	if _, exists, err := bindings.Lookup("missing"); err != nil || exists {
		t.Fatal("false binding existence")
	}
	if _, err := bindings.Resolve("missing", "en"); !errors.Is(err, ErrBindingMissing) {
		t.Fatal(err)
	}
	var readers sync.WaitGroup
	for _, locale := range []string{"en", "zh-CN", "ru", "ar", "ja"} {
		readers.Go(func() {
			for _, count := range []uint64{0, 1, 2, 11, 21, 1<<53 + 1, math.MaxUint64} {
				selected, err := bindings.Resolve(found.ID, locale)
				if err != nil {
					t.Error(err)
					return
				}
				result, err := selected.Render([]Argument{{Name: "place", Value: "shelf"}, {Name: "quantity", Value: count}})
				direct, _ := catalog.Resolve("example.inventory:items", locale)
				reference, referenceErr := direct.Render([]Argument{{Name: "place", Value: "shelf"}}, &count)
				if err != nil || referenceErr != nil || result != reference {
					t.Error(result, err, reference)
				}
				metadata, _ := selected.Metadata()
				if locale == "ja" && (metadata.Resource.Locale != "en" || metadata.Fallback != UnsupportedLocale) {
					t.Error("false resource source", metadata)
				}
				metadata.Resource.Arguments[0].Name = "changed"
			}
		})
	}
	readers.Wait()
	selected, _ := bindings.Resolve(found.ID, "en")
	for _, facts := range [][]Argument{
		nil, {{Name: "place", Value: "shelf"}},
		{{Name: "place", Value: "shelf"}, {Name: "quantity", Value: int64(1)}},
		{{Name: "place", Value: "shelf"}, {Name: "quantity", Value: nil}},
		{{Name: "place", Value: "shelf"}, {Name: "quantity", Value: float64(1)}},
		{{Name: "place", Value: panicFormatter{}}, {Name: "quantity", Value: uint64(1)}},
		{{Name: "place", Value: "shelf"}, {Name: "place", Value: uint64(1)}},
		{{Name: "place", Value: "shelf"}, {Name: "quantity", Value: uint64(1)}, {Name: "count", Value: uint64(2)}},
	} {
		if result, err := selected.Render(facts); !errors.Is(err, ErrArguments) || result.Text != "" {
			t.Fatal("invalid facts accepted", result, err)
		}
	}
	if _, err := (BoundSelection{}).Render(nil); !errors.Is(err, ErrBinding) {
		t.Fatal(err)
	}
	for _, invalid := range []*Bindings{nil, {}} {
		if _, err := invalid.Inspect(); !errors.Is(err, ErrBinding) {
			t.Fatal(err)
		}
		if _, _, err := invalid.Lookup(""); !errors.Is(err, ErrBinding) {
			t.Fatal(err)
		}
		if _, err := invalid.Resolve("", ""); !errors.Is(err, ErrBinding) {
			t.Fatal(err)
		}
	}
}

type panicFormatter struct{}

func TestBindingInspectionEnvelope(t *testing.T) {
	parameters := map[string]any{}
	input := failure.FactContract{ID: "test:input", Revision: "v1", Use: failure.PresentationInput, Access: failure.OwnerFacts}
	base := Binding{Input: "test:input", InputRevision: "v1", Surface: strings.Repeat("s", 64), Role: strings.Repeat("r", 64), Message: "test:text", MessageContract: "v1"}
	pattern := ""
	for index := range MaxArguments {
		name := string(rune('a' + index))
		parameters[name] = map[string]any{"kind": "string", "meaning": "Data."}
		input.Fields = append(input.Fields, failure.FactField{Name: name, Kind: failure.StringFact, Required: true})
		base.Arguments = append(base.Arguments, ArgumentBinding{Argument: name, Fact: name})
		pattern += "{" + name + "}"
	}
	definitions, err := failure.PrepareDefinitions(failure.ModuleDefinition{ID: "test", Source: "test", Contracts: []failure.FactContract{input}})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := Prepare(sourceJSON(t, "envelope", document(message("text", pattern, parameters, false))))
	if err != nil {
		t.Fatal(err)
	}
	var declarations []Binding
	charged, inputBytes := 0, 0
	for index := range MaxBindings {
		item := base
		item.ID = "test:" + strings.Repeat("x", 100) + fmt.Sprint(index)
		size := len(item.ID) + len(item.Input) + len(item.InputRevision) + len(item.Surface) + len(item.Role) + len(item.Message) + len(item.MessageContract)
		for _, argument := range item.Arguments {
			size += len(argument.Argument) + len(argument.Fact)
		}
		charged += 512 + 128*len(item.Arguments) + 6*size
		inputBytes += size
		for _, field := range input.Fields {
			inputBytes += len(field.Name) + len(field.Kind)
		}
		declarations = append(declarations, item)
		if charged <= MaxBindingInspectionBytes {
			continue
		}
		if inputBytes > MaxBindingBytes {
			t.Fatal("witness hit wrong budget")
		}
		if result, err := PrepareBindings(definitions, catalog, declarations...); !errors.Is(err, ErrLimit) || result != nil {
			t.Fatal("inspection envelope not enforced", err)
		}
		valid := declarations[:len(declarations)-1]
		bindings, err := PrepareBindings(definitions, catalog, valid...)
		if err != nil {
			t.Fatal("last admissible prefix", err)
		}
		snapshot, err := bindings.Inspect()
		encoded, encodeErr := json.Marshal(snapshot)
		previousCharge := charged - (512 + 128*len(item.Arguments) + 6*size)
		if err != nil || encodeErr != nil || len(snapshot) != len(valid) || len(encoded) > previousCharge {
			t.Fatal("incomplete/unbounded binding snapshot")
		}
		return
	}
	t.Fatal("witness never reached inspection budget")
}

func TestCountFieldGrammar(t *testing.T) {
	for _, name := range []string{"/", "quantity/value", "0", "quantity"} {
		module, declaration := bindingFixture(t)
		module.Contracts[0].Fields[1].Name = name
		declaration.CountFact = name
		definitions, err := failure.PrepareDefinitions(module)
		if err != nil {
			t.Fatal("valid metadata fixture", err)
		}
		bindings, err := PrepareBindings(definitions, preparedFixture(t), declaration)
		if name != "quantity" {
			if bindings != nil || !errors.Is(err, ErrBinding) {
				t.Fatal("compiled unusable count name", name, err)
			}
		} else if err != nil {
			t.Fatal("valid count name rejected", err)
		}
	}
}

func TestBoundOrdinaryArgumentBudget(t *testing.T) {
	for _, number := range []int{4, 5} {
		parameters := map[string]any{}
		fields := []failure.FactField{{Name: "quantity", Kind: failure.Uint64Fact, Required: true}}
		binding := Binding{ID: "test:binding", Input: "test:input", InputRevision: "v1", Surface: "test", Role: "text", Message: "test:text", MessageContract: "v1", CountFact: "quantity"}
		var arguments []Argument
		pattern := "{count}"
		for index := range number {
			name := fmt.Sprintf("a%d", index)
			parameters[name] = map[string]any{"kind": "string", "meaning": "Bounded data."}
			fields = append(fields, failure.FactField{Name: name, Kind: failure.StringFact, Required: true})
			binding.Arguments = append(binding.Arguments, ArgumentBinding{Argument: name, Fact: name})
			arguments = append(arguments, Argument{Name: name, Value: strings.Repeat("x", MaxStringBytes)})
			pattern += "{" + name + "}"
		}
		catalog, err := Prepare(sourceJSON(t, "budget", document(message("text", pattern, parameters, true))))
		if err != nil {
			t.Fatal(err)
		}
		definitions, err := failure.PrepareDefinitions(failure.ModuleDefinition{ID: "test", Source: "test", Contracts: []failure.FactContract{{ID: "test:input", Revision: "v1", Use: failure.PresentationInput, Access: failure.OwnerFacts, Fields: fields}}})
		if err != nil {
			t.Fatal(err)
		}
		bindings, err := PrepareBindings(definitions, catalog, binding)
		if err != nil {
			t.Fatal(err)
		}
		direct, _ := catalog.Resolve("test:text", "en")
		bound, _ := bindings.Resolve(binding.ID, "en")
		for _, count := range []uint64{0, math.MaxUint64} {
			expected, directErr := direct.Render(arguments, &count)
			actual, boundErr := bound.Render(append(slices.Clone(arguments), Argument{Name: "quantity", Value: count}))
			if number == 4 {
				if directErr != nil || boundErr != nil || actual != expected {
					t.Fatal("count consumed ordinary argument budget", directErr, boundErr)
				}
			} else if !errors.Is(directErr, ErrLimit) || !errors.Is(boundErr, ErrLimit) || actual != (Rendered{}) {
				t.Fatal("ordinary budget bypassed", directErr, boundErr)
			}
		}
		// One input field can feed multiple ordinary arguments; the renderer must
		// charge every mapping, not only distinct input-field storage.
		for index := range binding.Arguments {
			binding.Arguments[index].Fact = "a0"
		}
		bindings, err = PrepareBindings(definitions, catalog, binding)
		if err != nil {
			t.Fatal(err)
		}
		bound, _ = bindings.Resolve(binding.ID, "en")
		result, err := bound.Render([]Argument{{Name: "a0", Value: strings.Repeat("x", MaxStringBytes)}, {Name: "quantity", Value: uint64(0)}})
		if number == 5 && (!errors.Is(err, ErrLimit) || result != (Rendered{})) {
			t.Fatal("repeated mappings bypassed budget")
		}
		if number == 4 && err != nil {
			t.Fatal("inclusive repeated mapping boundary", err)
		}
	}
}

func TestBoundScalarPresence(t *testing.T) {
	module := failure.ModuleDefinition{ID: "example.inventory", Source: "test", Contracts: []failure.FactContract{{
		ID: "example.inventory:scalars", Revision: "v1", Use: failure.PresentationInput, Access: failure.OwnerFacts,
		Fields: []failure.FactField{{Name: "s", Kind: failure.StringFact, Required: true}, {Name: "i", Kind: failure.Int64Fact, Required: true}, {Name: "u", Kind: failure.Uint64Fact, Required: true}, {Name: "b", Kind: failure.BoolFact, Required: true}},
	}}}
	definitions, err := failure.PrepareDefinitions(module)
	if err != nil {
		t.Fatal(err)
	}
	declaration := Binding{ID: "example.inventory:scalars", Input: "example.inventory:scalars", InputRevision: "v1", Surface: "test", Role: "text", Message: "example.inventory:scalars", MessageContract: "v1"}
	for _, name := range []string{"s", "i", "u", "b"} {
		declaration.Arguments = append(declaration.Arguments, ArgumentBinding{Argument: name, Fact: name})
	}
	bindings, err := PrepareBindings(definitions, preparedFixture(t), declaration)
	if err != nil {
		t.Fatal(err)
	}
	selection, err := bindings.Resolve(declaration.ID, "en")
	if err != nil {
		t.Fatal(err)
	}
	facts := []Argument{{Name: "s", Value: ""}, {Name: "i", Value: int64(0)}, {Name: "u", Value: uint64(0)}, {Name: "b", Value: false}}
	if result, err := selection.Render(facts); err != nil || result.Text != "{} 0 0 false 100%" {
		t.Fatal("present zero values", result, err)
	}
	type named uint64
	facts[2].Value = named(0)
	if _, err := selection.Render(facts); !errors.Is(err, ErrArguments) {
		t.Fatal("named scalar accepted", err)
	}
	facts[2].Value = nil
	if _, err := selection.Render(facts); !errors.Is(err, ErrArguments) {
		t.Fatal("unknown became zero", err)
	}
}

func FuzzBindingFacts(f *testing.F) {
	module, declaration := bindingFixture(f)
	definitions, err := failure.PrepareDefinitions(module)
	if err != nil {
		f.Fatal(err)
	}
	bindings, err := PrepareBindings(definitions, preparedFixture(f), declaration)
	if err != nil {
		f.Fatal(err)
	}
	f.Add("shelf", uint64(0), "ru", false)
	f.Add("", uint64(math.MaxUint64), "zh-CN", true)
	f.Fuzz(func(t *testing.T, place string, count uint64, locale string, wrong bool) {
		if len(place) > MaxStringBytes+1 || len(locale) > 129 {
			return
		}
		selected, err := bindings.Resolve(declaration.ID, locale)
		if err != nil {
			return
		}
		var quantity any = count
		if wrong {
			quantity = int64(count)
		}
		result, err := selected.Render([]Argument{{Name: "place", Value: place}, {Name: "quantity", Value: quantity}})
		if wrong && err == nil {
			t.Fatal("wrong scalar accepted")
		}
		if err != nil && result.Text != "" {
			t.Fatal("partial output")
		}
		if len(result.Text) > MaxOutputBytes {
			t.Fatal("oversized output")
		}
	})
}

func (panicFormatter) String() string         { panic("foreign stringer") }
func (panicFormatter) Format(fmt.State, rune) { panic("foreign formatter") }

func TestBindingDeclarationsRejectIncompatibility(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*failure.ModuleDefinition, *Binding)
	}{
		{"condition-absent", func(_ *failure.ModuleDefinition, b *Binding) { b.Condition = "example.inventory.missing" }},
		{"condition-revision", func(_ *failure.ModuleDefinition, b *Binding) { b.ConditionContract = "v2" }},
		{"input-revision", func(_ *failure.ModuleDefinition, b *Binding) { b.InputRevision = "v2" }},
		{"input-absent", func(_ *failure.ModuleDefinition, b *Binding) { b.Input = "example.inventory:missing" }},
		{"module-absent", func(_ *failure.ModuleDefinition, b *Binding) { b.ID = "other.module:binding" }},
		{"message-absent", func(_ *failure.ModuleDefinition, b *Binding) { b.Message = "example.inventory:missing" }},
		{"message-revision", func(_ *failure.ModuleDefinition, b *Binding) { b.MessageContract = "v2" }},
		{"missing-mapping", func(_ *failure.ModuleDefinition, b *Binding) { b.Arguments = nil }},
		{"wrong-name", func(_ *failure.ModuleDefinition, b *Binding) { b.Arguments[0].Argument = "wrong" }},
		{"wrong-field", func(_ *failure.ModuleDefinition, b *Binding) { b.Arguments[0].Fact = "quantity" }},
		{"wrong-kind", func(m *failure.ModuleDefinition, _ *Binding) { m.Contracts[0].Fields[0].Kind = failure.BoolFact }},
		{"optional-field", func(m *failure.ModuleDefinition, _ *Binding) { m.Contracts[0].Fields[0].Required = false }},
		{"unknown-field", func(m *failure.ModuleDefinition, _ *Binding) { m.Contracts[0].Fields[0].UnknownAllowed = true }},
		{"absent-count", func(_ *failure.ModuleDefinition, b *Binding) { b.CountFact = "" }},
		{"wrong-count", func(_ *failure.ModuleDefinition, b *Binding) { b.CountFact = "place" }},
		{"optional-count", func(m *failure.ModuleDefinition, _ *Binding) { m.Contracts[0].Fields[1].Required = false }},
		{"unknown-count", func(m *failure.ModuleDefinition, _ *Binding) { m.Contracts[0].Fields[1].UnknownAllowed = true }},
		{"count-noncardinal", func(_ *failure.ModuleDefinition, b *Binding) { b.Message = "example.inventory:field" }},
		{"extra-mapping", func(_ *failure.ModuleDefinition, b *Binding) { b.Arguments = append(b.Arguments, b.Arguments[0]) }},
		{"conditionless-error-input", func(_ *failure.ModuleDefinition, b *Binding) { b.Condition = ""; b.ConditionContract = "" }},
		{"no-role", func(_ *failure.ModuleDefinition, b *Binding) { b.Role = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			module, declaration := bindingFixture(t)
			test.change(&module, &declaration)
			definitions, err := failure.PrepareDefinitions(module)
			if err != nil {
				t.Fatal("invalid test fixture", err)
			}
			if bindings, err := PrepareBindings(definitions, preparedFixture(t), declaration); err == nil || bindings != nil {
				t.Fatal("invalid binding publication")
			}
		})
	}
	module, declaration := bindingFixture(t)
	module.Contracts[0].Fields[0].Kind = failure.EnumFact
	module.Contracts[0].Fields[0].Values = []string{"shelf", "bin"}
	definitions, _ := failure.PrepareDefinitions(module)
	bindings, err := PrepareBindings(definitions, preparedFixture(t), declaration)
	if err != nil {
		t.Fatal(err)
	}
	selected, _ := bindings.Resolve(declaration.ID, "en")
	if _, err := selected.Render([]Argument{{Name: "place", Value: "unknown"}, {Name: "quantity", Value: uint64(0)}}); !errors.Is(err, ErrArguments) {
		t.Fatal("unknown enum", err)
	}
	if _, err := selected.Render([]Argument{{Name: "place", Value: "shelf"}, {Name: "quantity", Value: uint64(0)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareBindings(definitions, preparedFixture(t), declaration, declaration); !errors.Is(err, ErrBinding) {
		t.Fatal("duplicate accepted")
	}
	if _, err := PrepareBindings(nil, preparedFixture(t)); !errors.Is(err, ErrBinding) {
		t.Fatal("nil definitions")
	}
}

func TestBindingAdmissionLimits(t *testing.T) {
	definitions, _ := failure.PrepareDefinitions(failure.ModuleDefinition{ID: "example.inventory", Source: "test"})
	catalog := preparedFixture(t)
	declarations := make([]Binding, MaxBindings)
	for index := range declarations {
		declarations[index] = Binding{ID: fmt.Sprintf("example.inventory:b%d", index), Surface: "cli", Role: "text", Message: "example.inventory:identity", MessageContract: "v1"}
	}
	if _, err := PrepareBindings(definitions, catalog, declarations...); err != nil {
		t.Fatal("inclusive binding count", err)
	}
	if _, err := PrepareBindings(definitions, catalog, append(declarations, Binding{})...); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	_, err := PrepareBindings(definitions, catalog, Binding{ID: strings.Repeat("x", MaxKeyBytes+1)})
	if !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	for index := range declarations {
		declarations[index].ID = "example.inventory:" + strings.Repeat("x", 110) + fmt.Sprint(index)
		declarations[index].Surface = strings.Repeat("s", 64)
		declarations[index].Role = strings.Repeat("r", 64)
	}
	if _, err := PrepareBindings(definitions, catalog, declarations...); err != nil {
		t.Fatal("bounded long literals", err)
	}
	module, declaration := bindingFixture(t)
	module.Contracts[0].Fields[0].Kind = failure.EnumFact
	module.Contracts[0].Fields[0].Values = make([]string, failure.MaxEnumValues)
	for index := range module.Contracts[0].Fields[0].Values {
		module.Contracts[0].Fields[0].Values[index] = fmt.Sprintf("v%02d", index) + strings.Repeat("x", 61)
	}
	definitions, _ = failure.PrepareDefinitions(module)
	for index := range declarations {
		declarations[index] = declaration
		declarations[index].ID = fmt.Sprintf("example.inventory:b%d", index)
		declarations[index].Arguments = slices.Clone(declaration.Arguments)
	}
	if _, err := PrepareBindings(definitions, catalog, declarations...); !errors.Is(err, ErrLimit) {
		t.Fatal("aggregate compiled fields not charged", err)
	}
}

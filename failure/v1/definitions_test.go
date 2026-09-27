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
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/failure/v1"
)

func atlasModule() failure.ModuleDefinition {

	return failure.ModuleDefinition{
		ID: "example.inventory", Source: "inventory.definitions",
		Conditions: []failure.ConditionDefinition{{Condition: "example.inventory.rejected", Contract: "v1", Facts: "example.inventory:request"}},
		Contracts: []failure.FactContract{{
			ID: "example.inventory:request", Revision: "v1", Use: failure.ErrorFacts, Access: failure.PublicFacts,
			Fields: []failure.FactField{{Name: "state", Kind: failure.EnumFact, Required: true, UnknownAllowed: true, Values: []string{"ready", "unknown"}}, {Name: "count", Kind: failure.Uint64Fact, Unit: "items"}},
		}},
		Children: []failure.ModuleDefinition{{ID: "example.inventory.sync", Source: "sync", Conditions: []failure.ConditionDefinition{{Condition: "example.inventory.sync.failed", Contract: "v1"}}}},
	}
}

func TestDefinitionCatalogOwnershipAndQueries(t *testing.T) {
	declaration := atlasModule()
	catalog, err := failure.PrepareDefinitions(declaration, failure.ModuleDefinition{ID: "example.inventoryx", Source: "empty"})
	if err != nil {
		t.Fatal(err)
	}
	modules, _ := catalog.Modules()
	definitions, _ := catalog.Inspect()
	contracts, _ := catalog.Contracts()
	expected, _ := json.Marshal([]any{modules, definitions, contracts})
	if len(modules) != 3 || modules[0].Parent != "" || modules[1].Parent != "example.inventory" || len(definitions) != 2 {
		t.Fatal(modules, definitions)
	}
	if _, exists, _ := catalog.Module("example"); exists {
		t.Fatal("invented parent")
	}
	direct, exists, err := catalog.Definitions("example.inventory", false)
	subtree, _, _ := catalog.Definitions("example.inventory", true)
	empty, existsEmpty, _ := catalog.Definitions("example.inventoryx", true)
	if err != nil || !exists || len(direct) != 1 || len(subtree) != 2 || !existsEmpty || len(empty) != 0 {
		t.Fatal(direct, subtree, empty, err)
	}
	declaration.Children[0].Source = "changed"
	declaration.Conditions[0].Contract = "changed"
	declaration.Contracts[0].Fields[0].Values[0] = "changed"
	for _, mutate := range []func(){
		func() { modules[0].Children[0] = "changed" },
		func() { modules[0].Conditions[0] = "changed" },
		func() { modules[0].Contracts[0] = "changed" },
		func() { contracts[0].Fields[0].Name = "changed" },
		func() { contracts[0].Fields[1].Values[0] = "changed" },
		func() { definitions[0].Contract = "changed" },
	} {
		mutate()
		freshModules, _ := catalog.Modules()
		freshDefinitions, _ := catalog.Inspect()
		freshContracts, _ := catalog.Contracts()
		actual, _ := json.Marshal([]any{freshModules, freshDefinitions, freshContracts})
		if string(actual) != string(expected) {
			t.Fatal("caller alias", string(actual))
		}
	}
	var readers sync.WaitGroup
	for range 8 {
		readers.Go(func() {
			for range 50 {
				module, ok, err := catalog.Module("example.inventory")
				contract, found, _ := catalog.Contract("example.inventory:request")
				if err != nil || !ok || !found || module.Children[0] != "example.inventory.sync" || contract.Fields[1].Values[0] != "ready" {
					t.Error("shared catalog changed")
				}
				module.Children[0] = "local"
				contract.Fields[1].Values[0] = "local"
			}
		})
	}
	readers.Wait()
	for _, id := range []failure.Condition{"external.owner.failed", "", "INVALID"} {
		_, exists, err := catalog.Lookup(id)
		if err != nil || exists {
			t.Fatal("false exact existence")
		}
	}
	for range 2 {
		current, err := failure.New("external.owner.failed")
		if err != nil || !errors.Is(current, failure.Condition("external.owner.failed")) {
			t.Fatal("registration became mandatory")
		}
		if inspected, ok := failure.Inspect(current); !ok || inspected != current {
			t.Fatal("inspection changed")
		}
		_, _ = failure.PrepareDefinitions(declaration, declaration)
	}
	if _, exists, _ := catalog.Lookup("external.owner.failed"); exists {
		t.Fatal("runtime discovery")
	}
	emptyCatalog, err := failure.PrepareDefinitions()
	if modules, queryErr := emptyCatalog.Modules(); err != nil || queryErr != nil || len(modules) != 0 {
		t.Fatal("empty preparation invalid")
	}
	for _, invalid := range []*failure.DefinitionCatalog{nil, {}} {
		if _, err := invalid.Modules(); !errors.Is(err, failure.ErrDefinitionCatalog) {
			t.Fatal(err)
		}
		if _, _, err := invalid.Lookup(""); !errors.Is(err, failure.ErrDefinitionCatalog) {
			t.Fatal(err)
		}
		if _, _, err := invalid.Contract(""); !errors.Is(err, failure.ErrDefinitionCatalog) {
			t.Fatal(err)
		}
	}
}

func TestDefinitionCatalogRejectsInvalidForest(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*failure.ModuleDefinition)
	}{
		{"duplicate-child", func(m *failure.ModuleDefinition) { m.Children = append(m.Children, m.Children[0]) }},
		{"skipped-parent", func(m *failure.ModuleDefinition) { m.Children[0].ID = "example.inventory.missing.sync" }},
		{"prefix-lookalike", func(m *failure.ModuleDefinition) { m.Children[0].ID = "example.inventoryx.sync" }},
		{"wrong-condition-owner", func(m *failure.ModuleDefinition) { m.Conditions[0].Condition = "example.inventory.sync.rejected" }},
		{"duplicate-condition", func(m *failure.ModuleDefinition) { m.Conditions = append(m.Conditions, m.Conditions[0]) }},
		{"duplicate-contract", func(m *failure.ModuleDefinition) { m.Contracts = append(m.Contracts, m.Contracts[0]) }},
		{"duplicate-field", func(m *failure.ModuleDefinition) {
			m.Contracts[0].Fields = append(m.Contracts[0].Fields, m.Contracts[0].Fields[0])
		}},
		{"duplicate-enum", func(m *failure.ModuleDefinition) { m.Contracts[0].Fields[0].Values = []string{"ready", "ready"} }},
		{"missing-enum", func(m *failure.ModuleDefinition) { m.Contracts[0].Fields[0].Values = nil }},
		{"wrong-kind", func(m *failure.ModuleDefinition) { m.Contracts[0].Fields[0].Kind = "object" }},
		{"scalar-enum", func(m *failure.ModuleDefinition) { m.Contracts[0].Fields[0].Kind = failure.StringFact }},
		{"invalid-access", func(m *failure.ModuleDefinition) { m.Contracts[0].Access = "implicit" }},
		{"wrong-fact-use", func(m *failure.ModuleDefinition) { m.Contracts[0].Use = failure.PresentationInput }},
		{"missing-contract", func(m *failure.ModuleDefinition) { m.Conditions[0].Facts = "example.inventory:absent" }},
		{"foreign-contract", func(m *failure.ModuleDefinition) { m.Contracts[0].ID = "other:request" }},
		{"invalid-source", func(m *failure.ModuleDefinition) { m.Source = "private\nsource" }},
		{"invalid-name", func(m *failure.ModuleDefinition) { m.Contracts[0].Fields[0].Name = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			module := atlasModule()
			test.change(&module)
			if result, err := failure.PrepareDefinitions(module); err == nil || result != nil {
				t.Fatal("partial or invalid publication")
			}
		})
	}
	root := atlasModule()
	for _, roots := range [][]failure.ModuleDefinition{{root, root}, {root, root.Children[0]}, {root.Children[0], root}} {
		if result, err := failure.PrepareDefinitions(roots...); err == nil || result != nil {
			t.Fatal("overlapping roots accepted")
		}
	}
	cycle := []failure.ModuleDefinition{{ID: "example.cycle", Source: "cycle"}}
	cycle[0].Children = cycle
	if _, err := failure.PrepareDefinitions(cycle...); err == nil {
		t.Fatal("cyclic declaration accepted")
	}
	two := []failure.ModuleDefinition{{ID: "example.cycle.child", Source: "cycle", Children: cycle}}
	cycle[0].Children = two
	if _, err := failure.PrepareDefinitions(cycle...); err == nil {
		t.Fatal("two-node cycle accepted")
	}
	reordered := atlasModule()
	reordered.Contracts[0].Fields[0], reordered.Contracts[0].Fields[1] = reordered.Contracts[0].Fields[1], reordered.Contracts[0].Fields[0]
	first, _ := failure.PrepareDefinitions(atlasModule())
	second, _ := failure.PrepareDefinitions(reordered)
	a, _ := first.Contracts()
	b, _ := second.Contracts()
	if !reflect.DeepEqual(a, b) {
		t.Fatal("declaration order changed inspection")
	}
}

func declarationBytes(module failure.ModuleDefinition, parent string) int {
	total := len(module.ID) + len(module.Source) + len(parent)
	for _, condition := range module.Conditions {
		total += len(condition.Condition) + len(condition.Contract) + len(condition.Facts)
	}
	for _, contract := range module.Contracts {
		total += len(contract.ID) + len(contract.Revision) + len(contract.Use) + len(contract.Access)
		for _, field := range contract.Fields {
			total += len(field.Name) + len(field.Kind) + len(field.Unit)
			for _, value := range field.Values {
				total += len(value)
			}
		}
	}
	for _, child := range module.Children {
		total += declarationBytes(child, module.ID)
	}
	return total
}

func TestDefinitionAdmissionBounds(t *testing.T) {
	roots := make([]failure.ModuleDefinition, failure.MaxModules)
	for index := range roots {
		roots[index] = failure.ModuleDefinition{ID: fmt.Sprintf("example.m%d", index), Source: "test"}
	}
	if _, err := failure.PrepareDefinitions(roots...); err != nil {
		t.Fatal("module boundary", err)
	}
	tooMany := append(roots, failure.ModuleDefinition{})
	if allocations := testing.AllocsPerRun(20, func() { _, _ = failure.PrepareDefinitions(tooMany...) }); allocations != 0 {
		t.Fatal("oversized input allocated", allocations)
	}
	if _, err := failure.PrepareDefinitions(tooMany...); !errors.Is(err, failure.ErrDefinitionLimit) {
		t.Fatal(err)
	}
	chain := func(depth int) failure.ModuleDefinition {
		root := failure.ModuleDefinition{ID: "a", Source: "test"}
		current := &root
		for range depth - 1 {
			current.Children = []failure.ModuleDefinition{{ID: current.ID + ".x", Source: "test"}}
			current = &current.Children[0]
		}
		return root
	}
	if _, err := failure.PrepareDefinitions(chain(failure.MaxModuleDepth)); err != nil {
		t.Fatal("depth boundary", err)
	}
	if _, err := failure.PrepareDefinitions(chain(failure.MaxModuleDepth + 1)); !errors.Is(err, failure.ErrDefinitionLimit) {
		t.Fatal(err)
	}
	module := failure.ModuleDefinition{ID: "example.inventory", Source: "test"}
	for index := range failure.MaxDefinitions {
		module.Conditions = append(module.Conditions, failure.ConditionDefinition{Condition: failure.Condition(fmt.Sprintf("example.inventory.f%d", index)), Contract: "v1"})
	}
	if _, err := failure.PrepareDefinitions(module); err != nil {
		t.Fatal("condition boundary", err)
	}
	module.Conditions = append(module.Conditions, failure.ConditionDefinition{})
	if _, err := failure.PrepareDefinitions(module); !errors.Is(err, failure.ErrDefinitionLimit) {
		t.Fatal(err)
	}
	module = failure.ModuleDefinition{ID: "example.inventory", Source: "test"}
	for index := range failure.MaxFactContracts {
		module.Contracts = append(module.Contracts, failure.FactContract{ID: fmt.Sprintf("example.inventory:c%d", index), Revision: "v1", Use: failure.PresentationInput, Access: failure.OwnerFacts})
	}
	if _, err := failure.PrepareDefinitions(module); err != nil {
		t.Fatal("contract boundary", err)
	}
	module.Contracts = append(module.Contracts, failure.FactContract{})
	if _, err := failure.PrepareDefinitions(module); !errors.Is(err, failure.ErrDefinitionLimit) {
		t.Fatal(err)
	}
	for _, kind := range []string{"fields", "enums"} {
		module := atlasModule()
		module.Conditions = nil
		contract := &module.Contracts[0]
		if kind == "fields" {
			contract.Fields = nil
			for index := range failure.MaxFactFields {
				contract.Fields = append(contract.Fields, failure.FactField{Name: fmt.Sprintf("f%d", index), Kind: failure.StringFact})
			}
		} else {
			contract.Fields = contract.Fields[:1]
			contract.Fields[0].Values = nil
			for index := range failure.MaxEnumValues {
				contract.Fields[0].Values = append(contract.Fields[0].Values, fmt.Sprintf("v%d", index))
			}
		}
		if _, err := failure.PrepareDefinitions(module); err != nil {
			t.Fatal(kind, err)
		}
		if kind == "fields" {
			contract.Fields = append(contract.Fields, failure.FactField{})
		} else {
			contract.Fields[0].Values = append(contract.Fields[0].Values, "extra")
		}
		if _, err := failure.PrepareDefinitions(module); !errors.Is(err, failure.ErrDefinitionLimit) {
			t.Fatal(kind, err)
		}
	}
	module = failure.ModuleDefinition{ID: "example.inventory", Source: "test"}
	for index := range 32 {
		contract := failure.FactContract{ID: fmt.Sprintf("example.inventory:c%d", index), Revision: "v1", Use: failure.PresentationInput, Access: failure.OwnerFacts}
		for field := range failure.MaxFactFields {
			item := failure.FactField{Name: fmt.Sprintf("f%d", field), Kind: failure.EnumFact}
			for value := range failure.MaxEnumValues {
				item.Values = append(item.Values, fmt.Sprintf("v%02d", value))
			}
			contract.Fields = append(contract.Fields, item)
		}
		module.Contracts = append(module.Contracts, contract)
	}
	remaining := failure.MaxDefinitionBytes - declarationBytes(module, "")
	for ci := range module.Contracts {
		for fi := range module.Contracts[ci].Fields {
			for vi := range module.Contracts[ci].Fields[fi].Values {
				value := &module.Contracts[ci].Fields[fi].Values[vi]
				add := min(64-len(*value), remaining)
				*value += strings.Repeat("x", add)
				remaining -= add
			}
		}
	}
	if remaining != 0 || declarationBytes(module, "") != failure.MaxDefinitionBytes {
		t.Fatal("invalid aggregate boundary witness", remaining)
	}
	if _, err := failure.PrepareDefinitions(module); err != nil {
		t.Fatal("aggregate boundary", err)
	}
	module.Source += "x"
	if _, err := failure.PrepareDefinitions(module); !errors.Is(err, failure.ErrDefinitionLimit) {
		t.Fatal("aggregate overflow", err)
	}
}

func FuzzDefinitionPreparation(f *testing.F) {
	f.Add("example.module", "failed", uint8(2))
	f.Add("BAD\nnamespace", "", uint8(255))
	f.Fuzz(func(t *testing.T, id, name string, depth uint8) {
		if len(id) > 256 || len(name) > 256 {
			return
		}
		root := failure.ModuleDefinition{ID: id, Source: "fuzz", Conditions: []failure.ConditionDefinition{{Condition: failure.Condition(id + "." + name), Contract: "v1"}}}
		current := &root
		for range int(depth) % 12 {
			current.Children = []failure.ModuleDefinition{{ID: current.ID + ".child", Source: "fuzz"}}
			current = &current.Children[0]
		}
		catalog, err := failure.PrepareDefinitions(root)
		if err != nil {
			if catalog != nil {
				t.Fatal("partial publication")
			}
			return
		}
		modules, err := catalog.Modules()
		if err != nil || len(modules) > failure.MaxModules {
			t.Fatal("invalid accepted forest")
		}
		for _, module := range modules {
			if _, exists, err := catalog.Module(module.ID); err != nil || !exists {
				t.Fatal("missing own declaration")
			}
		}
	})
}

func TestDefinitionInspectionEnvelope(t *testing.T) {
	module := failure.ModuleDefinition{ID: "example.inventory", Source: "test"}
	charge := func(module failure.ModuleDefinition) int {
		size := 256 + 6*declarationBytes(module, "")
		for _, contract := range module.Contracts {
			size += 256
			for _, field := range contract.Fields {
				size += 192 + 32*len(field.Values)
			}
		}
		return size
	}
	for index := range failure.MaxFactContracts {
		contract := failure.FactContract{ID: fmt.Sprintf("example.inventory:c%d", index), Revision: "v1", Use: failure.PresentationInput, Access: failure.OwnerFacts}
		for field := range failure.MaxFactFields {
			item := failure.FactField{Name: fmt.Sprintf("f%02d", field), Kind: failure.EnumFact}
			for value := range failure.MaxEnumValues {
				item.Values = append(item.Values, fmt.Sprintf("v%02d", value))
			}
			contract.Fields = append(contract.Fields, item)
		}
		module.Contracts = append(module.Contracts, contract)
		if charge(module) <= failure.MaxDefinitionInspectionBytes {
			continue
		}
		if declarationBytes(module, "") > failure.MaxDefinitionBytes {
			t.Fatal("witness hit wrong budget")
		}
		if result, err := failure.PrepareDefinitions(module); !errors.Is(err, failure.ErrDefinitionLimit) || result != nil {
			t.Fatal("inspection envelope not enforced", err)
		}
		module.Contracts = module.Contracts[:len(module.Contracts)-1]
		catalog, err := failure.PrepareDefinitions(module)
		if err != nil {
			t.Fatal("last admissible prefix", err)
		}
		modules, _ := catalog.Modules()
		definitions, _ := catalog.Inspect()
		contracts, _ := catalog.Contracts()
		encoded, err := json.Marshal([]any{modules, definitions, contracts})
		if err != nil || len(contracts) != len(module.Contracts) || len(encoded) > charge(module) {
			t.Fatal("incomplete/unbounded snapshot", len(encoded), charge(module), err)
		}
		return
	}
	t.Fatal("witness never reached inspection budget")
}

func BenchmarkDefinitionOversize(b *testing.B) {

	input := make([]failure.ModuleDefinition, failure.MaxModules+1)
	b.ReportAllocs()
	for b.Loop() {
		_, _ = failure.PrepareDefinitions(input...)
	}
}

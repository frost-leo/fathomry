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

package adapters

import (
	"strings"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

// Module is a static vocabulary contribution, not a runtime factory or registry.
// All fields are borrowed during Catalogs; do not mutate them concurrently.
// Each declaration ID is explicit and unique, even for otherwise equal values.
type Module struct {
	Definition failure.ModuleDefinition
	Messages   []i18n.Source
	Grouping   i18n.Module
	Bindings   []i18n.Binding
}

// CatalogSet contains the existing immutable catalogs, not another lookup engine.
// Returned query results belong to the caller. No source or locale is captured.
type CatalogSet struct {
	Errors   *failure.DefinitionCatalog
	Messages *i18n.Catalog
	Bindings *i18n.Bindings
}

// Catalogs registers selected contributions in a new, explicit immutable set.
// Framework can reuse this bounded assembly with its own static declarations.
// Ancestor/child contributions are assembled before the existing validators run;
// duplicate explicit declarations, including reserved built-ins, always reject.
func Catalogs(modules ...Module) (CatalogSet, error) {
	if len(modules) > failure.MaxModules-2 {
		return CatalogSet{}, reject(ErrLimit)
	}
	all := make([]Module, 0, len(modules)+2)
	all = append(all, ModuleContribution(), Module{Definition: i18n.Definitions(), Grouping: i18n.Module{ID: "fathomry.i18n"}})
	all = append(all, modules...)
	sources, bindings, total := 0, 0, 0
	definitionNodes, groupingNodes := len(all), len(all)
	byID := make(map[string]int, len(all))
	for index, module := range all {
		if len(module.Definition.ID) > failure.MaxDefinitionIDBytes-2 {
			return CatalogSet{}, reject(ErrLimit)
		}
		if !failure.Condition(module.Definition.ID + ".x").Valid() {
			return CatalogSet{}, reject(ErrCatalog)
		}
		if module.Definition.ID != module.Grouping.ID {
			return CatalogSet{}, reject(ErrCatalog)
		}
		if _, exists := byID[module.Definition.ID]; exists {
			return CatalogSet{}, reject(ErrCatalog)
		}
		byID[module.Definition.ID] = index
		if len(module.Messages) > i18n.MaxSources-sources || len(module.Bindings) > i18n.MaxBindings-bindings ||
			len(module.Definition.Children) > failure.MaxModules-definitionNodes || len(module.Grouping.Children) > failure.MaxModules-groupingNodes {
			return CatalogSet{}, reject(ErrLimit)
		}
		definitionNodes += len(module.Definition.Children)
		groupingNodes += len(module.Grouping.Children)
		sources += len(module.Messages)
		bindings += len(module.Bindings)
		for _, input := range module.Messages {
			if len(input.Data) > i18n.MaxDocumentBytes || len(input.Data) > i18n.MaxInputBytes-total {
				return CatalogSet{}, reject(ErrLimit)
			}
			total += len(input.Data)
		}
	}
	children := make([][]int, len(all))
	var roots []int
	for index, module := range all {
		id := module.Definition.ID
		parent := ""
		if point := strings.LastIndexByte(id, '.'); point >= 0 {
			parent = id[:point]
		}
		if parentIndex, exists := byID[parent]; exists {
			children[parentIndex] = append(children[parentIndex], index)
		} else {
			roots = append(roots, index)
		}
	}
	var build func(int, int) (failure.ModuleDefinition, i18n.Module, error)
	build = func(index, depth int) (failure.ModuleDefinition, i18n.Module, error) {
		if depth > failure.MaxModuleDepth {
			return failure.ModuleDefinition{}, i18n.Module{}, reject(ErrLimit)
		}
		definition, group := all[index].Definition, all[index].Grouping
		if len(children[index]) > failure.MaxModules-len(definition.Children) || len(children[index]) > failure.MaxModules-len(group.Children) {
			return failure.ModuleDefinition{}, i18n.Module{}, reject(ErrLimit)
		}
		// Only bounded top-level slices are copied here; nested validation/traversal
		// remains with failure/i18n, including cyclic caller-supplied declarations.
		if len(children[index]) > 0 {
			definition.Children = append([]failure.ModuleDefinition(nil), definition.Children...)
			group.Children = append([]i18n.Module(nil), group.Children...)
		}
		for _, child := range children[index] {
			nested, grouped, err := build(child, depth+1)
			if err != nil {
				return definition, group, err
			}
			definition.Children = append(definition.Children, nested)
			group.Children = append(group.Children, grouped)
		}
		return definition, group, nil
	}
	var definitions []failure.ModuleDefinition
	var groups []i18n.Module
	for _, index := range roots {
		definition, group, err := build(index, 1)
		if err != nil {
			return CatalogSet{}, err
		}
		definitions = append(definitions, definition)
		groups = append(groups, group)
	}
	atlas, err := failure.PrepareDefinitions(definitions...)
	if err != nil {
		if err == failure.ErrDefinitionLimit {
			return CatalogSet{}, reject(ErrLimit, err)
		}
		return CatalogSet{}, reject(ErrCatalog, err)
	}
	inputs := make([]i18n.Source, 0, sources)
	associations := make([]i18n.Binding, 0, bindings)
	for _, module := range all {
		inputs = append(inputs, module.Messages...)
		associations = append(associations, module.Bindings...)
	}
	messages, err := i18n.Prepare(inputs...)
	if err != nil {
		return CatalogSet{}, err
	}
	messages, err = messages.WithModules(groups...)
	if err != nil {
		return CatalogSet{}, err
	}
	prepared, err := i18n.PrepareBindings(atlas, messages, associations...)
	if err != nil {
		return CatalogSet{}, err
	}
	return CatalogSet{Errors: atlas, Messages: messages, Bindings: prepared}, nil
}
func reject(condition failure.Condition, causes ...error) error {
	value, err := failure.New(condition, causes...)
	if err != nil {
		panic(err)
	}
	return value
}

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
	"slices"
	"strings"

	"github.com/frost-leo/fathomry/failure/v1"
)

// Module explicitly groups exact resource owners under a semantic namespace.
// ID follows failure module namespace rules; Owners retain the existing resource
// grammar and case, and need not equal ID. Children are immediate submodules.
// Grouping neither changes message identity nor introduces fallback/overrides.
type Module struct {
	ID       string
	Owners   []string
	Children []Module
}

// ModuleInfo is an owned, flat grouping snapshot. Empty Parent denotes a root.
// Owners and Children are exact, sorted direct members.
type ModuleInfo struct {
	ID       string
	Parent   string
	Owners   []string
	Children []string
}

// WithModules returns another immutable handle sharing prepared resources, with
// explicit complete module membership. Every resource owner must occur exactly
// once, including owners with identical-looking prefixes or differing case.
// Plain Prepare requires no grouping or error definitions. Empty/group-only
// modules are allowed; namespace registration is not proof of authority.
// Inputs are borrowed only during this call and must not change concurrently.
// Module count/depth/ID bounds are failure.MaxModules/MaxModuleDepth/
// MaxDefinitionIDBytes; owner bounds are the existing MaxSources/128-byte rules.
func (catalog *Catalog) WithModules(roots ...Module) (*Catalog, error) {
	if !catalog.valid() {
		return nil, reject(ErrCatalog)
	}
	if len(roots) > failure.MaxModules {
		return nil, reject(ErrLimit)
	}
	owners := make(map[string]bool)
	for _, entry := range catalog.ordered {
		owners[entry.definition.Owner] = false
	}
	groups := make(map[string][]string)
	seen := make(map[string]bool)
	var convert func(Module, int) (failure.ModuleDefinition, error)
	convert = func(module Module, depth int) (failure.ModuleDefinition, error) {
		if depth > failure.MaxModuleDepth || len(seen) >= failure.MaxModules ||
			len(module.Children) > failure.MaxModules-len(seen) || len(module.Owners) > MaxSources ||
			len(module.ID) > failure.MaxDefinitionIDBytes-2 {
			return failure.ModuleDefinition{}, reject(ErrLimit)
		}
		if !failure.Condition(module.ID+".x").Valid() || seen[module.ID] {
			return failure.ModuleDefinition{}, reject(ErrCatalog)
		}
		seen[module.ID] = true
		for _, owner := range module.Owners {
			if len(owner) > 128 {
				return failure.ModuleDefinition{}, reject(ErrLimit)
			}
			used, exists := owners[owner]
			if !exists || used {
				return failure.ModuleDefinition{}, reject(ErrCatalog)
			}
			owners[owner] = true
		}
		groups[module.ID] = module.Owners
		result := failure.ModuleDefinition{ID: module.ID, Source: "i18n"}
		for _, child := range module.Children {
			converted, err := convert(child, depth+1)
			if err != nil {
				return failure.ModuleDefinition{}, err
			}
			result.Children = append(result.Children, converted)
		}
		return result, nil
	}
	var declarations []failure.ModuleDefinition
	for _, root := range roots {
		converted, err := convert(root, 1)
		if err != nil {
			return nil, err
		}
		declarations = append(declarations, converted)
	}
	for _, used := range owners {
		if !used {
			return nil, reject(ErrCatalog)
		}
	}
	topology, err := failure.PrepareDefinitions(declarations...)
	if err != nil {
		return nil, definitionError(err, ErrCatalog)
	}
	modules, _ := topology.Modules()
	result := *catalog
	result.modules = make(map[string]ModuleInfo, len(modules))
	for _, module := range modules {
		group := ModuleInfo{ID: module.ID, Parent: module.Parent, Children: module.Children}
		for _, owner := range groups[module.ID] {
			group.Owners = append(group.Owners, strings.Clone(owner))
		}
		slices.Sort(group.Owners)
		result.modules[group.ID] = group
	}
	return &result, nil
}

func definitionError(err error, condition failure.Condition) error {
	if err == failure.ErrDefinitionLimit {
		return reject(ErrLimit)
	}
	return reject(condition)
}

// Modules returns complete grouping ordered by ID. An ungrouped valid catalog
// returns an empty list; its resources remain fully available through Inspect.
func (catalog *Catalog) Modules() ([]ModuleInfo, error) {
	if !catalog.valid() {
		return nil, reject(ErrCatalog)
	}
	keys := make([]string, 0, len(catalog.modules))
	for id := range catalog.modules {
		keys = append(keys, id)
	}
	slices.Sort(keys)
	result := make([]ModuleInfo, 0, len(keys))
	for _, id := range keys {
		module, _, _ := catalog.Module(id)
		result = append(result, module)
	}
	return result, nil
}

// Module looks up an exact group; bounded unknown IDs report absence.
func (catalog *Catalog) Module(id string) (ModuleInfo, bool, error) {
	if !catalog.valid() {
		return ModuleInfo{}, false, reject(ErrCatalog)
	}
	if len(id) > failure.MaxDefinitionIDBytes {
		return ModuleInfo{}, false, reject(ErrLimit)
	}
	module, exists := catalog.modules[id]
	module.Owners = slices.Clone(module.Owners)
	module.Children = slices.Clone(module.Children)
	return module, exists, nil
}

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

package failure

import (
	"cmp"
	"slices"
	"strings"
)

// Component describes a declared owner and its exact sorted codes. It contains no
// runtime instances or native SDK handles. Codes belongs to the returned snapshot.
type Component struct {
	Domain   Domain   `json:"domain"`
	Facility Facility `json:"facility"`
	Module   string   `json:"module"`
	Name     string   `json:"name"`
	Codes    []Code   `json:"codes"`
}

type componentKey struct{ module, component string }

// Catalog owns immutable explicit definitions. Copies share immutable state;
// callers must not overwrite shared handles. An explicitly prepared empty catalog
// is valid. Native errors, settings, locales and constructors are never registered.
type Catalog struct {
	codes       map[Code]Definition
	identifiers map[Identifier]Code
	ordered     []Code
}

// Prepare validates the complete set before publishing or cloning. Duplicate codes
// or identifiers reject even when declarations are otherwise identical. Each
// facility must identify exactly one module/component, and vice versa, including
// extension declarations. There is no override order or automatic remapping.
// Inputs must not be changed concurrently during this call.
func Prepare(definitions ...Definition) (*Catalog, error) {
	if len(definitions) > MaxDefinitions {
		return nil, reject(ErrLimit)
	}
	codes := make(map[Code]bool, len(definitions))
	identifiers := make(map[Identifier]bool, len(definitions))
	facilities := make(map[Facility]componentKey)
	owners := make(map[componentKey]Facility)
	total := 0
	for _, definition := range definitions {
		if problem := validateDefinition(definition); problem != 0 {
			return nil, reject(problem)
		}
		if codes[definition.Code] || identifiers[definition.Identifier] {
			return nil, reject(ErrDefinition)
		}
		facility := definition.Code.Facility()
		owner := componentKey{definition.Module, definition.Component}
		if existing, found := facilities[facility]; found && existing != owner {
			return nil, reject(ErrDefinition)
		}
		if existing, found := owners[owner]; found && existing != facility {
			return nil, reject(ErrDefinition)
		}
		cost := definitionBytes(definition)
		if cost > MaxCatalogBytes-total {
			return nil, reject(ErrLimit)
		}
		total += cost
		codes[definition.Code], identifiers[definition.Identifier] = true, true
		facilities[facility], owners[owner] = owner, facility
	}
	result := &Catalog{codes: make(map[Code]Definition, len(definitions)),
		identifiers: make(map[Identifier]Code, len(definitions))}
	for _, definition := range definitions {
		owned := copyDefinition(definition)
		result.codes[owned.Code] = owned
		result.identifiers[owned.Identifier] = owned.Code
		result.ordered = append(result.ordered, owned.Code)
	}
	slices.Sort(result.ordered)
	return result, nil
}

func (catalog *Catalog) valid() bool {
	return catalog != nil && catalog.codes != nil && catalog.identifiers != nil
}

// Lookup performs exact numeric lookup. An unknown valid code reports false;
// invalid input or an unprepared catalog returns an error.
func (catalog *Catalog) Lookup(code Code) (Definition, bool, error) {
	if !catalog.valid() {
		return Definition{}, false, reject(ErrCatalog)
	}
	if !code.Valid() {
		return Definition{}, false, reject(ErrCode)
	}
	definition, found := catalog.codes[code]
	return definition, found, nil
}

// LookupIdentifier performs exact symbolic lookup; it does not parse code strings.
func (catalog *Catalog) LookupIdentifier(identifier Identifier) (Definition, bool, error) {
	if !catalog.valid() {
		return Definition{}, false, reject(ErrCatalog)
	}
	if !identifier.Valid() {
		return Definition{}, false, reject(ErrDefinition)
	}
	code, found := catalog.identifiers[identifier]
	if !found {
		return Definition{}, false, nil
	}
	return catalog.codes[code], true, nil
}

// Inspect returns all definitions in numeric-code order with caller-owned storage.
func (catalog *Catalog) Inspect() ([]Definition, error) {
	if !catalog.valid() {
		return nil, reject(ErrCatalog)
	}
	result := make([]Definition, 0, len(catalog.ordered))
	for _, code := range catalog.ordered {
		result = append(result, catalog.codes[code])
	}
	return result, nil
}

// Components returns all registered module/component pairs in lexical order.
// It reports declarations, not loaded SDKs or running resource instances.
func (catalog *Catalog) Components() ([]Component, error) {
	if !catalog.valid() {
		return nil, reject(ErrCatalog)
	}
	grouped := make(map[componentKey][]Code)
	for _, code := range catalog.ordered {
		definition := catalog.codes[code]
		key := componentKey{definition.Module, definition.Component}
		grouped[key] = append(grouped[key], code)
	}
	result := make([]Component, 0, len(grouped))
	for key, codes := range grouped {
		result = append(result, Component{Domain: codes[0].Domain(), Facility: codes[0].Facility(), Module: key.module, Name: key.component, Codes: codes})
	}
	slices.SortFunc(result, func(left, right Component) int {
		return cmp.Or(strings.Compare(left.Module, right.Module), strings.Compare(left.Name, right.Name))
	})
	return result, nil
}

// InComponent returns exactly one declared owner's errors, in numeric order.
// Unknown owners report false; empty/invalid names do not select every owner.
func (catalog *Catalog) InComponent(module, component string) ([]Definition, bool, error) {
	if !catalog.valid() {
		return nil, false, reject(ErrCatalog)
	}
	if !namespace(module, 128) || !namespace(component, 64) {
		return nil, false, reject(ErrDefinition)
	}
	var result []Definition
	for _, code := range catalog.ordered {
		definition := catalog.codes[code]
		if definition.Module == module && definition.Component == component {
			result = append(result, definition)
		}
	}
	return result, len(result) != 0, nil
}

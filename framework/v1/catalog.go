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

package framework

import (
	"errors"

	adapters "github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

// Module is one Framework feature's owned static contribution. It has no runtime
// instances, callbacks, factories, selected locale or source settings.
type Module struct {
	Definition failure.ModuleDefinition
	Messages   []i18n.Source
	Grouping   i18n.Module
	Bindings   []i18n.Binding
}

// CatalogSet exposes the shared immutable catalogs through the Framework layer.
type CatalogSet struct {
	Errors   *failure.DefinitionCatalog
	Messages *i18n.Catalog
	Bindings *i18n.Bindings
}

// Catalogs composes explicit Framework and Adapter declarations once, including
// both layers' bootstrap errors and shared i18n definitions. The bounded conversion
// copies only top-level descriptors; the underlying assembly owns validation.
func Catalogs(modules []Module, selected ...adapters.Module) (CatalogSet, error) {
	if len(modules) > failure.MaxModules-3 || len(selected) > failure.MaxModules-3-len(modules) {
		return CatalogSet{}, reject(ErrLimit)
	}
	combined := make([]adapters.Module, 0, len(modules)+len(selected)+1)
	combined = append(combined, adapters.Module(ModuleContribution()))
	for _, module := range modules {
		combined = append(combined, adapters.Module(module))
	}
	combined = append(combined, selected...)
	catalogs, err := adapters.Catalogs(combined...)
	if err != nil {
		if errors.Is(err, adapters.ErrCatalog) {
			return CatalogSet{}, reject(ErrCatalog, err)
		}
		if errors.Is(err, adapters.ErrLimit) {
			return CatalogSet{}, reject(ErrLimit, err)
		}
		return CatalogSet{}, err
	}
	return CatalogSet{Errors: catalogs.Errors, Messages: catalogs.Messages, Bindings: catalogs.Bindings}, nil
}
func reject(condition failure.Condition, causes ...error) error {
	value, err := failure.New(condition, causes...)
	if err != nil {
		panic(err)
	}
	return value
}
